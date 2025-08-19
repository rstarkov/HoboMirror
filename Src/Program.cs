using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Security.AccessControl;
using System.Text;
using System.Text.RegularExpressions;
using Alphaleonis.Win32.Filesystem;
using RT.Util;
using RT.Util.CommandLine;
using RT.Util.Consoles;
using RT.Util.ExtensionMethods;
using RT.Util.Serialization;
using Windows.Win32.Storage.FileSystem;
using IO = System.IO;

// Notes:
// Sync* methods find and log the changes using LogChange. Act* methods perform and log modifications using LogAction
// All Act* methods catch and log any possible IO error, and don't propagate exceptions. Sync methods don't need to, but SyncDir has a generic handler to avoid aborting entire recursive operations on first unexpected error
// Critical errors are errors that don't come through "expected" error paths and indicate that HoboMirror has a bug / isn't handling all possible corner cases

namespace HoboMirror;

class Program
{
    static CmdLine Args;
    static Settings Settings;
    static bool UseVolumeShadowCopy = true;
    static bool RefreshAccessControl = true;
    static bool UpdateMetadata = true;
    static int Errors = 0;
    static int CriticalErrors = 0;

    static IO.StreamWriter ActionLog, ChangeLog, ErrorLog, CriticalErrorLog, DebugLog;

    static int Main(string[] args)
    {
        if (args.Length == 2 && args[0] == "--post-build-check")
            return Ut.RunPostBuildChecks(args[1], Assembly.GetExecutingAssembly());
        Console.OutputEncoding = Encoding.UTF8;

#if !DEBUG
        try
#endif
        {
            return DoMain(args);
        }
#if !DEBUG
        catch (Exception e)
        {
            ConsoleUtil.WriteLine("Unhandled exception:".Color(ConsoleColor.Red));
            foreach (var ex in e.SelectChain(ex => ex.InnerException))
            {
                ConsoleUtil.WriteLine(ex.GetType().Name.Color(ConsoleColor.Magenta));
                ConsoleUtil.WriteLine(ex.StackTrace);
            }
            return -1;
        }
#endif
    }

    static int DoMain(string[] args)
    {
        // Parse command-line arguments
        Args = CommandLineParser.ParseOrWriteUsageToConsole<CmdLine>(args);
        if (Args == null)
            return -1;

        // Load settings file
        if (Args.SettingsPath != null)
        {
            if (File.Exists(Args.SettingsPath))
                Settings = ClassifyJson.DeserializeFile<Settings>(Args.SettingsPath);
            else
            {
                Settings = new Settings();
                ClassifyJson.SerializeToFile(Settings, Args.SettingsPath);
            }
            RefreshAccessControl = Settings.SkipRefreshAccessControlDays == null || (Settings.LastRefreshAccessControl + TimeSpan.FromDays((double) Settings.SkipRefreshAccessControlDays) < DateTime.UtcNow);
            Console.WriteLine($"Refresh access control: {RefreshAccessControl}");
            Console.WriteLine($"Update metadata: {UpdateMetadata}");
        }

        // Initialise log files
        var startTime = DateTime.UtcNow;
        if (Args.LogPath != null)
        {
            if (Args.LogPath == "")
                Args.LogPath = PathUtil.AppPath;
            var enc = new UTF8Encoding(false, throwOnInvalidBytes: false); // allows us to log filenames that are not valid UTF-16 (unpaired surrogates)
            ActionLog = new IO.StreamWriter(IO.File.Open(Path.Combine(Args.LogPath, $"HoboMirror-Actions.{DateTime.Today:yyyy-MM-dd}.txt"), IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read), enc);
            ChangeLog = new IO.StreamWriter(IO.File.Open(Path.Combine(Args.LogPath, $"HoboMirror-Changes.{DateTime.Today:yyyy-MM-dd}.txt"), IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read), enc);
            ErrorLog = new IO.StreamWriter(IO.File.Open(Path.Combine(Args.LogPath, $"HoboMirror-Errors.{DateTime.Today:yyyy-MM-dd}.txt"), IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read), enc);
            CriticalErrorLog = new IO.StreamWriter(IO.File.Open(Path.Combine(Args.LogPath, $"HoboMirror-ErrorsCritical.{DateTime.Today:yyyy-MM-dd}.txt"), IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read), enc);
            DebugLog = new IO.StreamWriter(IO.File.Open(Path.Combine(Args.LogPath, $"HoboMirror-Debug.{DateTime.Today:yyyy-MM-dd}.txt"), IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read), enc);
        }

        try
        {
            // Parse volumes to be snapshotted
            var tasks = Args.FromPath.Zip(Args.ToPath, (from, to) => new
            {
                FromPath = from,
                ToPath = to,
                ToGuard = Path.Combine(to, "__HoboMirrorTarget__.txt"),
                FromVolume =
                    Regex.Match(from, @"^\\\\\?\\Volume{[^}]+}\\").Apply(match => match.Success ? match.Groups[0].Value : null)
                    ?? Regex.Match(from, @"^\w:\\").Apply(match => match.Success ? match.Groups[0].Value : null)
                    ?? Ut.Throw<string>(new InvalidOperationException($"Expected absolute path: {from}")) // this should be taken care of by the CmdLine specification, so throw here
            });

            // Log header
            LogAll("==============");
            LogAll($"Started at {DateTime.Now}");

            // Refuse to mirror without a guard file
            foreach (var task in tasks)
            {
                if (!File.Exists(task.ToGuard) || !File.ReadAllText(task.ToGuard).ToLower().Contains("allow"))
                {
                    LogError($"Target path is not marked with a guard file: {task.ToPath}");
                    LogError($"Due to the potentially destructive nature of mirroring, every mirror destination must contain a guard file. This path does not. Mirroring aborted.");
                    LogError($"To allow mirroring to this path, please create a file at {task.ToGuard}. The file must contain the word “allow”.");
                    LogError($"Remember that HoboMirror will delete files at this path without confirmation.");
                    return -1;
                }
            }

            // Enable the necessary privilege to read and write everything
            try
            {
                WinAPI.ModifyPrivilege(PrivilegeName.SeBackupPrivilege, true);
                WinAPI.ModifyPrivilege(PrivilegeName.SeRestorePrivilege, true);
                //WinAPI.ModifyPrivilege(PrivilegeName.SeSecurityPrivilege, true);
                //WinAPI.ModifyPrivilege(PrivilegeName.SeTakeOwnershipPrivilege, true);
            }
            catch (Win32Exception e)
            {
                LogError("Unable to obtain the necessary privileges. Some files and/or attributes will not be replicated.");
                LogError(e.Message);
            }

            // Perform the mirroring
            var volumes = tasks.GroupBy(t => t.FromVolume).Select(g => g.Key).ToArray();
            using (var vsc = UseVolumeShadowCopy ? new VolumeShadowCopy(volumes) : null)
            {
                var vscVolumes = UseVolumeShadowCopy ? vsc.Volumes : new ReadOnlyDictionary<string, VolumeShadowCopyVol>(volumes.ToDictionary(vol => vol, vol => new VolumeShadowCopyVol { Path = vol, SnapshotPath = vol }));
                foreach (var task in tasks)
                {
                    var fromPath = Path.Combine(vscVolumes[task.FromVolume].SnapshotPath, task.FromPath.Substring(task.FromVolume.Length));
                    LogAll($"    Mirror task: from “{task.FromPath}” to “{task.ToPath}” (volume snapshot path: {fromPath})");
                }
                foreach (var ignore in Args.IgnorePath.Concat(Settings.IgnorePaths).Order())
                    LogAll($"    Ignore path: “{ignore}”");
                foreach (var ignore in Settings.IgnoreDirNames)
                    LogAll($"    Ignore directory name: “{ignore}”");

                foreach (var task in tasks)
                {
                    GetOriginalSrcPath = str => str.Replace(vscVolumes[task.FromVolume].SnapshotPath, task.FromVolume).Replace(@"\\", @"\");
                    if (!Directory.Exists(task.ToPath))
                        ActCreateDirectory(task.ToPath);
                    var srcItem = new Item(new DirectoryInfo(Path.Combine(vscVolumes[task.FromVolume].SnapshotPath, task.FromPath.Substring(task.FromVolume.Length))), ItemType.Dir);
                    var tgtItem = CreateItem(new DirectoryInfo(task.ToPath));
                    if (tgtItem != null)
                        SyncDir(srcItem, tgtItem);
                    else
                        LogError($"Unable to execute mirror task: {task.FromPath}");
                }
            }

            // List changed directories and update change counts
            LogChange("", null);
            LogChange("DIRECTORIES WITH AT LEAST ONE CHANGE:", null);
            if (Settings == null)
            {
                foreach (var chg in ChangedDirs.Order())
                    LogChange("  " + chg, null);
            }
            else
            {
                foreach (var dir in ChangedDirs)
                    Settings.DirectoryChangeCount[dir].TimesChanged++;
                LogChange("(sorted from rarely changing to frequently changing)", null);
                var changes =
                    from dir in ChangedDirs
                    let match = Settings.GroupDirectoriesForChangeReport.Select(dg => dg.GetMatch(dir)).Where(m => m != null).MinElementOrDefault(s => s.Length)
                    group dir by match ?? dir into grp
                    let changeCounts = grp.Select(p => Settings.DirectoryChangeCount[p])
                    select new { path = grp.Key, changeFreq = changeCounts.Sum(ch => ch.TimesChanged) / (double) changeCounts.Sum(ch => ch.TimesScanned) };
                foreach (var chg in changes.OrderBy(ch => ch.changeFreq))
                    LogChange($"  {chg.path} — {chg.changeFreq:0.0%}", null);
            }

            if (RefreshAccessControl)
                Settings.LastRefreshAccessControl = DateTime.UtcNow;

            // Save settings file
            if (Args.SettingsPath != null)
                ClassifyJson.SerializeToFile(Settings, Args.SettingsPath);

            return CriticalErrors > 0 ? 2 : Errors > 0 ? 1 : 0;
        }
#if !DEBUG
        catch (Exception e)
        {
            LogCriticalError($"Unhandled exception ({e.GetType().Name}): {e.Message}");
            LogCriticalError(e.StackTrace);
            return 1;
        }
#endif
        finally
        {
            // Close log files
            if (Args.LogPath != null)
            {
                foreach (var log in new[] { ActionLog, ChangeLog, ErrorLog, CriticalErrorLog, DebugLog })
                {
                    log.WriteLine($"Ended at {DateTime.Now}. Time taken: {(DateTime.UtcNow - startTime).TotalMinutes:#,0.0} minutes");
                    log.Dispose();
                }
            }
        }
    }

    public static void LogAction(string text)
    {
        ConsoleUtil.WriteParagraphs(text.Color(ConsoleColor.White));
        ActionLog?.WriteLine(text);
        ActionLog?.Flush();
    }

    public static void LogChange(string text, string path, string whatChanged = null)
    {
        if (path != null)
            ChangedDirs.Add(Path.GetDirectoryName(path).WithSlash());
        var msg = text + path + whatChanged;
        ConsoleUtil.WriteParagraphs(msg.Color(ConsoleColor.Yellow));
        ChangeLog?.WriteLine(msg);
        ChangeLog?.Flush();
    }

    public static void LogError(string text)
    {
        Errors++;
        ConsoleUtil.WriteParagraphs(text.Color(ConsoleColor.Red));
        ErrorLog?.WriteLine(text);
        ErrorLog?.Flush();
    }

    public static void LogCriticalError(string text)
    {
        CriticalErrors++;
        ConsoleUtil.WriteParagraphs(text.Color(ConsoleColor.Red));
        CriticalErrorLog?.WriteLine(text);
        CriticalErrorLog?.Flush();
    }

    public static void LogDebug(string text)
    {
        //ConsoleUtil.WriteParagraphs(text.Color(ConsoleColor.DarkGray));
        DebugLog?.WriteLine(text);
        DebugLog?.Flush();
    }

    public static void LogAll(string text)
    {
        ConsoleUtil.WriteParagraphs(text.Color(ConsoleColor.Green));
        ActionLog?.WriteLine(text);
        ActionLog?.Flush();
        ChangeLog?.WriteLine(text);
        ChangeLog?.Flush();
        ErrorLog?.WriteLine(text);
        ErrorLog?.Flush();
        CriticalErrorLog?.WriteLine(text);
        CriticalErrorLog?.Flush();
        DebugLog?.WriteLine(text);
        DebugLog?.Flush();
    }

    private static HashSet<string> ChangedDirs = new HashSet<string>();

    private static Func<string, string> GetOriginalSrcPath;

    private static void TryCatchIoAction(string actionDesc, string affectedPath, Action action)
    {
        TryCatchIo(() =>
        {
            LogAction(actionDesc.Substring(0, 1).ToUpper() + actionDesc.Substring(1) + ": " + affectedPath);
            action();
        }, err => $"Unable to {actionDesc.Substring(0, 1).ToLower() + actionDesc.Substring(1)} ({err}): {affectedPath}");
    }

    private static T TryCatchIoAction<T>(string actionDesc, string affectedPath, Func<T> action)
    {
        return TryCatchIo(() =>
        {
            LogAction(actionDesc + ": " + affectedPath);
            return action();
        }, err => $"Unable to {actionDesc.Substring(0, 1).ToLower() + actionDesc.Substring(1)} ({err}): {affectedPath}");
    }

    private static void TryCatchIo(Action action, Func<string, string> formatError)
    {
        TryCatchIo<object>(() =>
        {
            action();
            return null;
        }, formatError);
    }

    private static T TryCatchIo<T>(Func<T> action, Func<string, string> formatError)
    {
        try
        {
            return action();
        }
        catch (UnauthorizedAccessException)
        {
            LogError(formatError("unauthorized access"));
        }
        catch (IO.FileNotFoundException)
        {
            // Can be thrown if permissions are extremely restrictive for some reason
            LogError(formatError("file not found"));
        }
        catch (Exception e)
        {
            LogError(formatError($"{e.GetType().Name}, {e.Message}"));
        }
        return default;
    }

    private static void CopyAccessControl(Item src, Item tgt)
    {
        if (!RefreshAccessControl)
            return;

        var acl = TryCatchIo(() =>
        {
            if (src.Info is FileInfo)
                return (FileSystemSecurity) (src.Info as FileInfo).GetAccessControl();
            else
                return (src.Info as DirectoryInfo).GetAccessControl();
        }, err => $"Unable to get {src.TypeDesc} access control ({err}): {GetOriginalSrcPath(src.Info.FullName)}");
        if (acl == null)
            return;

        TryCatchIo(() =>
        {
            if (tgt.Info is FileInfo)
                (tgt.Info as FileInfo).SetAccessControl((FileSecurity) acl);
            else
                (tgt.Info as DirectoryInfo).SetAccessControl((DirectorySecurity) acl);
        }, err => $"Unable to set {tgt.TypeDesc} access control ({err}): {tgt.Info.FullName}");
    }

    private static void CopyAttributes(Item src, Item tgt)
    {
        if (!UpdateMetadata)
            return;

        FILE_BASIC_INFO info = default;
        var ok = TryCatchIo(() =>
        {
            info = Filesys.GetTimestampsAndAttributes(src.Info.FullName);
            return true;
        }, err => $"Unable to get {src.TypeDesc} times/attributes ({err}): {GetOriginalSrcPath(src.Info.FullName)}");
        if (!ok)
            return;

        TryCatchIo(() =>
        {
            Filesys.SetTimestampsAndAttributes(tgt.Info.FullName, info);
        }, err => $"Unable to set {tgt.TypeDesc} times/attributes ({err}): {tgt.Info.FullName}");
    }

    /// <summary>
    ///     Compares paths for equality, ignoring differences in case, slash type, and trailing slash presence. Does not
    ///     ignore differences due to relative path (non-)expansion, different Windows prefixes (/??/, //?/ etc),
    ///     different ways to refer to the same filesystem (drive letter, junction mount point, volume ID).</summary>
    private static bool PathsEqual(string path1, string path2)
    {
        path1 = path1.Replace('/', '\\').WithSlash();
        path2 = path2.Replace('/', '\\').WithSlash();
        return string.Equals(path1, path2, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    ///     Compares source and target directory, detects/logs changes, and updates target to match source as necessary.
    ///     Assumes that both the source and the target exist, and that both are directories.</summary>
    private static void SyncDir(Item src, Item tgt)
    {
        try
        {
            var srcItems = GetDirectoryItems(src.DirInfo);
            var tgtItems = GetDirectoryItems(tgt.DirInfo);
            if (srcItems == null || tgtItems == null)
            {
                LogError($"Unable to mirror directory: {GetOriginalSrcPath(src.DirInfo.FullName)}");
                return;
            }
            var srcDict = srcItems.ToDictionary(t => t.Info.Name, StringComparer.OrdinalIgnoreCase);
            var tgtDict = tgtItems.ToDictionary(t => t.Info.Name, StringComparer.OrdinalIgnoreCase);

            // Completely ignore the guard file (in any directory)
            srcDict.Remove("__HoboMirrorTarget__.txt");
            tgtDict.Remove("__HoboMirrorTarget__.txt");
            // Ignore paths as requested: pretend they don't exist in source, which gets them deleted in target if present
            foreach (var srcItem in srcDict.Values.ToList())
            {
                if (Args.IgnorePath.Concat(Settings.IgnorePaths).Any(ignore => PathsEqual(GetOriginalSrcPath(srcItem.Info.FullName), ignore)))
                {
                    LogAction($"Ignoring path: {GetOriginalSrcPath(srcItem.Info.FullName)}");
                    srcDict.Remove(srcItem.Info.Name);
                }
                else if (srcItem.Type == ItemType.Dir && Settings.IgnoreDirNames.Any(ignore => ignore.EqualsNoCase(srcItem.DirInfo.Name)))
                {
                    LogAction($"Ignoring directory name: {GetOriginalSrcPath(srcItem.Info.FullName)}");
                    srcDict.Remove(srcItem.Info.Name);
                }
            }
            // Update the item arrays to remove filtered items, and sort them into final processing order
            srcItems = srcDict.Values.OrderBy(s => s.Type == ItemType.Dir ? 2 : 1).ThenBy(s => s.Info.Name.ToLowerInvariant()).ToArray();
            tgtItems = tgtDict.Values.OrderBy(s => s.Type == ItemType.Dir ? 2 : 1).ThenBy(s => s.Info.Name.ToLowerInvariant()).ToArray();

            Console.Title = GetOriginalSrcPath(src.DirInfo.FullName) + " (directory ACL)";
            CopyAccessControl(src, tgt); // this potentially modifies sub-items, so we must do it before syncing the sub-items

            // Phase 1: delete all target items which are missing in source, or are of a different item type
            foreach (var tgtItem in tgtItems)
            {
                var srcItem = srcDict.Get(tgtItem.Info.Name, null);
                if (srcItem != null && srcItem.Type == tgtItem.Type)
                    continue;
                Console.Title = GetOriginalSrcPath(src.DirInfo.FullNameWithName(tgtItem.Info.Name)) + " (delete)";

                if (srcItem == null)
                    LogChange($"Found deleted {tgtItem.TypeDesc}: ", GetOriginalSrcPath(Path.Combine(src.DirInfo.FullName, tgtItem.Info.Name)));
                else
                    LogChange($"Found {srcItem.TypeDesc} which used to be a {tgtItem.TypeDesc}: ", GetOriginalSrcPath(srcItem.Info.FullName));
                ActDelete(tgtItem);
                tgtDict.Remove(tgtItem.Info.Name);
            }
            tgtItems = tgtDict.Values.OrderBy(s => s.Type == ItemType.Dir ? 2 : 1).ThenBy(s => s.Info.Name.ToLowerInvariant()).ToArray();

            // Phase 2: sync all items that are present in both (and have matching types - which at this point is all items still present in both)
            foreach (var srcItem in srcItems)
            {
                var tgtItem = tgtDict.Get(srcItem.Info.Name, null);
                if (tgtItem == null)
                    continue;
                Console.Title = GetOriginalSrcPath(srcItem.Info.FullName) + " (sync)";

                if (srcItem.Type == ItemType.Dir && tgtItem.Type == ItemType.Dir)
                    SyncDir(srcItem, tgtItem);
                else if (srcItem.Type == ItemType.File && tgtItem.Type == ItemType.File)
                    SyncFile(srcItem, tgtItem);
                else if (srcItem.Type == ItemType.FileSymlink && tgtItem.Type == ItemType.FileSymlink)
                    SyncFileSymlink(srcItem, tgtItem);
                else if (srcItem.Type == ItemType.DirSymlink && tgtItem.Type == ItemType.DirSymlink)
                    SyncDirSymlink(srcItem, tgtItem);
                else if (srcItem.Type == ItemType.Junction && tgtItem.Type == ItemType.Junction)
                    SyncJunction(srcItem, tgtItem);
                else
                    throw new Exception("unreachable 83149");
            }

            // Phase 3: copy all items only present in source
            foreach (var srcItem in srcItems)
            {
                if (tgtDict.ContainsKey(srcItem.Info.Name))
                    continue;
                Console.Title = GetOriginalSrcPath(srcItem.Info.FullName) + " (copy)";

                LogChange($"Found new {srcItem.TypeDesc}: ", GetOriginalSrcPath(srcItem.Info.FullName));
                var tgtFullName = Path.Combine(tgt.DirInfo.FullName, srcItem.Info.Name);
                if (srcItem.Type == ItemType.Dir)
                    ActCopyDirectory(srcItem, tgtFullName);
                else if (srcItem.Type == ItemType.File)
                    ActCopyOrReplaceFile(srcItem.FileInfo, tgtFullName);
                else if (srcItem.Type == ItemType.FileSymlink)
                    ActCreateFileSymlink(tgtFullName, srcItem.LinkTarget);
                else if (srcItem.Type == ItemType.DirSymlink)
                    ActCreateDirSymlink(tgtFullName, srcItem.LinkTarget);
                else if (srcItem.Type == ItemType.Junction)
                    ActCreateJunction(tgtFullName, srcItem.LinkTarget, srcItem.PrintName);
                else
                    throw new Exception("unreachable 49612");
                var tgtItem = CreateItem(CreateInfo(srcItem.Type, tgtFullName));
                if (tgtItem != null)
                    tgtDict.Add(tgtItem.Info.Name, tgtItem);
            }
            tgtItems = tgtDict.Values.OrderBy(s => s.Type == ItemType.Dir ? 2 : 1).ThenBy(s => s.Info.Name.ToLowerInvariant()).ToArray();

            // Phase 4: sync access control and filesystem attributes
            foreach (var srcItem in srcItems)
            {
                var tgtItem = tgtDict.Get(srcItem.Info.Name, null);
                if (tgtItem == null)
                    continue;
                Console.Title = GetOriginalSrcPath(srcItem.Info.FullName) + " (attributes)";

                if (srcItem.Type != ItemType.Dir) // directories are handled by Copy* calls just before and just after these 4 phases
                {
                    CopyAccessControl(srcItem, tgtItem);
                    CopyAttributes(srcItem, tgtItem);
                }
            }
            CopyAttributes(src, tgt);

            // Update statistics
            if (Settings != null)
            {
                var path = GetOriginalSrcPath(src.Info.FullName).WithSlash();
                Settings.DirectoryChangeCount[path].TimesScanned++;
            }
        }
        catch (Exception e)
        {
            // none of the above code is supposed to throw under any known circumstances
            LogError($"Unable to sync directory ({e.Message}): {GetOriginalSrcPath(src.Info.FullName)}");
            LogCriticalError($"SyncDir: {GetOriginalSrcPath(src.Info.FullName)}\r\n    {e.GetType().Name}: {e.Message}\r\n{e.StackTrace}");
        }
    }

    /// <summary>
    ///     Compares source and target file, detects/logs changes, and updates target to match source if necessary.
    ///     Assumes that both the source and the target exist, and that both are files.</summary>
    private static void SyncFile(Item src, Item tgt)
    {
        if (src.FileInfo.LastWriteTimeUtc == tgt.FileInfo.LastWriteTimeUtc && src.FileInfo.Length == tgt.FileInfo.Length)
            return;
        LogChange($"Found a modified file: ", GetOriginalSrcPath(src.FileInfo.FullName), whatChanged: $"\r\n    length: {tgt.FileInfo.Length:#,0} -> {src.FileInfo.Length:#,0}\r\n    modified: {tgt.FileInfo.LastWriteTimeUtc} -> {src.FileInfo.LastWriteTimeUtc} (UTC)");
        ActCopyOrReplaceFile(src.FileInfo, tgt.FileInfo.FullNameWithName(src.FileInfo.Name));
    }

    /// <summary>
    ///     Compares source and target file symlinks, detects/logs changes, and updates target to match source if
    ///     necessary. Assumes that both the source and the target exist, and that both are file symlinks.</summary>
    private static void SyncFileSymlink(Item src, Item tgt)
    {
        if (src.LinkTarget == tgt.LinkTarget)
            return;
        LogChange($"Found a modified {src.TypeDesc}: ", GetOriginalSrcPath(src.FileInfo.FullName), whatChanged: $"\r\n    target: {tgt.LinkTarget} -> {src.LinkTarget}");
        ActDelete(tgt);
        ActCreateFileSymlink(tgt.FileInfo.FullNameWithName(src.FileInfo.Name), src.LinkTarget);
    }

    /// <summary>
    ///     Compares source and target directory symlinks, detects/logs changes, and updates target to match source if
    ///     necessary. Assumes that both the source and the target exist, and that both are directory symlinks.</summary>
    private static void SyncDirSymlink(Item src, Item tgt)
    {
        if (src.LinkTarget == tgt.LinkTarget)
            return;
        LogChange($"Found a modified {src.TypeDesc}: ", GetOriginalSrcPath(src.DirInfo.FullName), whatChanged: $"\r\n    target: {tgt.LinkTarget} -> {src.LinkTarget}");
        ActDelete(tgt);
        ActCreateDirSymlink(tgt.DirInfo.FullNameWithName(src.DirInfo.Name), src.LinkTarget);
    }

    /// <summary>
    ///     Compares source and target junction, detects/logs changes, and updates target to match source if necessary.
    ///     Assumes that both the source and the target exist, and that both are junctions.</summary>
    private static void SyncJunction(Item src, Item tgt)
    {
        if (src.LinkTarget == tgt.LinkTarget && src.PrintName == tgt.PrintName)
            return;
        LogChange($"Found a modified {src.TypeDesc}: ", GetOriginalSrcPath(src.DirInfo.FullName), whatChanged: $"\r\n    target: {tgt.LinkTarget} -> {src.LinkTarget}\r\n    print name: {tgt.PrintName} -> {src.PrintName}");
        ActDelete(tgt);
        ActCreateJunction(tgt.DirInfo.FullNameWithName(src.DirInfo.Name), src.LinkTarget, src.PrintName);
    }

    /// <summary>Deletes the specified item of any type. Assumes that the item exists.</summary>
    private static void ActDelete(Item tgt)
    {
        TryCatchIo(() =>
        {
            if (tgt.Type == ItemType.Dir)
            {
                // AlphaFS already does this, but just in case it stops doing this in a future release we do this explicitly, because the consequences of following a reparse point during a delete are dire.
                // Also this lets us log every action.
                var items = GetDirectoryItems(tgt.DirInfo);
                if (items == null)
                    throw new Exception("could not enumerate directory contents");
                foreach (var item in items.OrderBy(t => t.Type == ItemType.Dir ? 2 : 1).ThenBy(t => t.Info.Name))
                    ActDelete(item);
                TryCatchIoAction("delete empty directory", tgt.DirInfo.FullName, () =>
                {
                    tgt.DirInfo.Delete(recursive: false, ignoreReadOnly: true);
                });
            }
            else
            {
                LogAction($"Delete {tgt.TypeDesc}: {tgt.Info.FullName}");
                if (tgt.Type == ItemType.File || tgt.Type == ItemType.FileSymlink)
                    tgt.FileInfo.Delete(ignoreReadOnly: true);
                else if (tgt.Type == ItemType.DirSymlink || tgt.Type == ItemType.Junction)
                    tgt.DirInfo.Delete(recursive: false, ignoreReadOnly: true);
                else
                    throw new Exception("unreachable 14234");
            }
        }, err => $"Unable to delete {tgt.TypeDesc} ({err}): {tgt.Info.FullName}");
    }

    /// <summary>Creates the specified directory. Assumes that it doesn't exist.</summary>
    private static void ActCreateDirectory(string fullName)
    {
        TryCatchIoAction("create directory", fullName, () =>
        {
            Directory.CreateDirectory(fullName);
        });
    }

    /// <summary>Creates the specified file symlink. Assumes that it doesn't exist.</summary>
    private static void ActCreateFileSymlink(string fullName, string linkTarget)
    {
        TryCatchIoAction("create file-symlink", fullName, () =>
        {
            File.CreateSymbolicLink(fullName, linkTarget.StartsWith(@"\??\Volume") ? (@"\\?\" + linkTarget.Substring(4)) : linkTarget);
        });
    }

    /// <summary>Creates the specified directory symlink. Assumes that it doesn't exist.</summary>
    private static void ActCreateDirSymlink(string fullName, string linkTarget)
    {
        TryCatchIoAction("create directory-symlink", fullName, () =>
        {
            Directory.CreateSymbolicLink(fullName, linkTarget.StartsWith(@"\??\Volume") ? (@"\\?\" + linkTarget.Substring(4)) : linkTarget);
        });
    }

    /// <summary>Creates the specified junction. Assumes that it doesn't exist.</summary>
    private static void ActCreateJunction(string fullName, string linkTarget, string printName)
    {
        TryCatchIoAction("create junction", fullName, () =>
        {
            Directory.CreateDirectory(fullName);
            JunctionPoint.Create(fullName, linkTarget, printName);
        });
    }

    /// <summary>Copies a directory to the specified target path. Assumes that the target path does not exist.</summary>
    private static void ActCopyDirectory(Item srcItem, string tgtFullName)
    {
        ActCreateDirectory(tgtFullName);
        var tgtItem = CreateItem(new DirectoryInfo(tgtFullName));
        if (tgtItem == null)
        {
            LogError($"Unable to copy directory: {GetOriginalSrcPath(srcItem.DirInfo.FullName)}");
            return;
        }
        SyncDir(srcItem, tgtItem);
    }

    /// <summary>
    ///     Copies a file to the specified path. Always copies to a temporary file in the target directory first, followed
    ///     by a rename, to avoid errors leaving a half finished file looking like the real thing. Unlike other "act"
    ///     methods, this method allows the target file to already exist, and will replace it on successful copy.</summary>
    private static void ActCopyOrReplaceFile(FileInfo src, string tgtFullName)
    {
        var tgtTemp = Path.Combine(Path.GetDirectoryName(tgtFullName), $"~HoboMirror-{Rnd.GenerateString(16)}.tmp");

        bool success = TryCatchIoAction("copy file", GetOriginalSrcPath(src.FullName), () =>
        {
            // This must not directly call src.CopyTo because AlphaFS will modify the instance and make it point to the new file...
            var res = new FileInfo(src.FullName).CopyTo(tgtTemp, CopyOptions.FailIfExists, CopyProgress, null);
            if (res.ErrorCode != 0)
                throw new Exception(res.ErrorMessage);
            return true;
        });
        if (!success)
            return;

        success = TryCatchIo(() =>
        {
            if (File.Exists(tgtFullName))
            {
                LogAction($"Delete old version of this file: {tgtFullName}");
                new FileInfo(tgtFullName).Delete(ignoreReadOnly: true);
            }
            return true;
        }, err => $"Unable to delete old version of this file ({err}): {tgtFullName}");
        if (!success)
            return;

        TryCatchIo(() =>
        {
            File.Move(tgtTemp, tgtFullName, MoveOptions.None);
        }, err => $"Unable to rename temp copied file to final destination ({err}): {tgtFullName}");
    }

    /// <summary>
    ///     Safely enumerates the contents of a directory. If completely unable to enumerate, logs the error as
    ///     appropriate and returns null (which the caller is expected to handle by skipping whatever they were going to
    ///     do with this list). If able to enumerate, will safely obtain additional info about every entry, skipping those
    ///     that fail and logging errors as appropriate.</summary>
    private static Item[] GetDirectoryItems(DirectoryInfo dir)
    {
        var infos = TryCatchIo(() => dir.GetFileSystemInfos(), err => $"Unable to list directory ({err}): {dir.FullName}");
        if (infos == null)
            return null;
        return infos.Select(info => CreateItem(info)).Where(r => r != null).ToArray();
    }

    /// <summary>
    ///     Determines what type of item this filesystem entry is, while handling any potential errors. On failure, logs
    ///     an appropriate message and returns null (which the caller is expected to handle by skipping whatever they were
    ///     going to do with this item).</summary>
    /// <remarks>
    ///     Note that instantiating a <see cref="FileInfo"/> or <see cref="DirectoryInfo"/> is safe and will not throw, so
    ///     only this step needs to be wrapped in an error handler.</remarks>
    private static Item CreateItem(FileSystemInfo info)
    {
        return TryCatchIo(() => new Item(info), err => $"Unable to determine filesystem entry type ({err}): {info.FullName}");
    }

    /// <summary>Instantiates a file system info of the appropriate type based on item type.</summary>
    private static FileSystemInfo CreateInfo(ItemType type, string fullPath)
    {
        if (type == ItemType.Dir || type == ItemType.DirSymlink || type == ItemType.Junction)
            return new DirectoryInfo(fullPath);
        else if (type == ItemType.File || type == ItemType.FileSymlink)
            return new FileInfo(fullPath);
        else
            throw new Exception("unreachable 52210");
    }

    private static DateTime lastProgress;
    private static CopyMoveProgressResult CopyProgress(long totalFileSize, long totalBytesTransferred, long streamSize, long streamBytesTransferred, int streamNumber, CopyMoveProgressCallbackReason callbackReason, object userData)
    {
        if (lastProgress < DateTime.UtcNow - TimeSpan.FromMilliseconds(100))
        {
            lastProgress = DateTime.UtcNow;
            Console.Title = $"Copying {totalBytesTransferred / (double) totalFileSize * 100.0:0.0}% : {totalBytesTransferred / 1000000.0:#,0} MB of {totalFileSize / 1000000.0:#,0} MB";
        }
        return CopyMoveProgressResult.Continue;
    }
}

enum ItemType { File, Dir, FileSymlink, DirSymlink, Junction }

class Item
{
    public FileSystemInfo Info { get; private set; }
    public FileInfo FileInfo => (FileInfo) Info;
    public DirectoryInfo DirInfo => (DirectoryInfo) Info;
    public ItemType Type { get; private set; }
    public string LinkTarget { get; private set; } // null if not a symlink or a junction
    public string PrintName { get; private set; } // null if not a junction
    public string TypeDesc => Type == ItemType.Dir ? "directory" : Type == ItemType.DirSymlink ? "directory-symlink" : Type == ItemType.File ? "file" : Type == ItemType.FileSymlink ? "file-symlink" : Type == ItemType.Junction ? "junction" : throw new Exception("unreachable 63161");
    public override string ToString() => $"{TypeDesc}: {Info.FullName}{(LinkTarget == null ? "" : (" -> " + LinkTarget))}";

    public Item(FileSystemInfo info)
    {
        Info = info;
        if (info.IsReparsePoint())
        {
            if (JunctionPoint.Exists(info.FullName))
            {
                Type = ItemType.Junction;
                var reparse = JunctionPoint.GetTarget(info.FullName);
                LinkTarget = reparse.SubstituteName;
                PrintName = reparse.PrintName;
            }
            else
            {
                Type = info is FileInfo ? ItemType.FileSymlink : info is DirectoryInfo ? ItemType.DirSymlink : throw new Exception("unreachable 27117");
                LinkTarget = File.GetLinkTargetInfo(info.FullName).PrintName; // this should throw for reparse points of unknown types (ie neither junction nor symlink)
            }
        }
        else if (info is FileInfo)
            Type = ItemType.File;
        else if (info is DirectoryInfo)
            Type = ItemType.Dir;
        else
            throw new Exception("unreachable 61374");
    }

    /// <summary>
    ///     Helps create an instance for the shadow copy of the root of a volume, which presents as a reparse point but
    ///     must be treated like a directory.</summary>
    public Item(FileSystemInfo info, ItemType type)
    {
        Info = info;
        Type = type;
    }
}
