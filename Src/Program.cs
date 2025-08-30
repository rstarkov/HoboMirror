using System.ComponentModel;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using RT.CommandLine;
using RT.PostBuild;
using RT.Serialization;
using RT.Util;
using RT.Util.Consoles;
using RT.Util.ExtensionMethods;
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
            return PostBuildChecker.RunPostBuildChecks(args[1], Assembly.GetExecutingAssembly());
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
            RefreshAccessControl = Settings.SkipRefreshAccessControlDays == null || (Settings.LastRefreshAccessControl + TimeSpan.FromDays((double)Settings.SkipRefreshAccessControlDays) < DateTime.UtcNow);
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
                FromVolume = WinAPI.GetVolumeForPath(from),
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
                var sourcePaths = UseVolumeShadowCopy ? vsc.Snapshots.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.SnapshotPath) : volumes.ToDictionary(vol => vol, vol => vol);
                foreach (var task in tasks)
                {
                    var fromPath = Path.Combine(sourcePaths[task.FromVolume], task.FromPath.Substring(task.FromVolume.Length));
                    LogAll($"    Mirror task: from “{task.FromPath}” to “{task.ToPath}” (volume snapshot path: {fromPath})");
                }
                foreach (var ignore in Args.IgnorePath.Concat(Settings.IgnorePaths).Order())
                    LogAll($"    Ignore path: “{ignore}”");
                foreach (var ignore in Settings.IgnoreDirNames)
                    LogAll($"    Ignore directory name: “{ignore}”");

                foreach (var task in tasks)
                {
                    GetOriginalSrcPath = str => str.Replace(sourcePaths[task.FromVolume], task.FromVolume).Replace(@"\\", @"\");
                    if (!Directory.Exists(task.ToPath))
                        ActCreateDirectory(task.ToPath);
                    var srcItem = new Item(newDirectoryInfo(Path.Combine(sourcePaths[task.FromVolume], task.FromPath.Substring(task.FromVolume.Length))), ItemType.Dir);
                    var tgtItem = CreateItem(newDirectoryInfo(task.ToPath));
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
                    select new { path = grp.Key, changeFreq = changeCounts.Sum(ch => ch.TimesChanged) / (double)changeCounts.Sum(ch => ch.TimesScanned) };
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
            if (src.Info is FileInfo fi)
                return Filesys.GetSecurityInfo(fi);
            else if (src.Info is DirectoryInfo di)
                return Filesys.GetSecurityInfo(di);
            else
                throw new Exception("unreachable 24961");
        }, err => $"Unable to get {src.TypeDesc} access control ({err}): {GetOriginalSrcPath(src.FullPath)}");
        if (acl == null)
            return;

        TryCatchIo(() =>
        {
            if (tgt.Info is FileInfo fi)
                Filesys.SetSecurityInfo(fi, acl);
            else if (tgt.Info is DirectoryInfo di)
                Filesys.SetSecurityInfo(di, acl);
            else
                throw new Exception("unreachable 16943");
        }, err => $"Unable to set {tgt.TypeDesc} access control ({err}): {tgt.FullPath}");
    }

    private static void CopyAttributes(Item src, Item tgt)
    {
        if (!UpdateMetadata)
            return;
        TryCatchIo(() =>
        {
            Filesys.SetTimestampsAndAttributes(tgt.FullPath, src.Attrs);
        }, err => $"Unable to set {tgt.TypeDesc} times/attributes ({err}): {tgt.FullPath}");
    }

    /// <summary>
    ///     Compares paths for equality, ignoring differences in case, slash type, and trailing slash presence. Does not
    ///     ignore differences due to relative path (non-)expansion, different Windows prefixes (/??/, //?/ etc), different
    ///     ways to refer to the same filesystem (drive letter, junction mount point, volume ID).</summary>
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
                LogError($"Unable to mirror directory: {GetOriginalSrcPath(src.FullPath)}");
                return;
            }
            var srcDict = srcItems.ToDictionary(t => t.Name, StringComparer.OrdinalIgnoreCase);
            var tgtDict = tgtItems.ToDictionary(t => t.Name, StringComparer.OrdinalIgnoreCase);

            // Completely ignore the guard file (in any directory)
            srcDict.Remove("__HoboMirrorTarget__.txt");
            tgtDict.Remove("__HoboMirrorTarget__.txt");
            // Ignore paths as requested: pretend they don't exist in source, which gets them deleted in target if present
            foreach (var srcItem in srcDict.Values.ToList())
            {
                if (Args.IgnorePath.Concat(Settings.IgnorePaths).Any(ignore => PathsEqual(GetOriginalSrcPath(srcItem.FullPath), ignore)))
                {
                    LogAction($"Ignoring path: {GetOriginalSrcPath(srcItem.FullPath)}");
                    srcDict.Remove(srcItem.Name);
                }
                else if (srcItem.Type == ItemType.Dir && Settings.IgnoreDirNames.Any(ignore => ignore.EqualsIgnoreCase(srcItem.Name)))
                {
                    LogAction($"Ignoring directory name: {GetOriginalSrcPath(srcItem.FullPath)}");
                    srcDict.Remove(srcItem.Name);
                }
            }
            // Update the item arrays to remove filtered items, and sort them into final processing order
            srcItems = srcDict.Values.OrderBy(s => s.Type == ItemType.Dir ? 2 : 1).ThenBy(s => s.Name.ToLowerInvariant()).ToArray();
            tgtItems = tgtDict.Values.OrderBy(s => s.Type == ItemType.Dir ? 2 : 1).ThenBy(s => s.Name.ToLowerInvariant()).ToArray();

            Console.Title = GetOriginalSrcPath(src.FullPath) + " (directory ACL)";
            CopyAccessControl(src, tgt); // this potentially modifies sub-items, so we must do it before syncing the sub-items

            // Phase 1: delete all target items which are missing in source, or are of a different item type
            foreach (var tgtItem in tgtItems)
            {
                var srcItem = srcDict.Get(tgtItem.Name, null);
                if (srcItem != null && srcItem.Type == tgtItem.Type)
                    continue;
                Console.Title = GetOriginalSrcPath(src.FullPathWithName(tgtItem.Name)) + " (delete)";

                if (srcItem == null)
                    LogChange($"Found deleted {tgtItem.TypeDesc}: ", GetOriginalSrcPath(Path.Combine(src.FullPath, tgtItem.Name)));
                else
                    LogChange($"Found {srcItem.TypeDesc} which used to be a {tgtItem.TypeDesc}: ", GetOriginalSrcPath(srcItem.FullPath));
                ActDelete(tgtItem);
                tgtDict.Remove(tgtItem.Name);
            }
            tgtItems = tgtDict.Values.OrderBy(s => s.Type == ItemType.Dir ? 2 : 1).ThenBy(s => s.Name.ToLowerInvariant()).ToArray();

            // Phase 2: sync all items that are present in both (and have matching types - which at this point is all items still present in both)
            foreach (var srcItem in srcItems)
            {
                var tgtItem = tgtDict.Get(srcItem.Name, null);
                if (tgtItem == null)
                    continue;
                Console.Title = GetOriginalSrcPath(srcItem.FullPath) + " (sync)";

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
                if (tgtDict.ContainsKey(srcItem.Name))
                    continue;
                Console.Title = GetOriginalSrcPath(srcItem.FullPath) + " (copy)";

                LogChange($"Found new {srcItem.TypeDesc}: ", GetOriginalSrcPath(srcItem.FullPath));
                var tgtFullName = Path.Combine(tgt.FullPath, srcItem.Name);
                if (srcItem.Type == ItemType.Dir)
                    ActCopyDirectory(srcItem, tgtFullName);
                else if (srcItem.Type == ItemType.File)
                    ActCopyOrReplaceFile(srcItem.FileInfo, tgtFullName);
                else if (srcItem.Type == ItemType.FileSymlink)
                    ActCreateFileSymlink(tgtFullName, srcItem.Reparse);
                else if (srcItem.Type == ItemType.DirSymlink)
                    ActCreateDirSymlink(tgtFullName, srcItem.Reparse);
                else if (srcItem.Type == ItemType.Junction)
                    ActCreateJunction(tgtFullName, srcItem.Reparse);
                else
                    throw new Exception("unreachable 49612");
                var tgtItem = CreateItem(CreateInfo(srcItem.Type, tgtFullName));
                if (tgtItem != null)
                    tgtDict.Add(tgtItem.Name, tgtItem);
            }
            tgtItems = tgtDict.Values.OrderBy(s => s.Type == ItemType.Dir ? 2 : 1).ThenBy(s => s.Name.ToLowerInvariant()).ToArray();

            // Phase 4: sync access control and filesystem attributes
            foreach (var srcItem in srcItems)
            {
                var tgtItem = tgtDict.Get(srcItem.Name, null);
                if (tgtItem == null)
                    continue;
                Console.Title = GetOriginalSrcPath(srcItem.FullPath) + " (attributes)";

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
                var path = GetOriginalSrcPath(src.FullPath).WithSlash();
                Settings.DirectoryChangeCount[path].TimesScanned++;
            }
        }
        catch (Exception e)
        {
            // none of the above code is supposed to throw under any known circumstances
            LogError($"Unable to sync directory ({e.Message}): {GetOriginalSrcPath(src.FullPath)}");
            LogCriticalError($"SyncDir: {GetOriginalSrcPath(src.FullPath)}\r\n    {e.GetType().Name}: {e.Message}\r\n{e.StackTrace}");
        }
    }

    /// <summary>
    ///     Compares source and target file, detects/logs changes, and updates target to match source if necessary. Assumes
    ///     that both the source and the target exist, and that both are files.</summary>
    private static void SyncFile(Item src, Item tgt)
    {
        if (src.Attrs.LastWriteTime == tgt.Attrs.LastWriteTime && src.FileLength == tgt.FileLength)
            return;
        LogChange($"Found a modified file: ", GetOriginalSrcPath(src.FullPath), whatChanged: $"\r\n    length: {tgt.FileLength:#,0} -> {src.FileLength:#,0}\r\n    modified: {DateTime.FromFileTimeUtc(tgt.Attrs.LastWriteTime)} -> {DateTime.FromFileTimeUtc(src.Attrs.LastWriteTime)} (UTC)");
        ActCopyOrReplaceFile(src.FileInfo, tgt.FullPathWithName(src.Name));
    }

    /// <summary>
    ///     Compares source and target file symlinks, detects/logs changes, and updates target to match source if necessary.
    ///     Assumes that both the source and the target exist, and that both are file symlinks.</summary>
    private static void SyncFileSymlink(Item src, Item tgt)
    {
        var srcR = src.Reparse;
        var tgtR = tgt.Reparse;
        if (srcR.SubstituteName == tgtR.SubstituteName && srcR.PrintName == tgtR.PrintName && srcR.IsSymlinkRelative == tgtR.IsSymlinkRelative)
            return;
        LogChange($"Found a modified {src.TypeDesc}: ", GetOriginalSrcPath(src.FullPath), whatChanged: $"\r\n    target: {tgtR.SubstituteName} -> {srcR.SubstituteName}\r\n    print name: {tgtR.PrintName} -> {srcR.PrintName}\r\n    relative: {tgtR.IsSymlinkRelative} -> {srcR.IsSymlinkRelative}");
        ActDelete(tgt);
        ActCreateFileSymlink(tgt.FullPathWithName(src.Name), srcR);
    }

    /// <summary>
    ///     Compares source and target directory symlinks, detects/logs changes, and updates target to match source if
    ///     necessary. Assumes that both the source and the target exist, and that both are directory symlinks.</summary>
    private static void SyncDirSymlink(Item src, Item tgt)
    {
        var srcR = src.Reparse;
        var tgtR = tgt.Reparse;
        if (srcR.SubstituteName == tgtR.SubstituteName && srcR.PrintName == tgtR.PrintName && srcR.IsSymlinkRelative == tgtR.IsSymlinkRelative)
            return;
        LogChange($"Found a modified {src.TypeDesc}: ", GetOriginalSrcPath(src.FullPath), whatChanged: $"\r\n    target: {tgtR.SubstituteName} -> {srcR.SubstituteName}\r\n    print name: {tgtR.PrintName} -> {srcR.PrintName}\r\n    relative: {tgtR.IsSymlinkRelative} -> {srcR.IsSymlinkRelative}");
        ActDelete(tgt);
        ActCreateDirSymlink(tgt.FullPathWithName(src.Name), srcR);
    }

    /// <summary>
    ///     Compares source and target junction, detects/logs changes, and updates target to match source if necessary.
    ///     Assumes that both the source and the target exist, and that both are junctions.</summary>
    private static void SyncJunction(Item src, Item tgt)
    {
        var srcR = src.Reparse;
        var tgtR = tgt.Reparse;
        if (srcR.SubstituteName == tgtR.SubstituteName && srcR.PrintName == tgtR.PrintName)
            return;
        LogChange($"Found a modified {src.TypeDesc}: ", GetOriginalSrcPath(src.FullPath), whatChanged: $"\r\n    target: {tgtR.SubstituteName} -> {srcR.SubstituteName}\r\n    print name: {tgtR.PrintName} -> {srcR.PrintName}");
        ActDelete(tgt);
        ActCreateJunction(tgt.FullPathWithName(src.Name), srcR);
    }

    /// <summary>Deletes the specified item of any type. Assumes that the item exists.</summary>
    private static void ActDelete(Item tgt)
    {
        TryCatchIo(() =>
        {
            if (tgt.Type == ItemType.Dir)
            {
                var items = GetDirectoryItems(tgt.DirInfo);
                if (items == null)
                    throw new Exception("could not enumerate directory contents");
                foreach (var item in items.OrderBy(t => t.Type == ItemType.Dir ? 2 : 1).ThenBy(t => t.Name))
                    ActDelete(item);
                TryCatchIoAction("delete empty directory", tgt.FullPath, () =>
                {
                    Filesys.Delete(tgt.FullPath);
                });
            }
            else
            {
                LogAction($"Delete {tgt.TypeDesc}: {tgt.FullPath}");
                Filesys.Delete(tgt.FullPath);
            }
        }, err => $"Unable to delete {tgt.TypeDesc} ({err}): {tgt.FullPath}");
    }

    /// <summary>Creates the specified directory. Assumes that it doesn't exist.</summary>
    private static void ActCreateDirectory(string fullName)
    {
        TryCatchIoAction("create directory", fullName, () =>
        {
            Filesys.CreateDirectory(fullName);
        });
    }

    /// <summary>Creates the specified file symlink. Assumes that it doesn't exist.</summary>
    private static void ActCreateFileSymlink(string fullName, ReparsePointData rpd)
    {
        TryCatchIoAction("create file-symlink", fullName, () =>
        {
            Filesys.CreateFile(fullName);
            ReparsePoint.SetSymlinkData(fullName, rpd.SubstituteName, rpd.PrintName, rpd.IsSymlinkRelative);
        });
    }

    /// <summary>Creates the specified directory symlink. Assumes that it doesn't exist.</summary>
    private static void ActCreateDirSymlink(string fullName, ReparsePointData rpd)
    {
        TryCatchIoAction("create directory-symlink", fullName, () =>
        {
            Filesys.CreateDirectory(fullName);
            ReparsePoint.SetSymlinkData(fullName, rpd.SubstituteName, rpd.PrintName, rpd.IsSymlinkRelative);
        });
    }

    /// <summary>Creates the specified junction. Assumes that it doesn't exist.</summary>
    private static void ActCreateJunction(string fullName, ReparsePointData rpd)
    {
        TryCatchIoAction("create junction", fullName, () =>
        {
            Filesys.CreateDirectory(fullName);
            ReparsePoint.SetJunctionData(fullName, rpd.SubstituteName, rpd.PrintName);
        });
    }

    /// <summary>Copies a directory to the specified target path. Assumes that the target path does not exist.</summary>
    private static void ActCopyDirectory(Item srcItem, string tgtFullName)
    {
        ActCreateDirectory(tgtFullName);
        var tgtItem = CreateItem(newDirectoryInfo(tgtFullName));
        if (tgtItem == null)
        {
            LogError($"Unable to copy directory: {GetOriginalSrcPath(srcItem.FullPath)}");
            return;
        }
        SyncDir(srcItem, tgtItem);
    }

    /// <summary>
    ///     Copies a file to the specified path. Always copies to a temporary file in the target directory first, followed by
    ///     a rename, to avoid errors leaving a half finished file looking like the real thing. Unlike other "act" methods,
    ///     this method allows the target file to already exist, and will replace it on successful copy.</summary>
    private static void ActCopyOrReplaceFile(FileInfo src, string tgtFullName)
    {
        var tgtTemp = Path.Combine(Path.GetDirectoryName(tgtFullName), $"~HoboMirror-{Rnd.GenerateString(16)}.tmp");

        bool success = TryCatchIoAction("copy file", GetOriginalSrcPath(src.FullName), () =>
        {
            Filesys.CopyFile(src.FullName, tgtTemp, CopyProgress);
            return true;
        });
        if (!success)
            return;

        TryCatchIo(() =>
        {
            Filesys.Rename(tgtTemp, tgtFullName, overwrite: true);
        }, err => $"Unable to rename temp copied file to final destination ({err}): {tgtFullName}");
    }

    /// <summary>
    ///     Safely enumerates the contents of a directory. If completely unable to enumerate, logs the error as appropriate
    ///     and returns null (which the caller is expected to handle by skipping whatever they were going to do with this
    ///     list). If able to enumerate, will safely obtain additional info about every entry, skipping those that fail and
    ///     logging errors as appropriate.</summary>
    private static Item[] GetDirectoryItems(DirectoryInfo dir)
    {
        var infos = TryCatchIo(() => dir.GetFileSystemInfos(), err => $"Unable to list directory ({err}): {dir.FullName}");
        if (infos == null)
            return null;
        return infos.Select(info => CreateItem(info)).Where(r => r != null).ToArray();
    }

    /// <summary>
    ///     Determines what type of item this filesystem entry is, while handling any potential errors. On failure, logs an
    ///     appropriate message and returns null (which the caller is expected to handle by skipping whatever they were going
    ///     to do with this item).</summary>
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
            return newDirectoryInfo(fullPath);
        else if (type == ItemType.File || type == ItemType.FileSymlink)
            return new FileInfo(fullPath);
        else
            throw new Exception("unreachable 52210");
    }

    /// <summary>Workaround for https://github.com/dotnet/runtime/issues/119009</summary>
    private static DirectoryInfo newDirectoryInfo(string path)
    {
        const string vssPrefix = @"\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy";
        if (path.StartsWith(vssPrefix, StringComparison.OrdinalIgnoreCase))
        {
            var match = Regex.Match(path[vssPrefix.Length..], @"^[^\\]*(\\)?$");
            if (match.Success)
                return new DirectoryInfo(path + (match.Groups[1].Success ? @"\" : @"\\"));
        }
        return new DirectoryInfo(path);
    }

    private static DateTime lastProgress;
    private static void CopyProgress(Filesys.CopyFileProgress msg)
    {
        if (lastProgress < DateTime.UtcNow - TimeSpan.FromMilliseconds(100))
        {
            lastProgress = DateTime.UtcNow;
            Console.Title = $"Copying {msg.CopiedBytes / (double)msg.TotalBytes * 100.0:0.0}% : {msg.CopiedBytes / 1000000.0:#,0} MB of {msg.TotalBytes / 1000000.0:#,0} MB";
        }
    }
}

enum ItemType { File, Dir, FileSymlink, DirSymlink, Junction }

class Item
{
    public FileSystemInfo Info { get; private set; }
    public FileInfo FileInfo => (FileInfo)Info;
    public DirectoryInfo DirInfo => (DirectoryInfo)Info;
    public string FullPath => Info.FullName;
    public string Name => Info.Name;
    public ItemType Type { get; private set; }
    public ReparsePointData Reparse { get; private set; } // null if not a symlink or a junction
    public FILE_BASIC_INFO Attrs { get; private set; }
    public long FileLength { get; private set; }
    public string TypeDesc => Type == ItemType.Dir ? "directory" : Type == ItemType.DirSymlink ? "directory-symlink" : Type == ItemType.File ? "file" : Type == ItemType.FileSymlink ? "file-symlink" : Type == ItemType.Junction ? "junction" : throw new Exception("unreachable 63161");
    public override string ToString() => $"{TypeDesc}: {FullPath}{(Reparse == null ? "" : (" -> " + Reparse.SubstituteName))}";

    public Item(FileSystemInfo info)
    {
        Info = info;
        using var handle = Filesys.OpenHandle(info.FullName, (uint)FILE_ACCESS_RIGHTS.FILE_READ_ATTRIBUTES);
        Attrs = Filesys.GetTimestampsAndAttributes(handle);
        bool isDir = (Attrs.FileAttributes & (uint)FILE_FLAGS_AND_ATTRIBUTES.FILE_ATTRIBUTE_DIRECTORY) != 0;
        Reparse = ReparsePoint.GetReparseData(handle);
        FileLength = 0;
        if (Reparse != null)
        {
            if (Reparse.IsJunction)
                Type = ItemType.Junction;
            else if (Reparse.IsSymlink)
                Type = isDir ? ItemType.DirSymlink : ItemType.FileSymlink;
            else
                throw new Exception($"unrecognized reparse point type {Reparse.ReparseTag}");
        }
        else if (isDir)
            Type = ItemType.Dir;
        else
        {
            Type = ItemType.File;
            FileLength = Filesys.GetFileLength(handle);
        }
    }

    /// <summary>
    ///     Helps create an instance for the shadow copy of the root of a volume, which presents as a reparse point but must
    ///     be treated like a directory.</summary>
    public Item(FileSystemInfo info, ItemType type)
    {
        Info = info;
        Type = type;
    }

    /// <summary>
    ///     Replaces the name in this item's full path with the target name (for the purpose of perserving capitalisation).</summary>
    public string FullPathWithName(string name)
    {
        return Path.Combine(Path.GetDirectoryName(FullPath), name);
    }
}
