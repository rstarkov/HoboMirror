using System.ComponentModel;
using System.IO;
using System.Reflection;
using System.Text;
using RT.CommandLine;
using RT.PostBuild;
using RT.Serialization.Settings;
using RT.Util;
using RT.Util.Consoles;
using RT.Util.ExtensionMethods;
using Windows.Win32.Foundation;
using Windows.Win32.Storage.FileSystem;

// Notes:
// Sync* methods find and log the changes using LogChange. Act* methods perform and log modifications using LogAction
// All Act* methods catch and log any possible IO error, and don't propagate exceptions. Sync methods don't need to, but SyncDir has a generic handler to avoid aborting entire recursive operations on first unexpected error
// Critical errors are errors that don't come through "expected" error paths and indicate that HoboMirror has a bug / isn't handling all possible corner cases

namespace HoboMirror;

class Program
{
    static CmdLine Args;
    static SettingsFileXml<Settings> SettingsFile = null;
    static Settings Settings => SettingsFile?.Settings; // can be null if running without settings file command option
    static List<string> IgnorePaths;
    static HashSet<string> IgnoreDirNames;
    static HashSet<string> VolumeRoots;
    static bool UseVolumeShadowCopy = true;
    static bool ForceRefreshMetadata = true;
    static int Errors = 0;
    static int CriticalErrors = 0;

    static StreamWriter ActionLog, ChangeLog, ErrorLog, CriticalErrorLog;

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
            SettingsFile = new(throwOnError: true, Args.SettingsPath);
            ForceRefreshMetadata = Settings.SkipRefreshMetadataDays == null || (Settings.LastRefreshMetadata + TimeSpan.FromDays((double)Settings.SkipRefreshMetadataDays) < DateTime.UtcNow);
        }

        // Initialise log files
        var startTime = DateTime.UtcNow;
        if (Args.LogPath != null)
        {
            if (Args.LogPath == "")
                Args.LogPath = PathUtil.AppPath;
            var enc = new UTF8Encoding(false, throwOnInvalidBytes: false); // allows us to log filenames that are not valid UTF-16 (unpaired surrogates)
            ActionLog = openLog($"HoboMirror-Actions.{DateTime.Today:yyyy-MM-dd}.txt");
            ChangeLog = openLog($"HoboMirror-Changes.{DateTime.Today:yyyy-MM-dd}.txt");
            ErrorLog = openLog($"HoboMirror-Errors.{DateTime.Today:yyyy-MM-dd}.txt");
            CriticalErrorLog = openLog($"HoboMirror-ErrorsCritical.{DateTime.Today:yyyy-MM-dd}.txt");
            StreamWriter openLog(string filename) => new StreamWriter(File.Open(Path.Combine(Args.LogPath, filename), FileMode.Append, FileAccess.Write, FileShare.Read), enc);
        }
        // Initialise console title updater
        if (Windows.Win32.PInvoke.GetConsoleWindow() != HWND.Null)
            new Thread(StatusUpdaterThread) { IsBackground = true }.Start();

        try
        {
            // Log header
            LogAll("==============");
            LogAll($"Started at {DateTime.Now}");

            // Parse mirror paths
            var tasksPaths = Args.FromPath.Zip(Args.ToPath, (from, to) => (from, to));
            if (Settings?.MirrorTasks != null)
                tasksPaths = tasksPaths.Concat(Settings.MirrorTasks.Select(t => (Path.GetFullPath(t.From).WithSlash(), Path.GetFullPath(t.To).WithSlash())));
            tasksPaths = tasksPaths.Select(t => (from: t.Item1, to: t.Item2))
                .Select(t => (Path.GetFullPath(t.from).WithSlash(), Path.GetFullPath(t.to).WithSlash()))
                .ToList();
            // Verify paths
            foreach (var path in tasksPaths.SelectMany(p => new[] { p.from, p.to }))
                if (!Directory.Exists(path))
                {
                    LogError($"Directory not found: {path}");
                    return -1;
                }
            // Figure out volumes
            var tasks = tasksPaths
                .Select(t => new
                {
                    FromPath = t.from,
                    ToPath = t.to,
                    ToGuard = Path.Combine(t.to, "__HoboMirrorTarget__.txt"),
                    FromVolume = WinAPI.GetVolumeForPath(t.from),
                }).ToList();
            // Merge ignore paths
            IgnorePaths = Args.IgnorePath.Concat(Settings?.IgnorePaths ?? []).ToList();
            IgnoreDirNames = Args.IgnoreName.Concat(Settings?.IgnoreDirNames ?? []).ToHashSet(StringComparer.OrdinalIgnoreCase);

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
            VolumeRoots = volumes.Select(v => v.WithSlash()).ToHashSet(StringComparer.OrdinalIgnoreCase);
            using (var vsc = UseVolumeShadowCopy ? new VolumeShadowCopy(volumes) : null)
            {
                if (UseVolumeShadowCopy)
                    VolumeRoots.AddRange(vsc.Snapshots.Values.Select(s => s.SnapshotPath.WithSlash()));
                var sourcePaths = UseVolumeShadowCopy ? vsc.Snapshots.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.SnapshotPath) : volumes.ToDictionary(vol => vol, vol => vol);
                LogAction($"Configuration:");
                foreach (var task in tasks)
                {
                    var fromPath = Path.Combine(sourcePaths[task.FromVolume], task.FromPath.Substring(task.FromVolume.Length));
                    LogAction($"  mirror task:");
                    LogAction($"    from: {task.FromPath}");
                    LogAction($"    to: {task.ToPath}");
                    LogAction($"    source volume: {task.FromVolume}");
                    LogAction($"    volume snapshot: {fromPath}");
                }
                foreach (var ignore in IgnorePaths.Order())
                    LogAction($"  ignore path: “{ignore}”");
                foreach (var ignore in IgnoreDirNames.Order())
                    LogAction($"  ignore directory name: “{ignore}”");
                LogAction($"  refresh metadata: {ForceRefreshMetadata}");

                foreach (var task in tasks)
                {
                    GetOriginalSrcPath = str => str.Replace(sourcePaths[task.FromVolume], task.FromVolume).Replace(@"\\", @"\");
                    var src = CreateItem(Path.Combine(sourcePaths[task.FromVolume], task.FromPath.Substring(task.FromVolume.Length)).WithSlash());
                    var tgt = CreateItem(task.ToPath.WithSlash()); // must exist because we checked for the guard file
                    if (src != null && tgt != null)
                        SyncDir(src, tgt, true);
                    else
                        LogError($"Unable to execute mirror task: {task.FromPath}");
                }
            }

            // List changed directories and update change counts
            LogChange("", null);
            LogChange("DIRECTORIES WITH AT LEAST ONE CHANGE:", null);
            foreach (var chg in ChangedDirs.Order())
                LogChange("  " + chg, null);

            // Update settings file
            if (Settings != null)
            {
                if (ForceRefreshMetadata)
                    Settings.LastRefreshMetadata = DateTime.UtcNow;
                SettingsFile.Save();
            }

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
                foreach (var log in new[] { ActionLog, ChangeLog, ErrorLog, CriticalErrorLog })
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
    }

    public static volatile string StatusText = null;

    private static void StatusUpdaterThread()
    {
        while (true)
        {
            if (StatusText != null)
                Console.Title = StatusText;
            Thread.Sleep(50);
        }
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
        catch (FileNotFoundException)
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
    /// <remarks>
    ///     When toplevel is true, the items may be any combination of real dirs, junctions or directory symlinks. Otherwise
    ///     it's always dirs. Skips copying attributes for the top level dir, only because the current implementation can't
    ///     optionally set them on the symlink/junction target (todo).</remarks>
    private static void SyncDir(Item srcDir, Item tgtDir, bool toplevel = false)
    {
        try
        {
            StatusText = GetOriginalSrcPath(srcDir.FullPath) + " (enumerate)";
            var srcItems = GetDirectoryItems(srcDir.FullPath);
            var tgtItems = GetDirectoryItems(tgtDir.FullPath);
            if (srcItems == null || tgtItems == null)
            {
                LogError($"Unable to mirror directory: {GetOriginalSrcPath(srcDir.FullPath)}");
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
                if (IgnorePaths.Any(ignore => PathsEqual(GetOriginalSrcPath(srcItem.FullPath), ignore)))
                {
                    LogAction($"Skip by path: {GetOriginalSrcPath(srcItem.FullPath)}");
                    srcDict.Remove(srcItem.Name);
                }
                else if (srcItem.Type == ItemType.Dir && IgnoreDirNames.Contains(srcItem.Name))
                {
                    LogAction($"Skip directory by name: {GetOriginalSrcPath(srcItem.FullPath)}");
                    srcDict.Remove(srcItem.Name);
                }
            }
            // Update the item arrays to remove filtered items, and sort them into final processing order
            srcItems = srcDict.Values.OrderBy(s => s.Type == ItemType.Dir ? 2 : 1).ThenBy(s => s.Name.ToLowerInvariant()).ToArray();
            tgtItems = tgtDict.Values.OrderBy(s => s.Type == ItemType.Dir ? 2 : 1).ThenBy(s => s.Name.ToLowerInvariant()).ToArray();

            // Phase 1: delete all target items which are missing in source, or are of a different item type
            foreach (var tgtItem in tgtItems)
            {
                var srcItem = srcDict.Get(tgtItem.Name, null);
                if (srcItem != null && srcItem.Type == tgtItem.Type)
                    continue;
                StatusText = GetOriginalSrcPath(Path.Combine(srcDir.FullPath, tgtItem.Name)) + " (delete)";
                if (srcItem == null)
                    LogChange($"Found deleted {tgtItem.TypeDesc}: ", GetOriginalSrcPath(Path.Combine(srcDir.FullPath, tgtItem.Name)));
                else
                    LogChange($"Found {srcItem.TypeDesc} which used to be a {tgtItem.TypeDesc}: ", GetOriginalSrcPath(srcItem.FullPath));
                ActDelete(tgtItem);
                tgtDict.Remove(tgtItem.Name);
            }
            tgtItems = tgtDict.Values.OrderBy(s => s.Type == ItemType.Dir ? 2 : 1).ThenBy(s => s.Name.ToLowerInvariant()).ToArray();

            // Phase 2: sync all items that are present in both (and have matching types - which at this point is all items still present in both)
            // Every sync method is expected to sync metadata - unconditionally if Force'd or if it had to re-create the item, otherwise if metadata changed (we don't diff ACLs)
            foreach (var srcItem in srcItems)
            {
                var tgtItem = tgtDict.Get(srcItem.Name, null);
                if (tgtItem == null)
                    continue;
                StatusText = GetOriginalSrcPath(srcItem.FullPath) + " (sync)";

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
            // Every action method is expected to copy metadata, regardless of ForceRefreshMetadata
            foreach (var srcItem in srcItems)
            {
                if (tgtDict.ContainsKey(srcItem.Name))
                    continue;
                StatusText = GetOriginalSrcPath(srcItem.FullPath) + " (copy)";

                LogChange($"Found new {srcItem.TypeDesc}: ", GetOriginalSrcPath(srcItem.FullPath));
                var tgtFullPath = Path.Combine(tgtDir.FullPath, srcItem.Name);
                if (srcItem.Type == ItemType.Dir)
                    ActCopyDirectory(srcItem, tgtFullPath);
                else if (srcItem.Type == ItemType.File)
                    ActCopyOrReplaceFile(srcItem.FullPath, tgtFullPath);
                else if (srcItem.Type == ItemType.FileSymlink)
                    ActCopyFileSymlink(srcItem, tgtFullPath);
                else if (srcItem.Type == ItemType.DirSymlink)
                    ActCopyDirSymlink(srcItem, tgtFullPath);
                else if (srcItem.Type == ItemType.Junction)
                    ActCopyJunction(srcItem, tgtFullPath);
                else
                    throw new Exception("unreachable 49612");
            }

            // Phase 4: metadata on the directory itself
            TryCatchIo(tgtDir.Refresh, err => $"Unable to refresh directory attributes ({err}): {tgtDir.FullPath}"); // todo: we could track changes explicitly instead of re-reading last modified time
            if (srcDir.Attrs.LastWriteTime != tgtDir.Attrs.LastWriteTime)
                CopyMetadata(srcDir, tgtDir.FullPath); // directory has definitely changed (includes ActCopyDirectory path), so just copy unconditionally
            else
                SyncMetadata(srcDir, tgtDir); // doesn't look like any changes were made; update metadata only if Force'd or other attrs have changed
        }
        catch (Exception e)
        {
            // none of the above code is supposed to throw under any known circumstances
            LogError($"Unable to sync directory ({e.Message}): {GetOriginalSrcPath(srcDir.FullPath)}");
            LogCriticalError($"SyncDir: {GetOriginalSrcPath(srcDir.FullPath)}\r\n    {e.GetType().Name}: {e.Message}\r\n{e.StackTrace}");
        }
    }

    /// <summary>
    ///     Compares source and target file, detects/logs changes, and updates target to match source if necessary. Assumes
    ///     that both the source and the target exist, and that both are files.</summary>
    private static void SyncFile(Item src, Item tgt)
    {
        if (src.Attrs.LastWriteTime == tgt.Attrs.LastWriteTime && src.FileLength == tgt.FileLength)
        {
            SyncMetadata(src, tgt);
            return;
        }
        LogChange($"Found modified file: ", GetOriginalSrcPath(src.FullPath), whatChanged: $"\r\n    length: {tgt.FileLength:#,0} -> {src.FileLength:#,0}\r\n    modified: {DateTime.FromFileTimeUtc(tgt.Attrs.LastWriteTime)} -> {DateTime.FromFileTimeUtc(src.Attrs.LastWriteTime)} (UTC)");
        ActCopyOrReplaceFile(src.FullPath, tgt.FullPathSubstName(src.Name)); // also copies metadata
    }

    /// <summary>
    ///     Compares source and target file symlinks, detects/logs changes, and updates target to match source if necessary.
    ///     Assumes that both the source and the target exist, and that both are file symlinks.</summary>
    private static void SyncFileSymlink(Item src, Item tgt)
    {
        var srcR = src.Reparse;
        var tgtR = tgt.Reparse;
        if (srcR.SubstituteName == tgtR.SubstituteName && srcR.PrintName == tgtR.PrintName && srcR.IsSymlinkRelative == tgtR.IsSymlinkRelative)
        {
            SyncMetadata(src, tgt);
            return;
        }
        LogChange($"Found modified {src.TypeDesc}: ", GetOriginalSrcPath(src.FullPath), whatChanged: $"\r\n    target: {tgtR.SubstituteName} -> {srcR.SubstituteName}\r\n    print name: {tgtR.PrintName} -> {srcR.PrintName}\r\n    relative: {tgtR.IsSymlinkRelative} -> {srcR.IsSymlinkRelative}");
        ActDelete(tgt);
        ActCopyFileSymlink(src, tgt.FullPathSubstName(src.Name)); // also copies metadata
    }

    /// <summary>
    ///     Compares source and target directory symlinks, detects/logs changes, and updates target to match source if
    ///     necessary. Assumes that both the source and the target exist, and that both are directory symlinks.</summary>
    private static void SyncDirSymlink(Item src, Item tgt)
    {
        var srcR = src.Reparse;
        var tgtR = tgt.Reparse;
        if (srcR.SubstituteName == tgtR.SubstituteName && srcR.PrintName == tgtR.PrintName && srcR.IsSymlinkRelative == tgtR.IsSymlinkRelative)
        {
            SyncMetadata(src, tgt);
            return;
        }
        LogChange($"Found modified {src.TypeDesc}: ", GetOriginalSrcPath(src.FullPath), whatChanged: $"\r\n    target: {tgtR.SubstituteName} -> {srcR.SubstituteName}\r\n    print name: {tgtR.PrintName} -> {srcR.PrintName}\r\n    relative: {tgtR.IsSymlinkRelative} -> {srcR.IsSymlinkRelative}");
        ActDelete(tgt);
        ActCopyDirSymlink(src, tgt.FullPathSubstName(src.Name)); // also copies metadata
    }

    /// <summary>
    ///     Compares source and target junction, detects/logs changes, and updates target to match source if necessary.
    ///     Assumes that both the source and the target exist, and that both are junctions.</summary>
    private static void SyncJunction(Item src, Item tgt)
    {
        var srcR = src.Reparse;
        var tgtR = tgt.Reparse;
        if (srcR.SubstituteName == tgtR.SubstituteName && srcR.PrintName == tgtR.PrintName)
        {
            SyncMetadata(src, tgt);
            return;
        }
        LogChange($"Found modified {src.TypeDesc}: ", GetOriginalSrcPath(src.FullPath), whatChanged: $"\r\n    target: {tgtR.SubstituteName} -> {srcR.SubstituteName}\r\n    print name: {tgtR.PrintName} -> {srcR.PrintName}");
        ActDelete(tgt);
        ActCopyJunction(src, tgt.FullPathSubstName(src.Name)); // also copies metadata
    }

    /// <summary>
    ///     Updates attributes only if Force'd, or if the secondary attributes have changed (which is free to test as we've
    ///     already read them). Expected to only be called if we aren't otherwise aware that the object has changed, so that
    ///     we can skip filesystem writes if nothing seems to have changed, for speed. This is why we ignore LastWriteTime.</summary>
    private static void SyncMetadata(Item src, Item tgt)
    {
        if (!ForceRefreshMetadata) // in normal operation we only attempt this if timestamps/attrs changed, and if so we log this change too
        {
            if (src.Attrs.LastWriteTime != tgt.Attrs.LastWriteTime)
                LogError($"SyncMetadata called when LastWriteTime is different: {GetOriginalSrcPath(src.FullPath)}");
            var same = true;
            same &= src.Attrs.CreationTime == tgt.Attrs.CreationTime;
            same &= src.Attrs.LastAccessTime == tgt.Attrs.LastAccessTime;
            same &= src.Attrs.ChangeTime == tgt.Attrs.ChangeTime;
            same &= src.Attrs.FileAttributes == tgt.Attrs.FileAttributes;
            if (same)
                return;
            LogChange($"Found modified {tgt.TypeDesc} metadata: ", GetOriginalSrcPath(src.FullPath));
        }
        CopyMetadata(src, tgt.FullPath);
    }

    /// <summary>Deletes the specified item of any type. Assumes that the item exists.</summary>
    private static void ActDelete(Item tgt)
    {
        TryCatchIo(() =>
        {
            if (tgt.Type == ItemType.Dir)
            {
                var items = GetDirectoryItems(tgt.FullPath);
                if (items == null)
                    throw new Exception("could not enumerate directory contents");
                foreach (var item in items.OrderBy(t => t.Type == ItemType.Dir ? 2 : 1).ThenBy(t => t.Name))
                    ActDelete(item);
                TryCatchIoAction("Delete empty directory", tgt.FullPath, () =>
                {
                    Filesys.Delete(tgt.FullPath);
                });
            }
            else
            {
                TryCatchIoAction($"Delete {tgt.TypeDesc}", tgt.FullPath, () =>
                {
                    Filesys.Delete(tgt.FullPath);
                });
            }
        }, err => $"Unable to delete {tgt.TypeDesc} ({err}): {tgt.FullPath}");
    }

    /// <summary>Creates the specified file symlink. Assumes that it doesn't exist.</summary>
    private static void ActCopyFileSymlink(Item src, string tgtFullPath)
    {
        TryCatchIoAction("Copy file-symlink", tgtFullPath, () =>
        {
            Filesys.CreateEmptyFile(tgtFullPath);
            ReparsePoint.SetSymlinkData(tgtFullPath, src.Reparse.SubstituteName, src.Reparse.PrintName, src.Reparse.IsSymlinkRelative);
        });
        CopyMetadata(src, tgtFullPath);
    }

    /// <summary>Creates the specified directory symlink. Assumes that it doesn't exist.</summary>
    private static void ActCopyDirSymlink(Item src, string tgtFullPath)
    {
        TryCatchIoAction("Copy directory-symlink", tgtFullPath, () =>
        {
            Filesys.CreateEmptyDirectory(tgtFullPath);
            ReparsePoint.SetSymlinkData(tgtFullPath, src.Reparse.SubstituteName, src.Reparse.PrintName, src.Reparse.IsSymlinkRelative);
        });
        CopyMetadata(src, tgtFullPath);
    }

    /// <summary>Copies the specified junction. Assumes that it doesn't exist.</summary>
    private static void ActCopyJunction(Item src, string tgtFullPath)
    {
        TryCatchIoAction("Copy junction", tgtFullPath, () =>
        {
            Filesys.CreateEmptyDirectory(tgtFullPath);
            ReparsePoint.SetJunctionData(tgtFullPath, src.Reparse.SubstituteName, src.Reparse.PrintName);
        });
        CopyMetadata(src, tgtFullPath);
    }

    /// <summary>Copies a directory to the specified target path. Assumes that the target path does not exist.</summary>
    private static void ActCopyDirectory(Item src, string tgtFullPath)
    {
        TryCatchIoAction("Create directory", tgtFullPath, () =>
        {
            Filesys.CreateEmptyDirectory(tgtFullPath);
        });
        var tgt = CreateItem(tgtFullPath);
        if (tgt == null)
        {
            LogError($"Unable to copy directory: {GetOriginalSrcPath(src.FullPath)}");
            return;
        }
        SyncDir(src, tgt); // this will also unconditionally copy metadata for the directory itself, as it has a new LastWriteTime
    }

    /// <summary>
    ///     Copies a file to the specified path. Always copies to a temporary file in the target directory first, followed by
    ///     a rename, to avoid errors leaving a half finished file looking like the real thing. Unlike other "act" methods,
    ///     this method allows the target file to already exist, and will replace it on successful copy.</summary>
    private static void ActCopyOrReplaceFile(string srcFullPath, string tgtFullPath)
    {
        var tgtTemp = Path.Combine(Path.GetDirectoryName(tgtFullPath), $"~HoboMirror-{Rnd.GenerateString(16)}.tmp");

        bool success = TryCatchIoAction("Copy file", GetOriginalSrcPath(srcFullPath), () =>
        {
            Filesys.CopyFile(srcFullPath, tgtTemp, CopyProgress); // also copies timestamps, attributes, and security
            return true;
        });
        if (!success)
            return;

        TryCatchIo(() =>
        {
            Filesys.Rename(tgtTemp, tgtFullPath, overwrite: true);
        }, err => $"Unable to rename temp copied file to final destination ({err}): {tgtFullPath}");
    }

    private static void CopyMetadata(Item src, string tgtFullPath)
    {
        TryCatchIo(() =>
        {
            Filesys.CopySecurityInfo(src.FullPath, tgtFullPath, dontPropagateInheritable: src.Type == ItemType.Dir);
            var attrs = src.Attrs;
            if (VolumeRoots.Contains(src.FullPath.WithSlash())) // volume roots are marked hidden and system; don't copy that
                attrs.FileAttributes &= ~(uint)(FILE_FLAGS_AND_ATTRIBUTES.FILE_ATTRIBUTE_HIDDEN | FILE_FLAGS_AND_ATTRIBUTES.FILE_ATTRIBUTE_SYSTEM);
            Filesys.SetTimestampsAndAttributes(tgtFullPath, attrs);
        }, err => $"Unable to copy {src.TypeDesc} metadata ({err}): {tgtFullPath}");
        // not logged as an Action because a force refresh would log too much; as a result, also not called ActCopyMetadata
    }

    /// <summary>
    ///     Safely enumerates the contents of a directory. If completely unable to enumerate, logs the error as appropriate
    ///     and returns null (which the caller is expected to handle by skipping whatever they were going to do with this
    ///     list). If able to enumerate, will safely obtain additional info about every entry, skipping those that fail and
    ///     logging errors as appropriate.</summary>
    private static Item[] GetDirectoryItems(string path)
    {
        var start = DateTime.UtcNow;
        var paths = TryCatchIo(() => Filesys.ListDirectory(path), err => $"Unable to list directory ({err}): {path}");
        var time = (DateTime.UtcNow - start).TotalSeconds;
        if (time > 1.0)
            LogAction($"Enumerate directory: {time:0.00} seconds for {paths?.Count ?? -999:#,0} entries: {path}");
        if (paths == null)
            return null;
        return paths.Select(p => CreateItem(path, p)).Where(r => r != null).ToArray();
    }

    /// <summary>
    ///     Determines what type of item this filesystem entry is, while handling any potential errors. On failure, logs an
    ///     appropriate message and returns null (which the caller is expected to handle by skipping whatever they were going
    ///     to do with this item). Slow variant, must open file handle.</summary>
    private static Item CreateItem(string path)
    {
        return TryCatchIo(() => new Item(path), err => $"Unable to determine filesystem entry type ({err}): {path}");
    }
    /// <summary>
    ///     Determines what type of item this filesystem entry is, while handling any potential errors. On failure, logs an
    ///     appropriate message and returns null (which the caller is expected to handle by skipping whatever they were going
    ///     to do with this item). Fast variant, initialised from dir enumeration.</summary>
    private static Item CreateItem(string parentPath, Filesys.DirEntry e)
    {
        return TryCatchIo(() => new Item(parentPath, e), err => $"Unable to determine filesystem entry type ({err}): {Path.Combine(parentPath, e.Name)}");
    }

    private static DateTime lastProgress;
    private static void CopyProgress(Filesys.CopyFileProgress msg)
    {
        if (lastProgress < DateTime.UtcNow - TimeSpan.FromMilliseconds(100))
        {
            lastProgress = DateTime.UtcNow;
            StatusText = $"Copying {msg.CopiedBytes / (double)msg.TotalBytes * 100.0:0.0}% : {msg.CopiedBytes / 1000000.0:#,0} MB of {msg.TotalBytes / 1000000.0:#,0} MB";
        }
    }
}

enum ItemType { File, Dir, FileSymlink, DirSymlink, Junction }

class Item
{
    public string FullPath { get; private set; }
    public string Name { get; private set; }
    public ItemType Type { get; private set; }
    public ReparsePointData Reparse { get; private set; } // null if not a symlink or a junction
    public FILE_BASIC_INFO Attrs { get; private set; }
    public long FileLength { get; private set; }
    public string TypeDesc => Type == ItemType.Dir ? "directory" : Type == ItemType.DirSymlink ? "directory-symlink" : Type == ItemType.File ? "file" : Type == ItemType.FileSymlink ? "file-symlink" : Type == ItemType.Junction ? "junction" : throw new Exception("unreachable 63161");
    public override string ToString() => $"{TypeDesc}: {FullPath}{(Reparse == null ? "" : (" -> " + Reparse.SubstituteName))}";

    /// <summary>Slow init if all we have is a path - must open the handle.</summary>
    public Item(string path)
    {
        FullPath = path;
        Name = Path.GetFileName(path);
        Refresh();
    }

    /// <summary>Fast init if we have most data - only open the handle if it's a reparse point.</summary>
    public Item(string parentPath, Filesys.DirEntry e)
    {
        FullPath = Path.Combine(parentPath, e.Name);
        Name = e.Name;
        Attrs = e.Attrs;
        Reparse = (Attrs.FileAttributes & (uint)FILE_FLAGS_AND_ATTRIBUTES.FILE_ATTRIBUTE_REPARSE_POINT) != 0 ? ReparsePoint.GetReparseData(FullPath) : null;
        FileLength = e.Length;
        finishInit();
    }

    public void Refresh()
    {
        using var handle = Filesys.OpenHandle(FullPath, (uint)FILE_ACCESS_RIGHTS.FILE_READ_ATTRIBUTES);
        Attrs = Filesys.GetTimestampsAndAttributes(handle);
        Reparse = (Attrs.FileAttributes & (uint)FILE_FLAGS_AND_ATTRIBUTES.FILE_ATTRIBUTE_REPARSE_POINT) != 0 ? ReparsePoint.GetReparseData(handle) : null;
        finishInit();
        if (Type == ItemType.File)
            FileLength = Filesys.GetFileLength(handle);
    }

    private void finishInit()
    {
        bool isDir = (Attrs.FileAttributes & (uint)FILE_FLAGS_AND_ATTRIBUTES.FILE_ATTRIBUTE_DIRECTORY) != 0;
        if (Reparse != null)
        {
            FileLength = 0;
            if (Reparse.IsJunction)
                Type = ItemType.Junction;
            else if (Reparse.IsSymlink)
                Type = isDir ? ItemType.DirSymlink : ItemType.FileSymlink;
            else
                throw new Exception($"unrecognized reparse point type {Reparse.ReparseTag}");
        }
        else if (isDir)
        {
            Type = ItemType.Dir;
            FileLength = 0;
        }
        else
            Type = ItemType.File;
    }

    /// <summary>
    ///     Replaces the name in this item's full path with the target name (for the purpose of perserving capitalisation).</summary>
    public string FullPathSubstName(string name)
    {
        return Path.Combine(Path.GetDirectoryName(FullPath), name);
    }
}
