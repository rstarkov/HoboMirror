using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Security.AccessControl;
using System.Text.RegularExpressions;
using Alphaleonis.Win32.Filesystem;
using RT.Util;
using RT.Util.CommandLine;
using RT.Util.Consoles;
using RT.Util.ExtensionMethods;
using RT.Util.Serialization;
using IO = System.IO;

namespace HoboMirror
{
    class Program
    {
        static CmdLine Args;
        static Settings Settings;
        static bool UseVolumeShadowCopy = true;
        static bool RefreshAccessControl = true;
        static bool UpdateMetadata = true;

        static IO.StreamWriter ActionLog, ChangeLog, ErrorLog, CriticalErrorLog, DebugLog;

        static int Main(string[] args)
        {
            if (args.Length == 2 && args[0] == "--post-build-check")
                return Ut.RunPostBuildChecks(args[1], Assembly.GetExecutingAssembly());

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
                if (RefreshAccessControl)
                    Settings.LastRefreshAccessControl = DateTime.UtcNow;
                Console.WriteLine($"Refresh access control: {RefreshAccessControl}");
                Console.WriteLine($"Update metadata: {UpdateMetadata}");
            }

            // Initialise log files
            var startTime = DateTime.UtcNow;
            if (Args.LogPath != null)
            {
                if (Args.LogPath == "")
                    Args.LogPath = PathUtil.AppPath;
                ActionLog = new IO.StreamWriter(IO.File.Open(Path.Combine(Args.LogPath, $"HoboMirror-Actions.{DateTime.Today:yyyy-MM-dd}.txt"), IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read));
                ChangeLog = new IO.StreamWriter(IO.File.Open(Path.Combine(Args.LogPath, $"HoboMirror-Changes.{DateTime.Today:yyyy-MM-dd}.txt"), IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read));
                ErrorLog = new IO.StreamWriter(IO.File.Open(Path.Combine(Args.LogPath, $"HoboMirror-Errors.{DateTime.Today:yyyy-MM-dd}.txt"), IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read));
                CriticalErrorLog = new IO.StreamWriter(IO.File.Open(Path.Combine(Args.LogPath, $"HoboMirror-ErrorsCritical.{DateTime.Today:yyyy-MM-dd}.txt"), IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read));
                DebugLog = new IO.StreamWriter(IO.File.Open(Path.Combine(Args.LogPath, $"HoboMirror-Debug.{DateTime.Today:yyyy-MM-dd}.txt"), IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read));
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
                    foreach (var ignore in Args.IgnorePath)
                        LogAll($"    Ignore path: “{ignore}”");

                    foreach (var task in tasks)
                    {
                        var fromPath = Path.Combine(vscVolumes[task.FromVolume].SnapshotPath, task.FromPath.Substring(task.FromVolume.Length));
                        if (!Directory.Exists(task.ToPath))
                            CreateDirectory(task.ToPath);
                        Mirror(new DirectoryInfo(fromPath), new DirectoryInfo(task.ToPath), str => str.Replace(vscVolumes[task.FromVolume].SnapshotPath, task.FromVolume).Replace(@"\\", @"\"));
                    }
                }

                // Save settings file
                if (Args.SettingsPath != null)
                    ClassifyJson.SerializeToFile(Settings, Args.SettingsPath);

                // List changed directories
                LogChange("", null);
                LogChange("DIRECTORIES WITH AT LEAST ONE CHANGE:", null);
                if (Settings == null)
                {
                    foreach (var chg in Changes.Order())
                        LogChange("  " + chg, null);
                }
                else
                {
                    LogChange("(sorted from rarely changing to frequently changing)", null);
                    var changes =
                        from dir in Changes
                        let match = Settings.GroupDirectoriesForChangeReport.Select(dg => dg.GetMatch(dir)).Where(m => m != null).MinElementOrDefault(s => s.Length)
                        group dir by match ?? dir into grp
                        let changeCounts = grp.Select(p => Settings.DirectoryChangeCount[p])
                        select new { path = grp.Key, changeFreq = changeCounts.Sum(ch => ch.TimesChanged) / (double) changeCounts.Sum(ch => ch.TimesScanned) };
                    foreach (var chg in changes.OrderBy(ch => ch.changeFreq))
                        LogChange($"  {chg.path} — {chg.changeFreq:0.0%}", null);
                }

                return 0;
            }
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

        public static void LogChange(string text, string path)
        {
            if (path != null)
                Changes.Add(Path.GetDirectoryName(path).WithSlash());
            ConsoleUtil.WriteParagraphs((text + path).Color(ConsoleColor.Yellow));
            ChangeLog?.WriteLine(text + path);
            ChangeLog?.Flush();
        }

        public static void LogError(string text)
        {
            ConsoleUtil.WriteParagraphs(text.Color(ConsoleColor.Red));
            ErrorLog?.WriteLine(text);
            ErrorLog?.Flush();
        }

        public static void LogCriticalError(string text)
        {
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

        private static HashSet<string> Changes = new HashSet<string>();

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
                LogCriticalError(formatError($"{e.GetType().Name}, {e.Message}"));
            }
            return default;
        }

        private static void DeleteFile(FileInfo file)
        {
            if (!file.Exists)
            {
                LogError($"File to be deleted does not exist: {file.FullName}");
                return;
            }

            TryCatchIo(() =>
            {
                file.Delete(ignoreReadOnly: true);
                if (file.IsReparsePoint())
                    LogAction($"Delete file reparse point: {file.FullName}");
                else
                    LogAction($"Delete file: {file.FullName}");
            }, err => $"Unable to delete {(file.IsReparsePoint() ? "file reparse point" : "file")} ({err}): {file.FullName}");
        }

        private static void CreateDirectory(string fullName)
        {
            TryCatchIo(() =>
            {
                Directory.CreateDirectory(fullName);
                LogAction($"Create directory: {fullName}");
            }, err => $"Unable to create directory ({err}): {fullName}");
        }

        private static void DeleteDirectory(DirectoryInfo dir)
        {
            // AlphaFS already does this, but just in case it stops doing this in a future release we do this explicitly, because the consequences of following a reparse point during a delete are dire.
            // Also this lets us log every action.
            if (dir.IsReparsePoint())
            {
                TryCatchIo(() =>
                {
                    dir.Delete(recursive: false, ignoreReadOnly: true);
                    LogAction($"Delete directory reparse point: {dir.FullName}");
                }, err => $"Unable to delete directory reparse point ({err}): {dir.FullName}");
                return;
            }

            FileInfo[] files = null;
            DirectoryInfo[] dirs = null;
            var ok = TryCatchIo(() =>
            {
                files = dir.GetFiles();
                dirs = dir.GetDirectories();
                return true;
            }, err => $"Unable to list directory for deletion ({err}): {dir.FullName}");
            if (!ok)
                return;

            foreach (var file in files)
                DeleteFile(file);
            foreach (var subdir in dirs)
                DeleteDirectory(subdir);

            TryCatchIo(() =>
            {
                dir.Delete(recursive: false, ignoreReadOnly: true);
                LogAction($"Delete empty directory: {dir.FullName}");
            }, err => $"Unable to delete empty directory ({err}): {dir.FullName}");
        }

        private static FileSystemInfoMetadata GetMetadata(FileSystemInfo fsi)
        {
            if (!UpdateMetadata)
                return null;
            return TryCatchIo(() =>
            {
                var result = new FileSystemInfoMetadata();
                if (RefreshAccessControl)
                {
                    if (fsi is FileInfo)
                        result.FileSecurity = (fsi as FileInfo).GetAccessControl();
                    else
                        result.DirectorySecurity = (fsi as DirectoryInfo).GetAccessControl();
                }
                result.Attributes = fsi.Attributes;
                result.CreationTimeUtc = fsi.CreationTimeUtc;
                result.LastWriteTimeUtc = fsi.LastWriteTimeUtc;
                result.LastAccessTimeUtc = fsi.LastAccessTimeUtc;
#warning TODO: if the source is a reparse point, this probably copies timestamps from the linked file instead. If source points to non-existent file, this will probably fail
                return result;
            }, err => $"Unable to get {(fsi is FileInfo ? "file" : "directory")} metadata ({err}): {fsi.FullName}");
        }

        private static void SetMetadata(FileSystemInfo fsi, FileSystemInfoMetadata data)
        {
            if (!UpdateMetadata)
                return;
            if (data == null)
            {
                LogError($"Unable to set {(fsi is FileInfo ? "file" : "directory")} metadata (source metadata not available): {fsi.FullName}");
                return;
            }
            TryCatchIo(() =>
            {
                if (RefreshAccessControl)
                {
                    try
                    {
                        if (fsi is FileInfo)
                            (fsi as FileInfo).SetAccessControl(data.FileSecurity);
                        else
                            (fsi as DirectoryInfo).SetAccessControl(data.DirectorySecurity);
                    }
                    catch (IO.IOException)
                    {
                        LogError($"Unable to set {(fsi is FileInfo ? "file" : "directory")} security parameters: {fsi.FullName}");
                    }
                }
                fsi.Attributes = data.Attributes;
                if (fsi is FileInfo)
                    File.SetTimestampsUtc(fsi.FullName, data.CreationTimeUtc, data.LastAccessTimeUtc, data.LastWriteTimeUtc, true, PathFormat.FullPath);
                else
                    Directory.SetTimestampsUtc(fsi.FullName, data.CreationTimeUtc, data.LastAccessTimeUtc, data.LastWriteTimeUtc, true, PathFormat.FullPath);
            }, err => $"Unable to set {(fsi is FileInfo ? "file" : "directory")} metadata ({err}): {fsi.FullName}");
        }

        private static void Mirror(DirectoryInfo from, DirectoryInfo to, Func<string, string> getOriginalFromPath)
        {
            Console.Title = getOriginalFromPath(from.FullName);
            bool anyChanges = false;

            // Enumerate files and directories
            Dictionary<string, FileInfo> fromFiles, toFiles;
            Dictionary<string, DirectoryInfo> fromDirs, toDirs;
            try
            {
                fromFiles = from.GetFiles().ToDictionary(d => d.Name);
                toFiles = to.GetFiles().ToDictionary(d => d.Name);
                fromDirs = from.GetDirectories().ToDictionary(d => d.Name);
                toDirs = to.GetDirectories().ToDictionary(d => d.Name);
            }
            catch (UnauthorizedAccessException)
            {
                LogError($"Unauthorized access: {from.FullName}");
                return;
            }

            // Ignore paths as requested
            foreach (var fromDir in fromDirs.Values.ToList())
                foreach (var ignore in Args.IgnorePath)
                    if (getOriginalFromPath(fromDir.FullName).WithSlash().EqualsNoCase(ignore))
                    {
                        LogAction($"Ignoring directory: {getOriginalFromPath(fromDir.FullName)}");
                        fromDirs.Remove(fromDir.Name);
                        break;
                    }
            // Completely ignore the guard file (in any directory)
            fromFiles.RemoveAllByValue(f => f.Name == "__HoboMirrorTarget__.txt");
            toFiles.RemoveAllByValue(f => f.Name == "__HoboMirrorTarget__.txt");

            // Delete mirrored files missing in source
            foreach (var toFile in toFiles.Values.Where(toFile => !fromFiles.ContainsKey(toFile.Name)))
            {
                LogChange("Found deleted file: ", getOriginalFromPath(Path.Combine(from.FullName, toFile.Name)));
                anyChanges = true;
                DeleteFile(toFile);
            }

            // Delete mirrored directories missing in source
            foreach (var toDir in toDirs.Values.Where(toDir => !fromDirs.ContainsKey(toDir.Name)))
            {
                LogChange("Found deleted directory: ", getOriginalFromPath(Path.Combine(from.FullName, toDir.Name)));
                anyChanges = true;
                DeleteDirectory(toDir);
            }

            // Copy / update all files from source
            foreach (var fromFile in fromFiles.Values)
            {
                var toFile = toFiles.Get(fromFile.Name, null);
                bool notNew = false;

                // For existing files, check if the file contents are out of date
                if (toFile != null)
                {
                    if (fromFile.LastWriteTimeUtc != toFile.LastWriteTimeUtc || fromFile.Length != toFile.Length || fromFile.IsReparsePoint() != toFile.IsReparsePoint())
                    {
#warning TODO: if it was already a reparse point with a different target, this check will miss it, because changing the target does not change last write time
                        if (fromFile.IsReparsePoint() == toFile.IsReparsePoint())
                        {
                            LogChange("Found modified file: ", getOriginalFromPath(fromFile.FullName));
                            LogDebug($"Modified file: {getOriginalFromPath(fromFile.FullName)}");
                            LogDebug($"    Last write time: source={fromFile.LastWriteTimeUtc.ToIsoStringRoundtrip()}, target={toFile.LastWriteTimeUtc.ToIsoStringRoundtrip()}");
                            LogDebug($"    Length: source={fromFile.Length:#,0}, target={toFile.Length:#,0}");
                        }
                        else if (fromFile.IsReparsePoint())
                            LogChange("Found file reparse point which used to be a file: ", getOriginalFromPath(fromFile.FullName));
                        else
                            LogChange("Found file which used to be a file reparse point: ", getOriginalFromPath(fromFile.FullName));
                        anyChanges = true;
                        notNew = true;
                        toFile = null;
                    }
                }

                // Copy the file if required
                if (toFile == null)
                {
                    if (!notNew)
                        LogChange("Found new file: ", getOriginalFromPath(fromFile.FullName));
                    anyChanges = true;
                    var destPath = Path.Combine(to.FullName, fromFile.Name);
                    var destTemp = Path.Combine(to.FullName, $"~HoboMirror-{Rnd.GenerateString(16)}.tmp");
                    var res = new FileInfo(fromFile.FullName).CopyTo(destTemp, CopyOptions.CopySymbolicLink, CopyProgress, null);
#warning TODO: this does not distinguish critical and non-critical errors
                    if (res.ErrorCode != 0)
                        LogError($"Unable to copy file ({res.ErrorMessage}): {getOriginalFromPath(fromFile.FullName)}");
                    else
                    {
                        if (notNew)
                        {
                            var delFile = new FileInfo(destPath);
                            try
                            {
                                delFile.Delete(ignoreReadOnly: true);
                            }
                            catch (UnauthorizedAccessException)
                            {
                                LogError($"Unable to delete (for copy) {(delFile.IsReparsePoint() ? "file reparse point" : "file")} (unauthorized access): {delFile.FullName}");
                            }
                        }
                        File.Move(destTemp, destPath, MoveOptions.None);
                        toFile = new FileInfo(destPath);
                        LogAction($"Copy file: {destPath}\r\n   from: {getOriginalFromPath(fromFile.FullName)}");
                    }
                }

                // Update attributes
                if (toFile != null)
                    SetMetadata(toFile, GetMetadata(fromFile));
            }

            // Process source directories that are reparse points
            foreach (var fromDir in fromDirs.Values.Where(fromDir => fromDir.IsReparsePoint()))
            {
                // Target might not exist, might be a matching reparse point, a non-matching reparse point, or a full-blown directory.
                // The easiest thing to do is to always delete and re-create it.
                var toDir = toDirs.Get(fromDir.Name, null);
                if (toDir != null)
                    DeleteDirectory(toDir);
#warning TODO: detect change properly and improve logging
                var destPath = Path.Combine(to.FullName, fromDir.Name);
                var tgt = File.GetLinkTargetInfo(fromDir.FullName);
                LogAction($"Create reparse point for {getOriginalFromPath(fromDir.FullName)}\r\n   at {destPath}\r\n   linked to {tgt.PrintName}");
                File.CreateSymbolicLink(destPath, tgt.PrintName.StartsWith(@"\??\Volume") ? (@"\\?\" + tgt.PrintName.Substring(4)) : tgt.PrintName, SymbolicLinkTarget.Directory);
#warning What if it was a junction?...
                toDir = new DirectoryInfo(destPath);
                // Copy attributes
                SetMetadata(toDir, GetMetadata(fromDir));
            }

            // Process source directories which are not reparse points
            foreach (var fromDir in fromDirs.Values.Where(fromDir => !fromDir.IsReparsePoint()))
            {
                var toDir = toDirs.Get(fromDir.Name, null);
                bool notNew = false;

                // If target dir exists and is a reparse point, delete it
                if (toDir != null && toDir.IsReparsePoint())
                {
                    LogChange("Found directory which used to be a reparse point: ", getOriginalFromPath(fromDir.FullName));
                    anyChanges = true;
                    DeleteDirectory(toDir);
                    toDir = null;
                    notNew = true;
                }

                // If target dir does not exist, create it
                if (toDir == null)
                {
                    if (!notNew)
                        LogChange("Found new directory: ", getOriginalFromPath(fromDir.FullName));
                    anyChanges = true;
                    toDir = new DirectoryInfo(Path.Combine(to.FullName, fromDir.Name));
                    CreateDirectory(toDir.FullName);
                }

                // Recurse!
                Mirror(fromDir, toDir, getOriginalFromPath);

                // Update attributes
                SetMetadata(toDir, GetMetadata(fromDir));
            }

            // Update statistics
            if (Settings != null)
            {
                var path = getOriginalFromPath(from.FullName).WithSlash();
                Settings.DirectoryChangeCount[path].TimesScanned++;
                if (anyChanges)
                    Settings.DirectoryChangeCount[path].TimesChanged++;
            }
        }

        static DateTime lastProgress;
        static CopyMoveProgressResult CopyProgress(long totalFileSize, long totalBytesTransferred, long streamSize, long streamBytesTransferred, int streamNumber, CopyMoveProgressCallbackReason callbackReason, object userData)
        {
            if (lastProgress < DateTime.UtcNow - TimeSpan.FromMilliseconds(100))
            {
                lastProgress = DateTime.UtcNow;
                Console.Title = $"Copying {totalBytesTransferred / (double) totalFileSize * 100.0:0.0}% : {totalBytesTransferred / 1000000.0:#,0} MB of {totalFileSize / 1000000.0:#,0} MB";
            }
            return CopyMoveProgressResult.Continue;
        }

    }

    class FileSystemInfoMetadata
    {
        public DateTime CreationTimeUtc, LastWriteTimeUtc, LastAccessTimeUtc;
        public IO.FileAttributes Attributes;
        public FileSecurity FileSecurity;
        public DirectorySecurity DirectorySecurity;
    }
}
