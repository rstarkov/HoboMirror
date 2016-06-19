using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Security.AccessControl;
using System.Text.RegularExpressions;
using Alphaleonis.Win32.Filesystem;
using RT.Util;
using RT.Util.CommandLine;
using RT.Util.Consoles;
using RT.Util.ExtensionMethods;
using IO = System.IO;

// sort by how many times a folder has been seen to change before, so that unusual changes are at the top

namespace HoboMirror
{
    class Program
    {
        static CmdLine Args;

        static IO.StreamWriter ActionLog, ChangeLog, ErrorLog, DebugLog;

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

            // Initialise log files
            var startTime = DateTime.UtcNow;
            if (Args.LogPath != null)
            {
                if (Args.LogPath == "")
                    Args.LogPath = PathUtil.AppPath;
                ActionLog = new IO.StreamWriter(IO.File.Open(Path.Combine(Args.LogPath, $"HoboMirror-Actions.{DateTime.Today:yyyy-MM-dd}.txt"), IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read));
                ChangeLog = new IO.StreamWriter(IO.File.Open(Path.Combine(Args.LogPath, $"HoboMirror-Changes.{DateTime.Today:yyyy-MM-dd}.txt"), IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read));
                ErrorLog = new IO.StreamWriter(IO.File.Open(Path.Combine(Args.LogPath, $"HoboMirror-Errors.{DateTime.Today:yyyy-MM-dd}.txt"), IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read));
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
                foreach (var task in tasks)
                    LogAll($"    Mirror task: from “{task.FromPath}” to “{task.ToPath}”");
                foreach (var ignore in Args.IgnorePath)
                    LogAll($"    Ignore path: “{ignore}”");

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

                // Perform the mirroring
                var volumes = tasks.GroupBy(t => t.FromVolume).Select(g => g.Key).ToArray();
                using (var vsc = new VolumeShadowCopy(volumes))
                    foreach (var task in tasks)
                    {
                        var fromPath = Path.Combine(vsc.Volumes[task.FromVolume].SnapshotPath, task.FromPath.Substring(task.FromVolume.Length));
                        if (!Directory.Exists(task.ToPath))
                            CreateDirectory(task.ToPath);
                        Mirror(new DirectoryInfo(fromPath), new DirectoryInfo(task.ToPath), str => str.Replace(vsc.Volumes[task.FromVolume].SnapshotPath, task.FromVolume).Replace(@"\\", @"\"));
                    }

                // List changed directories
                LogChange("DIRECTORIES WITH AT LEAST ONE CHANGE: ", null);
                foreach (var chg in Changes.OrderBy(path => path.Count(ch => ch == '\\')).ThenBy(path => path))
                    LogChange(chg, null);

                return 0;
            }
            finally
            {
                // Close log files
                if (Args.LogPath != null)
                {
                    foreach (var log in new[] { ActionLog, ChangeLog, ErrorLog, DebugLog })
                    {
                        log.WriteLine($"Ended at {DateTime.Now}. Time taken: {(DateTime.UtcNow - startTime).TotalMinutes:#,0.0} minutes");
                        log.Dispose();
                    }
                }
            }
        }

        private static void LogAction(string text)
        {
            ConsoleUtil.WriteParagraphs(text.Color(ConsoleColor.White));
            ActionLog?.WriteLine(text);
            ActionLog?.Flush();
        }

        private static void LogChange(string text, string path)
        {
            if (path != null)
                Changes.Add(Path.GetDirectoryName(path));
            ConsoleUtil.WriteParagraphs((text + path).Color(ConsoleColor.Yellow));
            ChangeLog?.WriteLine(text + path);
            ChangeLog?.Flush();
        }

        private static void LogError(string text)
        {
            ConsoleUtil.WriteParagraphs(text.Color(ConsoleColor.Red));
            ErrorLog?.WriteLine(text);
            ErrorLog?.Flush();
        }

        private static void LogDebug(string text)
        {
            ConsoleUtil.WriteParagraphs(text.Color(ConsoleColor.DarkGray));
            DebugLog?.WriteLine(text);
            DebugLog?.Flush();
        }

        private static void LogAll(string text)
        {
            ConsoleUtil.WriteParagraphs(text.Color(ConsoleColor.Green));
            ActionLog?.WriteLine(text);
            ActionLog?.Flush();
            ChangeLog?.WriteLine(text);
            ChangeLog?.Flush();
            ErrorLog?.WriteLine(text);
            ErrorLog?.Flush();
            DebugLog?.WriteLine(text);
            DebugLog?.Flush();
        }

        private static HashSet<string> Changes = new HashSet<string>();

        private static void DeleteFile(FileInfo file)
        {
            if (file.IsReparsePoint())
                LogAction($"Delete file reparse point: {file.FullName}");
            else
                LogAction($"Delete file: {file.FullName}");
            file.Delete(ignoreReadOnly: true);
        }

        private static void CreateDirectory(string fullName)
        {
            LogAction($"Create directory: {fullName}");
            Directory.CreateDirectory(fullName);
        }

        private static void DeleteDirectory(DirectoryInfo dir)
        {
            // AlphaFS already does this, but just in case it stops doing this in a future release we do this explicitly, because the consequences of following a reparse point during a delete are dire.
            // Also this lets us log every action.
            if (dir.IsReparsePoint())
            {
                LogAction($"Delete directory reparse point: {dir.FullName}");
                dir.Delete(recursive: false, ignoreReadOnly: true);
                return;
            }

            foreach (var file in dir.GetFiles())
                DeleteFile(file);
            foreach (var subdir in dir.GetDirectories())
                DeleteDirectory(subdir);
            LogAction($"Delete empty directory: {dir.FullName}");
            dir.Delete(recursive: false, ignoreReadOnly: true);
        }

        private static FileSystemInfoMetadata GetMetadata(FileSystemInfo fsi)
        {
            try
            {
                var result = new FileSystemInfoMetadata();
                if (fsi is FileInfo)
                    result.FileSecurity = (fsi as FileInfo).GetAccessControl();
                else
                    result.DirectorySecurity = (fsi as DirectoryInfo).GetAccessControl();
                result.Attributes = fsi.Attributes;
                result.CreationTimeUtc = fsi.CreationTimeUtc;
                result.LastWriteTimeUtc = fsi.LastWriteTimeUtc;
                result.LastAccessTimeUtc = fsi.LastAccessTimeUtc;
#warning TODO: if the source is a reparse point, this probably copies timestamps from the linked file instead. If source points to non-existent file, this will probably fail
                return result;
            }
            catch (UnauthorizedAccessException)
            {
                LogError($"Unable to get {(fsi is FileInfo ? "file" : "directory")} metadata (unauthorized access): {fsi.FullName}");
                return null;
            }
        }

        private static void SetMetadata(FileSystemInfo fsi, FileSystemInfoMetadata data)
        {
            if (data == null)
            {
                LogError($"Unable to set {(fsi is FileInfo ? "file" : "directory")} metadata (source metadata not available): {fsi.FullName}");
                return;
            }
            try
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
#warning TODO: why does this fail on many files?
                }
                fsi.Attributes = data.Attributes;
                if (fsi is FileInfo)
                    File.SetTimestampsUtc(fsi.FullName, data.CreationTimeUtc, data.LastAccessTimeUtc, data.LastWriteTimeUtc, true, PathFormat.FullPath);
                else
                    Directory.SetTimestampsUtc(fsi.FullName, data.CreationTimeUtc, data.LastAccessTimeUtc, data.LastWriteTimeUtc, true, PathFormat.FullPath);
            }
            catch (UnauthorizedAccessException)
            {
                LogError($"Unable to set {(fsi is FileInfo ? "file" : "directory")} metadata (unauthorized access): {fsi.FullName}");
            }
        }

        private static void Mirror(DirectoryInfo from, DirectoryInfo to, Func<string, string> getOriginalFromPath)
        {
            Console.Title = getOriginalFromPath(from.FullName);

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
                        LogAction($"Ignoring directory: {fromDir.FullName}");
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
                DeleteFile(toFile);
            }

            // Delete mirrored directories missing in source
            foreach (var toDir in toDirs.Values.Where(toDir => !fromDirs.ContainsKey(toDir.Name)))
            {
                LogChange("Found deleted directory: ", getOriginalFromPath(Path.Combine(from.FullName, toDir.Name)));
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
                        DeleteFile(toFile);
                        notNew = true;
                        toFile = null;
                    }
                }

                // Copy the file if required
                if (toFile == null)
                {
                    if (!notNew)
                        LogChange("Found new file: ", getOriginalFromPath(fromFile.FullName));
                    var destPath = Path.Combine(to.FullName, fromFile.Name);
                    LogAction($"Copy file: {destPath}\r\n   from: {getOriginalFromPath(fromFile.FullName)}");
                    fromFile.CopyTo(destPath, CopyOptions.CopySymbolicLink, CopyProgress, null);
                    toFile = new FileInfo(destPath);
                }

                // Update attributes
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
                toDir.SetAccessControl(fromDir.GetAccessControl());
                toDir.Attributes = fromDir.Attributes;
                Directory.SetTimestampsUtc(destPath, fromDir.CreationTimeUtc, fromDir.LastAccessTimeUtc, fromDir.LastWriteTimeUtc, true, PathFormat.FullPath);
#warning TODO: if the source is a reparse point, this probably copies timestamps from the linked file instead. If source points to non-existent file, this will probably fail
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
                    DeleteDirectory(toDir);
                    toDir = null;
                    notNew = true;
                }

                // If target dir does not exist, create it
                if (toDir == null)
                {
                    if (!notNew)
                        LogChange("Found new directory: ", getOriginalFromPath(fromDir.FullName));
                    toDir = new DirectoryInfo(Path.Combine(to.FullName, fromDir.Name));
                    CreateDirectory(toDir.FullName);
                }

                // Recurse!
                Mirror(fromDir, toDir, getOriginalFromPath);

                // Update attributes
                SetMetadata(toDir, GetMetadata(fromDir));
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
