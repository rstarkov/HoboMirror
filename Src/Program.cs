using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.AccessControl;
using System.Text.RegularExpressions;
using Alphaleonis.Win32.Filesystem;
using RT.Util.Consoles;
using RT.Util.ExtensionMethods;

namespace HoboMirror
{
    class Program
    {
        static List<CopyTask> Tasks = new List<CopyTask>
        {
            new CopyTask(@"D:\", @"E:\Mirror\D\"),
        };

        static void Main(string[] args)
        {
            LogAction("===============");
            LogChange("===============", null);
            LogError("===============");
            var volumes = Tasks.GroupBy(t => t.FromVolume).Select(g => g.Key).ToArray();
            using (var vsc = new VolumeShadowCopy(volumes))
                foreach (var task in Tasks)
                {
                    var fromPath = Path.Combine(vsc.Volumes[task.FromVolume].SnapshotPath, task.FromPath.Substring(task.FromVolume.Length));
                    if (!Directory.Exists(task.ToPath))
                        CreateDirectory(task.ToPath);
                    Mirror(new DirectoryInfo(fromPath), new DirectoryInfo(task.ToPath), str => str.Replace(vsc.Volumes[task.FromVolume].SnapshotPath, task.FromVolume).Replace(@"\\", @"\"));
                }
            LogChange("DIRECTORIES WITH AT LEAST ONE CHANGE: ", null);
            foreach (var chg in Changes.OrderBy(path => path.Count(ch => ch == '\\')).ThenBy(path => path))
                LogChange(chg, null);
        }

        private static void LogAction(string text)
        {
            ConsoleUtil.WriteLine(text.Color(ConsoleColor.White));
            File.AppendAllLines("log-actions.txt", new[] { text });
        }

        private static void LogChange(string text, string path)
        {
            ConsoleUtil.WriteLine(text.Color(ConsoleColor.Yellow));
            File.AppendAllLines("log-changes.txt", new[] { text });
            if (path != null)
                Changes.Add(Path.GetDirectoryName(path));
        }

        private static void LogError(string text)
        {
            ConsoleUtil.WriteLine(text.Color(ConsoleColor.Red));
            File.AppendAllLines("log-errors.txt", new[] { text });
        }

        private static void LogDebug(string text)
        {
            ConsoleUtil.WriteLine(text.Color(ConsoleColor.DarkGray));
            File.AppendAllLines("log-debug.txt", new[] { text });
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
                catch (System.IO.IOException)
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

        private static void Mirror(DirectoryInfo from, DirectoryInfo to, Func<string, string> sourcePathForDisplay)
        {
            Console.Title = sourcePathForDisplay(from.FullName);

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

            // Delete mirrored files missing in source
            foreach (var toFile in toFiles.Values.Where(toFile => !fromFiles.ContainsKey(toFile.Name)))
            {
                LogChange("Found deleted file: ", sourcePathForDisplay(Path.Combine(from.FullName, toFile.Name)));
                DeleteFile(toFile);
            }

            // Delete mirrored directories missing in source
            foreach (var toDir in toDirs.Values.Where(toDir => !fromDirs.ContainsKey(toDir.Name)))
            {
                LogChange("Found deleted directory: ", sourcePathForDisplay(Path.Combine(from.FullName, toDir.Name)));
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
                            LogChange("Found modified file: ", sourcePathForDisplay(fromFile.FullName));
                            LogDebug($"Modified file: {sourcePathForDisplay(fromFile.FullName)}");
                            LogDebug($"    Last write time: source={fromFile.LastWriteTimeUtc.ToIsoStringRoundtrip()}, target={toFile.LastWriteTimeUtc.ToIsoStringRoundtrip()}");
                            LogDebug($"    Length: source={fromFile.Length:#,0}, target={toFile.Length:#,0}");
                        }
                        else if (fromFile.IsReparsePoint())
                            LogChange("Found file reparse point which used to be a file: ", sourcePathForDisplay(fromFile.FullName));
                        else
                            LogChange("Found file which used to be a file reparse point: ", sourcePathForDisplay(fromFile.FullName));
                        DeleteFile(toFile);
                        notNew = true;
                        toFile = null;
                    }
                }

                // Copy the file if required
                if (toFile == null)
                {
                    if (!notNew)
                        LogChange("Found new file: ", sourcePathForDisplay(fromFile.FullName));
                    var destPath = Path.Combine(to.FullName, fromFile.Name);
                    LogAction($"Copy file: {destPath}\r\n   from: {sourcePathForDisplay(fromFile.FullName)}");
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
                {
                    //Console.WriteLine($"Delete existing directory to copy reparse point: {toDir.FullName}");
                    DeleteDirectory(toDir);
                }
                var destPath = Path.Combine(to.FullName, fromDir.Name);
                var tgt = File.GetLinkTargetInfo(fromDir.FullName);
                Console.WriteLine($"Create reparse point for {sourcePathForDisplay(fromDir.FullName)}\r\n   at {destPath}\r\n   linked to {tgt.PrintName}");
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
                    LogChange("Found directory which used to be a reparse point: ", sourcePathForDisplay(fromDir.FullName));
                    DeleteDirectory(toDir);
                    toDir = null;
                    notNew = true;
                }

                // If target dir does not exist, create it
                if (toDir == null)
                {
                    if (!notNew)
                        LogChange("Found new directory: ", sourcePathForDisplay(fromDir.FullName));
                    toDir = new DirectoryInfo(Path.Combine(to.FullName, fromDir.Name));
                    CreateDirectory(toDir.FullName);
                }

                // Recurse!
                Mirror(fromDir, toDir, sourcePathForDisplay);

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

    class CopyTask
    {
        public string FromPath { get; private set; }
        public string FromVolume { get; private set; }
        public string ToPath { get; private set; }

        public CopyTask(string copyFrom, string copyTo)
        {
            FromPath = copyFrom;
            ToPath = copyTo;
            var match = Regex.Match(FromPath, @"^\\\\\?\\Volume{[^}]+}\\");
            if (match.Success)
            {
                FromVolume = match.Groups[0].Value;
            }
            else
            {
                match = Regex.Match(FromPath, @"^\w:\\");
                if (match.Success)
                    FromVolume = match.Groups[0].Value;
                else
                    throw new Exception();
            }
        }
    }

    class FileSystemInfoMetadata
    {
        public DateTime CreationTimeUtc, LastWriteTimeUtc, LastAccessTimeUtc;
        public System.IO.FileAttributes Attributes;
        public FileSecurity FileSecurity;
        public DirectorySecurity DirectorySecurity;
    }
}
