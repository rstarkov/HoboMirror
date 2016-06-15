using System;
using System.Collections.Generic;
using System.Linq;
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
            var volumes = Tasks.GroupBy(t => t.FromVolume).Select(g => g.Key).ToArray();
            using (var vsc = new VolumeShadowCopy(volumes))
                foreach (var task in Tasks)
                {
                    var fromPath = Path.Combine(vsc.Volumes[task.FromVolume].SnapshotPath, task.FromPath.Substring(task.FromVolume.Length));
                    if (!Directory.Exists(task.ToPath))
                        CreateDirectory(task.ToPath);
                    Mirror(new DirectoryInfo(fromPath), new DirectoryInfo(task.ToPath), str => str.Replace(vsc.Volumes[task.FromVolume].SnapshotPath, task.FromVolume).Replace(@"\\", @"\"));
                }
        }

        private static void LogAction(string text)
        {
            ConsoleUtil.WriteLine(text.Color(ConsoleColor.White));
        }

        private static void LogChange(string text)
        {
            ConsoleUtil.WriteLine(text.Color(ConsoleColor.Yellow));
        }

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

        private static void Mirror(DirectoryInfo from, DirectoryInfo to, Func<string, string> sourcePathForDisplay)
        {
            Console.Title = sourcePathForDisplay(from.FullName);

            // Enumerate files and directories
            var fromFiles = from.GetFiles().ToDictionary(d => d.Name);
            var toFiles = to.GetFiles().ToDictionary(d => d.Name);
            var fromDirs = from.GetDirectories().ToDictionary(d => d.Name);
            var toDirs = to.GetDirectories().ToDictionary(d => d.Name);

            // Delete mirrored files missing in source
            foreach (var toFile in toFiles.Values.Where(toFile => !fromFiles.ContainsKey(toFile.Name)))
            {
                LogChange($"Found deleted file: {sourcePathForDisplay(Path.Combine(from.FullName, toFile.Name))}");
                DeleteFile(toFile);
            }

            // Delete mirrored directories missing in source
            foreach (var toDir in toDirs.Values.Where(toDir => !fromDirs.ContainsKey(toDir.Name)))
            {
                LogChange($"Found deleted directory: {sourcePathForDisplay(Path.Combine(from.FullName, toDir.Name))}");
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
                            LogChange($"Found modified file: {sourcePathForDisplay(fromFile.FullName)}");
                        else if (fromFile.IsReparsePoint())
                            LogChange($"Found file reparse point which used to be a file: {sourcePathForDisplay(fromFile.FullName)}");
                        else
                            LogChange($"Found file which used to be a file reparse point: {sourcePathForDisplay(fromFile.FullName)}");
                        DeleteFile(toFile);
                        notNew = true;
                        toFile = null;
                    }
                }

                // Copy the file if required
                if (toFile == null)
                {
                    if (!notNew)
                        LogChange($"Found new file: \"{sourcePathForDisplay(fromFile.FullName)}");
                    var destPath = Path.Combine(to.FullName, fromFile.Name);
                    Console.WriteLine($"Mirror file: \"{sourcePathForDisplay(fromFile.FullName)}\"\r\n   to \"{destPath}\"");
                    fromFile.CopyTo(destPath, CopyOptions.CopySymbolicLink, CopyProgress, null);
                    toFile = new FileInfo(destPath);
                }

                // Update attributes
                try { toFile.SetAccessControl(fromFile.GetAccessControl()); }
                catch
                {
#warning TODO: figure out what's happening here.
                    Console.WriteLine($"Could not SetAccessControl on {toFile.FullName}");
                }
                toFile.Attributes = fromFile.Attributes;
                File.SetTimestampsUtc(toFile.FullName, fromFile.CreationTimeUtc, fromFile.LastAccessTimeUtc, fromFile.LastWriteTimeUtc, true, PathFormat.FullPath);
#warning TODO: if the source is a reparse point, this probably copies timestamps from the linked file instead. If source points to non-existent file, this will probably fail
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
                    LogChange($"Found directory which used to be a reparse point: {sourcePathForDisplay(fromDir.FullName)}");
                    DeleteDirectory(toDir);
                    toDir = null;
                    notNew = true;
                }

                // If target dir does not exist, create it
                if (toDir == null)
                {
                    if (!notNew)
                        LogChange($"Found new directory: {sourcePathForDisplay(fromDir.FullName)}");
                    toDir = new DirectoryInfo(Path.Combine(to.FullName, fromDir.Name));
                    CreateDirectory(toDir.FullName);
                }

                // Recurse!
                Mirror(fromDir, toDir, sourcePathForDisplay);

                // Update attributes
                try { toDir.SetAccessControl(fromDir.GetAccessControl()); }
                catch
                {
#warning TODO: figure out what's happening here.
                    Console.WriteLine($"Could not SetAccessControl on {toDir.FullName}");
                }
                toDir.Attributes = fromDir.Attributes;
                Directory.SetTimestampsUtc(toDir.FullName, fromDir.CreationTimeUtc, fromDir.LastAccessTimeUtc, fromDir.LastWriteTimeUtc, PathFormat.FullPath);
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
}
