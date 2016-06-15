using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Alphaleonis.Win32.Filesystem;
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
                    Directory.CreateDirectory(task.ToPath);
                    doCopy(new DirectoryInfo(fromPath), new DirectoryInfo(task.ToPath), str => str.Replace(vsc.Volumes[task.FromVolume].SnapshotPath, task.FromVolume).Replace(@"\\", @"\"));
                }
        }

        private static void doCopy(DirectoryInfo from, DirectoryInfo to, Func<string, string> sourcePathForDisplay)
        {
            Console.Title = from.FullName;

            // Enumerate files and directories
            var fromFiles = from.GetFiles().ToDictionary(d => d.Name);
            var toFiles = to.GetFiles().ToDictionary(d => d.Name);
            var fromDirs = from.GetDirectories().ToDictionary(d => d.Name);
            var toDirs = to.GetDirectories().ToDictionary(d => d.Name);

            // Delete mirrored files missing in source
            foreach (var onlyToFile in toFiles.Values.Where(toFile => !fromFiles.ContainsKey(toFile.Name)))
            {
                Console.WriteLine($"Delete file: {onlyToFile.FullName}");
                onlyToFile.Delete(true);
            }

            // Delete mirrored directories missing in source
            foreach (var onlyToDir in toDirs.Values.Where(toDir => !fromDirs.ContainsKey(toDir.Name)))
            {
                safeDeleteDirWithoutFollowingReparses(onlyToDir);
            }

            // Copy / update all files from source
            foreach (var fromFile in fromFiles.Values)
            {
                var toFile = toFiles.Get(fromFile.Name, null);

                // For existing files, check if the file contents are out of date
                if (toFile != null)
                {
                    if (fromFile.LastWriteTimeUtc != toFile.LastWriteTimeUtc || fromFile.Length != toFile.Length || fromFile.IsReparsePoint() != toFile.IsReparsePoint())
                    {
                        Console.WriteLine($"File changed: \"{sourcePathForDisplay(fromFile.FullName)}\".\r\n   deleting mirror at \"{toFile.FullName}\".");
                        toFile.Delete(true);
                        toFile = null;
                    }
                }

                // Copy the file if required
                if (toFile == null)
                {
                    var destPath = Path.Combine(to.FullName, fromFile.Name);
                    Console.WriteLine($"Mirror file: \"{sourcePathForDisplay(fromFile.FullName)}\"\r\n   to \"{destPath}\"");
                    fromFile.CopyTo(destPath, CopyOptions.CopySymbolicLink, progress, null);
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
                    safeDeleteDirWithoutFollowingReparses(toDir);
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

                // If target dir exists and is a reparse point, delete it
                if (toDir != null && toDir.IsReparsePoint())
                {
                    safeDeleteDirWithoutFollowingReparses(toDir);
                    toDir = null;
                }

                // If target dir does not exist, create it
                if (toDir == null)
                {
                    toDir = new DirectoryInfo(Path.Combine(to.FullName, fromDir.Name));
                    Console.WriteLine($"Create directory: {toDir.FullName}");
                    toDir.Create();
                }

                // Recurse!
                doCopy(fromDir, toDir, sourcePathForDisplay);

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

        private static void safeDeleteDirWithoutFollowingReparses(DirectoryInfo dir)
        {
            // AlphaFS already does this, but just in case it stops doing this in a future release we do this explicitly, because the consequences of following a reparse point during a delete are dire
            if (dir.IsReparsePoint())
            {
                Console.WriteLine($"Delete directory reparse point: {dir.FullName}");
                dir.Delete(false, true);
                return;
            }

            foreach (var file in dir.GetFiles())
            {
                Console.WriteLine($"Delete file: {file.FullName}");
                file.Delete(true);
            }
            foreach (var subdir in dir.GetDirectories())
                safeDeleteDirWithoutFollowingReparses(subdir);
            Console.WriteLine($"Delete empty directory: {dir.FullName}");
            dir.Delete(false, true);
        }

        static DateTime lastProgress;
        static CopyMoveProgressResult progress(long totalFileSize, long totalBytesTransferred, long streamSize, long streamBytesTransferred, int streamNumber, CopyMoveProgressCallbackReason callbackReason, object userData)
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
