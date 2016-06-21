using System.Collections.Generic;
using Alphaleonis.Win32.Filesystem;

namespace HoboMirror
{
    static class ExtensionMethods
    {
        public static bool IsReparsePoint(this FileSystemInfo fsi)
        {
            return fsi.Attributes.HasFlag(System.IO.FileAttributes.ReparsePoint);
        }

        public static string WithSlash(this string path)
        {
            return path.EndsWith("\\") ? path : (path + "\\");
        }

        public static IEnumerable<string> AllParentPaths(this string path)
        {
            var root = Path.GetPathRoot(path);
            while (true)
            {
                yield return path.WithSlash();
                if (path == root)
                    yield break;
                path = Path.GetDirectoryName(path);
            }
        }
    }
}
