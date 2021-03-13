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

        public static string ParentFullName(this FileSystemInfo fsi)
        {
            return Path.GetDirectoryName(fsi.FullName);
        }

        public static string WithName(this FileSystemInfo fsi, string name)
        {
            return Path.Combine(fsi.ParentFullName(), name);
        }
    }
}
