using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Permissions;
using System.Text;
using Alphaleonis.Win32.Filesystem;
using Alphaleonis.Win32.Vss;

namespace HoboMirror
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var vsc = new VolumeShadowCopy(@"C:\", @"\\?\Volume{00000000-0000-0000-0000-000000000000}\"))
            {
                var x1 = vsc.Volumes[@"C:\"].SnapshotPath;
                var x2 = vsc.Volumes[@"\\?\Volume{00000000-0000-0000-0000-000000000000}\"].SnapshotPath;
                File.Copy(Path.Combine(vsc.Volumes[@"C:\"].SnapshotPath, @"testfile.txt"), @"D:\testfile.txt", CopyOptions.CopySymbolicLink, true, progr, null);
            }
        }

        static CopyMoveProgressResult progr(long totalFileSize, long totalBytesTransferred, long streamSize, long streamBytesTransferred, uint streamNumber, CopyMoveProgressCallbackReason callbackReason, object userData)
        {
            return CopyMoveProgressResult.Continue;
        }

    }

}
