using System.ComponentModel;
using Microsoft.Win32.SafeHandles;
using Windows.Wdk.Storage.FileSystem;
using Windows.Win32;
using Windows.Win32.Foundation;
using Windows.Win32.Storage.FileSystem;

namespace HoboMirror;

public static class ReparsePoint
{
    public static string NiceNameToRawName(string niceName)
    {
        if (niceName.StartsWith(@"\\?\Volume{"))
            return @"\??\" + niceName.Substring(4);
        if (!niceName.StartsWith(@"\??\"))
            return @"\??\" + niceName;
        return niceName;
    }

    public static string RawNameToNiceName(string rawName)
    {
        if (rawName.StartsWith(@"\??\Volume{"))
            return @"\\?\" + rawName.Substring(4);
        if (rawName.StartsWith(@"\??\"))
            return rawName.Substring(4);
        return rawName;
    }

    public static unsafe void Create(string junctionPoint, string substituteName, string printName)
    {
        const int bufsize = 0x4000;
        byte* buf = stackalloc byte[bufsize];
        byte* bufend = buf + bufsize;
        var data = (REPARSE_DATA_BUFFER*)buf;

        data->ReparseTag = PInvoke.IO_REPARSE_TAG_MOUNT_POINT;
        var mprb = &data->Anonymous.MountPointReparseBuffer;
        var nametgt = (byte*)&mprb->PathBuffer;

        mprb->SubstituteNameOffset = (ushort)PtrDiff<byte>(nametgt, &mprb->PathBuffer);
        substituteName.CopyTo(SpanFromPtrStartEnd<char>(nametgt, bufend));
        nametgt += substituteName.Length * 2;
        mprb->SubstituteNameLength = (ushort)(substituteName.Length * 2);
        *nametgt++ = 0; // null char terminator
        *nametgt++ = 0;

        mprb->PrintNameOffset = (ushort)PtrDiff<byte>(nametgt, &mprb->PathBuffer);
        printName.CopyTo(SpanFromPtrStartEnd<char>(nametgt, bufend));
        nametgt += printName.Length * 2;
        mprb->PrintNameLength = (ushort)(printName.Length * 2);
        *nametgt++ = 0; // null char terminator
        *nametgt++ = 0;

        data->ReparseDataLength = (ushort)PtrDiff<byte>(nametgt, mprb);

        using var handle = OpenReparsePoint(junctionPoint, GENERIC_ACCESS_RIGHTS.GENERIC_WRITE);
        uint bytesReturned;
        uint buflen = (uint)PtrDiff<byte>(nametgt, buf);
        bool result = PInvoke.DeviceIoControl(handle, PInvoke.FSCTL_SET_REPARSE_POINT, buf, buflen, null, 0, &bytesReturned, null);
        if (!result)
            throw new Win32Exception();
    }

    private static unsafe int PtrDiff<T>(void* p1, void* p2) where T : unmanaged => (int)((T*)p1 - (T*)p2);
    private static unsafe Span<T> SpanFromPtrStartEnd<T>(void* start, void* end) where T : unmanaged => new Span<T>(start, PtrDiff<T>(end, start));
    private static unsafe Span<T> SpanFromPtrAndByteLength<T>(void* start, int byteLen) where T : unmanaged => new Span<T>(start, byteLen / sizeof(T));

    /// <summary>
    /// Deletes only the reparse point data for a junction. Does not delete the target path.
    /// </summary>
    public static unsafe void DeleteJunctionData(string junctionPoint)
    {
        var data = new REPARSE_DATA_BUFFER();
        data.ReparseTag = PInvoke.IO_REPARSE_TAG_MOUNT_POINT;
        data.ReparseDataLength = 0;
        using var handle = OpenReparsePoint(junctionPoint, GENERIC_ACCESS_RIGHTS.GENERIC_WRITE);
        uint bytesReturned;
        bool result = PInvoke.DeviceIoControl(handle, PInvoke.FSCTL_DELETE_REPARSE_POINT, &data, 8, null, 0, &bytesReturned, null);
        if (!result)
            throw new Win32Exception();
    }

    public class ReparsePointInfo
    {
        public uint ReparseTag;
        public string SubstituteName;
        public string PrintName;
        public uint Flags;

        public bool IsJunction => ReparseTag == PInvoke.IO_REPARSE_TAG_MOUNT_POINT;
        public bool IsSymlink => ReparseTag == PInvoke.IO_REPARSE_TAG_SYMLINK;
        public bool IsSymlinkRelative => IsSymlink && (Flags & 1) != 0;
    }

    /// <summary>
    /// Returns null only if the specified path exists and is not a reparse point. Throws for other errors.
    /// </summary>
    public static unsafe ReparsePointInfo GetTarget(string junctionPoint)
    {
        using var handle = OpenReparsePoint(junctionPoint, GENERIC_ACCESS_RIGHTS.GENERIC_READ);

        const int bufsize = 0x4000;
        byte* buf = stackalloc byte[bufsize];
        var data = (REPARSE_DATA_BUFFER*)buf;
        uint bytesReturned;
        bool result = PInvoke.DeviceIoControl(handle, PInvoke.FSCTL_GET_REPARSE_POINT, null, 0, data, bufsize, &bytesReturned, null);

        if (WinAPI.GetLastError() == WIN32_ERROR.ERROR_NOT_A_REPARSE_POINT)
            return null;
        if (!result)
            throw new Win32Exception();

        var res = new ReparsePointInfo();
        res.ReparseTag = data->ReparseTag;

        if (res.IsJunction)
        {
            var mprb = &data->Anonymous.MountPointReparseBuffer;
            var mprbuf = SpanFromPtrAndByteLength<char>(&mprb->PathBuffer, data->ReparseDataLength);
            res.SubstituteName = new string(mprbuf[(mprb->SubstituteNameOffset / 2)..][..(mprb->SubstituteNameLength / 2)]);
            res.PrintName = new string(mprbuf[(mprb->PrintNameOffset / 2)..][..(mprb->PrintNameLength / 2)]);
        }
        else if (res.IsSymlink)
        {
            var slrb = &data->Anonymous.SymbolicLinkReparseBuffer;
            var slrbuf = SpanFromPtrAndByteLength<char>(&slrb->PathBuffer, data->ReparseDataLength);
            res.SubstituteName = new string(slrbuf[(slrb->SubstituteNameOffset / 2)..][..(slrb->SubstituteNameLength / 2)]);
            res.PrintName = new string(slrbuf[(slrb->PrintNameOffset / 2)..][..(slrb->PrintNameLength / 2)]);
            res.Flags = slrb->Flags;
        }

        return res;
    }

    private static SafeFileHandle OpenReparsePoint(string reparsePoint, GENERIC_ACCESS_RIGHTS accessMode)
    {
        var reparsePointHandle = PInvoke.CreateFile(reparsePoint, (uint)accessMode,
            FILE_SHARE_MODE.FILE_SHARE_READ | FILE_SHARE_MODE.FILE_SHARE_WRITE | FILE_SHARE_MODE.FILE_SHARE_DELETE,
            null, FILE_CREATION_DISPOSITION.OPEN_EXISTING,
            FILE_FLAGS_AND_ATTRIBUTES.FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAGS_AND_ATTRIBUTES.FILE_FLAG_OPEN_REPARSE_POINT, null);

        if (WinAPI.GetLastError() != 0)
            throw new Win32Exception();

        return reparsePointHandle;
    }
}
