using System.ComponentModel;
using Microsoft.Win32.SafeHandles;
using Windows.Wdk.Storage.FileSystem;
using Windows.Win32;
using Windows.Win32.Foundation;
using Windows.Win32.Storage.FileSystem;

namespace HoboMirror;

/// <summary>
///     Reparse point helpers for junctions and symlinks. All methods use backup semantics to bypass access control checks
///     (requires SeBackup/SeRestore), and support long file paths.</summary>
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

    /// <summary>
    ///     Sets junction reparse data on the specified directory. The path must exist, be a directory, and be empty
    ///     (otherwise throws). Will overwrite existing junction data, but not existing symlink data (throws).</summary>
    public static unsafe void SetJunctionData(string path, string substituteName, string printName)
    {
        const int bufsize = 0x4000;
        byte* buf = stackalloc byte[bufsize];
        byte* bufend = buf + bufsize;
        var data = (REPARSE_DATA_BUFFER*)buf;

        data->ReparseTag = PInvoke.IO_REPARSE_TAG_MOUNT_POINT;
        var mprb = &data->Anonymous.MountPointReparseBuffer;

        var bufpos = (byte*)&mprb->PathBuffer;
        (mprb->SubstituteNameOffset, mprb->SubstituteNameLength) = writeNameToBuffer(substituteName, ref bufpos, (byte*)&mprb->PathBuffer, bufend);
        (mprb->PrintNameOffset, mprb->PrintNameLength) = writeNameToBuffer(printName, ref bufpos, (byte*)&mprb->PathBuffer, bufend);
        data->ReparseDataLength = (ushort)Ptr.Diff<byte>(bufpos, mprb);

        using var handle = openReparsePoint(path, FILE_ACCESS_RIGHTS.FILE_WRITE_ATTRIBUTES);
        uint bytesReturned;
        uint buflen = (uint)Ptr.Diff<byte>(bufpos, buf);
        if (!PInvoke.DeviceIoControl(handle, PInvoke.FSCTL_SET_REPARSE_POINT, buf, buflen, null, 0, &bytesReturned, null))
            throw new Win32Exception();
    }

    /// <summary>
    ///     Sets junction reparse data on the specified file or directory, which must exist. If the target is a directory it
    ///     must be empty; if it's a file it must be zero-length. Throws if the these conditions are violated. Overwrites
    ///     existing symlink data if any, but will throw if the existing reparse point is a junction.</summary>
    public static unsafe void SetSymlinkData(string path, string substituteName, string printName, bool relative)
    {
        const int bufsize = 0x4000;
        byte* buf = stackalloc byte[bufsize];
        byte* bufend = buf + bufsize;
        var data = (REPARSE_DATA_BUFFER*)buf;

        data->ReparseTag = PInvoke.IO_REPARSE_TAG_SYMLINK;
        var slrb = &data->Anonymous.SymbolicLinkReparseBuffer;

        var bufpos = (byte*)&slrb->PathBuffer;
        (slrb->SubstituteNameOffset, slrb->SubstituteNameLength) = writeNameToBuffer(substituteName, ref bufpos, (byte*)&slrb->PathBuffer, bufend);
        (slrb->PrintNameOffset, slrb->PrintNameLength) = writeNameToBuffer(printName, ref bufpos, (byte*)&slrb->PathBuffer, bufend);
        slrb->Flags = (relative ? 1u /*SYMLINK_FLAG_RELATIVE*/ : 0u);
        data->ReparseDataLength = (ushort)Ptr.Diff<byte>(bufpos, slrb);

        using var handle = openReparsePoint(path, FILE_ACCESS_RIGHTS.FILE_WRITE_ATTRIBUTES);
        uint bytesReturned;
        uint buflen = (uint)Ptr.Diff<byte>(bufpos, buf);
        if (!PInvoke.DeviceIoControl(handle, PInvoke.FSCTL_SET_REPARSE_POINT, buf, buflen, null, 0, &bytesReturned, null))
            throw new Win32Exception();
    }

    private static unsafe (ushort offset, ushort length) writeNameToBuffer(string name, ref byte* bufpos, byte* bufstart, byte* bufend)
    {
        var offset = (ushort)Ptr.Diff<byte>(bufpos, bufstart);
        name.CopyTo(Ptr.SpanFromPtrStartEnd<char>(bufpos, bufend));
        bufpos += name.Length * 2;
        *bufpos++ = 0; // null char terminator
        *bufpos++ = 0;
        return (offset, length: (ushort)(name.Length * 2));
    }

    /// <summary>Deletes only the reparse point data for a junction. Does not delete the target path. Throws for symlinks.</summary>
    public static unsafe void DeleteJunctionData(string path)
    {
        deleteReparseData(path, PInvoke.IO_REPARSE_TAG_MOUNT_POINT);
    }
    /// <summary>Deletes only the reparse point data for a symlink. Does not delete the target path. Throws for junctions.</summary>
    public static unsafe void DeleteSymlinkData(string path)
    {
        deleteReparseData(path, PInvoke.IO_REPARSE_TAG_SYMLINK);
    }

    private static unsafe void deleteReparseData(string path, uint tag)
    {
        var data = new REPARSE_DATA_BUFFER();
        data.ReparseTag = tag;
        data.ReparseDataLength = 0;
        using var handle = openReparsePoint(path, FILE_ACCESS_RIGHTS.FILE_WRITE_ATTRIBUTES);
        uint bytesReturned;
        if (!PInvoke.DeviceIoControl(handle, PInvoke.FSCTL_DELETE_REPARSE_POINT, &data, 8, null, 0, &bytesReturned, null))
            throw new Win32Exception();
    }

    /// <summary>Returns null only if the specified path exists and is not a reparse point. Throws for other errors.</summary>
    public static ReparsePointData GetReparseData(string path)
    {
        using var handle = openReparsePoint(path, FILE_ACCESS_RIGHTS.FILE_READ_ATTRIBUTES);
        return GetReparseData(handle);
    }
    /// <summary>Returns null only if the specified path exists and is not a reparse point. Throws for other errors.</summary>
    public static unsafe ReparsePointData GetReparseData(SafeFileHandle handle)
    {
        const int bufsize = 0x4000;
        byte* buf = stackalloc byte[bufsize];
        var data = (REPARSE_DATA_BUFFER*)buf;
        uint bytesReturned;
        bool result = PInvoke.DeviceIoControl(handle, PInvoke.FSCTL_GET_REPARSE_POINT, null, 0, data, bufsize, &bytesReturned, null);

        if (WinAPI.GetLastError() == WIN32_ERROR.ERROR_NOT_A_REPARSE_POINT)
            return null;
        if (!result)
            throw new Win32Exception();

        var res = new ReparsePointData();
        res.ReparseTag = data->ReparseTag;

        if (res.IsJunction)
        {
            var mprb = &data->Anonymous.MountPointReparseBuffer;
            var mprbuf = Ptr.SpanFromPtrAndByteLength<char>(&mprb->PathBuffer, data->ReparseDataLength);
            res.SubstituteName = new string(mprbuf[(mprb->SubstituteNameOffset / 2)..][..(mprb->SubstituteNameLength / 2)]);
            res.PrintName = new string(mprbuf[(mprb->PrintNameOffset / 2)..][..(mprb->PrintNameLength / 2)]);
        }
        else if (res.IsSymlink)
        {
            var slrb = &data->Anonymous.SymbolicLinkReparseBuffer;
            var slrbuf = Ptr.SpanFromPtrAndByteLength<char>(&slrb->PathBuffer, data->ReparseDataLength);
            res.SubstituteName = new string(slrbuf[(slrb->SubstituteNameOffset / 2)..][..(slrb->SubstituteNameLength / 2)]);
            res.PrintName = new string(slrbuf[(slrb->PrintNameOffset / 2)..][..(slrb->PrintNameLength / 2)]);
            res.SymlinkFlags = slrb->Flags;
        }

        return res;
    }

    private static SafeFileHandle openReparsePoint(string path, FILE_ACCESS_RIGHTS accessMode)
    {
        return WinAPI.CreateFile(path, (uint)accessMode,
            FILE_SHARE_MODE.FILE_SHARE_READ | FILE_SHARE_MODE.FILE_SHARE_WRITE | FILE_SHARE_MODE.FILE_SHARE_DELETE,
            null, FILE_CREATION_DISPOSITION.OPEN_EXISTING,
            FILE_FLAGS_AND_ATTRIBUTES.FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAGS_AND_ATTRIBUTES.FILE_FLAG_OPEN_REPARSE_POINT, null);
    }
}

public class ReparsePointData
{
    public uint ReparseTag;
    public string SubstituteName;
    public string PrintName;
    public uint SymlinkFlags;

    public bool IsJunction => ReparseTag == PInvoke.IO_REPARSE_TAG_MOUNT_POINT;
    public bool IsSymlink => ReparseTag == PInvoke.IO_REPARSE_TAG_SYMLINK;
    public bool IsSymlinkRelative => IsSymlink && (SymlinkFlags & 1 /*SYMLINK_FLAG_RELATIVE*/) != 0;
}
