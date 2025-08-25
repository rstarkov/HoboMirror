using System.ComponentModel;
using System.Runtime.InteropServices;
using Windows.Wdk.Storage.FileSystem;
using Windows.Win32;
using Windows.Win32.Foundation;
using Windows.Win32.Storage.FileSystem;

namespace HoboMirror;

static class Filesys
{
    private const FILE_SHARE_MODE FileShareAll = FILE_SHARE_MODE.FILE_SHARE_READ | FILE_SHARE_MODE.FILE_SHARE_WRITE | FILE_SHARE_MODE.FILE_SHARE_DELETE;
    private const FILE_CREATION_DISPOSITION FileDispExisting = FILE_CREATION_DISPOSITION.OPEN_EXISTING;
    private const FILE_CREATION_DISPOSITION FileDispNew = FILE_CREATION_DISPOSITION.CREATE_NEW;
    private const FILE_FLAGS_AND_ATTRIBUTES Semantics = FILE_FLAGS_AND_ATTRIBUTES.FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAGS_AND_ATTRIBUTES.FILE_FLAG_OPEN_REPARSE_POINT;

    /// <summary>
    ///     Gets timestamps and attributes for path. Uses backup semantics to bypass access control checks (requires
    ///     SeBackup/SeRestore). For reparse points, reads the reparse point itself, not its target.</summary>
    public static unsafe FILE_BASIC_INFO GetTimestampsAndAttributes(string path)
    {
        using var handle = WinAPI.CreateFile(path, (uint)FILE_ACCESS_RIGHTS.FILE_READ_ATTRIBUTES, FileShareAll, null, FileDispExisting, Semantics, null);
        FILE_BASIC_INFO info;
        if (!PInvoke.GetFileInformationByHandleEx(handle, FILE_INFO_BY_HANDLE_CLASS.FileBasicInfo, &info, (uint)Marshal.SizeOf<FILE_BASIC_INFO>()))
            throw new Win32Exception();
        return info;
    }

    /// <summary>
    ///     Sets timestamps and attributes for path. Uses backup semantics to bypass access control checks (requires
    ///     SeBackup/SeRestore). For reparse points, updates the reparse point itself, not its target.</summary>
    public static unsafe void SetTimestampsAndAttributes(string path, FILE_BASIC_INFO info)
    {
        using var handle = WinAPI.CreateFile(path, (uint)FILE_ACCESS_RIGHTS.FILE_WRITE_ATTRIBUTES, FileShareAll, null, FileDispExisting, Semantics, null);
        if (!PInvoke.SetFileInformationByHandle(handle, FILE_INFO_BY_HANDLE_CLASS.FileBasicInfo, &info, (uint)Marshal.SizeOf<FILE_BASIC_INFO>()))
            throw new Win32Exception();
    }

    /// <summary>
    ///     Raw delete of a file or an empty directory. Uses backup semantics to bypass access control checks (requires
    ///     SeBackup/SeRestore). For junctions and symlinks, deletes the junction/symlink - not the target, and not just the
    ///     reparse data. It does not matter whether the link target is valid. Deletes read-only entries. Does not delete
    ///     non-empty directories (throws).</summary>
    public static unsafe void Delete(string path)
    {
        using var handle = WinAPI.CreateFile(path, (uint)FILE_ACCESS_RIGHTS.DELETE, FileShareAll, null, FileDispExisting, Semantics, null);
        FILE_DISPOSITION_INFORMATION_EX info;
        info.Flags = FILE_DISPOSITION_INFORMATION_EX_FLAGS.FILE_DISPOSITION_DELETE | FILE_DISPOSITION_INFORMATION_EX_FLAGS.FILE_DISPOSITION_IGNORE_READONLY_ATTRIBUTE;
        if (!PInvoke.SetFileInformationByHandle(handle, FILE_INFO_BY_HANDLE_CLASS.FileDispositionInfoEx, &info, (uint)Marshal.SizeOf<FILE_DISPOSITION_INFORMATION_EX>()))
            throw new Win32Exception();
    }

    /// <summary>
    ///     Renames the file or direcotry. Uses backup semantics to bypass access control checks (requires
    ///     SeBackup/SeRestore). For reparse points, renames the reparse point itself, not its target.</summary>
    /// <param name="overwrite">
    ///     If true, will overwrite an existing file at the target path (throws otherwise). If the target is a directory, an
    ///     overwrite attempt fails with "access denied". A directory rename can "overwrite" a file, deleting the file. The
    ///     overwrite employs backup semantics too, and successfully bypasses access control checks (requires
    ///     SeBackup/SeRestore).</param>
    public static unsafe void Rename(string path, string newpath, bool overwrite = false)
    {
        using var handle = WinAPI.CreateFile(path, (uint)FILE_ACCESS_RIGHTS.DELETE, FileShareAll, null, FileDispExisting, Semantics, null);
        int bufbytes = FILE_RENAME_INFO.SizeOf(newpath.Length + 1); // including null terminator (though this SizeOf over-estimates size due to alignment)
        byte* buf = stackalloc byte[bufbytes];
        FILE_RENAME_INFO* info = (FILE_RENAME_INFO*)buf;
        info->Anonymous.ReplaceIfExists = overwrite;
        info->RootDirectory = HANDLE.Null;
        info->FileNameLength = (uint)(newpath.Length * 2); // in bytes, excluding null terminator (though the API appears to ignore this and relies on the null terminator)
        var tgtSpan = info->FileName.AsSpan(newpath.Length + 1); // including null terminator
        newpath.AsSpan().CopyTo(tgtSpan);
        tgtSpan[newpath.Length] = '\0';
        if (!PInvoke.SetFileInformationByHandle(handle, FILE_INFO_BY_HANDLE_CLASS.FileRenameInfo, info, (uint)bufbytes))
            throw new Win32Exception();
    }

    /// <summary>
    ///     Copies a file from source to destination. Uses backup semantics to bypass access control checks (requires
    ///     SeBackup/SeRestore).</summary>
    /// <remarks>
    ///     Does not copy sparse or compressed status; file attributes/times; alt data streams.</remarks>
    public static unsafe void CopyFile(string source, string destination, Action<CopyFileProgress> progress = null)
    {
        var semantics = FILE_FLAGS_AND_ATTRIBUTES.FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAGS_AND_ATTRIBUTES.FILE_FLAG_SEQUENTIAL_SCAN;
        using var srcH = WinAPI.CreateFile(source, (uint)GENERIC_ACCESS_RIGHTS.GENERIC_READ, FileShareAll, null, FileDispExisting, semantics, null);
        using var dstH = WinAPI.CreateFile(destination, (uint)GENERIC_ACCESS_RIGHTS.GENERIC_WRITE, FileShareAll, null, FileDispNew, semantics, null);
        if (!PInvoke.GetFileSizeEx(srcH, out var filesize))
            throw new Win32Exception();

        var buffer = new byte[128 * 1024];
        long copied = 0;
        while (true)
        {
            progress?.Invoke(new CopyFileProgress() { TotalBytes = filesize, CopiedBytes = copied }); // gets called with copied = 0 as well as copied = filesize
            uint bytesRead;
            if (!PInvoke.ReadFile(srcH, buffer, &bytesRead, null))
            {
                var err = WinAPI.GetLastError();
                if (err == WIN32_ERROR.ERROR_HANDLE_EOF)
                    break;
                throw new Win32Exception();
            }
            if (bytesRead == 0)
                break;
            uint bytesWritten;
            if (!PInvoke.WriteFile(dstH, buffer.AsSpan()[..(int)bytesRead], &bytesWritten, null))
                throw new Win32Exception();
            if (bytesWritten != bytesRead)
                throw new Exception("WriteFile did not write the requested number of bytes"); // should never happen
            copied += bytesWritten;
        }
    }

    public struct CopyFileProgress
    {
        public long TotalBytes;
        public long CopiedBytes;
    }
}
