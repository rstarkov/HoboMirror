using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Text.RegularExpressions;
using Microsoft.Win32.SafeHandles;
using Windows.Win32;
using Windows.Win32.Foundation;
using Windows.Win32.Security;
using Windows.Win32.Storage.FileSystem;

namespace HoboMirror;

/// <summary>
///     File system methods. All methods use backup semantics to bypass access control checks (requires SeBackup/SeRestore),
///     and support long file paths.</summary>
static class Filesys
{
    private const FILE_SHARE_MODE FileShareAll = FILE_SHARE_MODE.FILE_SHARE_READ | FILE_SHARE_MODE.FILE_SHARE_WRITE | FILE_SHARE_MODE.FILE_SHARE_DELETE;
    private const FILE_CREATION_DISPOSITION FileDispExisting = FILE_CREATION_DISPOSITION.OPEN_EXISTING;
    private const FILE_CREATION_DISPOSITION FileDispNew = FILE_CREATION_DISPOSITION.CREATE_NEW;
    private const FILE_FLAGS_AND_ATTRIBUTES Semantics = FILE_FLAGS_AND_ATTRIBUTES.FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAGS_AND_ATTRIBUTES.FILE_FLAG_OPEN_REPARSE_POINT;

    /// <summary>Calls PInvoke.CreateFile. Handles long paths, and ensures the handle is valid or throws.</summary>
    private static unsafe SafeFileHandle create(string lpFileName, uint dwDesiredAccess, SECURITY_ATTRIBUTES? lpSecurityAttributes, FILE_CREATION_DISPOSITION dwCreationDisposition, FILE_FLAGS_AND_ATTRIBUTES dwFlagsAndAttributes)
    {
        var handle = PInvoke.CreateFile(LongPath(lpFileName), dwDesiredAccess, FileShareAll, lpSecurityAttributes, dwCreationDisposition, dwFlagsAndAttributes, null);
        if (handle.IsInvalid)
            throw new Win32Exception();
        return handle;
    }

    /// <summary>
    ///     Disables path processing by prefixing with \\?\. This enables long file paths on older systems without the global
    ///     enable, and also allows weird paths such as "foo." or "foo " to be mirrored as-is.</summary>
    private static string LongPath(string path)
    {
        if (!path.StartsWith(@"\"))
            path = @"\\?\" + path;
        return path;
    }

    public static SafeFileHandle OpenHandle(string path, uint dwDesiredAccess)
    {
        return create(path, dwDesiredAccess, null, FileDispExisting, Semantics);
    }

    /// <summary>
    ///     Gets timestamps and attributes for path. Uses backup semantics to bypass access control checks (requires
    ///     SeBackup/SeRestore). For reparse points, reads the reparse point itself, not its target.</summary>
    public static FILE_BASIC_INFO GetTimestampsAndAttributes(string path)
    {
        using var handle = create(path, (uint)FILE_ACCESS_RIGHTS.FILE_READ_ATTRIBUTES, null, FileDispExisting, Semantics);
        return GetTimestampsAndAttributes(handle);
    }
    /// <summary>
    ///     Gets timestamps and attributes for path. Uses backup semantics to bypass access control checks (requires
    ///     SeBackup/SeRestore). For reparse points, reads the reparse point itself, not its target.</summary>
    public static unsafe FILE_BASIC_INFO GetTimestampsAndAttributes(SafeFileHandle handle)
    {
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
        using var handle = create(path, (uint)FILE_ACCESS_RIGHTS.FILE_WRITE_ATTRIBUTES, null, FileDispExisting, Semantics);
        SetTimestampsAndAttributes(handle, info);
    }
    /// <summary>
    ///     Sets timestamps and attributes for path. Uses backup semantics to bypass access control checks (requires
    ///     SeBackup/SeRestore). For reparse points, updates the reparse point itself, not its target.</summary>
    public static unsafe void SetTimestampsAndAttributes(SafeFileHandle handle, FILE_BASIC_INFO info)
    {
        if (!PInvoke.SetFileInformationByHandle(handle, FILE_INFO_BY_HANDLE_CLASS.FileBasicInfo, &info, (uint)Marshal.SizeOf<FILE_BASIC_INFO>()))
            throw new Win32Exception();
    }

    public static long GetFileLength(string path)
    {
        using var handle = create(path, (uint)FILE_ACCESS_RIGHTS.FILE_READ_ATTRIBUTES, null, FileDispExisting, Semantics);
        return GetFileLength(handle);
    }
    public static unsafe long GetFileLength(SafeFileHandle handle)
    {
        if (!PInvoke.GetFileSizeEx(handle, out var lpFileSize))
            throw new Win32Exception();
        return lpFileSize;
    }

    /// <summary>
    ///     Raw delete of a file or an empty directory. Uses backup semantics to bypass access control checks (requires
    ///     SeBackup/SeRestore). For junctions and symlinks, deletes the junction/symlink - not the target, and not just the
    ///     reparse data. It does not matter whether the link target is valid. Deletes read-only entries. Does not delete
    ///     non-empty directories (throws).</summary>
    public static unsafe void Delete(string path)
    {
        using var handle = create(path, (uint)FILE_ACCESS_RIGHTS.DELETE, null, FileDispExisting, Semantics);
        FILE_DISPOSITION_INFO_EX info;
        info.Flags = FILE_DISPOSITION_INFO_EX_FLAGS.FILE_DISPOSITION_FLAG_DELETE | FILE_DISPOSITION_INFO_EX_FLAGS.FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE;
        if (!PInvoke.SetFileInformationByHandle(handle, FILE_INFO_BY_HANDLE_CLASS.FileDispositionInfoEx, &info, (uint)Marshal.SizeOf<FILE_DISPOSITION_INFO_EX>()))
            throw new Win32Exception();
    }

    /// <summary>
    ///     Renames the file or directory. Uses backup semantics to bypass access control checks (requires
    ///     SeBackup/SeRestore). For reparse points, renames the reparse point itself, not its target.</summary>
    /// <param name="overwrite">
    ///     If true, will overwrite an existing file at the target path (throws otherwise). If the target is a directory, an
    ///     overwrite attempt fails with "access denied". A directory rename can "overwrite" a file, deleting the file. The
    ///     overwrite employs backup semantics too, and successfully bypasses access control checks (requires
    ///     SeBackup/SeRestore). Read-only flag is ignored on overwrite.</param>
    public static unsafe void Rename(string path, string newpath, bool overwrite = false)
    {
        using var handle = create(path, (uint)FILE_ACCESS_RIGHTS.DELETE, null, FileDispExisting, Semantics);
        newpath = LongPath(newpath);
        int bufbytes = FILE_RENAME_INFO.SizeOf(newpath.Length + 1); // including null terminator (though this SizeOf over-estimates size due to alignment)
        byte* buf = stackalloc byte[bufbytes];
        FILE_RENAME_INFO* info = (FILE_RENAME_INFO*)buf;
        info->Anonymous.Flags = overwrite ? 0x41u /*FILE_RENAME_REPLACE_IF_EXISTS | FILE_RENAME_IGNORE_READONLY_ATTRIBUTE */ : 0u;
        info->RootDirectory = HANDLE.Null;
        info->FileNameLength = (uint)(newpath.Length * 2); // in bytes, excluding null terminator (though the API appears to ignore this and relies on the null terminator)
        var tgtSpan = info->FileName.AsSpan(newpath.Length + 1); // including null terminator
        newpath.AsSpan().CopyTo(tgtSpan);
        tgtSpan[newpath.Length] = '\0';
        if (!PInvoke.SetFileInformationByHandle(handle, FILE_INFO_BY_HANDLE_CLASS.FileRenameInfoEx, info, (uint)bufbytes))
            throw new Win32Exception();
    }

    /// <summary>
    ///     Copies a file from source to destination. Uses backup semantics to bypass access control checks (requires
    ///     SeBackup/SeRestore).</summary>
    /// <remarks>
    ///     Copies timestamps and basic attrs. Does not copy sparse or compressed status; alt data streams.</remarks>
    public static unsafe void CopyFile(string source, string destination, Action<CopyFileProgress> progress = null)
    {
        var semantics = FILE_FLAGS_AND_ATTRIBUTES.FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAGS_AND_ATTRIBUTES.FILE_FLAG_SEQUENTIAL_SCAN;
        using var srcH = create(source, (uint)GENERIC_ACCESS_RIGHTS.GENERIC_READ, null, FileDispExisting, semantics);
        using var dstH = create(destination, (uint)GENERIC_ACCESS_RIGHTS.GENERIC_WRITE, null, FileDispNew, semantics);
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

        SetTimestampsAndAttributes(dstH, GetTimestampsAndAttributes(srcH));
    }

    public struct CopyFileProgress
    {
        public long TotalBytes;
        public long CopiedBytes;
    }

    /// <summary>Gets file security info (owner, ACLs, inheritability) in binary form. Uses backup semantics.</summary>
    public static byte[] GetSecurityInfoFile(string path)
    {
        return new FileInfo(LongPath(path)).GetAccessControl(AccessControlSections.All).GetSecurityDescriptorBinaryForm();
    }
    /// <summary>Gets directory security info (owner, ACLs, inheritability) in binary form. Uses backup semantics.</summary>
    public static byte[] GetSecurityInfoDir(string path)
    {
        return new DirectoryInfo(LongPath(path)).GetAccessControl(AccessControlSections.All).GetSecurityDescriptorBinaryForm();
    }
    /// <summary>Sets file security info (owner, ACLs, inheritability). Uses backup semantics.</summary>
    public static void SetSecurityInfoFile(string path, byte[] fileSecurity)
    {
        var sec = new FileSecurity(); // per docs, must construct a new object otherwise nothing gets applied
        sec.SetSecurityDescriptorBinaryForm(fileSecurity);
        new FileInfo(LongPath(path)).SetAccessControl(sec);
    }
    /// <summary>
    ///     Sets directory security info (owner, ACLs, inheritability). Uses backup semantics. Appears to apply inheriable
    ///     ACLs recursively (todo).</summary>
    public static void SetSecurityInfoDir(string path, byte[] fileSecurity)
    {
        var sec = new DirectorySecurity(); // per docs, must construct a new object otherwise nothing gets applied
        sec.SetSecurityDescriptorBinaryForm(fileSecurity);
        new DirectoryInfo(LongPath(path)).SetAccessControl(sec);
    }

    /// <summary>Creates a new empty file at the specified path. Throws if the path already exists.</summary>
    public static void CreateFile(string path)
    {
        using var handle = create(path, (uint)FILE_ACCESS_RIGHTS.FILE_GENERIC_WRITE, null, FileDispNew, Semantics);
    }

    /// <summary>Creates a new empty directory at the specified path. Throws if the path already exists.</summary>
    public static void CreateDirectory(string path)
    {
        Directory.CreateDirectory(LongPath(path)); // verified to use backup semantics, i.e. ignoring ACLs if SeRestorePrivilege is enabled
    }

    /// <summary>Lists paths contained inside the specified directory. Returns full paths.</summary>
    public static string[] ListDirectory(string path)
    {
        var rootPath = untrimRootPath(path);
        if (rootPath == path)
            return Directory.GetFileSystemEntries(LongPath(path)); // verified to use backup semantics, i.e. ignoring ACLs if SeBackupPrivilege is enabled
        // we must also fixup the paths returned as they now have an extra slash
        return Directory.GetFileSystemEntries(rootPath)
            .Select(p => Path.Combine(path, p[rootPath.Length..]))
            .ToArray();
    }

    /// <summary>
    ///     Workaround for https://github.com/dotnet/runtime/issues/119009</summary>
    /// <remarks>
    ///     Adds one or two backslashes if the path is a shadow copy root path. Doesn't change any other paths.</remarks>
    private static string untrimRootPath(string path)
    {
        const string vssPrefix = @"\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy";
        if (path.StartsWith(vssPrefix, StringComparison.OrdinalIgnoreCase))
        {
            var match = Regex.Match(path[vssPrefix.Length..], @"^[^\\]*(\\)?$");
            if (match.Success)
                return path + (match.Groups[1].Success ? @"\" : @"\\");
        }
        return path;
    }
}
