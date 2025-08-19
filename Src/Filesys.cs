using System.ComponentModel;
using System.Runtime.InteropServices;
using Windows.Wdk.Storage.FileSystem;
using Windows.Win32;
using Windows.Win32.Storage.FileSystem;

namespace HoboMirror;

static class Filesys
{
    private const FILE_SHARE_MODE FileShareAll = FILE_SHARE_MODE.FILE_SHARE_READ | FILE_SHARE_MODE.FILE_SHARE_WRITE | FILE_SHARE_MODE.FILE_SHARE_DELETE;
    private const FILE_CREATION_DISPOSITION FileDispExisting = FILE_CREATION_DISPOSITION.OPEN_EXISTING;
    private const FILE_FLAGS_AND_ATTRIBUTES Semantics = FILE_FLAGS_AND_ATTRIBUTES.FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAGS_AND_ATTRIBUTES.FILE_FLAG_OPEN_REPARSE_POINT;

    /// <summary>
    ///     Gets timestamps and attributes for path. Uses backup semantics to bypass access control checks (requires
    ///     SeBackup/SeRestore). For reparse points, reads the reparse point itself, not its target.</summary>
    public static unsafe FILE_BASIC_INFO GetTimestampsAndAttributes(string path)
    {
        using var handle = PInvoke.CreateFile(path, (uint)FILE_ACCESS_RIGHTS.FILE_READ_ATTRIBUTES, FileShareAll, null, FileDispExisting, Semantics, null);
        if (WinAPI.GetLastError() != 0) throw new Win32Exception();
        FILE_BASIC_INFO info;
        PInvoke.GetFileInformationByHandleEx(handle, FILE_INFO_BY_HANDLE_CLASS.FileBasicInfo, &info, (uint)Marshal.SizeOf<FILE_BASIC_INFO>());
        if (WinAPI.GetLastError() != 0) throw new Win32Exception();
        return info;
    }

    /// <summary>
    ///     Sets timestamps and attributes for path. Uses backup semantics to bypass access control checks (requires
    ///     SeBackup/SeRestore). For reparse points, updates the reparse point itself, not its target.</summary>
    public static unsafe void SetTimestampsAndAttributes(string path, FILE_BASIC_INFO info)
    {
        using var handle = PInvoke.CreateFile(path, (uint)FILE_ACCESS_RIGHTS.FILE_WRITE_ATTRIBUTES, FileShareAll, null, FileDispExisting, Semantics, null);
        if (WinAPI.GetLastError() != 0) throw new Win32Exception();
        PInvoke.SetFileInformationByHandle(handle, FILE_INFO_BY_HANDLE_CLASS.FileBasicInfo, &info, (uint)Marshal.SizeOf<FILE_BASIC_INFO>());
        if (WinAPI.GetLastError() != 0) throw new Win32Exception();
    }

    /// <summary>
    ///     Raw delete of a file or an empty directory. Uses backup semantics to bypass access control checks (requires
    ///     SeBackup/SeRestore). For junctions and symlinks, deletes the junction/symlink - not the target, and not just the
    ///     reparse data. It does not matter whether the link target is valid. Deletes read-only entries. Does not delete
    ///     non-empty directories (throws).</summary>
    public static unsafe void Delete(string path)
    {
        using var handle = PInvoke.CreateFile(path, (uint)FILE_ACCESS_RIGHTS.DELETE, FileShareAll, null, FileDispExisting, Semantics, null);
        if (WinAPI.GetLastError() != 0) throw new Win32Exception();

        FILE_DISPOSITION_INFORMATION_EX info;
        info.Flags = FILE_DISPOSITION_INFORMATION_EX_FLAGS.FILE_DISPOSITION_DELETE | FILE_DISPOSITION_INFORMATION_EX_FLAGS.FILE_DISPOSITION_IGNORE_READONLY_ATTRIBUTE;
        PInvoke.SetFileInformationByHandle(handle, FILE_INFO_BY_HANDLE_CLASS.FileDispositionInfoEx, &info, (uint)Marshal.SizeOf<FILE_DISPOSITION_INFORMATION_EX>());
        if (WinAPI.GetLastError() != 0) throw new Win32Exception();
    }

    /// <summary>Does NOT use backup semantics (todo).</summary>
    public static unsafe void CopyFile(string source, string destination, Func<COPYFILE2_MESSAGE, COPYFILE2_MESSAGE_ACTION> progress = null)
    {
        COPYFILE2_EXTENDED_PARAMETERS param = new();
        param.dwSize = (uint)Marshal.SizeOf<COPYFILE2_EXTENDED_PARAMETERS>();
        param.pProgressRoutine = (msg, _) => progress(*msg);
        PInvoke.CopyFile2(source, destination, param);
        if (WinAPI.GetLastError() != 0) throw new Win32Exception();
    }
}
