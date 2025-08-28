using System.ComponentModel;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using RT.Util.ExtensionMethods;
using Windows.Win32;
using Windows.Win32.Foundation;
using Windows.Win32.Security;
using Windows.Win32.Storage.FileSystem;

namespace HoboMirror;

static class WinAPI
{
    /// <summary>
    ///     Enables or disables the specified privilege on the primary access token of the current process. Throws if the
    ///     token is not assigned the specified privilege at all, in which case it cannot be enabled or disabled.</summary>
    /// <param name="privilege">
    ///     Privilege to enable or disable.</param>
    /// <param name="enable">
    ///     True to enable the privilege, false to disable it.</param>
    /// <returns>
    ///     True if the privilege was enabled prior to the change, false if it was disabled.</returns>
    public static unsafe bool ModifyPrivilege(PrivilegeName privilege, bool enable)
    {
        LUID luid;
        if (!PInvoke.LookupPrivilegeValue(null, privilege.ToString(), out luid))
            throw new Win32Exception();

        SafeFileHandle hToken;
        if (!PInvoke.OpenProcessToken(PInvoke.GetCurrentProcess_SafeHandle(), TOKEN_ACCESS_MASK.TOKEN_ADJUST_PRIVILEGES | TOKEN_ACCESS_MASK.TOKEN_QUERY, out hToken))
            throw new Win32Exception();
        using (hToken)
        {
            var newPriv = new TOKEN_PRIVILEGES();
            newPriv.PrivilegeCount = 1;
            newPriv.Privileges[0].Luid = luid;
            newPriv.Privileges[0].Attributes = enable ? TOKEN_PRIVILEGES_ATTRIBUTES.SE_PRIVILEGE_ENABLED : 0;

            var prevPriv = new TOKEN_PRIVILEGES();
            prevPriv.PrivilegeCount = 1;
            uint returnedBytes;

            if (!PInvoke.AdjustTokenPrivileges(hToken, false, &newPriv, (uint)Marshal.SizeOf(prevPriv), &prevPriv, &returnedBytes))
                throw new Win32Exception();
            if (WinAPI.GetLastError() != 0)
                throw new Win32Exception(); // probably means process needs elevation or user does not have the privilege

            return prevPriv.PrivilegeCount == 0 ? enable /* didn't make a change */ : ((prevPriv.Privileges[0].Attributes & TOKEN_PRIVILEGES_ATTRIBUTES.SE_PRIVILEGE_ENABLED) != 0);
        }
    }

    public static WIN32_ERROR GetLastError() => (WIN32_ERROR)Marshal.GetLastWin32Error();

    /// <summary>
    ///     Returns all volume mount paths for the given volume GUID path. Note that there may be none, in which case the
    ///     volume is only accessible via its volume GUID path. Throws for mount paths (must be a GUID path).</summary>
    public static unsafe List<string> GetVolumeMountPaths(string volumeGuidPath)
    {
        const int bufchars = 4096;
        char* buf = stackalloc char[bufchars];

        if (!PInvoke.GetVolumePathNamesForVolumeName(volumeGuidPath, buf, bufchars, out var returnLen))
            throw new Win32Exception();

        var names = new List<string>();

        for (int i = 0; i < returnLen;)
        {
            var str = Ptr.SpanFromNullStr(buf + i);
            if (str.Length == 0) break; // double terminator for lists
            i += str.Length + 1; // incl null terminator
            names.Add(str.ToString());
        }

        return names;
    }

    /// <summary>Returns the shortest volume mount path for the given volume GUID path, or null if there are no mount paths.</summary>
    public static string GetVolumeMountPath(string volumeGuidPath)
    {
        var paths = GetVolumeMountPaths(volumeGuidPath);
        if (paths.Count == 0)
            return null;
        return paths.MinElement(p => p.Length);
    }

    /// <summary>
    ///     Gets a volume mount point path for the given path. Works for non-existent paths so long as a parent path is a
    ///     valid volume. Don't pass relative paths. Don't use on SUBST drives as the results are incorrect. Works for C:\ and
    ///     C:. If mounted at C:\Mnt\Foo, passing "C:\Mnt\Foo" without the trailing slash returns "C:\Mnt\Foo\", not C:</summary>
    public static unsafe string GetVolumeForPath(string path)
    {
        Span<char> result = stackalloc char[4096];
        if (!PInvoke.GetVolumePathName(path, result))
            throw new Win32Exception();
        return Ptr.SpanFromNullStr(result).ToString();
    }

    /// <summary>Calls PInvoke.CreateFile. Handles long paths, and ensures the handle is valid or throws.</summary>
    public static unsafe SafeFileHandle CreateFile(string lpFileName, uint dwDesiredAccess, FILE_SHARE_MODE dwShareMode, SECURITY_ATTRIBUTES? lpSecurityAttributes, FILE_CREATION_DISPOSITION dwCreationDisposition, FILE_FLAGS_AND_ATTRIBUTES dwFlagsAndAttributes, SafeHandle hTemplateFile)
    {
        var handle = PInvoke.CreateFile(LongPath(lpFileName), dwDesiredAccess, dwShareMode, lpSecurityAttributes, dwCreationDisposition, dwFlagsAndAttributes, hTemplateFile);
        if (handle.IsInvalid)
            throw new Win32Exception();
        return handle;
    }

    public static string LongPath(string path)
    {
        if (path.Length >= 260 && !path.StartsWith(@"\"))
            path = @"\\?\" + Path.GetFullPath(path);
        return path;
    }
}

enum PrivilegeName
{
    SeAssignPrimaryTokenPrivilege,
    SeAuditPrivilege,
    SeBackupPrivilege,
    SeChangeNotifyPrivilege,
    SeCreateGlobalPrivilege,
    SeCreatePagefilePrivilege,
    SeCreatePermanentPrivilege,
    SeCreateSymbolicLinkPrivilege,
    SeCreateTokenPrivilege,
    SeDebugPrivilege,
    SeEnableDelegationPrivilege,
    SeImpersonatePrivilege,
    SeIncreaseBasePriorityPrivilege,
    SeIncreaseQuotaPrivilege,
    SeIncreaseWorkingSetPrivilege,
    SeLoadDriverPrivilege,
    SeLockMemoryPrivilege,
    SeMachineAccountPrivilege,
    SeManageVolumePrivilege,
    SeProfileSingleProcessPrivilege,
    SeRelabelPrivilege,
    SeRemoteShutdownPrivilege,
    SeRestorePrivilege,
    SeSecurityPrivilege,
    SeShutdownPrivilege,
    SeSyncAgentPrivilege,
    SeSystemEnvironmentPrivilege,
    SeSystemProfilePrivilege,
    SeSystemtimePrivilege,
    SeTakeOwnershipPrivilege,
    SeTcbPrivilege,
    SeTimeZonePrivilege,
    SeTrustedCredManAccessPrivilege,
    SeUndockPrivilege,
    SeUnsolicitedInputPrivilege,
}
