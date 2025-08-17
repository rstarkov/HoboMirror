using System.ComponentModel;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using Windows.Win32;
using Windows.Win32.Foundation;
using Windows.Win32.Security;

namespace HoboMirror
{
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
                if (Marshal.GetLastWin32Error() != 0)
                    throw new Win32Exception(); // probably means process needs elevation or user does not have the privilege

                return prevPriv.PrivilegeCount == 0 ? enable /* didn't make a change */ : ((prevPriv.Privileges[0].Attributes & TOKEN_PRIVILEGES_ATTRIBUTES.SE_PRIVILEGE_ENABLED) != 0);
            }
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
}
