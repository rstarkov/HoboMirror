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
using RT.Util.ExtensionMethods;

namespace HoboMirror
{
    class VolumeShadowCopyVol { public string Path; public string UniquePath; public string DisplayPath; public string SnapshotPath; }

    class VolumeShadowCopy : IDisposable
    {
        private IVssBackupComponents bkpComponents;
        private bool _needsCleanup = false;
        private List<VssWriterDescriptor> writers;

        public ReadOnlyDictionary<string, VolumeShadowCopyVol> Volumes { get; private set; }

        public VolumeShadowCopy(IEnumerable<string> volumes)
        {
            try
            {
                initialize(volumes);
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        public VolumeShadowCopy(params string[] volumes)
            : this(volumes.AsEnumerable())
        {
        }

        public void Dispose()
        {
            if (bkpComponents != null)
            {
                if (_needsCleanup)
                {
                    Console.WriteLine("Deleting the snapshot / cleaning up...");
                    bkpComponents.BackupComplete();
                    EnsureNoWritersFailed(bkpComponents, writers);
                    _needsCleanup = false;
                    Console.WriteLine("   done.");
                }
                bkpComponents.Dispose();
                bkpComponents = null;
            }
        }

        private void initialize(IEnumerable<string> paths)
        {
            var vss = VssUtils.LoadImplementation();

            Volumes = new ReadOnlyDictionary<string,VolumeShadowCopyVol>(paths.ToDictionary(k => k, k => new VolumeShadowCopyVol { Path = k }));
            foreach (var vol in Volumes.Values)
            {
                if (!Volume.IsVolume(vol.Path))
                    throw new ArgumentException(String.Format("{0} is not a valid volume.", vol.Path), nameof(paths));
                vol.UniquePath = Volume.GetUniqueVolumeNameForPath(vol.Path) ?? vol.Path;
                vol.DisplayPath = GetDisplayNameForVolume(vol.UniquePath);
            }

            var context = VssSnapshotContext.Backup;
            var contextAttr = (VssVolumeSnapshotAttributes) context;

            bkpComponents = vss.CreateVssBackupComponents();

            bkpComponents.InitializeForBackup(null);
            if (context != VssSnapshotContext.Backup)
                bkpComponents.SetContext(context);
            bkpComponents.SetBackupState(true, true, VssBackupType.Full, false);

            Console.WriteLine("Gathering writer metadata...");
            using (var result = bkpComponents.BeginGatherWriterMetadata(null, null))
                result.AsyncWaitHandle.WaitOne();

            writers = new List<VssWriterDescriptor>(bkpComponents.WriterMetadata.Select(wm => new VssWriterDescriptor(wm)));

            SelectComponentsForBackup(Volumes.Values.Select(v => v.Path), bkpComponents, writers);

            var snapshotSetId = bkpComponents.StartSnapshotSet();
            foreach (var vol in Volumes.Values)
            {
                Console.WriteLine("Adding volume {0} [aka {1}, unique {2}]...", vol.Path, vol.DisplayPath, vol.UniquePath);
                bkpComponents.AddToSnapshotSet(vol.UniquePath);
            }

            bkpComponents.PrepareForBackup();
            EnsureNoWritersFailed(bkpComponents, writers);
            Console.WriteLine("Creating the shadow copy...");
            bkpComponents.DoSnapshotSet();
            _needsCleanup = true;

            EnsureNoWritersFailed(bkpComponents, writers);
            Console.WriteLine("   done.");

            var snapshots = bkpComponents.QuerySnapshots();
            foreach (var vol in Volumes.Values)
            {
                var snap = snapshots.Where(s => string.Equals(s.OriginalVolumeName, vol.UniquePath, StringComparison.OrdinalIgnoreCase)).MaxElement(s => s.CreationTimestamp);
                vol.SnapshotPath = snap.SnapshotDeviceObject;
                Console.WriteLine("Volume {0} [aka {1}, unique {2}] snapshot UNC: {3}", vol.Path, vol.DisplayPath, vol.UniquePath, vol.SnapshotPath);
            }
        }

        private static void EnsureNoWritersFailed(IVssBackupComponents bkpComponents, List<VssWriterDescriptor> writers)
        {
            bkpComponents.GatherWriterStatus();
            foreach (var writer in bkpComponents.WriterStatus)
            {
                if (!writers.Any(wr => wr.WriterMetadata.InstanceId == writer.InstanceId && !wr.IsExcluded))
                    continue;

                switch (writer.State)
                {
                    case VssWriterState.FailedAtIdentify:
                    case VssWriterState.FailedAtPrepareBackup:
                    case VssWriterState.FailedAtPrepareSnapshot:
                    case VssWriterState.FailedAtFreeze:
                    case VssWriterState.FailedAtThaw:
                    case VssWriterState.FailedAtPostSnapshot:
                    case VssWriterState.FailedAtBackupComplete:
                    case VssWriterState.FailedAtPreRestore:
                    case VssWriterState.FailedAtPostRestore:
                    case VssWriterState.FailedAtBackupShutdown:
                        break;
                    default:
                        continue;
                }

                Console.WriteLine("Selected writer '{0}' is in failed state!", writer.Name);
                Console.WriteLine("Status: " + writer.State);
                Console.WriteLine("Writer Failure Code: " + writer.Failure);
                Console.WriteLine("Writer ID: " + writer.ClassId);
                Console.WriteLine("Instance ID: " + writer.InstanceId);
                throw new Exception();
            }
        }

        private static void SelectComponentsForBackup(IEnumerable<string> shadowSourceVolumes, IVssBackupComponents bkpComponents, List<VssWriterDescriptor> writers)
        {
            DiscoverNonShadowedExcludedComponents(shadowSourceVolumes, writers);
            DiscoverAllExcludedComponents(writers);
            DiscoverExcludedWriters(writers);
            DiscoverExplicitelyIncludedComponents(writers);
            SelectExplicitelyIncludedComponents(bkpComponents, writers);
        }

        private static void DiscoverNonShadowedExcludedComponents(IEnumerable<string> shadowSourceVolumes, List<VssWriterDescriptor> writers)
        {
            Console.WriteLine("Discover components that reside outside the shadow set...");

            // Discover components that should be excluded from the shadow set 
            // This means components that have at least one File Descriptor requiring 
            // volumes not in the shadow set. 
            foreach (VssWriterDescriptor writer in writers)
            {
                if (writer.IsExcluded)
                    continue;

                // Check if the component is excluded
                foreach (var component in writer.ComponentDescriptors)
                {
                    // Check to see if this component is explicitly excluded
                    if (component.IsExcluded)
                        continue;

                    // Try to find an affected volume outside the shadow set
                    // If yes, exclude the component
                    for (int i = 0; i < component.AffectedVolumes.Count; i++)
                    {
                        if (NativeMethods.ClusterIsPathOnSharedVolume(component.AffectedVolumes[i]))
                        {
                            component.AffectedVolumes[i] = ClusterGetVolumeNameForVolumeMountPoint(component.AffectedVolumes[i]);
                        }

                        if (!shadowSourceVolumes.Contains(component.AffectedVolumes[i]))
                        {
                            string localVolume;
                            try
                            {
                                localVolume = GetDisplayNameForVolume(component.AffectedVolumes[i]);
                            }
                            catch
                            {
                                localVolume = null;
                            }

                            if (localVolume != null)
                            {
                                Console.WriteLine("- Component '{0}' from writer '{1}' is excluded from backup (it requires {2} in the shadow set)",
                                   component.FullPath, writer.WriterMetadata.WriterName, localVolume);
                            }
                            else
                            {
                                Console.WriteLine("- Component '{0}' from writer '{1}' is excluded from backup.", component.FullPath, writer.WriterMetadata.WriterName);
                            }
                            component.IsExcluded = true;
                            break;
                        }
                    }
                }
            }
        }

        private static void DiscoverAllExcludedComponents(List<VssWriterDescriptor> writers)
        {
            Console.WriteLine("Discover all excluded components ...");

            // Discover components that should be excluded from the shadow set 
            // This means components that have at least one File Descriptor requiring 
            // volumes not in the shadow set. 
            foreach (var writer in writers.Where(w => w.IsExcluded == false))
            {
                foreach (var component in writer.ComponentDescriptors)
                {
                    // Check if this component has any excluded children
                    // If yes, deselect it
                    foreach (var descendent in writer.ComponentDescriptors)
                    {
                        if (component.IsAncestorOf(descendent) && descendent.IsExcluded)
                        {
                            Console.WriteLine("- Component '{0}' from writer '{1} is excluded from backup (it has an excluded descendent: '{2}').", component.FullPath, writer.WriterMetadata.WriterName, descendent.WriterName);
                            component.IsExcluded = true;
                            break;
                        }
                    }
                }
            }
        }

        private static void DiscoverExcludedWriters(List<VssWriterDescriptor> writers)
        {
            Console.WriteLine("Discover excluded writers...");

            // Enumerate writers
            foreach (VssWriterDescriptor writer in writers.Where(w => w.IsExcluded == false))
            {
                // Discover if we have any:
                // - non-excluded selectable components 
                // - or non-excluded top-level non-selectable components
                // If we have none, then the whole writer must be excluded from the backup
                writer.IsExcluded = !writer.ComponentDescriptors.Any(comp => comp.CanBeExplicitlyIncluded);

                // No included components were found
                if (writer.IsExcluded)
                {
                    Console.WriteLine("- The writer '{0} is now entierly excluded from the backup (it does not contain any components that can be potentially included in the backup).", writer.WriterMetadata.WriterName);
                    continue;
                }

                // Now, discover if we have any top-level excluded non-selectable component 
                // If this is true, then the whole writer must be excluded from the backup
                foreach (VssComponentDescriptor component in writer.ComponentDescriptors)
                {
                    if (component.IsTopLevel && !component.IsSelectable && component.IsExcluded)
                    {
                        Console.WriteLine("- The writer '{0}' is now entierly excluded from the backup (the top-level non-selectable component '{1}' is an excluded component.",
                           writer.WriterMetadata.WriterName, component.FullPath);
                        writer.IsExcluded = true;
                        break;
                    }
                }
            }
        }

        private static void DiscoverExplicitelyIncludedComponents(List<VssWriterDescriptor> writers)
        {
            Console.WriteLine("Discover explicitly included components...");

            // Enumerate all writers
            foreach (var writer in writers.Where(w => w.IsExcluded == false))
            {
                foreach (var component in writer.ComponentDescriptors.Where(c => c.CanBeExplicitlyIncluded))
                {
                    // Test if our component has a parent that is also included
                    // If so this cannot be explicitely included since we have another ancestor that that must be (implictely or explicitely) included
                    component.IsExplicitlyIncluded = !writer.ComponentDescriptors.Any(ancestor => ancestor.IsAncestorOf(component) && ancestor.CanBeExplicitlyIncluded);
                }
            }
        }

        private static void SelectExplicitelyIncludedComponents(IVssBackupComponents bkpComponents, List<VssWriterDescriptor> writers)
        {
            Console.WriteLine("Select explicitly included components ...");

            foreach (VssWriterDescriptor writer in writers.Where(w => !w.IsExcluded))
            {
                Console.WriteLine(" * Writer '{0}':", writer.WriterMetadata.WriterName);

                // Compute the roots of included components
                foreach (var component in writer.ComponentDescriptors.Where(c => c.IsExplicitlyIncluded))
                {
                    Console.WriteLine("    - Add component {0}", component.FullPath);
                    bkpComponents.AddComponent(writer.WriterMetadata.InstanceId, writer.WriterMetadata.WriterId, component.ComponentType, component.LogicalPath, component.ComponentName);
                }
            }
        }

        private static string ClusterGetVolumeNameForVolumeMountPoint(string volumeMountPoint)
        {
            var result = new StringBuilder(NativeMethods.MAX_PATH);
            if (!NativeMethods.ClusterGetVolumeNameForVolumeMountPointW(volumeMountPoint, result, (uint) result.Capacity))
                throw new Win32Exception();
            return result.ToString();
        }

        [SecurityPermission(SecurityAction.LinkDemand, UnmanagedCode = true)]
        private static string GetDisplayNameForVolume(string volumeName)
        {
            string[] volumeMountPoints = GetVolumePathNamesForVolume(volumeName);

            if (volumeMountPoints.Length == 0)
                return null;

            string smallestMountPoint = volumeMountPoints[0];
            for (int i = 1; i < volumeMountPoints.Length; i++)
            {
                if (volumeMountPoints[i].Length < smallestMountPoint.Length)
                    smallestMountPoint = volumeMountPoints[i];
            }
            return smallestMountPoint;
        }

        [SecurityPermission(SecurityAction.LinkDemand, UnmanagedCode = true)]
        private static string[] GetVolumePathNamesForVolume(string volumeName)
        {
            if (volumeName == null)
                throw new ArgumentNullException("volumeName");

            uint requiredLength = 0;
            char[] buffer = new char[NativeMethods.MAX_PATH];

            if (!NativeMethods.GetVolumePathNamesForVolumeNameW(volumeName, buffer, (uint) buffer.Length, ref requiredLength))
            {
                int errorCode = Marshal.GetLastWin32Error();
                if (errorCode == NativeMethods.ERROR_MORE_DATA || errorCode == NativeMethods.ERROR_INSUFFICIENT_BUFFER)
                {
                    buffer = new char[requiredLength];
                    if (!NativeMethods.GetVolumePathNamesForVolumeNameW(volumeName, buffer, (uint) buffer.Length, ref requiredLength))
                        Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error());
                }
                else
                {
                    throw new Win32Exception();
                }
            }

            List<string> displayNames = new List<string>();
            StringBuilder displayName = new StringBuilder();

            for (int i = 0; i < requiredLength; i++)
            {
                if (buffer[i] == '\0')
                {
                    if (displayName.Length > 0)
                        displayNames.Add(displayName.ToString());
                    displayName.Length = 0;
                }
                else
                {
                    displayName.Append(buffer[i]);
                }
            }

            return displayNames.ToArray();
        }

        private static class NativeMethods
        {
            public const int MAX_PATH = 261;
            public const uint ERROR_INSUFFICIENT_BUFFER = 122;
            public const uint ERROR_MORE_DATA = 234;

            [DllImport("ResUtils.dll", CharSet = CharSet.Unicode, SetLastError = true)]
            [return: MarshalAs(UnmanagedType.Bool)]
            public static extern bool ClusterGetVolumeNameForVolumeMountPointW([In] string lpszVolumeMountPoint, [Out] StringBuilder lpszVolumeName, uint cchBufferLength);

            [DllImport("ResUtils.dll", CharSet = CharSet.Unicode, SetLastError = true)]
            [return: MarshalAs(UnmanagedType.Bool)]
            internal static extern bool ClusterIsPathOnSharedVolume(string lpszPathName);

            [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
            [return: MarshalAs(UnmanagedType.Bool)]
            public static extern bool GetVolumePathNamesForVolumeNameW(string lpszVolumeName, char[] lpszVolumePathNames, uint cchBuferLength, ref uint lpcchReturnLength);

        }

        private class VssWriterDescriptor : IDisposable
        {
            private List<VssComponentDescriptor> _components;

            public IVssExamineWriterMetadata WriterMetadata { get; private set; }
            public IList<VssComponentDescriptor> ComponentDescriptors { get { return _components; } }
            public bool IsExcluded { get; set; }

            public VssWriterDescriptor(IVssExamineWriterMetadata writerMetadata)
            {
                if (writerMetadata == null)
                    throw new ArgumentNullException("writerMetadata", "writerMetadata is null.");

                WriterMetadata = writerMetadata;
                _components = new List<VssComponentDescriptor>(writerMetadata.Components.Select(c => new VssComponentDescriptor(WriterMetadata.WriterName, c)));

                for (int i = 0; i < _components.Count; i++)
                {
                    _components[i].IsTopLevel = true;
                    for (int j = 0; j < _components.Count; j++)
                    {
                        if (_components[j].IsAncestorOf(_components[i]))
                            _components[i].IsTopLevel = false;
                    }
                }
            }

            public void Dispose()
            {
                if (WriterMetadata != null)
                {
                    WriterMetadata.Dispose();
                    WriterMetadata = null;
                }
            }
        }

        private class VssComponentDescriptor
        {
            private List<string> _affectedPaths;
            private List<string> _affectedVolumes;

            private static string AppendBackslash(string str)
            {
                if (str == null)
                    return "\\";
                else if (str.EndsWith("\\"))
                    return str;
                else
                    return str + "\\";
            }

            private static string GetExpandedPath(VssWMFileDescriptor fileDesc)
            {
                return AppendBackslash(Environment.ExpandEnvironmentVariables(fileDesc.Path));
            }

            private static string GetAffectedVolume(VssWMFileDescriptor fileDesc)
            {
                string expandedPath = AppendBackslash(Environment.ExpandEnvironmentVariables(fileDesc.Path));

                try
                {
                    return Volume.GetUniqueVolumeNameForPath(expandedPath);
                }
                catch
                {
                    return expandedPath;
                }
            }

            public VssComponentDescriptor(string writerName, IVssWMComponent component)
            {
                if (component == null)
                    throw new ArgumentNullException("component", "component is null.");

                WriterName = writerName;

                FullPath = AppendBackslash(component.LogicalPath) + component.ComponentName;
                if (!FullPath.StartsWith("\\"))
                    FullPath = "\\" + FullPath;

                var affectedPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var affectedVolumes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var file in component.Files)
                {
                    affectedPaths.Add(GetExpandedPath(file));
                    affectedVolumes.Add(GetAffectedVolume(file));
                }

                foreach (var file in component.DatabaseFiles)
                {
                    affectedPaths.Add(GetExpandedPath(file));
                    affectedVolumes.Add(GetAffectedVolume(file));
                }

                foreach (var file in component.DatabaseLogFiles)
                {
                    affectedPaths.Add(GetExpandedPath(file));
                    affectedVolumes.Add(GetAffectedVolume(file));
                }

                _affectedPaths = new List<string>(affectedPaths.OrderBy(path => path, StringComparer.OrdinalIgnoreCase));
                _affectedVolumes = new List<string>(affectedVolumes.OrderBy(path => path, StringComparer.OrdinalIgnoreCase));
                Caption = component.Caption;
                ComponentName = component.ComponentName;
                LogicalPath = component.LogicalPath;
                RestoreMetadata = component.RestoreMetadata;
                IsSelectable = component.Selectable;
                ComponentType = component.Type;
                NotifyOnBackupComplete = component.NotifyOnBackupComplete;

                Files = new List<VssWMFileDescriptor>(component.Files);
                DatabaseFiles = new List<VssWMFileDescriptor>(component.DatabaseFiles);
                DatabaseLogFiles = new List<VssWMFileDescriptor>(component.DatabaseLogFiles);
                Dependencies = new List<VssWMDependency>(component.Dependencies);
            }

            public bool IsAncestorOf(VssComponentDescriptor descendent)
            {
                // The child must have a longer full path
                if (descendent.FullPath.Length <= FullPath.Length)
                    return false;

                string fullPathWithBackslash = AppendBackslash(FullPath);

                return AppendBackslash(descendent.FullPath).StartsWith(AppendBackslash(FullPath), StringComparison.OrdinalIgnoreCase);
            }

            public IList<string> AffectedPaths
            {
                get
                {
                    return new ReadOnlyCollection<string>(_affectedPaths);
                }
            }

            public IList<string> AffectedVolumes
            {
                get
                {
                    return new ReadOnlyCollection<string>(_affectedVolumes);
                }
            }


            public bool CanBeExplicitlyIncluded
            {
                get
                {
                    if (IsExcluded)
                        return false;

                    if (IsSelectable)
                        return true;

                    if (IsTopLevel)
                        return true;

                    return false;
                }
            }

            public bool IsTopLevel { get; set; }
            public string FullPath { get; private set; }
            public string WriterName { get; set; }
            public bool IsExcluded { get; set; }
            public bool IsExplicitlyIncluded { get; set; }
            public string Caption { get; set; }
            public string ComponentName { get; set; }
            public string LogicalPath { get; set; }
            public bool RestoreMetadata { get; set; }
            public bool IsSelectable { get; set; }
            public VssComponentType ComponentType { get; set; }
            public bool NotifyOnBackupComplete { get; set; }
            public List<VssWMFileDescriptor> Files { get; set; }
            public List<VssWMFileDescriptor> DatabaseLogFiles { get; set; }
            public List<VssWMFileDescriptor> DatabaseFiles { get; set; }
            public List<VssWMDependency> Dependencies { get; set; }
        }
    }
}
