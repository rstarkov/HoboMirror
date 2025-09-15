using System.Management;
using System.Security.Cryptography;
using System.Text;
using RT.Util.ExtensionMethods;

namespace HoboMirror;

class VssSnapshotInfo
{
    private ManagementObject _raw;

    public string Id => (string)_raw["ID"];
    public string HashId { get; private set; }
    public string VolumeGuidPath => (string)_raw["VolumeName"];
    public string SnapshotPath => (string)_raw["DeviceObject"];
    public string VolumeDisplayPath => WinAPI.GetVolumeMountPath(VolumeGuidPath) ?? VolumeGuidPath;
    public DateTimeOffset CreatedAt { get; private set; }

    public string AllProps => string.Join("\r\n", _raw.Properties.Cast<PropertyData>().Select(p => $"{p.Name}={p.Value}"));

    public VssSnapshotInfo(ManagementObject raw)
    {
        _raw = raw;
        HashId = SHA256.Create().ComputeHash(Encoding.UTF8.GetBytes(Id)).Base64UrlEncode().Replace("-", "").Replace("_", "").ToLower().Substring(0, 16);
        CreatedAt = parseWmiDate((string)_raw["InstallDate"]);
    }

    private static DateTimeOffset parseWmiDate(string wd)
    {
        return new DateTimeOffset( //eg: 20250819235741.120000+060
            wd[0..4].ParseInt(), wd[4..6].ParseInt(), wd[6..8].ParseInt(),
            wd[8..10].ParseInt(), wd[10..12].ParseInt(), wd[12..14].ParseInt(),
            wd[15..18].ParseInt(), wd[18..21].ParseInt(), TimeSpan.FromMinutes(wd[21..].ParseInt()));
    }

    public override string ToString() => $"{HashId} at {CreatedAt} for {VolumeDisplayPath} => {SnapshotPath}";

    public void Delete()
    {
        _raw.Delete();
    }
}

static class Vss
{
    /// <summary>
    ///     Creates a persistent VSS snapshot for the specified volume. Throws if creation failed.</summary>
    /// <param name="volume">
    ///     Path to the volume. Can be any valid volume path: C:\, C:\Mount\Path, \\?\Volume{GUID}\.</param>
    public static VssSnapshotInfo CreateSnapshot(string volume)
    {
        var mc = new ManagementClass("Win32_ShadowCopy");
        var inParams = mc.GetMethodParameters("Create");
        inParams["Volume"] = volume;
        inParams["Context"] = "ClientAccessible";
        var outParams = mc.InvokeMethod("Create", inParams, null);
        var id = (string)outParams["ShadowID"];
        var snapshots = GetSnapshots();
        return snapshots.Single(s => s.Id == id);
    }

    public static List<VssSnapshotInfo> GetSnapshots()
    {
        var objs = new ManagementObjectSearcher("SELECT * FROM Win32_ShadowCopy").Get();
        var result = objs.Cast<ManagementObject>().Select(o => new VssSnapshotInfo(o)).ToList(); // throws if not admin
        return result;
    }
}

class VolumeShadowCopy : IDisposable
{
    public Dictionary<string, VssSnapshotInfo> Snapshots { get; private set; } = [];

    public VolumeShadowCopy(IEnumerable<string> volumes)
    {
        try
        {
            foreach (var volume in volumes)
            {
                Program.LogAction($"Create VSS snapshot for {volume}");
                Snapshots[volume] = Vss.CreateSnapshot(volume);
            }
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    public void Dispose()
    {
        if (Snapshots == null || Snapshots.Count == 0)
            return;
        foreach (var snap in Snapshots.Values)
        {
            Program.LogAction($"Delete VSS snapshot: {snap.Id} for {snap.VolumeDisplayPath}");
            snap.Delete();
        }
        Snapshots = null;
    }
}
