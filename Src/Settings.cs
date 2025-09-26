namespace HoboMirror;

class Settings
{
    public decimal? SkipRefreshMetadataDays = null;
    public DateTime LastRefreshMetadata = default;
    public MirrorTask[] MirrorTasks = [];
    public string[] IgnoreDirNames = [];
    public string[] IgnorePaths = [];
    public string UrlPingTemplate = null;
}

class MirrorTask
{
    public string From = null;
    public string To = null;
}
