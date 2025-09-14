namespace HoboMirror;

class Settings
{
    public decimal? SkipRefreshAccessControlDays = null;
    public DateTime LastRefreshAccessControl = default;
    public MirrorTask[] MirrorTasks = [];
    public string[] IgnoreDirNames = [];
    public string[] IgnorePaths = [];
}

class MirrorTask
{
    public string From = null;
    public string To = null;
}
