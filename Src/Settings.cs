namespace HoboMirror;

class Settings
{
    public decimal? SkipRefreshAccessControlDays = null;
    public DateTime LastRefreshAccessControl = default;
    public string[] IgnoreDirNames = [];
    public string[] IgnorePaths = [];
}
