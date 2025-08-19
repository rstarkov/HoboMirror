using RT.Serialization;
using RT.Util.Collections;

namespace HoboMirror;

class Settings
{
    public List<DirectoryGrouping> GroupDirectoriesForChangeReport = new List<DirectoryGrouping>();
    public AutoDictionary<string, ChangeCount> DirectoryChangeCount = new AutoDictionary<string, ChangeCount>(StringComparer.OrdinalIgnoreCase, _ => new ChangeCount());
    public decimal? SkipRefreshAccessControlDays = null;
    public DateTime LastRefreshAccessControl = default(DateTime);
    public string[] IgnoreDirNames = new string[0];
    public string[] IgnorePaths = new string[0];
}

class ChangeCount
{
    public int TimesScanned;
    public int TimesChanged;
}

class DirectoryGrouping : IClassifyObjectProcessor
{
    public bool StartsWith = false;
    public string Match = null;

    void IClassifyObjectProcessor.BeforeSerialize()
    {
    }

    void IClassifyObjectProcessor.AfterDeserialize()
    {
        if (!Match.EndsWith("\\"))
            Match = Match + "\\";
        if (!StartsWith && !Match.StartsWith("\\"))
            Match = "\\" + Match;
    }

    public string GetMatch(string path)
    {
        if (StartsWith)
            return path.StartsWith(Match, StringComparison.OrdinalIgnoreCase) ? Match : null;
        else
        {
            int index = path.IndexOf(Match, StringComparison.OrdinalIgnoreCase);
            return index < 0 ? null : path.Substring(0, index + 1);
        }
    }
}
