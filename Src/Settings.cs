using System;
using System.Collections.Generic;
using RT.Util.Collections;
using RT.Util.Serialization;

namespace HoboMirror
{
    class Settings
    {
        public List<DirectoryGrouping> GroupDirectoriesForChangeReport = new List<DirectoryGrouping>();
        public AutoDictionary<string, ChangeCount> DirectoryChangeCount = new AutoDictionary<string, ChangeCount>(StringComparer.OrdinalIgnoreCase, _ => new ChangeCount());
        public decimal? SkipRefreshAccessControlDays = null;
        public DateTime LastRefreshAccessControl = default(DateTime);
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
}
