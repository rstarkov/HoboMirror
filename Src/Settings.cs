using System;
using System.Collections.Generic;
using RT.Util.Collections;

namespace HoboMirror
{
    class Settings
    {
        public HashSet<string> GroupDirectoriesForChangeReport = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public AutoDictionary<string, ChangeCount> DirectoryChangeCount = new AutoDictionary<string, ChangeCount>(StringComparer.OrdinalIgnoreCase, _ => new ChangeCount());
    }

    class ChangeCount
    {
        public int TimesScanned;
        public int TimesChanged;
    }
}
