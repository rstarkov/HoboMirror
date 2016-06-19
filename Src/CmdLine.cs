using System.Linq;
using Alphaleonis.Win32.Filesystem;
using RT.Util;
using RT.Util.CommandLine;
using RT.Util.Consoles;

namespace HoboMirror
{
    [CommandLine]
    [DocumentationRhoML("Mirrors the contents of one directory to another. New and modified files are copied to the destination directory. Files and directories missing in the source directory are {h}permanently deleted without confirmation{}. File and directory attributes, creation/modification times and security attributes are also replicated. Junctions and symbolic links are copied as links and are never recursed into. The source volumes are snapshotted using VSS to ensure all files are in a consistent state, including files currently open by running programs.")]
    class CmdLine : ICommandLineValidatable
    {
        [Option("-f", "--from"), IsMandatory]
        [DocumentationRhoML("{h}One or more path to be mirrored.{}")]
        public string[] FromPath = null;

        [Option("-t", "--to"), IsMandatory]
        [DocumentationRhoML("{h}One or more path to be used as mirror destination.{}\nThe number of {option}--to{} options must match the number of {option}--from{} options. Each pair specifies a separate mirroring operation.")]
        public string[] ToPath = null;

        [Option("-l", "--log")]
        [DocumentationRhoML("{h}Specifies log file directory.{}\nHoboMirror creates several log files at this path. No log files are created if this option is omitted. Specify the empty string ({h}\"\"{}) to log to the directory containing HoboMirror.exe.")]
        public string LogPath = null;

        [Option("-i", "--ignore")]
        [DocumentationRhoML("{h}Specifies one or more source paths to ignore.{}\nThe specified paths will not be mirrored, and will be deleted from the target if already present.")]
        public string[] IgnorePath = null;

        public ConsoleColoredString Validate()
        {
            if (FromPath.Length != ToPath.Length)
                return CommandLineParser.Colorize(RhoML.Parse("The number of {option}--from{} arguments must match the number of {option}--to{} arguments."));
            for (int i = 0; i < FromPath.Length; i++)
            {
                FromPath[i] = Path.GetFullPath(FromPath[i]).WithSlash();
                ToPath[i] = Path.GetFullPath(ToPath[i]).WithSlash();
            }
            for (int i = 0; i < IgnorePath.Length; i++)
            {
                IgnorePath[i] = Path.GetFullPath(IgnorePath[i]).WithSlash();
            }
            foreach (var path in FromPath.Concat(ToPath))
                if (!Directory.Exists(path))
                    return CommandLineParser.Colorize(RhoML.Parse($"Directory not found: {{h}}{path}{{}}."));
            return null;
        }

        private static void PostBuildCheck(IPostBuildReporter rep)
        {
            CommandLineParser.PostBuildStep<CmdLine>(rep, null);
        }
    }
}
