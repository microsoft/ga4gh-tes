using System.Diagnostics;
using System.Reflection;

namespace Tes.RunnerCLI.Commands
{
    public class ProcessLauncher
    {

        public ProcessExecutionResult LaunchProcessAndWait(string[] options)
        {
            var process = new Process();
            process.StartInfo.FileName = GetExecutableFullPath();
            process.StartInfo.Arguments = ParseArguments(options);
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.Start();
            process.WaitForExit();

            return ToProcessExecutionResult(process);
        }

        private static string? GetExecutableFullPath()
        {
            return Process.GetCurrentProcess().MainModule?.FileName;
        }

        private string ParseArguments(string[] options)
        {
            var argList = new List<string>(options);
            var assemblyName = Assembly.GetExecutingAssembly().Location;

            if (!string.IsNullOrEmpty(assemblyName))
            {
                //the application is not running as a single file executable
                argList.Insert(0, assemblyName);
            }

            return string.Join(" ", argList.ToArray());
        }

        private ProcessExecutionResult ToProcessExecutionResult(Process process)
        {
            return new ProcessExecutionResult(
                process.StandardOutput.ReadToEnd(),
                process.StandardError.ReadToEnd(),
                process.ExitCode);
        }
    }

    public record ProcessExecutionResult(string StandardOutput, string StandardError, int ExitCode)
    {
    }
}
