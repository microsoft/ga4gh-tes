using System.Diagnostics;
using System.Reflection;

namespace Tes.RunnerCLI.Commands
{
    public class ProcessLauncher
    {

        public ProcessExecutionResult LaunchProcessAndWait(string[] options)
        {
            var process = new Process();
            process.StartInfo.FileName = Process.GetCurrentProcess().MainModule?.FileName;
            process.StartInfo.Arguments = ParseArguments(options);
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.Start();
            process.WaitForExit();

            return ToProcessExecutionResult(process);
        }

        private string ParseArguments(string[] options)
        {
            var argList = new List<string>(options);
            var assemblyName = Assembly.GetExecutingAssembly().Location;

            if (assemblyName is null)
            {
                throw new InvalidOperationException("Invalid execution context. The assembly name was not found.");
            }

            argList.Insert(0, assemblyName);
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
