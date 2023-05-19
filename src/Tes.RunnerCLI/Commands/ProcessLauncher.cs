using System.Diagnostics;
using System.Reflection;

namespace Tes.RunnerCLI.Commands
{
    public class ProcessLauncher
    {

        public async Task<ProcessExecutionResult> LaunchProcessAndWaitAsync(string[] options)
        {
            var process = new Process();
            process.StartInfo.FileName = GetExecutableFullPath();
            process.StartInfo.Arguments = ParseArguments(options);
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.Start();
            await process.WaitForExitAsync();

            return await ToProcessExecutionResultAsync(process);
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

        private async Task<ProcessExecutionResult> ToProcessExecutionResultAsync(Process process)
        {
            return new ProcessExecutionResult(
                await process.StandardOutput.ReadToEndAsync(),
                await process.StandardError.ReadToEndAsync(),
                process.ExitCode);
        }
    }
}
