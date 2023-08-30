// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
using System.Reflection;

namespace Tes.RunnerCLI.Commands
{
    public class ProcessLauncher
    {
        private string standardOut = string.Empty;
        private string standardError = string.Empty;

        public async Task<ProcessExecutionResult> LaunchProcessAndWaitAsync(string[] options)
        {
            var process = new Process();

            SetupProcessStartInfo(options, process);

            SetupErrorAndOutputReaders(process);

            await StartAndWaitForExitAsync(process);

            return new ProcessExecutionResult(standardOut, standardError, process.ExitCode);
        }

        private static async Task StartAndWaitForExitAsync(Process process)
        {
            process.Start();
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            await process.WaitForExitAsync();
        }

        private void SetupErrorAndOutputReaders(Process process)
        {
            standardOut = string.Empty;
            standardError = string.Empty;
            process.ErrorDataReceived += ProcessOnErrorDataReceived;
            process.OutputDataReceived += ProcessOnOutputDataReceived;
        }

        private void ProcessOnOutputDataReceived(object sender, DataReceivedEventArgs e)
        {
            standardOut += e.Data;
        }

        private void ProcessOnErrorDataReceived(object sender, DataReceivedEventArgs e)
        {
            standardError += e.Data;
        }

        private void SetupProcessStartInfo(string[] options, Process process)
        {
            process.StartInfo.FileName = GetExecutableFullPath();
            process.StartInfo.Arguments = ParseArguments(options);
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
        }

        private static string? GetExecutableFullPath()
        {
            return Process.GetCurrentProcess().MainModule?.FileName;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("SingleFile", "IL3000:Avoid accessing Assembly file path when publishing as a single file", Justification = "<Pending>")]
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
    }
}
