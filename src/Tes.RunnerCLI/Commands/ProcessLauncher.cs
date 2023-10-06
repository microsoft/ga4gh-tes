// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.Logging;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    public class ProcessLauncher
    {
        private readonly StringBuilder standardOut = new StringBuilder();
        private readonly StringBuilder standardError = new StringBuilder();
        private readonly ILogger logger = PipelineLoggerFactory.Create<ProcessLauncher>();

        public async Task<ProcessExecutionResult> LaunchProcessAndWaitAsync(string[] options)
        {
            var process = new Process();

            SetupProcessStartInfo(options, process);

            SetupErrorAndOutputReaders(process);

            await StartAndWaitForExitAsync(process);

            return new ProcessExecutionResult(standardOut.ToString(), standardError.ToString(), process.ExitCode);
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
            standardOut.Clear();
            standardError.Clear();
            process.ErrorDataReceived += ProcessOnErrorDataReceived;
            process.OutputDataReceived += ProcessOnOutputDataReceived;
        }

        private void ProcessOnOutputDataReceived(object sender, DataReceivedEventArgs e)
        {

            standardOut.Append(e.Data);
            standardOut.Append('\n');
        }

        private void ProcessOnErrorDataReceived(object sender, DataReceivedEventArgs e)
        {
            standardError.Append(e.Data);
            standardError.Append('\n');
        }

        private void SetupProcessStartInfo(string[] options, Process process)
        {
            process.StartInfo.FileName = GetExecutableFullPath();
            process.StartInfo.Arguments = ParseArguments(options);
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;

            logger.LogInformation($"Starting process: {process.StartInfo.FileName} {process.StartInfo.Arguments}");
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
