// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
using System.Reflection;
using Microsoft.Extensions.Logging;
using Tes.Runner.Logs;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    public class ProcessLauncher
    {
        const int LogWaitTimeout = 30;

        private readonly IStreamLogReader logReader;
        private readonly ILogger logger = PipelineLoggerFactory.Create<ProcessLauncher>();

        public ProcessLauncher(IStreamLogReader logReader)
        {
            ArgumentNullException.ThrowIfNull(logReader);

            this.logReader = logReader;
        }

        public async Task<ProcessExecutionResult> LaunchProcessAndWaitAsync(string[] options)
        {
            var process = new Process();

            SetupProcessStartInfo(options, process);

            process.Start();

            var processName = "NA";

            if (!process.HasExited)
            {
                processName = process.ProcessName;
            }

            SetupErrorAndOutputReaders(process);

            await StartAndWaitForExitAsync(process);

            return new ProcessExecutionResult(processName, process.ExitCode);
        }

        private async Task StartAndWaitForExitAsync(Process process)
        {
            await process.WaitForExitAsync();

            await logReader.WaitUntilAsync(timeout: TimeSpan.FromSeconds(LogWaitTimeout));

            logger.LogInformation($"Process exited. Arguments: {process.StartInfo.Arguments}");
        }

        private void SetupErrorAndOutputReaders(Process process)
        {
            logReader.StartReadingFromLogStreams(process.StandardOutput, process.StandardError);
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

        public static async Task<ProcessLauncher> CreateLauncherAsync(FileInfo file, string logNamePrefix)
        {
            ArgumentNullException.ThrowIfNull(file);

            var nodeTask = await NodeTaskUtils.DeserializeNodeTaskAsync(file.FullName);

            var logPublisher = await LogPublisher.CreateStreamReaderLogPublisherAsync(nodeTask, logNamePrefix);

            return new ProcessLauncher(logPublisher);
        }
    }
}
