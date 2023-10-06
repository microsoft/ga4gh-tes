// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
using System.Reflection;
using System.Text;
using Azure.Storage.Sas;
using Microsoft.Extensions.Logging;
using Tes.Runner.Logs;
using Tes.Runner.Storage;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    public class ProcessLauncher
    {
        const int LogWaitTimeout = 30;
        const BlobSasPermissions LogLocationPermissions = BlobSasPermissions.Read | BlobSasPermissions.Create | BlobSasPermissions.Write | BlobSasPermissions.Add;

        // private readonly StringBuilder standardOut = new StringBuilder();
        // private readonly StringBuilder standardError = new StringBuilder();
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
            // process.BeginOutputReadLine();
            // process.BeginErrorReadLine();

            SetupErrorAndOutputReaders(process);

            await StartAndWaitForExitAsync(process);

            return new ProcessExecutionResult(process.ProcessName, process.ExitCode);
        }

        private async Task StartAndWaitForExitAsync(Process process)
        {

            await process.WaitForExitAsync();


            await logReader.WaitUntilAsync(timeout: TimeSpan.FromSeconds(LogWaitTimeout));
        }

        private void SetupErrorAndOutputReaders(Process process)
        {
            //standardOut.Clear();
            //standardError.Clear();
            logReader.StartReadingFromLogStreams(process.StandardOutput, process.StandardError);
        }

        //
        // private void ProcessOnOutputDataReceived(object sender, DataReceivedEventArgs e)
        // {
        //     standardOut.Append(e.Data);
        //     standardOut.Append('\n');
        // }
        //
        // private void ProcessOnErrorDataReceived(object sender, DataReceivedEventArgs e)
        // {
        //     standardError.Append(e.Data);
        //     standardError.Append('\n');
        // }

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

            if (nodeTask.RuntimeOptions?.StreamingLogPublisher?.TargetUrl is not null)
            {

                var transformedUrl = UrlTransformationStrategyFactory.GetTransformedUrlAsync(
                    nodeTask.RuntimeOptions,
                    nodeTask.RuntimeOptions.StreamingLogPublisher,
                    LogLocationPermissions);

                var appendBlobLogPublisher = new AppendBlobLogPublisher(
                    transformedUrl.ToString()!,
                    $"{logNamePrefix}_stdout",
                    $"{logNamePrefix}_stderr");

                return new ProcessLauncher(appendBlobLogPublisher);
            }

            return new ProcessLauncher(new ConsoleStreamLogPublisher());
        }
    }
}
