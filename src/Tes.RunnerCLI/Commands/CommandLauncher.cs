// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Exceptions;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    /// <summary>
    /// Launches CLI commands as a sub-processes.
    /// </summary>
    public class CommandLauncher(ProcessLauncherFactory processLauncherFactory)
    {
        private readonly ProcessLauncherFactory processLauncherFactory = processLauncherFactory ?? throw new ArgumentNullException(nameof(processLauncherFactory));

        public static BlobPipelineOptions CreateBlobPipelineOptions(int blockSize, int writers, int readers,
            int bufferCapacity, string apiVersion, bool setContentMd5OnUploads)
        {
            var options = new BlobPipelineOptions(
                BlockSizeBytes: blockSize,
                NumberOfWriters: writers,
                NumberOfReaders: readers,
                ReadWriteBuffersCapacity: bufferCapacity,
                MemoryBufferCapacity: bufferCapacity,
                ApiVersion: apiVersion,
                CalculateFileContentMd5: setContentMd5OnUploads);

            return options;
        }

        private static void HandleResult(ProcessExecutionResult results, string command)
        {
            if (results.ExitCode != 0)
            {
                throw new CommandExecutionException(results.ExitCode, $"Failed to execute command: {command}. Exit code returned by sub-process: {results.ExitCode}");
            }
        }

        /// <summary>
        /// Executes a transfer command as a sub-process. A transfer command is upload or download.
        /// </summary>
        /// <param name="command">Transfer command to execute</param>
        /// <param name="file">Node task definition file</param>
        /// <param name="options">Transfer options</param>
        ///<exception cref = "CommandExecutionException" > Thrown when the process launcher or launcher sub-process fail</exception>
        public async Task LaunchTransferCommandAsSubProcessAsync(string command, Runner.Models.NodeTask nodeTask, FileInfo file, BlobPipelineOptions options)
        {
            ProcessExecutionResult results = null!;
            try
            {
                var processLauncher = await processLauncherFactory.CreateLauncherAsync(nodeTask, logNamePrefix: command, apiVersion: options.ApiVersion);
                results = await processLauncher.LaunchProcessAndWaitAsync(BlobPipelineOptionsConverter.ToCommandArgs(command, file.FullName, options));
            }
            catch (Exception ex)
            {
                HandleFatalLauncherError(command, ex);
            }

            HandleResult(results, command);
        }

        public static void HandleFatalLauncherError(string command, Exception ex)
        {
            var exitCode = ex switch
            {
                IdentityUnavailableException => (int)ProcessExitCode.IdentityUnavailable,
                _ => (int)ProcessExitCode.UncategorizedError
            };


            throw new CommandExecutionException(exitCode, $"Failed to launch command: {command}", ex);
        }

        /// <summary>
        /// Executes the exec (executor) command as a sub-process.
        /// </summary>
        /// <param name="nodeTask">Node task definition</param>
        /// <param name="file">Node task definition file</param>
        /// <param name="apiVersion"></param>
        ///<exception cref = "CommandExecutionException" > Thrown when the process launcher or launcher sub-process fail</exception>
        /// <param name="dockerUri">Docker API URI</param>
        public async Task LaunchesExecutorCommandAsSubProcessAsync(Runner.Models.NodeTask nodeTask, FileInfo file, string apiVersion, Uri dockerUri)
        {
            ProcessExecutionResult results = null!;
            try
            {
                var processLauncher = await processLauncherFactory.CreateLauncherAsync(nodeTask, logNamePrefix: CommandFactory.ExecutorCommandName, apiVersion: apiVersion);

                var args = new List<string>() {
                    CommandFactory.ExecutorCommandName,
                    $"--{BlobPipelineOptionsConverter.ApiVersionOption} {apiVersion}",
                    $"--{CommandFactory.DockerUriOption} {dockerUri}" };

                if (!string.IsNullOrEmpty(file?.FullName))
                {
                    args.Add($"--{BlobPipelineOptionsConverter.FileOption} {file.FullName}");
                }

                results = await processLauncher.LaunchProcessAndWaitAsync([.. args]);
            }
            catch (Exception ex)
            {
                HandleFatalLauncherError(CommandFactory.ExecutorCommandName, ex);
            }

            HandleResult(results, CommandFactory.ExecutorCommandName);
        }
    }
}
