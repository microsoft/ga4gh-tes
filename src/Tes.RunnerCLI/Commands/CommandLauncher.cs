// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Exceptions;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    /// <summary>
    /// Launches CLI commands as a sub-processes.
    /// </summary>
    public class CommandLauncher
    {
        public static BlobPipelineOptions CreateBlobPipelineOptions(int blockSize, int writers, int readers,
            int bufferCapacity, string apiVersion)
        {
            var options = new BlobPipelineOptions(
                BlockSizeBytes: blockSize,
                NumberOfWriters: writers,
                NumberOfReaders: readers,
                ReadWriteBuffersCapacity: bufferCapacity,
                MemoryBufferCapacity: bufferCapacity,
                ApiVersion: apiVersion);

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
        public static async Task LaunchTransferCommandAsSubProcessAsync(string command, FileInfo file, BlobPipelineOptions options)
        {
            ProcessExecutionResult results = null!;
            try
            {
                var processLauncher = await ProcessLauncher.CreateLauncherAsync(file, logNamePrefix: command);
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
        /// <param name="file">Node task definition file</param>
        /// <param name="dockerUri">Docker API URI</param>
        ///<exception cref = "CommandExecutionException" > Thrown when the process launcher or launcher sub-process fail</exception>
        public static async Task LaunchesExecutorCommandAsSubProcessAsync(FileInfo file, Uri dockerUri)
        {
            ProcessExecutionResult results = null!;
            try
            {
                var processLauncher = await ProcessLauncher.CreateLauncherAsync(file, logNamePrefix: CommandFactory.ExecutorCommandName);

                var args = new List<string>() {
                    CommandFactory.ExecutorCommandName,
                    $"--{CommandFactory.DockerUriOption} {dockerUri}" };

                if (!string.IsNullOrEmpty(file.FullName))
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
