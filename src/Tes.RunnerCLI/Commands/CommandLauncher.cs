// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
                throw new Exception(
                    $"Task operation failed. Command: {command}. Exit Code: {results.ExitCode}{Environment.NewLine}Process Name: {results.ProcessName}");
            }
        }
        /// <summary>
        /// Executes a transfer command as a sub-process. A transfer command is upload or download.
        /// </summary>
        /// <param name="command">Transfer command to execute</param>
        /// <param name="file">Node task definition file</param>
        /// <param name="options">Transfer options</param>
        /// <returns></returns>
        public static async Task LaunchTransferCommandAsSubProcessAsync(string command, FileInfo file, BlobPipelineOptions options)
        {
            var processLauncher = await ProcessLauncher.CreateLauncherAsync(file, logNamePrefix: command);
            
            var results = await processLauncher.LaunchProcessAndWaitAsync(BlobPipelineOptionsConverter.ToCommandArgs(command, file.FullName, options));

            HandleResult(results, command);
        }

        /// <summary>
        /// Executes the exec (executor) command as a sub-process.
        /// </summary>
        /// <param name="file">Node task definition file</param>
        /// <param name="dockerUri">Docker API URI</param>
        /// <returns></returns>
        public static async Task LaunchesExecutorCommandAsSubProcessAsync(FileInfo file, Uri dockerUri)
        {
            var processLauncher = await ProcessLauncher.CreateLauncherAsync(file, logNamePrefix: CommandFactory.ExecutorCommandName);

            var args = new List<string>() {
                CommandFactory.ExecutorCommandName,
                $"--{CommandFactory.DockerUriOption} {dockerUri}" };

            if (!string.IsNullOrEmpty(file.FullName))
            {
                args.Add($"--{BlobPipelineOptionsConverter.FileOption} {file.FullName}");
            }

            var results = await processLauncher.LaunchProcessAndWaitAsync(args.ToArray());

            HandleResult(results, CommandFactory.ExecutorCommandName);
        }
    }
}
