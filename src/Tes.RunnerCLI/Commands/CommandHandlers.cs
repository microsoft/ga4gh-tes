// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Tes.Runner;
using Tes.Runner.Docker;
using Tes.Runner.Events;
using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    internal class CommandHandlers
    {
        private static readonly ILogger Logger = PipelineLoggerFactory.Create<CommandHandlers>();
        private const int SuccessExitCode = 0;
        private const int ErrorExitCode = 1;

        internal static async Task<int> ExecuteNodeTaskAsync(FileInfo file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion,
            Uri dockerUri)
        {
            try
            {
                var duration = Stopwatch.StartNew();

                var nodeTask = await NodeTaskUtils.DeserializeNodeTaskAsync(file.FullName);

                await using var eventsPublisher = await EventsPublisher.CreateEventsPublisherAsync(nodeTask);

                try
                {
                    await ExecuteAllTaskOperationsAsync(file, blockSize, writers, readers, bufferCapacity, apiVersion, dockerUri);

                    await eventsPublisher.PublishTaskCompletionEventAsync(nodeTask, duration.Elapsed,
                        EventsPublisher.SuccessStatus, errorMessage: string.Empty);
                }
                catch (Exception e)
                {
                    await eventsPublisher.PublishTaskCompletionEventAsync(nodeTask, duration.Elapsed,
                        EventsPublisher.FailedStatus, errorMessage: e.Message);
                    throw;
                }
            }
            catch (Exception e)
            {
                Logger.LogError(e, $"Failed to execute Node Task: {file.FullName}");

                return ErrorExitCode;
            }

            return SuccessExitCode;
        }

        internal static async Task ExecuteNodeContainerTaskAsync(FileInfo file, Uri dockerUri)
        {
            try
            {
                var nodeTask = await NodeTaskUtils.DeserializeNodeTaskAsync(file.FullName);

                await using var executor = await Executor.CreateExecutorAsync(nodeTask);

                var result = await executor.ExecuteNodeContainerTaskAsync(new DockerExecutor(dockerUri));

                if (result is null)
                {
                    throw new InvalidOperationException("The container task failed to return results");
                }

                Logger.LogInformation($"Docker container execution status code: {result.ContainerResult.ExitCode}");

                if (!string.IsNullOrWhiteSpace(result.ContainerResult.Error))
                {
                    Logger.LogInformation($"Docker container result error: {result.ContainerResult.Error}");
                }
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Failed to execute the task.");
                throw;
            }
        }

        internal static async Task<int> ExecuteUploadTaskAsync(FileInfo file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion)
        {
            var options = CreateBlobPipelineOptions(blockSize, writers, readers, bufferCapacity, apiVersion);

            Logger.LogInformation("Starting upload operation.");

            return await ExecuteTransferTaskAsync(file, exec => exec.UploadOutputsAsync(options));
        }

        internal static async Task<int> ExecuteDownloadTaskAsync(FileInfo file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion)
        {
            var options = CreateBlobPipelineOptions(blockSize, writers, readers, bufferCapacity, apiVersion);

            Logger.LogInformation("Starting download operation.");

            return await ExecuteTransferTaskAsync(file, exec => exec.DownloadInputsAsync(options));
        }

        private static async Task ExecuteAllTaskOperationsAsync(FileInfo file, int blockSize, int writers, int readers, int bufferCapacity,
            string apiVersion, Uri dockerUri)
        {
            var options =
                BlobPipelineOptionsConverter.ToBlobPipelineOptions(blockSize, writers, readers, bufferCapacity,
                    apiVersion);

            await ExecuteTransferAsSubProcessAsync(CommandFactory.DownloadCommandName, file, options);

            await ExecuteExecutorAsSubProcessAsync(file, dockerUri);

            await ExecuteTransferAsSubProcessAsync(CommandFactory.UploadCommandName, file, options);
        }

        private static BlobPipelineOptions CreateBlobPipelineOptions(int blockSize, int writers, int readers,
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

        private static async Task ExecuteTransferAsSubProcessAsync(string command, FileInfo file, BlobPipelineOptions options)
        {
            var processLauncher = await ProcessLauncher.CreateLauncherAsync(file, logNamePrefix: command);

            var results = await processLauncher.LaunchProcessAndWaitAsync(BlobPipelineOptionsConverter.ToCommandArgs(command, file.FullName, options));

            HandleResult(results, command);
        }

        private static async Task ExecuteExecutorAsSubProcessAsync(FileInfo file, Uri dockerUri)
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

        private static async Task<int> ExecuteTransferTaskAsync(FileInfo taskDefinitionFile, Func<Executor, Task<long>> transferOperation)
        {
            try
            {
                var nodeTask = await NodeTaskUtils.DeserializeNodeTaskAsync(taskDefinitionFile.FullName);

                await using var executor = await Executor.CreateExecutorAsync(nodeTask);

                var result = await transferOperation(executor);

                Logger.LogInformation($"Total bytes transferred: {result:n0}");

                return SuccessExitCode;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to perform transfer. Error: {e.Message} Operation: {transferOperation.Method.Name}");
                Logger.LogError(e, $"Failed to perform transfer. Operation: {transferOperation}");
                return ErrorExitCode;
            }
        }
    }
}
