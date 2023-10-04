﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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

                var nodeTask = await DeserializeNodeTaskAsync(file.FullName);

                await using var eventsPublisher = await EventsPublisher.CreateEventsPublisherAsync(nodeTask);

                try
                {
                    await ExecuteNodeTaskAsync(file, blockSize, writers, readers, bufferCapacity, apiVersion, dockerUri,
                        nodeTask);

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

        private static async Task ExecuteNodeTaskAsync(FileInfo file, int blockSize, int writers, int readers, int bufferCapacity,
            string apiVersion, Uri dockerUri, NodeTask nodeTask)
        {
            var options =
                BlobPipelineOptionsConverter.ToBlobPipelineOptions(blockSize, writers, readers, bufferCapacity,
                    apiVersion);

            await ExecuteTransferAsSubProcessAsync(CommandFactory.DownloadCommandName, file, options);

            await ExecuteNodeContainerTaskAsync(nodeTask, dockerUri);

            await ExecuteTransferAsSubProcessAsync(CommandFactory.UploadCommandName, file, options);
        }

        private static async Task ExecuteNodeContainerTaskAsync(NodeTask nodeTask, Uri dockerUri)
        {
            try
            {
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

        private static void HandleResult(ProcessExecutionResult results, string command)
        {
            if (results.ExitCode != 0)
            {
                throw new Exception(
                    $"Task operation failed. Command: {command}. Exit Code: {results.ExitCode}{Environment.NewLine}Error: {results.StandardError}{Environment.NewLine}Output: {results.StandardOutput}");
            }

            Console.WriteLine($"{results.StandardOutput}"); //writing the result to the console to keep formatting
        }

        private static async Task ExecuteTransferAsSubProcessAsync(string command, FileInfo file, BlobPipelineOptions options)
        {
            var processLauncher = new ProcessLauncher();

            var results = await processLauncher.LaunchProcessAndWaitAsync(BlobPipelineOptionsConverter.ToCommandArgs(command, file.FullName, options));

            HandleResult(results, command);
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

        private static async Task<int> ExecuteTransferTaskAsync(FileInfo taskDefinitionFile, Func<Executor, Task<long>> transferOperation)
        {
            try
            {
                var nodeTask = await DeserializeNodeTaskAsync(taskDefinitionFile.FullName);

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

        private static async Task<NodeTask> DeserializeNodeTaskAsync(string tesNodeTaskFilePath)
        {
            try
            {
                var nodeTaskText = await File.ReadAllTextAsync(tesNodeTaskFilePath);

                var nodeTask = JsonSerializer.Deserialize<NodeTask>(nodeTaskText, new JsonSerializerOptions() { PropertyNameCaseInsensitive = true }) ?? throw new InvalidOperationException("The JSON data provided is invalid.");

                AddDefaultValuesIfMissing(nodeTask);

                return nodeTask;
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Failed to deserialize task JSON file.");
                throw;
            }
        }

        private static void AddDefaultValuesIfMissing(NodeTask nodeTask)
        {
            nodeTask.RuntimeOptions ??= new RuntimeOptions();
        }
    }
}
