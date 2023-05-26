﻿using Microsoft.Extensions.Logging;
using Tes.Runner;
using Tes.Runner.Docker;
using Tes.Runner.Transfer;
using Tes.RunnerCLI.Services;

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
            bool skipMissingSources,
            int bufferCapacity,
            MetricsFormatterOptions? downloadFormatter,
            MetricsFormatterOptions? uploadFormatter,
            string apiVersion,
            Uri dockerUri)
        {
            try
            {
                var options = BlobPipelineOptionsConverter.ToBlobPipelineOptions(blockSize, writers, readers, skipMissingSources, bufferCapacity, apiVersion);

                await ExecuteTransferAsSubProcessAsync(CommandFactory.DownloadCommandName, file, options, BlobPipelineOptionsConverter.DownloaderFormatterOption, downloadFormatter);

                await ExecuteNodeContainerTaskAsync(file, dockerUri, options);

                await ExecuteTransferAsSubProcessAsync(CommandFactory.UploadCommandName, file, options, BlobPipelineOptionsConverter.UploaderFormatterOption, uploadFormatter);

                return SuccessExitCode;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to execute the task. Error: {e.Message}");
                Logger.LogError(e, "Failed to execute the task");
                return ErrorExitCode;
            }
        }

        private static async Task ExecuteNodeContainerTaskAsync(FileInfo file, Uri dockerUri, BlobPipelineOptions options)
        {
            try
            {
                var executor = new Executor(file.FullName, options);

                var result = await executor.ExecuteNodeContainerTaskAsync(new DockerExecutor(dockerUri));

                if (result is null)
                {
                    throw new InvalidOperationException("The container task failed to return results");
                }

                var logs = await result.ContainerResult.Logs.ReadOutputToEndAsync(CancellationToken.None);

                Console.WriteLine($"Execution Status Code: {result.ContainerResult.StatusCode}. Error: {result.ContainerResult.Error}");
                Console.WriteLine($"StdOutput: {logs.stdout}");
                Console.WriteLine($"StdError: {logs.stderr}");
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Failed to execute the task.");
                throw;
            }
        }

        private static BlobPipelineOptions CreateBlobPipelineOptions(int blockSize, int writers, int readers,
            bool skipMissingSources, int bufferCapacity, string apiVersion)
        {
            var options = new BlobPipelineOptions(
                BlockSizeBytes: blockSize,
                NumberOfWriters: writers,
                NumberOfReaders: readers,
                SkipMissingSources: skipMissingSources,
                ReadWriteBuffersCapacity: bufferCapacity,
                MemoryBufferCapacity: bufferCapacity,
                ApiVersion: apiVersion);

            options = PipelineOptionsOptimizer.OptimizeOptionsIfApplicable(options);

            return options;
        }

        internal static async Task<int> ExecuteUploadTaskAsync(FileInfo file,
            int blockSize,
            int writers,
            int readers,
            bool skipMissingSources,
            int bufferCapacity,
            MetricsFormatterOptions? formatterOptions,
            string apiVersion)
        {
            var options = CreateBlobPipelineOptions(blockSize, writers, readers, skipMissingSources, bufferCapacity, apiVersion);

            Console.WriteLine("Starting upload operation.");

            return await ExecuteTransferTaskAsync(file, options, (exec) => exec.UploadOutputsAsync(), formatterOptions);
        }

        private static void HandleResult(ProcessExecutionResult results, string command)
        {
            if (results.ExitCode != 0)
            {
                throw new Exception(
                    $"Task operation failed. Command: {command}. Exit Code: {results.ExitCode}{Environment.NewLine}Error: {results.StandardError}{Environment.NewLine}Output: {results.StandardOutput}");
            }

            Console.WriteLine($"Result: {results.StandardOutput}");
        }

        private static async Task ExecuteTransferAsSubProcessAsync(string command, FileInfo file, BlobPipelineOptions options, string formatterCommand, MetricsFormatterOptions? formatterOptions)
        {
            var processLauncher = new ProcessLauncher();

            var results = await processLauncher.LaunchProcessAndWaitAsync(BlobPipelineOptionsConverter.ToCommandArgs(command, file.FullName, options, formatterCommand, formatterOptions));

            HandleResult(results, command);
        }

        internal static async Task<int> ExecuteDownloadTaskAsync(FileInfo file,
            int blockSize,
            int writers,
            int readers,
            bool skipMissingSources,
            int bufferCapacity,
            MetricsFormatterOptions? formatterOptions,
            string apiVersion)
        {
            var options = CreateBlobPipelineOptions(blockSize, writers, readers, skipMissingSources, bufferCapacity, apiVersion);

            Console.WriteLine("Starting download operation.");

            return await ExecuteTransferTaskAsync(file, options, (exec) => exec.DownloadInputsAsync(), formatterOptions);
        }

        private static async Task<int> ExecuteTransferTaskAsync(FileInfo taskDefinitionFile, BlobPipelineOptions options, Func<Executor, Task<long>> transferOperation, MetricsFormatterOptions? formatterOptions)
        {
            try
            {
                var executor = new Executor(taskDefinitionFile.FullName, options);

                var result = await transferOperation(executor);

                Console.WriteLine($"Total bytes transfer: {result:n0}");

                if (formatterOptions is not null)
                {
                    await new MetricsFormatter(formatterOptions).Write(result);
                }

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
