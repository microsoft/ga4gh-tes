// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Tes.Runner.Docker;
using Tes.Runner.Models;
using Tes.Runner.Storage;
using Tes.Runner.Transfer;

namespace Tes.Runner
{
    public class Executor
    {
        private readonly ILogger logger = PipelineLoggerFactory.Create<Executor>();
        private readonly NodeTask tesNodeTask;
        private readonly BlobPipelineOptions blobPipelineOptions;
        private readonly ResolutionPolicyHandler resolutionPolicyHandler;

        public Executor(string tesNodeTaskFilePath, BlobPipelineOptions blobPipelineOptions)
        {
            ArgumentException.ThrowIfNullOrEmpty(tesNodeTaskFilePath);
            ArgumentNullException.ThrowIfNull(blobPipelineOptions);

            this.blobPipelineOptions = blobPipelineOptions;

            var content = File.ReadAllText(tesNodeTaskFilePath);

            tesNodeTask = DeserializeNodeTask(content);

            resolutionPolicyHandler = new ResolutionPolicyHandler();
        }

        public async Task<NodeTaskResult> ExecuteNodeContainerTaskAsync(DockerExecutor dockerExecutor)
        {
            ArgumentNullException.ThrowIfNull(blobPipelineOptions);

            var result = await dockerExecutor.RunOnContainerAsync(tesNodeTask.ImageName, tesNodeTask.ImageTag, tesNodeTask.CommandsToExecute);

            return new NodeTaskResult(result);
        }


        private NodeTask DeserializeNodeTask(string nodeTask)
        {
            try
            {
                return JsonSerializer.Deserialize<NodeTask>(nodeTask, new JsonSerializerOptions() { PropertyNameCaseInsensitive = true }) ?? throw new InvalidOperationException("The JSON data provided is invalid.");
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to deserialize task JSON file.");
                throw;
            }
        }

        private async Task<long> TransferIOCommonAsync(bool skipMissingSources, string? metricsFormat, TransferIOAsyncDelegate asyncDelegate)
        {
            var memoryBufferChannel = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(blobPipelineOptions.MemoryBufferCapacity, blobPipelineOptions.BlockSizeBytes);

            var bytesTransfered = await asyncDelegate(memoryBufferChannel, skipMissingSources);

            if (!string.IsNullOrWhiteSpace(tesNodeTask.MetricsFilename) && !string.IsNullOrWhiteSpace(metricsFormat))
            {
                await new MetricsFormatter(tesNodeTask.MetricsFilename, metricsFormat).Write(bytesTransfered);
            }

            return bytesTransfered;
        }

        private delegate ValueTask<long> TransferIOAsyncDelegate(Channel<byte[]> memoryBufferChannel, bool skipMissingSources);

        public Task<long> UploadOutputsAsync(bool skipMissingSources)
        {
            return TransferIOCommonAsync(skipMissingSources, tesNodeTask.OutputsMetricsFormat, UploadOutputsAsync);
        }

        private async ValueTask<long> UploadOutputsAsync(Channel<byte[]> memoryBufferChannel, bool skipMissingSources)
        {
            if (tesNodeTask.Outputs is null || tesNodeTask.Outputs.Count == 0)
            {
                logger.LogInformation("No outputs provided");
                return 0;
            }

            LogStartConfig();

            logger.LogInformation($"{tesNodeTask.Outputs.Count} outputs to upload.");

            var uploader = new BlobUploader(blobPipelineOptions, memoryBufferChannel);

            var outputs = await resolutionPolicyHandler.ApplyResolutionPolicyAsync(tesNodeTask.Outputs);

            if (outputs != null)
            {
                var executionResult = await TimedExecutionAsync(async () => await uploader.UploadAsync(outputs, skipMissingSources));

                logger.LogInformation($"Executed Upload. Time elapsed: {executionResult.Elapsed} Bandwidth: {BlobSizeUtils.ToBandwidth(executionResult.Result, executionResult.Elapsed.TotalSeconds)} MiB/s");

                return executionResult.Result;
            }

            return 0;
        }

        public Task<long> DownloadInputsAsync(bool skipMissingSources)
        {
            return TransferIOCommonAsync(skipMissingSources, tesNodeTask.InputsMetricsFormat, DownloadInputsAsync);
        }

        private async ValueTask<long> DownloadInputsAsync(Channel<byte[]> memoryBufferChannel, bool skipMissingSources)
        {
            if (tesNodeTask.Inputs is null || tesNodeTask.Inputs.Count == 0)
            {
                logger.LogInformation("No inputs provided");
                return 0;
            }

            LogStartConfig();
            logger.LogInformation($"{tesNodeTask.Inputs.Count} inputs to download.");
            var downloader = new BlobDownloader(blobPipelineOptions, memoryBufferChannel);

            var inputs = await resolutionPolicyHandler.ApplyResolutionPolicyAsync(tesNodeTask.Inputs);

            if (inputs != null)
            {
                var executionResult = await TimedExecutionAsync(async () => await downloader.DownloadAsync(inputs, skipMissingSources));

                logger.LogInformation($"Executed Download. Time elapsed: {executionResult.Elapsed} Bandwidth: {BlobSizeUtils.ToBandwidth(executionResult.Result, executionResult.Elapsed.TotalSeconds)} MiB/s");

                return executionResult.Result;
            }

            return 0;
        }

        private void LogStartConfig()
        {
            logger.LogInformation($"Writers:{blobPipelineOptions.NumberOfWriters}");
            logger.LogInformation($"Readers:{blobPipelineOptions.NumberOfReaders}");
            logger.LogInformation($"Capacity:{blobPipelineOptions.ReadWriteBuffersCapacity}");
            logger.LogInformation($"BlockSize:{blobPipelineOptions.BlockSizeBytes}");
        }

        private static async Task<TimedExecutionResult<T>> TimedExecutionAsync<T>(Func<Task<T>> execution)
        {
            var sw = Stopwatch.StartNew();
            var result = await execution();
            sw.Stop();

            return new TimedExecutionResult<T>(sw.Elapsed, result);
        }

        private record TimedExecutionResult<T>(TimeSpan Elapsed, T Result);
    }
}
