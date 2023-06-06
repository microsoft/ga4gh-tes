// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
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

        public Executor(NodeTask tesNodeTask, BlobPipelineOptions blobPipelineOptions)
        {
            ArgumentNullException.ThrowIfNull(tesNodeTask);
            ArgumentNullException.ThrowIfNull(blobPipelineOptions);

            this.blobPipelineOptions = blobPipelineOptions;

            this.tesNodeTask = tesNodeTask;

            resolutionPolicyHandler = new ResolutionPolicyHandler();
        }

        public async Task<NodeTaskResult> ExecuteNodeContainerTaskAsync(DockerExecutor dockerExecutor)
        {
            ArgumentNullException.ThrowIfNull(blobPipelineOptions);

            var result = await dockerExecutor.RunOnContainerAsync(tesNodeTask.ImageName, tesNodeTask.ImageTag, tesNodeTask.CommandsToExecute);

            return new NodeTaskResult(result);
        }

        private async ValueTask AppendMetrics(string? metricsFormat, long bytesTransfered)
        {
            if (!string.IsNullOrWhiteSpace(tesNodeTask.MetricsFilename) && !string.IsNullOrWhiteSpace(metricsFormat))
            {
                await new MetricsFormatter(tesNodeTask.MetricsFilename, metricsFormat).Write(bytesTransfered);
            }
        }

        public async Task<long> UploadOutputsAsync()
        {
            var memoryBufferChannel = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(blobPipelineOptions.MemoryBufferCapacity, blobPipelineOptions.BlockSizeBytes);

            var bytesTransfered = await UploadOutputsAsync(memoryBufferChannel);

            await AppendMetrics(tesNodeTask.OutputsMetricsFormat, bytesTransfered);

            return bytesTransfered;
        }

        private async ValueTask<long> UploadOutputsAsync(Channel<byte[]> memoryBufferChannel)
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

            if ((outputs?.Count ?? 0) > 0)
            {
                var executionResult = await TimedExecutionAsync(async () => await uploader.UploadAsync(outputs!));

                logger.LogInformation($"Executed Upload. Time elapsed: {executionResult.Elapsed} Bandwidth: {BlobSizeUtils.ToBandwidth(executionResult.Result, executionResult.Elapsed.TotalSeconds)} MiB/s");

                return executionResult.Result;
            }

            return 0;
        }

        public async Task<long> DownloadInputsAsync()
        {
            var memoryBufferChannel = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(blobPipelineOptions.MemoryBufferCapacity, blobPipelineOptions.BlockSizeBytes);

            var bytesTransfered = await DownloadInputsAsync(memoryBufferChannel);

            await AppendMetrics(tesNodeTask.InputsMetricsFormat, bytesTransfered);

            return bytesTransfered;
        }

        private async ValueTask<long> DownloadInputsAsync(Channel<byte[]> memoryBufferChannel)
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

            if ((inputs?.Count ?? 0) > 0)
            {
                var executionResult = await TimedExecutionAsync(async () => await downloader.DownloadAsync(inputs!));

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
