﻿// Copyright (c) Microsoft Corporation.
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

        public async Task<long> UploadOutputsAsync(BlobPipelineOptions blobPipelineOptions)
        {
            ArgumentNullException.ThrowIfNull(blobPipelineOptions, nameof(blobPipelineOptions));

            if (tesNodeTask.Outputs is null || tesNodeTask.Outputs.Count == 0)
            {
                logger.LogInformation("No outputs provided");
                return 0;
            }

            var optimizedOptions = OptimizeBlobPipelineOptionsForUpload(blobPipelineOptions);

            var memoryBufferChannel = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(optimizedOptions.MemoryBufferCapacity, optimizedOptions.BlockSizeBytes);

            return await UploadOutputsAsync(optimizedOptions, memoryBufferChannel);
            var bytesTransfered = await UploadOutputsAsync(memoryBufferChannel);

            await AppendMetrics(tesNodeTask.OutputsMetricsFormat, bytesTransfered);

            return bytesTransfered;
        }

        private async ValueTask<long> UploadOutputsAsync(Channel<byte[]> memoryBufferChannel)
        private BlobPipelineOptions OptimizeBlobPipelineOptionsForUpload(BlobPipelineOptions blobPipelineOptions)
        {
            var optimizedOptions =
                PipelineOptionsOptimizer.OptimizeOptionsIfApplicable(blobPipelineOptions, tesNodeTask.Outputs);

            LogStartConfig(optimizedOptions);

            logger.LogInformation($"{tesNodeTask.Outputs?.Count} outputs to upload.");
            return optimizedOptions;
        }

        private BlobPipelineOptions OptimizeBlobPipelineOptionsForDownload(BlobPipelineOptions blobPipelineOptions)
        {
            var optimizedOptions =
                PipelineOptionsOptimizer.OptimizeOptionsIfApplicable(blobPipelineOptions, default);

            LogStartConfig(optimizedOptions);

            logger.LogInformation($"{tesNodeTask.Inputs?.Count} inputs to download.");
            return optimizedOptions;
        }

        private async Task<long> UploadOutputsAsync(BlobPipelineOptions blobPipelineOptions, Channel<byte[]> memoryBufferChannel)
        {
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

        public async Task<long> DownloadInputsAsync(BlobPipelineOptions blobPipelineOptions)
        {
            ArgumentNullException.ThrowIfNull(blobPipelineOptions, nameof(blobPipelineOptions));
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

            var optimizedOptions = OptimizeBlobPipelineOptionsForDownload(blobPipelineOptions);

            var memoryBufferChannel = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(optimizedOptions.MemoryBufferCapacity, optimizedOptions.BlockSizeBytes);

            return await DownloadInputsAsync(optimizedOptions, memoryBufferChannel);
        }

        private async Task<long> DownloadInputsAsync(BlobPipelineOptions blobPipelineOptions, Channel<byte[]> memoryBufferChannel)
        {
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

        private void LogStartConfig(BlobPipelineOptions blobPipelineOptions)
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
