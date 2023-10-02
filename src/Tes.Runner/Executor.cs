// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Tes.Runner.Docker;
using Tes.Runner.Events;
using Tes.Runner.Models;
using Tes.Runner.Storage;
using Tes.Runner.Transfer;

namespace Tes.Runner
{
    public class Executor
    {
        public const long ZeroBytesTransferred = 0;
        const long DefaultErrorExitCode = -1;
        private readonly ILogger logger = PipelineLoggerFactory.Create<Executor>();
        private readonly NodeTask tesNodeTask;
        private readonly FileOperationResolver operationResolver;
        private readonly VolumeBindingsGenerator volumeBindingsGenerator = new VolumeBindingsGenerator();
        private readonly EventsPublisher eventsPublisher;

        public Executor(NodeTask tesNodeTask, EventsPublisher eventsPublisher) : this(tesNodeTask, new FileOperationResolver(tesNodeTask), eventsPublisher)
        {
        }

        public static async Task<Executor> CreateExecutorAsync(NodeTask nodeTask)
        {
            var publisher = await EventsPublisher.CreateEventsPublisherAsync(nodeTask);

            return new Executor(nodeTask, publisher);
        }

        public Executor(NodeTask tesNodeTask, FileOperationResolver operationResolver, EventsPublisher eventsPublisher)
        {
            ArgumentNullException.ThrowIfNull(tesNodeTask);
            ArgumentNullException.ThrowIfNull(operationResolver);
            ArgumentNullException.ThrowIfNull(eventsPublisher);

            this.tesNodeTask = tesNodeTask;
            this.operationResolver = operationResolver;
            this.eventsPublisher = eventsPublisher;
        }

        public async Task<NodeTaskResult> ExecuteNodeContainerTaskAsync(DockerExecutor dockerExecutor)
        {
            try
            {
                await eventsPublisher.PublishExecutorEventStartAsync(tesNodeTask);

                var bindings = volumeBindingsGenerator.GenerateVolumeBindings(tesNodeTask.Inputs, tesNodeTask.Outputs);

                var result = await dockerExecutor.RunOnContainerAsync(tesNodeTask.ImageName, tesNodeTask.ImageTag, tesNodeTask.CommandsToExecute, bindings, tesNodeTask.ContainerWorkDir);

                await eventsPublisher.PublishExecutorEventEndAsync(tesNodeTask, result.ExitCode, ToStatusMessage(result), result.Error);

                return new NodeTaskResult(result);
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to execute container");

                await eventsPublisher.PublishExecutorEventEndAsync(tesNodeTask, DefaultErrorExitCode, EventsPublisher.FailedStatus, e.Message);

                throw;
            }
        }

        private string ToStatusMessage(ContainerExecutionResult result)
        {
            if (result.ExitCode == 0 && string.IsNullOrWhiteSpace(result.Error))
            {
                return EventsPublisher.FailedStatus;
            }

            return EventsPublisher.SuccessStatus;
        }


        private async ValueTask AppendMetrics(string? metricsFormat, long bytesTransferred)
        {
            if (!string.IsNullOrWhiteSpace(tesNodeTask.MetricsFilename) && !string.IsNullOrWhiteSpace(metricsFormat))
            {
                await new MetricsFormatter(tesNodeTask.MetricsFilename, metricsFormat).Write(bytesTransferred);
            }
        }

        public async Task<long> UploadOutputsAsync(BlobPipelineOptions blobPipelineOptions)
        {
            var statusMessage = EventsPublisher.SuccessStatus;
            var bytesTransferred = ZeroBytesTransferred;
            var numberOfOutputs = 0;
            var errorMessage = string.Empty;
            try
            {
                await eventsPublisher.PublishUploadEventStartAsync(tesNodeTask);

                ArgumentNullException.ThrowIfNull(blobPipelineOptions, nameof(blobPipelineOptions));

                var outputs = await CreateUploadOutputsAsync();

                if (outputs is null)
                {
                    return bytesTransferred;
                }

                if (outputs.Count == 0)
                {
                    logger.LogWarning("No output files were found.");
                    return bytesTransferred;
                }

                numberOfOutputs = outputs.Count;

                var optimizedOptions = OptimizeBlobPipelineOptionsForUpload(blobPipelineOptions, outputs);

                bytesTransferred = await UploadOutputsAsync(optimizedOptions, outputs);

                await AppendMetrics(tesNodeTask.OutputsMetricsFormat, bytesTransferred);

                return bytesTransferred;
            }
            catch (Exception e)
            {
                logger.LogError(e, "Upload operation failed");
                statusMessage = EventsPublisher.FailedStatus;
                errorMessage = e.Message;
                throw;
            }
            finally
            {
                await eventsPublisher.PublishUploadEventEndAsync(tesNodeTask, numberOfOutputs, bytesTransferred, statusMessage, errorMessage);
            }
        }

        private async Task<long> UploadOutputsAsync(BlobPipelineOptions blobPipelineOptions, List<UploadInfo> outputs)
        {
            var memoryBufferChannel = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(blobPipelineOptions.MemoryBufferCapacity, blobPipelineOptions.BlockSizeBytes);

            var uploader = new BlobUploader(blobPipelineOptions, memoryBufferChannel);

            var executionResult = await TimedExecutionAsync(async () => await uploader.UploadAsync(outputs));

            logger.LogInformation($"Executed Upload. Time elapsed: {executionResult.Elapsed} Bandwidth: {BlobSizeUtils.ToBandwidth(executionResult.Result, executionResult.Elapsed.TotalSeconds)} MiB/s");

            return executionResult.Result;
        }

        private async Task<List<UploadInfo>?> CreateUploadOutputsAsync()
        {
            if (tesNodeTask.Outputs is null || tesNodeTask.Outputs.Count == 0)
            {
                logger.LogInformation("No outputs provided");
                {
                    return default;
                }
            }

            return await operationResolver.ResolveOutputsAsync();
        }

        private BlobPipelineOptions OptimizeBlobPipelineOptionsForUpload(BlobPipelineOptions blobPipelineOptions, List<UploadInfo> outputs)
        {
            var optimizedOptions =
                PipelineOptionsOptimizer.OptimizeOptionsIfApplicable(blobPipelineOptions, outputs);

            ValidateBlockSize(optimizedOptions.BlockSizeBytes);

            LogStartConfig(optimizedOptions);

            logger.LogInformation($"{outputs.Count} outputs to upload.");
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

        public async Task<long> DownloadInputsAsync(BlobPipelineOptions blobPipelineOptions)
        {
            var statusMessage = EventsPublisher.SuccessStatus;
            var bytesTransferred = ZeroBytesTransferred;
            var numberOfInputs = 0;
            var errorMessage = string.Empty;

            try
            {
                await eventsPublisher.PublishDownloadEventStartAsync(tesNodeTask);

                ArgumentNullException.ThrowIfNull(blobPipelineOptions, nameof(blobPipelineOptions));

                var inputs = await CreateDownloadInputsAsync();

                if (inputs is null)
                {
                    return numberOfInputs;
                }

                var optimizedOptions = OptimizeBlobPipelineOptionsForDownload(blobPipelineOptions);

                bytesTransferred = await DownloadInputsAsync(optimizedOptions, inputs);

                await AppendMetrics(tesNodeTask.InputsMetricsFormat, bytesTransferred);

                return bytesTransferred;
            }
            catch (Exception e)
            {
                logger.LogError(e, "Download operation failed");
                statusMessage = EventsPublisher.FailedStatus;
                errorMessage = e.Message;
                throw;
            }
            finally
            {
                await eventsPublisher.PublishDownloadEventEndAsync(tesNodeTask, numberOfInputs, bytesTransferred, statusMessage, errorMessage);
            }
        }

        private async Task<long> DownloadInputsAsync(BlobPipelineOptions blobPipelineOptions, List<DownloadInfo> inputs)
        {
            var memoryBufferChannel = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(blobPipelineOptions.MemoryBufferCapacity, blobPipelineOptions.BlockSizeBytes);

            var downloader = new BlobDownloader(blobPipelineOptions, memoryBufferChannel);

            var executionResult = await TimedExecutionAsync(async () => await downloader.DownloadAsync(inputs));

            logger.LogInformation($"Executed Download. Time elapsed: {executionResult.Elapsed} Bandwidth: {BlobSizeUtils.ToBandwidth(executionResult.Result, executionResult.Elapsed.TotalSeconds)} MiB/s");

            return executionResult.Result;
        }

        private async Task<List<DownloadInfo>?> CreateDownloadInputsAsync()
        {
            if (tesNodeTask.Inputs is null || tesNodeTask.Inputs.Count == 0)
            {
                logger.LogInformation("No inputs provided");
                {
                    return default;
                }
            }

            return await operationResolver.ResolveInputsAsync();
        }

        private static void ValidateBlockSize(int blockSizeBytes)
        {
            if (blockSizeBytes % BlobSizeUtils.BlockSizeIncrementUnitInBytes > 0)
            {
                throw new InvalidOperationException($"The provided block size: {blockSizeBytes:n0} is not valid for the upload operation. The block size must be a multiple of {BlobSizeUtils.BlockSizeIncrementUnitInBytes / BlobSizeUtils.MiB} MiB ({BlobSizeUtils.BlockSizeIncrementUnitInBytes:n0} bytes)");
            }
        }

        private void LogStartConfig(BlobPipelineOptions blobPipelineOptions)
        {
            logger.LogInformation($"Writers: {blobPipelineOptions.NumberOfWriters}");
            logger.LogInformation($"Readers: {blobPipelineOptions.NumberOfReaders}");
            logger.LogInformation($"Capacity: {blobPipelineOptions.ReadWriteBuffersCapacity}");
            logger.LogInformation($"BlockSize: {blobPipelineOptions.BlockSizeBytes}");
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
