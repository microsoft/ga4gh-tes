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
    public class Executor : IAsyncDisposable
    {
        public const long ZeroBytesTransferred = 0;
        public const long DefaultErrorExitCode = 1;
        private readonly ILogger logger = PipelineLoggerFactory.Create<Executor>();
        private readonly NodeTask tesNodeTask;
        private readonly FileOperationResolver operationResolver;
        private readonly VolumeBindingsGenerator volumeBindingsGenerator = new();
        private readonly EventsPublisher eventsPublisher;
        private readonly ITransferOperationFactory transferOperationFactory;

        public Executor(NodeTask tesNodeTask, EventsPublisher eventsPublisher, string apiVersion)
            : this(tesNodeTask, new(tesNodeTask, apiVersion), eventsPublisher, new TransferOperationFactory())
        { }

        public static Host.IRunnerHost RunnerHost { get; internal set; } = new Host.AzureBatchRunnerHost();

        public static async Task<Executor> CreateExecutorAsync(NodeTask nodeTask, string apiVersion)
        {
            var publisher = await EventsPublisher.CreateEventsPublisherAsync(nodeTask, apiVersion);

            return new Executor(nodeTask, publisher, apiVersion);
        }

        public Executor(NodeTask tesNodeTask, FileOperationResolver operationResolver, EventsPublisher eventsPublisher, ITransferOperationFactory transferOperationFactory)
        {
            ArgumentNullException.ThrowIfNull(transferOperationFactory);
            ArgumentNullException.ThrowIfNull(tesNodeTask);
            ArgumentNullException.ThrowIfNull(operationResolver);
            ArgumentNullException.ThrowIfNull(eventsPublisher);

            this.tesNodeTask = tesNodeTask;
            this.operationResolver = operationResolver;
            this.eventsPublisher = eventsPublisher;
            this.transferOperationFactory = transferOperationFactory;

        }

        public async Task<NodeTaskResult> ExecuteNodeContainerTaskAsync(DockerExecutor dockerExecutor)
        {
            try
            {
                await eventsPublisher.PublishExecutorStartEventAsync(tesNodeTask);

                var bindings = volumeBindingsGenerator.GenerateVolumeBindings(tesNodeTask.Inputs, tesNodeTask.Outputs);

                var executionOptions = CreateExecutionOptions(bindings);

                var result = await dockerExecutor.RunOnContainerAsync(executionOptions);

                await eventsPublisher.PublishExecutorEndEventAsync(tesNodeTask, result.ExitCode, ToStatusMessage(result), result.Error);

                return new NodeTaskResult(result);
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to execute container");

                await eventsPublisher.PublishExecutorEndEventAsync(tesNodeTask, DefaultErrorExitCode, EventsPublisher.FailedStatus, e.Message);

                throw;
            }
        }

        private ExecutionOptions CreateExecutionOptions(List<string> bindings)
        {
            return new(tesNodeTask.ImageName, tesNodeTask.ImageTag, tesNodeTask.CommandsToExecute, bindings,
                tesNodeTask.ContainerWorkDir, tesNodeTask.RuntimeOptions, tesNodeTask.ContainerDeviceRequests);
        }

        private static string ToStatusMessage(ContainerExecutionResult result)
        {
            if (result.ExitCode == 0 && string.IsNullOrWhiteSpace(result.Error))
            {
                return EventsPublisher.SuccessStatus;
            }

            return EventsPublisher.FailedStatus;
        }


        private async ValueTask AppendMetrics(string? metricsFormat, long bytesTransferred)
        {
            if (!string.IsNullOrWhiteSpace(tesNodeTask.MetricsFilename) && !string.IsNullOrWhiteSpace(metricsFormat))
            {
                await new MetricsFormatter(tesNodeTask.MetricsFilename, metricsFormat).WriteSize(bytesTransferred);
            }
        }

        public async ValueTask AppendMetrics()
        {
            foreach (var bashMetric in tesNodeTask.BashScriptMetricsFormats ?? [])
            {
                if (!string.IsNullOrWhiteSpace(tesNodeTask.MetricsFilename) && !string.IsNullOrWhiteSpace(bashMetric))
                {
                    await new MetricsFormatter(tesNodeTask.MetricsFilename, bashMetric).WriteWithBash();
                }
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
                await eventsPublisher.PublishUploadStartEventAsync(tesNodeTask);

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
                await eventsPublisher.PublishUploadEndEventAsync(tesNodeTask, numberOfOutputs, bytesTransferred, statusMessage, errorMessage);
            }
        }

        private async Task<long> UploadOutputsAsync(BlobPipelineOptions blobPipelineOptions, List<UploadInfo> outputs)
        {
            var uploader = await transferOperationFactory.CreateBlobUploaderAsync(blobPipelineOptions);

            var executionResult = await TimedExecutionAsync(async () => await uploader.UploadAsync(outputs));

            logger.LogInformation("Executed Upload. Time elapsed: {ElapsedTime} Bandwidth: {Bandwidth} MiB/s", executionResult.Elapsed, BlobSizeUtils.ToBandwidth(executionResult.Result, executionResult.Elapsed.TotalSeconds));

            return executionResult.Result;
        }

        private async Task<List<UploadInfo>?> CreateUploadOutputsAsync()
        {
            if ((tesNodeTask.Outputs ?? []).Count == 0)
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

            logger.LogInformation("{CountOfUploads} outputs to upload.", outputs.Count);
            return optimizedOptions;
        }

        private BlobPipelineOptions OptimizeBlobPipelineOptionsForDownload(BlobPipelineOptions blobPipelineOptions)
        {
            var optimizedOptions =
                PipelineOptionsOptimizer.OptimizeOptionsIfApplicable(blobPipelineOptions, default);

            LogStartConfig(optimizedOptions);

            logger.LogInformation("{CountOfDownloads} inputs to download.", tesNodeTask.Inputs?.Count);
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
                await eventsPublisher.PublishDownloadStartEventAsync(tesNodeTask);

                ArgumentNullException.ThrowIfNull(blobPipelineOptions, nameof(blobPipelineOptions));

                var inputs = await CreateDownloadInputsAsync();

                if (inputs is null)
                {
                    return numberOfInputs;
                }

                numberOfInputs = inputs.Count;

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
                await eventsPublisher.PublishDownloadEndEventAsync(tesNodeTask, numberOfInputs, bytesTransferred, statusMessage, errorMessage);
            }
        }

        private async Task<long> DownloadInputsAsync(BlobPipelineOptions blobPipelineOptions, List<DownloadInfo> inputs)
        {
            var downloader = await transferOperationFactory.CreateBlobDownloaderAsync(blobPipelineOptions);

            var executionResult = await TimedExecutionAsync(async () => await downloader.DownloadAsync(inputs));

            logger.LogInformation("Executed Download. Time elapsed: {ElapsedTime} Bandwidth: {Bandwidth} MiB/s", executionResult.Elapsed, BlobSizeUtils.ToBandwidth(executionResult.Result, executionResult.Elapsed.TotalSeconds));

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
            logger.LogInformation("Writers: {NumberOfWriters}", blobPipelineOptions.NumberOfWriters);
            logger.LogInformation("Readers: {NumberOfReaders}", blobPipelineOptions.NumberOfReaders);
            logger.LogInformation("Capacity: {ReadWriteBuffersCapacity}", blobPipelineOptions.ReadWriteBuffersCapacity);
            logger.LogInformation("BlockSize: {BlockSizeBytes}", blobPipelineOptions.BlockSizeBytes);
        }

        private static async Task<TimedExecutionResult<T>> TimedExecutionAsync<T>(Func<Task<T>> execution)
        {
            var sw = Stopwatch.StartNew();
            var result = await execution();
            sw.Stop();

            return new(sw.Elapsed, result);
        }

        private record TimedExecutionResult<T>(TimeSpan Elapsed, T Result);

        public async ValueTask DisposeAsync()
        {
            await eventsPublisher.FlushPublishersAsync();
            GC.SuppressFinalize(this);
        }
    }
}
