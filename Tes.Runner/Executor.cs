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

        public async Task<NodeTaskResult> ExecuteNodeTaskAsync(DockerExecutor dockerExecutor)
        {
            ArgumentNullException.ThrowIfNull(blobPipelineOptions);

            var memoryBufferChannel = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(blobPipelineOptions.MemoryBufferCapacity, blobPipelineOptions.BlockSize);

            var inputLength = await DownloadInputsAsync(memoryBufferChannel);

            var result = await dockerExecutor.RunOnContainerAsync(tesNodeTask.ImageName, tesNodeTask.ImageTag, tesNodeTask.CommandsToExecute);

            var outputLength = await UploadOutputsAsync(memoryBufferChannel);

            return new NodeTaskResult(result, inputLength, outputLength);
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

        public async Task<long> UploadOutputsAsync()
        {
            var memoryBufferChannel = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(blobPipelineOptions.MemoryBufferCapacity, blobPipelineOptions.BlockSize);

            return await UploadOutputsAsync(memoryBufferChannel);
        }

        private async Task<long> UploadOutputsAsync(Channel<byte[]> memoryBufferChannel)
        {
            if (tesNodeTask.Outputs is null || tesNodeTask.Outputs.Count == 0)
            {
                logger.LogInformation("No outputs provided");
                return 0;
            }

            LogStartConfig();

            logger.LogInformation($"{tesNodeTask.Outputs.Count} inputs to upload.");

            var uploader = new BlobUploader(blobPipelineOptions, memoryBufferChannel);

            var outputs = await resolutionPolicyHandler.ApplyResolutionPolicyAsync(tesNodeTask.Outputs);

            if (outputs != null)
            {
                var executionResult = await TimedExecutionAsync(async () => await uploader.UploadAsync(outputs));

                logger.LogInformation($"Executed Upload. Time elapsed: {executionResult.Elapsed} Bandwidth: {BlobSizeUtils.ToBandwidth(executionResult.Result, executionResult.Elapsed.TotalSeconds)} MiB/s");

                return executionResult.Result;
            }

            return 0;
        }

        public async Task<long> DownloadInputsAsync()
        {
            var memoryBufferChannel = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(blobPipelineOptions.MemoryBufferCapacity, blobPipelineOptions.BlockSize);

            return await DownloadInputsAsync(memoryBufferChannel);
        }

        private async Task<long> DownloadInputsAsync(Channel<byte[]> memoryBufferChannel)
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
                var executionResult = await TimedExecutionAsync(async () => await downloader.DownloadAsync(inputs));

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
            logger.LogInformation($"BlockSize:{blobPipelineOptions.BlockSize}");
        }

        private static async Task<TimeExecutionResult<T>> TimedExecutionAsync<T>(Func<Task<T>> execution)
        {
            var sw = new Stopwatch();
            sw.Start();
            var result = await execution();
            sw.Stop();

            return new TimeExecutionResult<T>(sw.Elapsed, result);
        }

        private record TimeExecutionResult<T>(TimeSpan Elapsed, T Result);
    }
}
