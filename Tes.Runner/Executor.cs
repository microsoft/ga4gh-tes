// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Tes.Runner.Docker;
using Tes.Runner.Models;
using Tes.Runner.Storage;
using Tes.Runner.Transfer;
using YamlDotNet.Serialization;

namespace Tes.Runner
{
    public class Executor
    {
        private readonly ILogger logger = PipelineLoggerFactory.Create<Executor>();
        private readonly NodeTask tesNodeTask;
        private readonly BlobPipelineOptions blobPipelineOptions;

        public Executor(string tesNodeTaskFilePath, BlobPipelineOptions blobPipelineOptions)
        {
            ArgumentException.ThrowIfNullOrEmpty(tesNodeTaskFilePath);
            ArgumentNullException.ThrowIfNull(blobPipelineOptions);

            this.blobPipelineOptions = blobPipelineOptions;

            var content = File.ReadAllText(tesNodeTaskFilePath);

            tesNodeTask = DeserializeNodeTask(content);
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

        private static NodeTask DeserializeNodeTask(string nodeTask)
        {
            var deserializer = new DeserializerBuilder().Build();

            var tesTask = deserializer.Deserialize<NodeTask>(nodeTask);

            return tesTask;
        }

        public async Task<long> UploadOutputsAsync()
        {
            var memoryBufferChannel = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(blobPipelineOptions.MemoryBufferCapacity, blobPipelineOptions.BlockSize);

            return await UploadOutputsAsync(memoryBufferChannel);
        }

        private async Task<long> UploadOutputsAsync(Channel<byte[]> memoryBufferChannel)
        {
            var uploader = new BlobUploader(blobPipelineOptions, memoryBufferChannel);

            var outputs = await ApplyResolutionPolicyAsync(tesNodeTask.Outputs);

            if (outputs != null)
            {
                var sw = new Stopwatch();
                sw.Start();
                var outputLength = await uploader.UploadAsync(outputs);
                sw.Stop();
                logger.LogInformation($"Executed Upload. Time elapsed:{sw.Elapsed} Bandwidth:{ToBandwidth(outputLength, sw.Elapsed.TotalSeconds)} MiB/s");

                return outputLength;
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

            logger.LogInformation($"{tesNodeTask.Inputs.Count} inputs to download.");

            var downloader = new BlobDownloader(blobPipelineOptions, memoryBufferChannel);

            var inputs = await ApplyResolutionPolicyAsync(tesNodeTask.Inputs);

            if (inputs != null)
            {
                var sw = new Stopwatch();
                sw.Start();
                var inputLength = await downloader.DownloadAsync(inputs);
                sw.Stop();

                logger.LogInformation($"Executed Download. Time elapsed:{sw.Elapsed} Bandwidth:{ToBandwidth(inputLength, sw.Elapsed.TotalSeconds)} MiB/s");

                return inputLength;
            }

            return 0;
        }

        private static double ToBandwidth(long length, double seconds)
        {
            return Math.Round(length / (1024d * 1024d) / seconds, 2);
        }

        private async Task<List<DownloadInfo>?> ApplyResolutionPolicyAsync(List<FileInput>? tesTaskInputs)
        {
            if (tesTaskInputs is null)
            {
                return null;
            }

            var list = new List<DownloadInfo>();

            foreach (var input in tesTaskInputs)
            {
                var uri = await ApplySasResolutionToUrlAsync(input.SourceUrl, input.SasStrategy);

                if (string.IsNullOrEmpty(input.FullFileName))
                {
                    throw new ArgumentException("A task input is missing the full filename. Please check the task definition.");
                }

                list.Add(new DownloadInfo(input.FullFileName, uri));
            }

            return list;
        }
        private async Task<List<UploadInfo>?> ApplyResolutionPolicyAsync(List<FileOutput>? testTaskOutputs)
        {
            if (testTaskOutputs is null)
            {
                return null;
            }

            var list = new List<UploadInfo>();

            foreach (var output in testTaskOutputs)
            {
                var uri = await ApplySasResolutionToUrlAsync(output.TargetUrl, output.SasStrategy);

                if (string.IsNullOrEmpty(output.FullFileName))
                {
                    throw new ArgumentException("A task output is missing the full filename. Please check the task definition.");
                }

                list.Add(new UploadInfo(output.FullFileName, uri));
            }

            return list;
        }

        private async Task<Uri> ApplySasResolutionToUrlAsync(string? sourceUrl, SasResolutionStrategy strategy)
        {
            ArgumentException.ThrowIfNullOrEmpty(sourceUrl);

            var strategyImpl =
                SasResolutionStrategyFactory.CreateSasResolutionStrategy(strategy);

            return await strategyImpl.CreateSasTokenWithStrategyAsync(sourceUrl);
        }
    }

    public record NodeTaskResult(ContainerExecutionResult ContainerResult, long InputsLength, long OutputsLength);
}
