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
        private readonly DockerExecutor dockerExecutor;
        private readonly ILogger logger;

        public Executor(DockerExecutor dockerExecutor)
        {
            this.dockerExecutor = dockerExecutor;
            logger = PipelineLoggerFactory.Create<Executor>();
        }

        public async Task<NodeTaskResult> ExecuteNodeTaskAsync(string nodeTask, BlobPipelineOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);
            ArgumentException.ThrowIfNullOrEmpty(nodeTask);

            var memoryBufferChannel = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(options.MemoryBufferCapacity, options.BlockSize);

            var tesTask = DeserializeNodeTask(nodeTask);

            var inputLength = await DownloadInputs(tesTask, options, memoryBufferChannel);

            var result = await dockerExecutor.RunOnContainerAsync(tesTask.ImageName, tesTask.ImageTag, tesTask.CommandsToExecute);

            var outputLength = await UploadOutputs(tesTask, options, memoryBufferChannel);

            return new NodeTaskResult(result, inputLength, outputLength);
        }

        private static NodeTask DeserializeNodeTask(string nodeTask)
        {
            var deserializer = new DeserializerBuilder().Build();

            var tesTask = deserializer.Deserialize<NodeTask>(nodeTask);

            return tesTask;
        }

        private async Task<long> UploadOutputs(NodeTask tesTask, BlobPipelineOptions options,
            Channel<byte[]> memoryBufferChannel)
        {
            var uploader = new BlobUploader(options, memoryBufferChannel);

            var sw = new Stopwatch();
            sw.Start();

            var outputs = await ApplyResolutionPolicyAsync(tesTask.Outputs);

            var outputLength = await uploader.UploadAsync(outputs);

            sw.Stop();
            logger.LogInformation($"Executed Upload. Time elapsed:{sw.Elapsed} Bandwidth:{ToBandwidth(outputLength, sw.Elapsed.TotalSeconds)} MiB/s");

            return outputLength;
        }

        private async Task<long> DownloadInputs(NodeTask tesTask, BlobPipelineOptions options,
            Channel<byte[]> memoryBufferChannel)
        {
            if (tesTask.Inputs is null || tesTask.Inputs.Count == 0)
            {
                logger.LogInformation("No inputs provided");
                return 0;
            }

            logger.LogInformation($"{tesTask.Inputs.Count} inputs to download.");

            var downloader = new BlobDownloader(options, memoryBufferChannel);

            var sw = new Stopwatch();
            sw.Start();

            var inputs = await ApplyResolutionPolicyAsync(tesTask.Inputs);

            var inputLength = await downloader.DownloadAsync(inputs);
            sw.Stop();

            logger.LogInformation($"Executed Download. Time elapsed:{sw.Elapsed} Bandwidth:{ToBandwidth(inputLength, sw.Elapsed.TotalSeconds)} MiB/s");

            return inputLength;
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
