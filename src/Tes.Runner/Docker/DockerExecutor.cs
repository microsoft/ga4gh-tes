// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Docker.DotNet;
using Docker.DotNet.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tes.ApiClients;
using Tes.ApiClients.Options;
using Tes.Runner.Logs;
using Tes.Runner.Transfer;

namespace Tes.Runner.Docker
{
    public class DockerExecutor
    {
        private readonly IDockerClient dockerClient = null!;
        private readonly ILogger logger = PipelineLoggerFactory.Create<DockerExecutor>();
        private readonly NetworkUtility networkUtility = new NetworkUtility();
        private readonly RetryHandler retryHandler = new RetryHandler(Options.Create(new RetryPolicyOptions()));
        private readonly IStreamLogReader streamLogReader = null!;

        const int LogStreamingMaxWaitTimeInSeconds = 30;

        public DockerExecutor(Uri dockerHost) : this(new DockerClientConfiguration(dockerHost)
            .CreateClient(), new ConsoleStreamLogReader())
        {
        }

        public DockerExecutor(IDockerClient dockerClient, IStreamLogReader streamLogReader)
        {
            ArgumentNullException.ThrowIfNull(dockerClient);
            ArgumentNullException.ThrowIfNull(streamLogReader);

            this.dockerClient = dockerClient;
            this.streamLogReader = streamLogReader;
        }

        /// <summary>
        /// Parameter-less constructor for mocking
        /// </summary>
        protected DockerExecutor()
        {

        }

        public virtual async Task<ContainerExecutionResult> RunOnContainerAsync(string? imageName, string? tag, List<string>? commandsToExecute, List<string>? volumeBindings, string? workingDir)
        {
            ArgumentException.ThrowIfNullOrEmpty(imageName);
            ArgumentNullException.ThrowIfNull(commandsToExecute);

            await PullImageWithRetriesAsync(imageName, tag);

            await ConfigureNetworkAsync();

            var createResponse = await CreateContainerAsync(imageName, tag, commandsToExecute, volumeBindings, workingDir);

            await dockerClient.Containers.StartContainerAsync(createResponse.ID,
                new ContainerStartParameters());

            // var logs = await StartContainerWithStreamingOutput(createResponse);
            //
            // streamLogReader.StartReadingFromLogStreams(logs);

            var runResponse = await dockerClient.Containers.WaitContainerAsync(createResponse.ID);

            await streamLogReader.WaitUntilAsync(TimeSpan.FromSeconds(LogStreamingMaxWaitTimeInSeconds));

            return new ContainerExecutionResult(createResponse.ID, runResponse.Error?.Message, runResponse.StatusCode);
        }


        private async Task<MultiplexedStream> StartContainerWithStreamingOutput(CreateContainerResponse createResponse)
        {
            var logs = await StreamStdOutAndErrorAsync(createResponse.ID);

            await dockerClient.Containers.StartContainerAsync(createResponse.ID,
                new ContainerStartParameters());
            return logs;
        }

        private async Task<MultiplexedStream> StreamStdOutAndErrorAsync(string containerId)
        {
            return await dockerClient.Containers.AttachContainerAsync(
                containerId,
                false,
                new ContainerAttachParameters
                {
                    Stream = true,
                    Stdout = true,
                    Stderr = true
                });
        }

        private async Task<CreateContainerResponse> CreateContainerAsync(string imageName, string? imageTag,
            List<string> commandsToExecute, List<string>? volumeBindings, string? workingDir)
        {
            var imageWithTag = ToImageNameWithTag(imageName, imageTag);
            logger.LogInformation($"Creating container with image name: {imageWithTag}");

            var createResponse = await dockerClient.Containers.CreateContainerAsync(
                new CreateContainerParameters
                {
                    Image = imageWithTag,
                    Cmd = commandsToExecute,
                    AttachStdout = true,
                    AttachStderr = true,
                    WorkingDir = workingDir,
                    HostConfig = new HostConfig
                    {
                        AutoRemove = true,
                        Binds = volumeBindings
                    }
                });
            return createResponse;
        }

        private static string ToImageNameWithTag(string imageName, string? imageTag)
        {
            if (string.IsNullOrWhiteSpace(imageTag))
            {
                return imageName;
            }

            return $"{imageName}:{imageTag}";
        }

        private async Task PullImageWithRetriesAsync(string imageName, string? tag, AuthConfig? authConfig = null)
        {
            logger.LogInformation($"Pulling image name: {imageName} image tag: {tag}");

            await retryHandler.AsyncRetryPolicy.ExecuteAsync(async () =>
            {
                await dockerClient.Images.CreateImageAsync(
                    new ImagesCreateParameters() { FromImage = imageName, Tag = tag },
                    authConfig,
                    new Progress<JSONMessage>(message => logger.LogDebug(message.Status)));
            });
        }

        /// <summary>
        /// Configures the host machine's network security prior to running user code
        /// </summary>
        private async Task ConfigureNetworkAsync()
        {
            await BlockDockerContainerAccessToAzureInstanceMetadataService();
        }

        /// <summary>
        /// Blocks access to IMDS via the iptables command
        /// </summary>
        private async Task BlockDockerContainerAccessToAzureInstanceMetadataService()
        {
            const string imdsIpAddress = "169.254.169.254"; // https://learn.microsoft.com/en-us/azure/virtual-machines/instance-metadata-service

            await networkUtility.BlockIpAddressAsync(imdsIpAddress);
        }
    }
}
