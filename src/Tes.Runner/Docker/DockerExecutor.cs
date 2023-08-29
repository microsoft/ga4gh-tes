// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Docker.DotNet;
using Docker.DotNet.Models;
using Microsoft.Extensions.Logging;
using Tes.Runner.Transfer;

namespace Tes.Runner.Docker
{
    public class DockerExecutor
    {
        private readonly IDockerClient dockerClient;
        private readonly ILogger logger = PipelineLoggerFactory.Create<DockerExecutor>();
        private readonly NetworkUtility networkUtility = new NetworkUtility();

        public DockerExecutor(Uri dockerHost)
        {
            dockerClient = new DockerClientConfiguration(dockerHost)
                .CreateClient();
        }

        public async Task<ContainerExecutionResult> RunOnContainerAsync(string? imageName, string? tag, List<string>? commandsToExecute, List<string>? volumeBindings)
        {
            ArgumentException.ThrowIfNullOrEmpty(imageName);
            ArgumentException.ThrowIfNullOrEmpty(tag);
            ArgumentNullException.ThrowIfNull(commandsToExecute);

            await PullImageAsync(imageName, tag);

            await ConfigureNetworkAsync();

            var createResponse = await CreateContainerAsync(imageName, commandsToExecute, volumeBindings);

            var logs = await StartContainerWithStreamingOutput(createResponse);

            var runResponse = await dockerClient.Containers.WaitContainerAsync(createResponse.ID);

            return new ContainerExecutionResult(createResponse.ID, runResponse.Error?.Message, runResponse.StatusCode, logs);
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

        private async Task<CreateContainerResponse> CreateContainerAsync(string imageName, List<string> commandsToExecute, List<string>? volumeBindings)
        {
            var createResponse = await dockerClient.Containers.CreateContainerAsync(
                new CreateContainerParameters
                {
                    Image = imageName,
                    Entrypoint = commandsToExecute,
                    AttachStdout = true,
                    AttachStderr = true,
                    WorkingDir = "/",
                    HostConfig = new HostConfig
                    {
                        Binds = volumeBindings
                    }
                });
            return createResponse;
        }

        private async Task PullImageAsync(string imageName, string tag, AuthConfig? authConfig = null)
        {
            await dockerClient.Images.CreateImageAsync(
                new ImagesCreateParameters()
                {
                    FromImage = imageName,
                    Tag = tag
                },
                authConfig,
                new Progress<JSONMessage>(message => logger.LogInformation(message.Status)));
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
