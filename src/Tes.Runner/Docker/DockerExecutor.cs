﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net;
using System.Text;
using CommonUtilities;
using CommonUtilities.Options;
using Docker.DotNet;
using Docker.DotNet.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tes.Runner.Authentication;
using Tes.Runner.Logs;
using Tes.Runner.Models;
using Tes.Runner.Transfer;
using static CommonUtilities.RetryHandler;

namespace Tes.Runner.Docker
{
    public class DockerExecutor
    {
        internal const string LastImageFile = "last-docker-image";
        private readonly IDockerClient dockerClient = null!;
        private readonly Host.IRunnerHost runnerHost = null!;
        private readonly ILogger logger = PipelineLoggerFactory.Create<DockerExecutor>();
        private readonly NetworkUtility networkUtility = new();
        private readonly AsyncRetryHandlerPolicy dockerPullRetryPolicy = null!;
        private readonly AsyncRetryHandlerPolicy gcrDockerPullRetryPolicy = null!;
        private readonly ContainerRegistryAuthorizationManager containerRegistryAuthorizationManager = null!;

        // Exception filter to exclude non-retriable errors from the docker daemon when attempting to pull images.
        private static readonly Func<DockerApiException, bool> IsNotAuthFailure = e =>
            // Immediately fail calls with either 'Unauthorized' or 'Forbidden' status codes
            !(e.StatusCode == HttpStatusCode.Unauthorized || e.StatusCode == HttpStatusCode.Forbidden ||
                // Immediately fail calls with a status code of 'InternalServerError' and
                // an HTTP body consisting of a JSON object with a single string property named "message" where the content is a colon-delimited string of three parts:
                // a user-readable rendition/description of the failure, the status code from the remote registry server, and the textual description of that status code
                // (often with a web link). Note that complete validation of the structure of the error report object is not performed.
                (e.StatusCode == HttpStatusCode.InternalServerError && (
                    (e.ResponseBody?.Contains(": unauthorized:", StringComparison.OrdinalIgnoreCase) ?? false) ||
                    (e.ResponseBody?.Contains(": forbidden:", StringComparison.OrdinalIgnoreCase) ?? false)
        )));

        private static readonly Predicate<DockerApiException> IsAuthFailure = e => !IsNotAuthFailure(e);

        // Workaround for GCR issue where NotFound is returned as a form of traffic control
        private static readonly Predicate<DockerApiException> IsGcrNotFound = e => e.StatusCode == HttpStatusCode.NotFound;

        const int LogStreamingMaxWaitTimeInSeconds = 30;

        public DockerExecutor(Uri dockerHost) : this(new DockerClientConfiguration(dockerHost)
            .CreateClient(), new ContainerRegistryAuthorizationManager(new CredentialsManager()), Executor.RunnerHost)
        { }

        // Retry for ~91s for ACR 1-minute throttle window
        internal static RetryPolicyOptions dockerPullRetryPolicyOptions = new()
        {
            MaxRetryCount = 6,
            ExponentialBackOffExponent = 2
        };

        public DockerExecutor(IDockerClient dockerClient, ContainerRegistryAuthorizationManager containerRegistryAuthorizationManager, Host.IRunnerHost runnerHost)
        {
            ArgumentNullException.ThrowIfNull(dockerClient);
            ArgumentNullException.ThrowIfNull(containerRegistryAuthorizationManager);
            ArgumentNullException.ThrowIfNull(runnerHost);

            this.dockerClient = dockerClient;
            this.containerRegistryAuthorizationManager = containerRegistryAuthorizationManager;
            this.runnerHost = runnerHost;

            dockerPullRetryPolicy = new RetryPolicyBuilder(Options.Create(dockerPullRetryPolicyOptions))
                .PolicyBuilder.OpinionatedRetryPolicy(Polly.Policy
                    .Handle(IsNotAuthFailure)
                    .Or<IOException>()
                    .Or<HttpRequestException>(e => e.InnerException is IOException || e.StatusCode >= HttpStatusCode.InternalServerError || e.StatusCode == HttpStatusCode.RequestTimeout))
                .WithRetryPolicyOptionsWait()
                .SetOnRetryBehavior(logger)
                .AsyncBuild();

            gcrDockerPullRetryPolicy = new RetryPolicyBuilder(Options.Create(dockerPullRetryPolicyOptions))
                .PolicyBuilder.OpinionatedRetryPolicy(Polly.Policy
                    .Handle<DockerApiException>(e => IsNotAuthFailure(e) || IsGcrNotFound(e))
                    .Or<IOException>()
                    .Or<HttpRequestException>(e => e.InnerException is IOException || e.StatusCode >= HttpStatusCode.InternalServerError || e.StatusCode == HttpStatusCode.RequestTimeout))
                .WithRetryPolicyOptionsWait()
                .SetOnRetryBehavior(logger)
                .AsyncBuild();
        }

        /// <summary>
        /// Parameter-less constructor for mocking
        /// </summary>
        protected DockerExecutor()
        { }


        private void SetLastImage(string imagePath)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(imagePath);
            runnerHost.WriteSharedFile(LastImageFile, Encoding.UTF8.GetBytes(imagePath));
        }

        private bool IsLastImageSame(string imagePath, out string? previousImage)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(imagePath);
            using var buffer = runnerHost.ReadSharedFile(LastImageFile);

            if (!buffer.IsDefault)
            {
                previousImage = Encoding.UTF8.GetString(buffer.Buffer!.Value.Span);
                return imagePath.Equals(previousImage, StringComparison.Ordinal);
            }
            else
            {
                previousImage = null;
                return false;
            }
        }

        public async Task NodeCleanupAsync(ExecutionOptions executionOptions, ILogger? cleanupLogger = default)
        {
            _ = await dockerClient.Volumes.PruneAsync();

            if (!string.IsNullOrWhiteSpace(executionOptions.ImageName))
            {
                if (!IsLastImageSame(ToImageNameWithTag(executionOptions.ImageName, executionOptions.Tag), out var previousImage) && !string.IsNullOrWhiteSpace(previousImage))
                {
                    try
                    {
                        await DeleteImageAsync(previousImage);
                    }
                    catch (DockerApiException ex) when (ex.StatusCode == HttpStatusCode.BadRequest && ex.ResponseBody.Contains("invalid reference format", StringComparison.OrdinalIgnoreCase))
                    {
                        cleanupLogger?.LogWarning(ex, "Unable to cleanup previous image due to invalid reference format.");
                    }
                    // Note: all other exceptions are caught and logged by the caller, as they are considered fatal to the effort to cleanup the node.
                }
            }
        }

        public virtual async Task<ContainerExecutionResult> RunOnContainerAsync(ExecutionOptions executionOptions, Func<string, Task<IStreamLogReader>> logPublisherFactory)
        {
            ArgumentNullException.ThrowIfNull(executionOptions);
            ArgumentException.ThrowIfNullOrEmpty(executionOptions.ImageName);
            ArgumentNullException.ThrowIfNull(executionOptions.CommandsToExecute);

            Stream? stdIn = default;
            Stream? stdErr = default;
            Stream? stdOut = default;

            try
            {
                stdIn = GetFileStream(executionOptions.ContainerStdIn, forWrite: false);
                stdErr = GetFileStream(executionOptions.ContainerStdErr, forWrite: true);
                stdOut = GetFileStream(executionOptions.ContainerStdOut, forWrite: true);

                try
                {
                    try
                    {
                        await PullImageWithRetriesAsync(executionOptions.ImageName, executionOptions.Tag);
                    }
                    catch (DockerApiException e) when (IsAuthFailure(e))
                    {
                        var authConfig = await containerRegistryAuthorizationManager.TryGetAuthConfigForAzureContainerRegistryAsync(executionOptions.ImageName, executionOptions.Tag, executionOptions.RuntimeOptions);

                        if (authConfig is not null)
                        {
                            await PullImageWithRetriesAsync(executionOptions.ImageName, executionOptions.Tag, authConfig);
                        }
                        else
                        {
                            throw;
                        }
                    }
                }
                catch
                {
                    _ = await dockerClient.Images.PruneImagesAsync();
                    throw;
                }

                var imageWithTag = ToImageNameWithTag(executionOptions.ImageName, executionOptions.Tag);
                SetLastImage(imageWithTag);

                await ConfigureNetworkAsync();

                var createResponse = await CreateContainerAsync(imageWithTag, executionOptions.CommandsToExecute, executionOptions.VolumeBindings, executionOptions.WorkingDir,
                    stdIn is not null, executionOptions.ContainerEnv, executionOptions.ContainerDeviceRequests);
                _ = await dockerClient.Containers.InspectContainerAsync(createResponse.ID);

                var streamLogReader = await logPublisherFactory("task-executor-1");

                var logs = await StartContainerWithStreamingOutput(createResponse, stdIn is not null);

                streamLogReader.StartReadingFromLogStreams(logs, stdIn, stdOut, stdErr);

                var runResponse = await dockerClient.Containers.WaitContainerAsync(createResponse.ID);

                await streamLogReader.WaitUntilAsync(TimeSpan.FromSeconds(LogStreamingMaxWaitTimeInSeconds));

                return new ContainerExecutionResult(createResponse.ID, runResponse.Error?.Message, runResponse.StatusCode);
            }
            finally
            {
                stdOut?.Dispose();
                stdErr?.Dispose();
                stdIn?.Dispose();
            }
        }

        private async Task<MultiplexedStream> StartContainerWithStreamingOutput(CreateContainerResponse createResponse, bool streamStdIn)
        {
            var logs = await StreamStdOutAndErrorAsync(createResponse.ID, streamStdIn);

            await dockerClient.Containers.StartContainerAsync(createResponse.ID,
                new ContainerStartParameters());
            return logs;
        }

        private async Task<MultiplexedStream> StreamStdOutAndErrorAsync(string containerId, bool streamStdIn)
        {
            return await dockerClient.Containers.AttachContainerAsync(
                containerId,
                false,
                new ContainerAttachParameters
                {
                    Stream = true,
                    Stdin = streamStdIn,
                    Stdout = true,
                    Stderr = true
                });
        }

        private async Task<CreateContainerResponse> CreateContainerAsync(string imageWithTag,
            List<string> commandsToExecute, List<string>? volumeBindings, string? workingDir, bool streamStdIn,
            IDictionary<string, string>? env = default, List<ContainerDeviceRequest>? deviceRequests = default)
        {
            logger.LogInformation(@"Creating container with image name: {ImageWithTag}", imageWithTag);

            var createResponse = await dockerClient.Containers.CreateContainerAsync(
                new()
                {
                    Image = imageWithTag,
                    Cmd = commandsToExecute,
                    Env = env?.Select(pair => $"{pair.Key}={pair.Value}").ToList(),
                    AttachStdin = false,
                    AttachStdout = false,
                    AttachStderr = false,
                    OpenStdin = streamStdIn,
                    StdinOnce = streamStdIn,
                    WorkingDir = workingDir,
                    HostConfig = new()
                    {
                        AutoRemove = true,
                        Binds = volumeBindings,
                        DeviceRequests = deviceRequests?
                            .Select(request => new DeviceRequest()
                            {
                                Driver = request.Driver ?? default,
                                Count = request.Count ?? default,
                                DeviceIDs = request.DeviceIDs ?? default,
                                Capabilities = request.Capabilities ?? default,
                                Options = request.Options ?? default,
                            })
                            .ToList()
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

            // https://docs.docker.com/engine/reference/commandline/tag/#description
            var separator = imageTag.Contains(':') ? "@" : ":";
            return $"{imageName}{separator}{imageTag}";
        }

        private async Task PullImageWithRetriesAsync(string imageName, string? tag, AuthConfig? authConfig = null)
        {
            logger.LogInformation(@"Pulling image name: {ImageName} image tag: {ImageTag}", imageName, tag);

            await GetPolicy(imageName).ExecuteWithRetryAsync(
                () => dockerClient.Images.CreateImageAsync(
                    new ImagesCreateParameters() { FromImage = imageName, Tag = tag },
                    authConfig,
                    new Progress<JSONMessage>(message => logger.LogDebug("{ProgressStatus}", message.Status))));

            AsyncRetryHandlerPolicy GetPolicy(string imageName)
            {
                var imageNameParts = imageName.Split('/', 2);
                return imageNameParts.Length > 1 && imageNameParts[0].EndsWith(".gcr.io", StringComparison.OrdinalIgnoreCase)
                    ? gcrDockerPullRetryPolicy
                    : dockerPullRetryPolicy;
            }
        }

        private async Task DeleteImageAsync(string imageName)
        {
            try
            {
                await dockerClient.Images.DeleteImageAsync(imageName, new ImageDeleteParameters { Force = true });
                logger.LogInformation(@"Deleted Docker image {ImageName}", imageName);
            }
            catch (Exception e)
            {
                logger.LogError(@"Failed to delete image {ImageName}. Error: {ErrorMessage}", imageName, e.Message);
                throw;
            }
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

        private static FileStream? GetFileStream(string? path, bool forWrite)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                return default;
            }

            var file = Executor.RunnerHost.GetTaskWorkingHostFile(path);

            if (file.Exists || forWrite)
            {
                return forWrite
                    ? file.Open(FileMode.Append)
                    : file.OpenRead();
            }
            else
            {
                throw new FileNotFoundException(null, path);
            }
        }
    }

    public record ExecutionOptions(string? ImageName, string? Tag, List<string>? CommandsToExecute,
        List<string>? VolumeBindings, string? WorkingDir, RuntimeOptions RuntimeOptions,
        List<ContainerDeviceRequest>? ContainerDeviceRequests, Dictionary<string, string>? ContainerEnv = default,
        string? ContainerStdIn = default, string? ContainerStdOut = default, string? ContainerStdErr = default);
}
