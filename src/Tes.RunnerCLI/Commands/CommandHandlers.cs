// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Tes.Runner;
using Tes.Runner.Docker;
using Tes.Runner.Events;

namespace Tes.RunnerCLI.Commands
{
    internal class CommandHandlers(Runner.Models.NodeTask nodeTask, [FromKeyedServices(Executor.ApiVersion)] string apiVersion, Executor executor, CommandLauncher commandLauncher, Func<Uri, DockerExecutor> dockerExecutor, Lazy<Task<EventsPublisher>> eventsPublisher, ILogger<CommandHandlers> logger)
    {
        private readonly ILogger Logger = logger ?? throw new ArgumentNullException(nameof(logger));
        private readonly Runner.Models.NodeTask nodeTask = nodeTask ?? throw new ArgumentNullException(nameof(nodeTask));
        private readonly string apiVersion = apiVersion ?? throw new ArgumentNullException(nameof(apiVersion));
        private readonly Executor executor = executor ?? throw new ArgumentNullException(nameof(executor));
        private readonly CommandLauncher commandLauncher = commandLauncher ?? throw new ArgumentNullException(nameof(commandLauncher));
        private readonly Func<Uri, DockerExecutor> dockerExecutor = dockerExecutor ?? throw new ArgumentNullException(nameof(dockerExecutor));
        private readonly Task<EventsPublisher> eventsPublisher = (eventsPublisher ?? throw new ArgumentNullException(nameof(eventsPublisher))).Value;

        /// <summary>
        /// Root command of the CLI. Executes all operations (download, executor, upload) as sub-processes.
        /// </summary>
        /// <param name="fileUri">Node task definition uri</param>
        /// <param name="file">Node task definition file</param>
        /// <param name="blockSize">Blob block size in bytes</param>
        /// <param name="writers">Number of concurrent writers</param>
        /// <param name="readers">Number of concurrent readers</param>
        /// <param name="bufferCapacity">Pipeline buffer capacity</param>
        /// <param name="apiVersion">Azure Storage API version</param>
        /// <param name="dockerUri">Local docker engine endpoint</param>
        /// <returns></returns>
        internal static async Task<int> ExecuteRootCommandAsync(
            Uri? fileUri,
            FileInfo? file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion,
            Uri dockerUri)
        {
            var duration = Stopwatch.StartNew();

            var nodeTask = await Services.Create<NodeTaskResolver>(logger => new(logger))
                .ResolveNodeTaskAsync(file, fileUri, apiVersion, saveDownload: true);
            file ??= new(CommandFactory.DefaultTaskDefinitionFile);

            return await Services.BuildAndRunAsync<CommandHandlers, int>(
                handler => handler.ExecuteRootCommandAsync(fileUri, file, blockSize, writers, readers, bufferCapacity, dockerUri, duration),
                Services.ConfigureParameters(nodeTask, apiVersion));
        }

        private async Task<int> ExecuteRootCommandAsync(
            Uri? fileUri,
            FileInfo file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            Uri dockerUri,
            Stopwatch duration)
        {
            try
            {
                await using var publisher = await eventsPublisher;

                try
                {
                    await publisher.PublishTaskCommencementEventAsync(nodeTask);

                    await RootCommandNodeCleanupAsync(nodeTask, dockerUri);

                    await ExecuteAllOperationsAsSubProcessesAsync(nodeTask, file, blockSize, writers, readers, bufferCapacity, apiVersion, dockerUri);

                    await executor.AppendMetrics();

                    await publisher.PublishTaskCompletionEventAsync(nodeTask, duration.Elapsed,
                        EventsPublisher.SuccessStatus, errorMessage: string.Empty);
                }
                catch (Exception e)
                {
                    await publisher.PublishTaskCompletionEventAsync(nodeTask, duration.Elapsed,
                        EventsPublisher.FailedStatus, errorMessage: e.Message);
                    throw;
                }
            }
            catch (Exception e)
            {
                try
                {
                    CommandLauncher.HandleFatalLauncherError(CommandFactory.ExecutorCommandName, e);
                }
                catch (CommandExecutionException commandExecutionException)
                {
                    Logger.LogError(commandExecutionException, "Failed to execute Node Task: {NodeTaskPath}", file?.FullName ?? fileUri?.AbsoluteUri ?? "<missing>");
                    return commandExecutionException.ExitCode;
                }

                Logger.LogError(e, "Failed to execute Node Task: {NodeTaskPath}", file?.FullName ?? fileUri?.AbsoluteUri ?? "<missing>");

                return (int)ProcessExitCode.UncategorizedError;
            }

            return (int)ProcessExitCode.Success;
        }

        private async Task RootCommandNodeCleanupAsync(Runner.Models.NodeTask nodeTask, Uri dockerUri)
        {
            await dockerExecutor(dockerUri).NodeCleanupAsync(new(nodeTask.ImageName, nodeTask.ImageTag, default, default, default, new()));
            await Executor.RunnerHost.NodeCleanupPreviousTasksAsync();
        }

        /// <summary>
        /// Executor (exec) command. Executes the executor operation as defined in the node task definition file.
        /// </summary>
        /// <param name="fileUri">Node task definition uri</param>
        /// <param name="file">Node task definition file</param>
        /// <param name="apiVersion">Azure Storage API version</param>
        /// <param name="dockerUri">Local docker engine endpoint</param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        internal static async Task<long> ExecuteExecCommandAsync(Uri? fileUri, FileInfo? file, string apiVersion, Uri dockerUri)
        {
            var nodeTask = await Services.Create<NodeTaskResolver>(logger => new(logger))
                .ResolveNodeTaskAsync(file, fileUri, apiVersion, saveDownload: false);

            return await Services.BuildAndRunAsync<CommandHandlers, long>(
                handler => handler.ExecuteExecCommandAsync(dockerUri),
                Services.ConfigureParameters(nodeTask, apiVersion));
        }


        private async Task<long> ExecuteExecCommandAsync(Uri dockerUri)
        {
            try
            {
                Logger.LogDebug("Executing commands in container for Task ID: {NodeTaskId}", nodeTask.Id);

                var result = await executor.ExecuteNodeContainerTaskAsync(dockerExecutor(dockerUri)) ?? throw new InvalidOperationException("The container task failed to return results");

                Logger.LogInformation("Docker container execution status code: {ContainerResultExitCode}", result.ContainerResult.ExitCode);

                if (!string.IsNullOrWhiteSpace(result.ContainerResult.Error))
                {
                    Logger.LogInformation("Docker container result error: {ContainerResultError}", result.ContainerResult.Error);
                }

                return result.ContainerResult.ExitCode;
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Failed to execute the task.");
                throw;
            }
        }

        /// <summary>
        /// Upload (upload) command. Executes the upload of outputs as defined in the node task definition file.
        /// </summary>
        /// <param name="fileUri">Node task definition uri</param>
        /// <param name="file">Node task definition file</param>
        /// <param name="blockSize">Blob block size in bytes</param>
        /// <param name="writers">Number of concurrent writers</param>
        /// <param name="readers">Number of concurrent readers</param>
        /// <param name="bufferCapacity">Pipeline buffer capacity</param>
        /// <param name="apiVersion">Azure Storage API version</param>
        /// <returns></returns>
        internal static async Task<int> ExecuteUploadCommandAsync(
            Uri? fileUri,
            FileInfo file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion)
        {
            var nodeTask = await Services.Create<NodeTaskResolver>(logger => new(logger))
                .ResolveNodeTaskAsync(file, fileUri, apiVersion, saveDownload: false);

            return await Services.BuildAndRunAsync<CommandHandlers, int>(
                handler => handler.ExecuteUploadCommandAsync(blockSize, writers, readers, bufferCapacity),
                Services.ConfigureParameters(nodeTask, apiVersion));
        }

        private async Task<int> ExecuteUploadCommandAsync(
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity)
        {
            //TODO: Eventually all the options should come from the node runner task and we should remove the CLI flags as they are not used
            var options = CommandLauncher.CreateBlobPipelineOptions(blockSize, writers, readers, bufferCapacity, apiVersion, nodeTask.RuntimeOptions?.SetContentMd5OnUpload ?? false);

            Logger.LogDebug("Starting upload operation.");

            return await ExecuteTransferTaskAsync(exec => exec.UploadOutputsAsync(options));
        }

        /// <summary>
        /// Download (download) command. Executes the download of inputs as defined in the node task definition file.
        /// </summary>
        /// <param name="fileUri">Node task definition uri</param>
        /// <param name="file">Node task definition file</param>
        /// <param name="blockSize">Blob block size in bytes</param>
        /// <param name="writers">Number of concurrent writers</param>
        /// <param name="readers">Number of concurrent readers</param>
        /// <param name="bufferCapacity">Pipeline buffer capacity</param>
        /// <param name="apiVersion">Azure Storage API version</param>
        /// <returns></returns>
        internal static async Task<int> ExecuteDownloadCommandAsync(
            Uri? fileUri,
            FileInfo file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion)
        {
            var nodeTask = await Services.Create<NodeTaskResolver>(logger => new(logger))
                .ResolveNodeTaskAsync(file, fileUri, apiVersion, saveDownload: false);

            return await Services.BuildAndRunAsync<CommandHandlers, int>(
                handler => handler.ExecuteDownloadCommandAsync(blockSize, writers, readers, bufferCapacity),
                Services.ConfigureParameters(nodeTask, apiVersion));
        }

        private async Task<int> ExecuteDownloadCommandAsync(
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity)
        {
            var options = CommandLauncher.CreateBlobPipelineOptions(blockSize, writers, readers, bufferCapacity, apiVersion, setContentMd5OnUploads: false);

            Logger.LogDebug("Starting download operation.");

            return await ExecuteTransferTaskAsync(exec => exec.DownloadInputsAsync(options));
        }

        private async Task ExecuteAllOperationsAsSubProcessesAsync(Runner.Models.NodeTask nodeTask, FileInfo file, int blockSize, int writers, int readers, int bufferCapacity,
            string apiVersion, Uri dockerUri)
        {
            ArgumentNullException.ThrowIfNull(nodeTask);
            ArgumentNullException.ThrowIfNull(file);

            if (!file.Exists)
            {
                throw new ArgumentException($"Node task definition file '{file.FullName}' not found.", nameof(file));
            }

            var options =
                BlobPipelineOptionsConverter.ToBlobPipelineOptions(blockSize, writers, readers, bufferCapacity,
                    apiVersion);

            await commandLauncher.LaunchTransferCommandAsSubProcessAsync(CommandFactory.DownloadCommandName, nodeTask, file, options);

            await commandLauncher.LaunchesExecutorCommandAsSubProcessAsync(nodeTask, file, apiVersion, dockerUri);

            await commandLauncher.LaunchTransferCommandAsSubProcessAsync(CommandFactory.UploadCommandName, nodeTask, file, options);
        }

        private async Task<int> ExecuteTransferTaskAsync(Func<Executor, Task<long>> transferOperation)
        {
            try
            {
                _ = await transferOperation(executor);

                return (int)ProcessExitCode.Success;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to perform transfer. Error: {e.Message} Operation: {transferOperation.Method.Name}");
                Logger.LogError(e, "Failed to perform transfer. Operation: {TransferOperation}", transferOperation.Method.Name);
                return (int)ProcessExitCode.UncategorizedError;
            }
        }
    }
}
