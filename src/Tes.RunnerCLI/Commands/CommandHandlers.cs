// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Tes.Runner;
using Tes.Runner.Docker;
using Tes.Runner.Events;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    internal class CommandHandlers
    {
        private static readonly NodeTaskResolver nodeTaskUtils = new();
        private static readonly ILogger Logger = PipelineLoggerFactory.Create<CommandHandlers>();

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
            try
            {
                var duration = Stopwatch.StartNew();

                var nodeTask = await nodeTaskUtils.ResolveNodeTaskAsync(file, fileUri, apiVersion, saveDownload: true);
                file ??= new(CommandFactory.DefaultTaskDefinitionFile);

                await using var eventsPublisher = await EventsPublisher.CreateEventsPublisherAsync(nodeTask, apiVersion);

                try
                {
                    await eventsPublisher.PublishTaskCommencementEventAsync(nodeTask);

                    await RootCommandNodeCleanupAsync(nodeTask, dockerUri);

                    await ExecuteAllOperationsAsSubProcessesAsync(nodeTask, file, blockSize, writers, readers, bufferCapacity, apiVersion, dockerUri);

                    {
                        await using var executor = await Executor.CreateExecutorAsync(nodeTask, apiVersion);
                        await executor.AppendMetrics();
                    }

                    await eventsPublisher.PublishTaskCompletionEventAsync(nodeTask, duration.Elapsed,
                        EventsPublisher.SuccessStatus, errorMessage: string.Empty);
                }
                catch (Exception e)
                {
                    await eventsPublisher.PublishTaskCompletionEventAsync(nodeTask, duration.Elapsed,
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

        private static async Task RootCommandNodeCleanupAsync(Runner.Models.NodeTask nodeTask, Uri dockerUri)
        {
            Task[]? cleanupTasks = [];
            try
            {
                cleanupTasks =
                [
                    new DockerExecutor(dockerUri).NodeCleanupAsync(new(nodeTask.ImageName, nodeTask.ImageTag, default, default, default, new()), Logger),
                    Executor.RunnerHost.NodeCleanupPreviousTasksAsync()
                ];
                await Task.WhenAll(cleanupTasks);
            }
            catch (AggregateException ex)
            {
                foreach (var e in ex.InnerExceptions)
                {
                    Logger.LogWarning(e, "({ExceptionType}): {ExceptionMessage}\n{ExceptionStackTrace}", e.GetType().FullName, e.Message, e.StackTrace);
                }
            }
            catch (Exception)
            {
                foreach (var e in cleanupTasks?.Where(t => t.IsFaulted).Select(t => t.Exception) ?? [])
                {
                    Logger.LogWarning(e!, "({ExceptionType}): {ExceptionMessage}\n{ExceptionStackTrace}", e!.GetType().FullName, e!.Message, e!.StackTrace);
                }
            }
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
        internal static async Task<int> ExecuteExecCommandAsync(Uri? fileUri, FileInfo? file, string apiVersion, Uri dockerUri)
        {
            try
            {
                var nodeTask = await nodeTaskUtils.ResolveNodeTaskAsync(file, fileUri, apiVersion);

                Logger.LogDebug("Executing commands in container for Task ID: {NodeTaskId}", nodeTask.Id);

                await using var executor = await Executor.CreateExecutorAsync(nodeTask, apiVersion);

                var result = await executor.ExecuteNodeContainerTaskAsync(new DockerExecutor(dockerUri)) ?? throw new InvalidOperationException("The container task failed to return results");

                Logger.LogInformation("Docker container execution status code: {ContainerResultExitCode}", result.ContainerResult.ExitCode);

                if (!string.IsNullOrWhiteSpace(result.ContainerResult.Error))
                {
                    Logger.LogInformation("Docker container result error: {ContainerResultError}", result.ContainerResult.Error);
                }

                return result.ContainerResult.ExitCode switch
                {
                    var code when code == 0 => 0,
                    var code when code < 0 => 255,
                    var code when code > 255 => 255,
                    _ => (int)result.ContainerResult.ExitCode,
                };
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

            Logger.LogDebug("Starting upload operation.");

            var nodeTask = await nodeTaskUtils.ResolveNodeTaskAsync(file, fileUri, apiVersion);

            //TODO: Eventually all the options should come from the node runner task and we should remove the CLI flags as they are not used            
            var options = CommandLauncher.CreateBlobPipelineOptions(blockSize, writers, readers, bufferCapacity, apiVersion, nodeTask.RuntimeOptions?.SetContentMd5OnUpload ?? false);

            return await ExecuteTransferTaskAsync(nodeTask, exec => exec.UploadOutputsAsync(options), apiVersion);
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
            var options = CommandLauncher.CreateBlobPipelineOptions(blockSize, writers, readers, bufferCapacity, apiVersion, setContentMd5OnUploads: false);

            Logger.LogDebug("Starting download operation.");

            var nodeTask = await nodeTaskUtils.ResolveNodeTaskAsync(file, fileUri, apiVersion);

            return await ExecuteTransferTaskAsync(nodeTask, exec => exec.DownloadInputsAsync(options), apiVersion);
        }

        private static async Task ExecuteAllOperationsAsSubProcessesAsync(Runner.Models.NodeTask nodeTask, FileInfo file, int blockSize, int writers, int readers, int bufferCapacity,
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

            await CommandLauncher.LaunchTransferCommandAsSubProcessAsync(CommandFactory.DownloadCommandName, nodeTask, file, options);

            await CommandLauncher.LaunchesExecutorCommandAsSubProcessAsync(nodeTask, file, apiVersion, dockerUri);

            await CommandLauncher.LaunchTransferCommandAsSubProcessAsync(CommandFactory.UploadCommandName, nodeTask, file, options);
        }

        private static async Task<int> ExecuteTransferTaskAsync(Runner.Models.NodeTask nodeTask, Func<Executor, Task<long>> transferOperation, string apiVersion)
        {
            try
            {
                await using var executor = await Executor.CreateExecutorAsync(nodeTask, apiVersion);

                await transferOperation(executor);

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
