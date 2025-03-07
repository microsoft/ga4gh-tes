// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
using CommonUtilities;
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
                    new DockerExecutor(dockerUri).NodeCleanupAsync(new(nodeTask.ImageName, nodeTask.ImageTag, default, default, default, new(), default), Logger),
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
        /// Start task command. Executes all operations (download, scripts) as sub-processes.
        /// </summary>
        /// <param name="fileUri">Node task definition uri</param>
        /// <param name="file">Node task definition file</param>
        /// <param name="blockSize">Blob block size in bytes</param>
        /// <param name="writers">Number of concurrent writers</param>
        /// <param name="readers">Number of concurrent readers</param>
        /// <param name="bufferCapacity">Pipeline buffer capacity</param>
        /// <param name="apiVersion">Azure Storage API version</param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Interoperability", "CA1416:Validate platform compatibility", Justification = "This tool is currently only supported on Linux.")]
        internal static async Task<int> ExecuteStartTaskCommandAsync(
            Uri? fileUri,
            FileInfo? file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion)
        {
            try
            {
                var duration = Stopwatch.StartNew();

                var nodeTask = await nodeTaskUtils.ResolveNodeTaskAsync(file, fileUri, apiVersion, saveDownload: false);
                file ??= new(CommandFactory.DefaultTaskDefinitionFile);

                nodeTask.Id = Environment.GetEnvironmentVariable("AZ_BATCH_NODE_ID") ?? new Guid().ToString();

                nodeTask.RuntimeOptions.StorageEventSink = PersonalizeStorageTargetLocation(nodeTask.RuntimeOptions.StorageEventSink);
                nodeTask.RuntimeOptions.StreamingLogPublisher = PersonalizeStorageTargetLocation(nodeTask.RuntimeOptions.StreamingLogPublisher);
                nodeTask.TaskOutputs?.ForEach(output => output.TargetUrl = ExpandEnvironmentVariablesInUriPath(output.TargetUrl));

                await using var eventsPublisher = await EventsPublisher.CreateEventsPublisherAsync(nodeTask, apiVersion);

                try
                {
                    await eventsPublisher.PublishTaskCommencementEventAsync(nodeTask);

                    var options =
                        BlobPipelineOptionsConverter.ToBlobPipelineOptions(blockSize, writers, readers, bufferCapacity,
                            apiVersion);

                    try
                    {
                        if (nodeTask.StartTask?.StartTaskScripts is not null)
                        {
                            Runner.Models.NodeTask download = new()
                            {
                                Id = nodeTask.Id,
                                Inputs = nodeTask.StartTask.StartTaskScripts?
                                    .Select(script => new Runner.Models.FileInput() { SourceUrl = script.SourceUrl, Path = script.Path, TransformationStrategy = script.TransformationStrategy })
                                    .ToList(),
                                RuntimeOptions = nodeTask.RuntimeOptions,
                            };

                            if (download.Inputs is not null)
                            {
                                await File.WriteAllTextAsync(file.FullName, System.Text.Json.JsonSerializer.Serialize(download, System.Text.Json.JsonSerializerOptions.Default));
                                file.Refresh();
                                await ExecuteDownloadsAsSubProcessesAsync(download, file, options);

                                if (!OperatingSystem.IsWindows())
                                {
                                    (nodeTask.StartTask.StartTaskScripts ?? [])
                                        .Where(script => script.SetExecute)
                                        .Select(script => new FileInfo(script.Path!))
                                        .Where(file => file.Exists)
                                        .ForEach(file => file.UnixFileMode |= UnixFileMode.UserExecute | UnixFileMode.GroupExecute);
                                }
                            }

                            foreach (var script in (nodeTask.StartTask.StartTaskScripts ?? []).Where(script => script.Run))
                            {
                                ProcessStartInfo startInfo = new(new FileInfo(script.Path!).FullName, [])
                                {
                                    CreateNoWindow = true,
                                    ErrorDialog = false,
                                    UseShellExecute = false,
                                };

                                using var process = Process.Start(startInfo) ?? throw new InvalidOperationException("Unable to start script.");

                                await process.WaitForExitAsync();

                                if (process.ExitCode != 0)
                                {
                                    throw new CommandExecutionException(process.ExitCode, "Start task script failed.");
                                }
                            }
                        }
                    }
                    finally
                    {
                        try
                        {
                            await using var executor = await Executor.CreateExecutorAsync(nodeTask, apiVersion);
                            await executor.UploadTaskOutputsAsync(options);
                        }
                        catch (Exception e)
                        {
                            Logger.LogError(e, "Failed to perform transfer. Operation: {TransferOperation}", nameof(Executor.UploadTaskOutputsAsync));
                        }
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

        static Runner.Models.StorageTargetLocation? PersonalizeStorageTargetLocation(Runner.Models.StorageTargetLocation? location)
            => location is null ? default : new() { TransformationStrategy = location.TransformationStrategy, TargetUrl = ExpandEnvironmentVariablesInUriPath(location.TargetUrl)! };

        static string? ExpandEnvironmentVariablesInUriPath(string? uri)
        {
            if (uri is null)
            {
                return default;
            }

            UriBuilder builder = new(uri);

            if (!string.IsNullOrEmpty(builder.Path))
            {
                builder.Path = Environment.ExpandEnvironmentVariables(System.Web.HttpUtility.UrlDecode(builder.Path));
            }

            return builder.Uri.AbsoluteUri;
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

                Logger.LogTrace("Executing commands in container for Task ID: {NodeTaskId}", nodeTask.Id);

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

            Logger.LogTrace("Starting upload operation.");

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

            Logger.LogTrace("Starting download operation.");

            var nodeTask = await nodeTaskUtils.ResolveNodeTaskAsync(file, fileUri, apiVersion);

            return await ExecuteTransferTaskAsync(nodeTask, exec => exec.DownloadInputsAsync(options), apiVersion);
        }

        private static async Task ExecuteDownloadsAsSubProcessesAsync(Runner.Models.NodeTask nodeTask, FileInfo file, BlobPipelineOptions options)
        {
            ArgumentNullException.ThrowIfNull(nodeTask);
            ArgumentNullException.ThrowIfNull(file);

            if (!file.Exists)
            {
                throw new ArgumentException($"Node task definition file '{file.FullName}' not found.", nameof(file));
            }

            await CommandLauncher.LaunchTransferCommandAsSubProcessAsync(CommandFactory.DownloadCommandName, nodeTask, file, options);
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

            try
            {
                await CommandLauncher.LaunchTransferCommandAsSubProcessAsync(CommandFactory.DownloadCommandName, nodeTask, file, options);

                await CommandLauncher.LaunchesExecutorCommandAsSubProcessAsync(nodeTask, file, apiVersion, dockerUri);

                await CommandLauncher.LaunchTransferCommandAsSubProcessAsync(CommandFactory.UploadCommandName, nodeTask, file, options);
            }
            finally
            {
                try
                {
                    await using var executor = await Executor.CreateExecutorAsync(nodeTask, apiVersion);
                    await executor.AppendMetrics();
                    await executor.UploadTaskOutputsAsync(options);
                }
                catch (Exception e)
                {
                    Logger.LogError(e, "Failed to perform transfer. Operation: {TransferOperation}", nameof(Executor.UploadTaskOutputsAsync));
                }
            }
        }

        private static async Task<int> ExecuteTransferTaskAsync(Runner.Models.NodeTask nodeTask, Func<Executor, Task<long>> transferOperation, string apiVersion)
        {
            try
            {
                await using var executor = await Executor.CreateExecutorAsync(nodeTask, apiVersion);

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
