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
        private static readonly ILogger Logger = PipelineLoggerFactory.Create<CommandHandlers>();

        /// <summary>
        /// Root command of the CLI. Executes all operations (download, executor, upload) as sub-processes.
        /// </summary>
        /// <param name="file"></param>
        /// <param name="blockSize"></param>
        /// <param name="writers"></param>
        /// <param name="readers"></param>
        /// <param name="bufferCapacity"></param>
        /// <param name="apiVersion"></param>
        /// <param name="dockerUri"></param>
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

                var nodeTask = await ResolveNodeTaskAsync(file, fileUri);

                await using var eventsPublisher = await EventsPublisher.CreateEventsPublisherAsync(nodeTask);

                try
                {
                    if (!file?.Exists ?? true)
                    {
                        file ??= new(CommandFactory.DefaultTaskDefinitionFile);
                        await NodeTaskUtils.SerializeNodeTaskAsync(nodeTask, file.FullName);
                        file.Refresh();
                    }

                    await ExecuteAllOperationsAsSubProcessesAsync(file!, blockSize, writers, readers, bufferCapacity, apiVersion, dockerUri);

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

        /// <summary>
        /// Executor (exec) command. Executes the executor operation as defined in the node task definition file.
        /// </summary>
        /// <param name="file"></param>
        /// <param name="dockerUri"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        internal static async Task ExecuteExecCommandAsync(FileInfo file, Uri dockerUri)
        {
            try
            {
                var nodeTask = await NodeTaskUtils.DeserializeNodeTaskAsync(file.FullName);

                Logger.LogDebug("Executing commands in container for Task ID: {NodeTaskId}", nodeTask.Id);

                await using var executor = await Executor.CreateExecutorAsync(nodeTask);

                var result = await executor.ExecuteNodeContainerTaskAsync(new DockerExecutor(dockerUri));

                if (result is null)
                {
                    throw new InvalidOperationException("The container task failed to return results");
                }

                Logger.LogInformation("Docker container execution status code: {ContainerResultExitCode}", result.ContainerResult.ExitCode);

                if (!string.IsNullOrWhiteSpace(result.ContainerResult.Error))
                {
                    Logger.LogInformation("Docker container result error: {ContainerResultError}", result.ContainerResult.Error);
                }
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
        /// <param name="file"></param>
        /// <param name="blockSize"></param>
        /// <param name="writers"></param>
        /// <param name="readers"></param>
        /// <param name="bufferCapacity"></param>
        /// <param name="apiVersion"></param>
        /// <returns></returns>
        internal static async Task<int> ExecuteUploadCommandAsync(FileInfo file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion)
        {
            var options = CommandLauncher.CreateBlobPipelineOptions(blockSize, writers, readers, bufferCapacity, apiVersion);

            Logger.LogDebug("Starting upload operation.");

            return await ExecuteTransferTaskAsync(file, exec => exec.UploadOutputsAsync(options));
        }

        /// <summary>
        /// Download (download) command. Executes the download of inputs as defined in the node task definition file.
        /// </summary>
        /// <param name="file"></param>
        /// <param name="blockSize"></param>
        /// <param name="writers"></param>
        /// <param name="readers"></param>
        /// <param name="bufferCapacity"></param>
        /// <param name="apiVersion"></param>
        /// <returns></returns>
        internal static async Task<int> ExecuteDownloadCommandAsync(FileInfo file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion)
        {
            var options = CommandLauncher.CreateBlobPipelineOptions(blockSize, writers, readers, bufferCapacity, apiVersion);

            Logger.LogDebug("Starting download operation.");

            return await ExecuteTransferTaskAsync(file, exec => exec.DownloadInputsAsync(options));
        }

        private static async Task ExecuteAllOperationsAsSubProcessesAsync(FileInfo file, int blockSize, int writers, int readers, int bufferCapacity,
            string apiVersion, Uri dockerUri)
        {
            var options =
                BlobPipelineOptionsConverter.ToBlobPipelineOptions(blockSize, writers, readers, bufferCapacity,
                    apiVersion);

            await CommandLauncher.LaunchTransferCommandAsSubProcessAsync(CommandFactory.DownloadCommandName, file, options);

            await CommandLauncher.LaunchesExecutorCommandAsSubProcessAsync(file, dockerUri);

            await CommandLauncher.LaunchTransferCommandAsSubProcessAsync(CommandFactory.UploadCommandName, file, options);
        }

        private static async Task<int> ExecuteTransferTaskAsync(FileInfo taskDefinitionFile, Func<Executor, Task<long>> transferOperation)
        {
            try
            {
                var nodeTask = await NodeTaskUtils.DeserializeNodeTaskAsync(taskDefinitionFile.FullName);

                await using var executor = await Executor.CreateExecutorAsync(nodeTask);

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

        private static async ValueTask<Runner.Models.NodeTask> ResolveNodeTaskAsync(FileInfo? file, Uri? uri)
        {
            try
            {
                return await NodeTaskUtils.ResolveNodeTaskAsync(file, uri, GetNodeTaskResolverOptions());
            }
            catch (ArgumentException e) when (e.ParamName == "options")
            {
                throw new InvalidOperationException($"Environment variable '{nameof(Runner.Models.NodeTaskResolverOptions)}' is missing.", e);
            }
        }

        private static Runner.Models.NodeTaskResolverOptions? GetNodeTaskResolverOptions()
        {
            var optionsValue = Environment.GetEnvironmentVariable(nameof(Runner.Models.NodeTaskResolverOptions));

            return string.IsNullOrWhiteSpace(optionsValue) ? null : NodeTaskUtils.DeserializeJson<Runner.Models.NodeTaskResolverOptions>(optionsValue);
        }
    }
}
