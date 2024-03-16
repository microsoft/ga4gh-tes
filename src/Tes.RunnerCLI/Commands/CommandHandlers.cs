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
        /// <param name="fileUri"></param>
        /// <param name="blockSize"></param>
        /// <param name="writers"></param>
        /// <param name="readers"></param>
        /// <param name="bufferCapacity"></param>
        /// <param name="apiVersion"></param>
        /// <param name="dockerUri"></param>
        /// <returns></returns>
        internal static async Task<int> ExecuteRootCommandAsync(FileInfo file, Uri fileUri,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion,
            Uri dockerUri)
        {
            if (fileUri is not null)
            {
                return await ExecutePreparatoryCommandAsync(fileUri, blockSize, writers, readers, bufferCapacity, apiVersion, dockerUri);
            }

            try
            {
                var duration = Stopwatch.StartNew();

                var nodeTask = await NodeTaskUtils.DeserializeNodeTaskAsync(file.FullName);

                await using var eventsPublisher = await EventsPublisher.CreateEventsPublisherAsync(nodeTask);

                try
                {
                    await ExecuteAllOperationsAsSubProcessesAsync(file, blockSize, writers, readers, bufferCapacity, apiVersion, dockerUri);

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
                    Logger.LogError(commandExecutionException, $"Failed to execute Node Task: {file.FullName}");
                    return commandExecutionException.ExitCode;
                }

                Logger.LogError(e, $"Failed to execute Node Task: {file.FullName}");

                return (int)ProcessExitCode.UncategorizedError;
            }

            return (int)ProcessExitCode.Success;
        }

        /// <summary>
        /// Root command of the CLI. Executes all operations (download, executor, upload) as sub-processes.
        /// </summary>
        /// <param name="fileUri"></param>
        /// <param name="blockSize"></param>
        /// <param name="writers"></param>
        /// <param name="readers"></param>
        /// <param name="bufferCapacity"></param>
        /// <param name="apiVersion"></param>
        /// <param name="dockerUri"></param>
        /// <returns></returns>
        private static async Task<int> ExecutePreparatoryCommandAsync(Uri fileUri,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion,
            Uri dockerUri)
        {
            FileInfo file = new($"{CommandFactory.PreparatoryCommandName}.json");

            try
            {
                await GetDownloadNodeTask(fileUri).SerializeNodeTaskAsync(file.FullName);
                await ExecuteAquireAndRunTaskAsSubProcessAsync(file, blockSize, writers, readers, bufferCapacity, apiVersion, dockerUri);
            }
            catch (Exception e)
            {
                try
                {
                    CommandLauncher.HandleFatalLauncherError(CommandFactory.ExecutorCommandName, e);
                }
                catch (CommandExecutionException commandExecutionException)
                {
                    Logger.LogError(commandExecutionException, $"Failed to execute Node Task: {file.FullName}");
                    return commandExecutionException.ExitCode;
                }

                Logger.LogError(e, $"Failed to execute Node Task: {file.FullName}");

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

                Logger.LogDebug($"Executing commands in container for Task ID: {nodeTask.Id}");

                await using var executor = await Executor.CreateExecutorAsync(nodeTask);

                var result = await executor.ExecuteNodeContainerTaskAsync(new DockerExecutor(dockerUri));

                if (result is null)
                {
                    throw new InvalidOperationException("The container task failed to return results");
                }

                Logger.LogInformation($"Docker container execution status code: {result.ContainerResult.ExitCode}");

                if (!string.IsNullOrWhiteSpace(result.ContainerResult.Error))
                {
                    Logger.LogInformation($"Docker container result error: {result.ContainerResult.Error}");
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

        private static async Task ExecuteAquireAndRunTaskAsSubProcessAsync(FileInfo file, int blockSize, int writers, int readers, int bufferCapacity,
            string apiVersion, Uri dockerUri)
        {
            var options =
                BlobPipelineOptionsConverter.ToBlobPipelineOptions(blockSize, writers, readers, bufferCapacity,
                    apiVersion);

            await CommandLauncher.LaunchTransferCommandAsSubProcessAsync(CommandFactory.PreparatoryCommandName, file, options, CommandFactory.DownloadCommandName);

            await CommandLauncher.LaunchesRootCommandAsSubProcessAsync(new FileInfo(CommandFactory.DefaultTaskDefinitionFile), options, dockerUri);
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
                Logger.LogError(e, $"Failed to perform transfer. Operation: {transferOperation}");
                return (int)ProcessExitCode.UncategorizedError;
            }
        }

        private static Runner.Models.NodeTask GetDownloadNodeTask(Uri uri)
        {
            var optionsValue = Environment.GetEnvironmentVariable(nameof(Runner.Models.PreparatoryOptions)) ?? throw new InvalidOperationException($"Environment variable '{nameof(Runner.Models.PreparatoryOptions)}' is missing.");
            var options = NodeTaskUtils.DeserializeJson<Runner.Models.PreparatoryOptions>(optionsValue);

            return new()
            {
                RuntimeOptions = options.RuntimeOptions ?? new(),
                Inputs = new([new Runner.Models.FileInput()
                {
                    SourceUrl = uri.AbsoluteUri,
                    Path = new FileInfo(CommandFactory.DefaultTaskDefinitionFile).FullName,
                    TransformationStrategy = options.TransformationStrategy
                }]),
            };
        }
    }
}
