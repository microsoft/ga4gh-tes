using Microsoft.Extensions.Logging;
using Tes.Runner;
using Tes.Runner.Docker;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    internal class CommandHandlers
    {
        private static readonly ILogger Logger = PipelineLoggerFactory.Create<CommandHandlers>();

        internal static async Task ExecuteNodeTaskAsync(FileInfo file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion,
            Uri dockerUri)
        {
            try
            {
                var options = BlobPipelineOptionsConverter.ToBlobPipelineOptions(blockSize, writers, readers, bufferCapacity, apiVersion);

                ExecuteDownloadTaskAsSubProcess(file, options);

                await ExecuteNodeContainerTaskAsync(file, dockerUri, options);

                ExecuteUploadTaskAsSubProcess(file, options);

            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to execute the task. Error: {e.Message}");
            }
        }

        private static async Task ExecuteNodeContainerTaskAsync(FileInfo file, Uri dockerUri, BlobPipelineOptions options)
        {
            try
            {
                var executor = new Executor(file.FullName, options);

                var result = await executor.ExecuteNodeContainerTaskAsync(new DockerExecutor(dockerUri));

                if (result is null)
                {
                    throw new InvalidOperationException("The container task failed to return results");
                }

                var logs = await result.ContainerResult.Logs.ReadOutputToEndAsync(CancellationToken.None);

                Console.WriteLine($"Execution Status Code: {result.ContainerResult.StatusCode}. Error: {result.ContainerResult.Error}");
                Console.WriteLine($"StdOutput: {logs.stdout}");
                Console.WriteLine($"StdError: {logs.stderr}");
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Failed to execute the task.");
                throw;
            }
        }

        private static BlobPipelineOptions CreateBlobPipelineOptions(int blockSize, int writers, int readers,
            int bufferCapacity, string apiVersion)
        {
            var options = new BlobPipelineOptions(
                BlockSizeBytes: blockSize,
                NumberOfWriters: writers,
                NumberOfReaders: readers,
                ReadWriteBuffersCapacity: bufferCapacity,
                MemoryBufferCapacity: bufferCapacity,
                ApiVersion: apiVersion);

            options = PipelineOptionsOptimizer.OptimizeOptionsIfApplicable(options);

            return options;
        }

        internal static async Task ExecuteUploadTaskAsync(FileInfo file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion)
        {
            var options = CreateBlobPipelineOptions(blockSize, writers, readers, bufferCapacity, apiVersion);

            var executor = new Executor(file.FullName, options);

            var result = await executor.UploadOutputsAsync();

            Console.WriteLine($"Total bytes uploaded: {result:n0}");
        }

        internal static void ExecuteDownloadTaskAsSubProcess(FileInfo file, BlobPipelineOptions options)
        {
            var processLauncher = new ProcessLauncher();

            var results = processLauncher.LaunchProcessAndWait(BlobPipelineOptionsConverter.ToCommandArgs(CommandFactory.DownloadCommandName, file.FullName, options));

            HandleResult(results);
        }

        private static void HandleResult(ProcessExecutionResult results)
        {
            if (results.ExitCode != 0)
            {
                throw new Exception(
                    $"Command failed. Exit Code: {results.ExitCode} Error: {results.StandardError}");
            }

            Console.WriteLine($"Result: {results.StandardOutput}");
        }

        internal static void ExecuteUploadTaskAsSubProcess(FileInfo file, BlobPipelineOptions options)
        {
            var processLauncher = new ProcessLauncher();

            var results = processLauncher.LaunchProcessAndWait(BlobPipelineOptionsConverter.ToCommandArgs(CommandFactory.UploadCommandName, file.FullName, options));

            HandleResult(results);
        }

        internal static async Task ExecuteDownloadTaskAsync(FileInfo file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion)
        {
            var options = CreateBlobPipelineOptions(blockSize, writers, readers, bufferCapacity, apiVersion);

            var executor = new Executor(file.FullName, options);

            var result = await executor.DownloadInputsAsync();

            Console.WriteLine($"Total bytes downloaded: {result:n0}");
        }
    }
}
