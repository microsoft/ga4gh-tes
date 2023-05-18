using Tes.Runner;
using Tes.Runner.Docker;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    internal class CommandHandlers
    {
        internal static async Task ExecuteNodeTaskAsync(FileInfo file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion,
            Uri dockerUri)
        {
            var options = BlobPipelineOptionsConverter.ToBlobPipelineOptions(blockSize, writers, readers, bufferCapacity, apiVersion);

            var downloadLogs = ExecuteDownloadTaskAsSubProcess(file, options);

            Console.WriteLine($"Download result: {downloadLogs.StandardOutput}");

            var result = await ExecuteNodeContainerTaskAsync(file, dockerUri, options);

            Console.WriteLine($"Execution result: {result.ContainerResult.StatusCode} Error: {result.ContainerResult.Error}");

            var uploadLogs = ExecuteUploadTaskAsSubProcess(file, options);

            Console.WriteLine($"Download result: {uploadLogs.StandardOutput}");

        }

        private static async Task<NodeTaskResult> ExecuteNodeContainerTaskAsync(FileInfo file, Uri dockerUri, BlobPipelineOptions options)
        {
            var executor = new Executor(file.FullName, options);

            var result = await executor.ExecuteNodeContainerTaskAsync(new DockerExecutor(dockerUri));

            var logs = await result.ContainerResult.Logs.ReadOutputToEndAsync(CancellationToken.None);

            Console.WriteLine($"Execution result: {logs}");
            return result;
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

        internal static ProcessExecutionResult ExecuteDownloadTaskAsSubProcess(FileInfo file, BlobPipelineOptions options)
        {
            var processLauncher = new ProcessLauncher();

            return processLauncher.LaunchProcess(BlobPipelineOptionsConverter.ToCommandArgs(CommandFactory.DownloadCommandName, file.FullName, options));
        }

        internal static ProcessExecutionResult ExecuteUploadTaskAsSubProcess(FileInfo file, BlobPipelineOptions options)
        {
            var processLauncher = new ProcessLauncher();

            return processLauncher.LaunchProcess(BlobPipelineOptionsConverter.ToCommandArgs(CommandFactory.UploadCommandName, file.FullName, options));
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
