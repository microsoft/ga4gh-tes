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
            var options = CreateBlobPipelineOptions(blockSize, writers, readers, bufferCapacity, apiVersion);

            var executor = new Executor(file.FullName, options);

            var result = await executor.ExecuteNodeTaskAsync(new DockerExecutor(dockerUri));

            var logs = await result.ContainerResult.Logs.ReadOutputToEndAsync(CancellationToken.None);

            Console.WriteLine($"Execution result: {logs}");
            Console.WriteLine($"Total bytes downloaded: {result.InputsLength:n0} Total bytes uploaded: {result.OutputsLength:n0}");
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
