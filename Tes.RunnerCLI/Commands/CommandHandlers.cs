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
            var options = new BlobPipelineOptions(
                BlockSize: blockSize,
                NumberOfWriters: writers,
                NumberOfReaders: readers,
                ReadWriteBuffersCapacity: bufferCapacity,
                MemoryBufferCapacity: bufferCapacity,
                ApiVersion: apiVersion);

            var executor = new Executor(file.FullName, options);

            var result = await executor.ExecuteNodeTaskAsync(new DockerExecutor(dockerUri));

            var logs = await result.ContainerResult.Logs.ReadOutputToEndAsync(CancellationToken.None);

            Console.WriteLine($"Execution Result: {logs}");
            Console.WriteLine($"Total Bytes Downloaded: {result.InputsLength} Total Bytes Uploaded: {result.OutputsLength}");
        }
        internal static async Task ExecuteUploadTaskAsync(FileInfo file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion)
        {
            var options = new BlobPipelineOptions(
                BlockSize: blockSize,
                NumberOfWriters: writers,
                NumberOfReaders: readers,
                ReadWriteBuffersCapacity:bufferCapacity,
                MemoryBufferCapacity:bufferCapacity,
                ApiVersion: apiVersion);

            var executor = new Executor(file.FullName, options);

            var result = await executor.UploadOutputsAsync();

            Console.WriteLine($"Total Bytes Uploaded: {result}");
        }

        internal static async Task ExecuteDownloadTaskAsync(FileInfo file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion)
        {
            var options = new BlobPipelineOptions(
                BlockSize: blockSize,
                NumberOfWriters: writers,
                NumberOfReaders: readers,
                ReadWriteBuffersCapacity: bufferCapacity,
                MemoryBufferCapacity: bufferCapacity,
                ApiVersion: apiVersion);

            var executor = new Executor(file.FullName, options);

            var result = await executor.DownloadInputsAsync();

            Console.WriteLine($"Total Bytes Downloaded: {result}");
        }
    }
}
