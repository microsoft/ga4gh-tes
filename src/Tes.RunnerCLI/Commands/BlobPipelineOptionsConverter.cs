using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    internal class BlobPipelineOptionsConverter
    {
        internal const string FileOption = "file";
        internal const string BlockSizeOption = "blockSize";
        internal const string WritersOption = "writers";
        internal const string ReadersOption = "readers";
        internal const string BufferCapacityOption = "bufferCapacity";
        internal const string ApiVersionOption = "apiVersion";

        internal static string[] ToCommandArgs(string command, string fileOption, BlobPipelineOptions blobPipelineOptions)
        {
            ArgumentException.ThrowIfNullOrEmpty(command, nameof(command));
            ArgumentNullException.ThrowIfNull(blobPipelineOptions, nameof(blobPipelineOptions));

            var args = new List<string>()
            {
                command,
                $"--{BlockSizeOption} {blobPipelineOptions.BlockSizeBytes}",
                $"--{WritersOption} {blobPipelineOptions.NumberOfWriters}",
                $"--{ReadersOption} {blobPipelineOptions.NumberOfReaders}",
                $"--{BufferCapacityOption} {blobPipelineOptions.ReadWriteBuffersCapacity}",
                $"--{ApiVersionOption} {blobPipelineOptions.ApiVersion}"
            };

            if (!string.IsNullOrEmpty(fileOption))
            {
                args.Add($"--{fileOption} {fileOption}");
            }

            return args.ToArray();
        }

        internal static BlobPipelineOptions ToBlobPipelineOptions(int blockSize, int writers, int readers,
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
    }
}
