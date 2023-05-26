using Tes.Runner.Transfer;
using Tes.RunnerCLI.Services;

namespace Tes.RunnerCLI.Commands
{
    public class BlobPipelineOptionsConverter
    {
        public const string FileOption = "file";
        public const string BlockSizeOption = "blockSize";
        public const string WritersOption = "writers";
        public const string ReadersOption = "readers";
        public const string SkipMissingSources = "skipMissingSources";
        public const string BufferCapacityOption = "bufferCapacity";
        public const string DownloaderFormatterOption = "downloaderFormatter";
        public const string UploaderFormatterOption = "uploaderFormatter";
        public const string ApiVersionOption = "apiVersion";

        public static string[] ToCommandArgs(string command, string fileOption, BlobPipelineOptions blobPipelineOptions, string formatterCommand, MetricsFormatterOptions? formatterOptions)
        {
            ArgumentException.ThrowIfNullOrEmpty(command);
            ArgumentNullException.ThrowIfNull(blobPipelineOptions);

            if (!string.IsNullOrEmpty(formatterCommand))
            {
                ArgumentNullException.ThrowIfNull(formatterOptions);
            }

            if (formatterOptions is not null)
            {
                ArgumentException.ThrowIfNullOrEmpty(formatterCommand);
            }

            var args = new List<string>()
            {
                command,
                $"--{BlockSizeOption} {blobPipelineOptions.BlockSizeBytes}",
                $"--{WritersOption} {blobPipelineOptions.NumberOfWriters}",
                $"--{ReadersOption} {blobPipelineOptions.NumberOfReaders}",
                $"--{SkipMissingSources} {blobPipelineOptions.SkipMissingSources}",
                $"--{BufferCapacityOption} {blobPipelineOptions.ReadWriteBuffersCapacity}",
                $"--{ApiVersionOption} {blobPipelineOptions.ApiVersion}"
            };

            if (!string.IsNullOrEmpty(formatterCommand) && formatterOptions is not null)
            {
                args.Add($"--{formatterCommand} \"{formatterOptions.MetricsFile}\" \'{formatterOptions.FileLogFormat}\'");
            }

            if (!string.IsNullOrEmpty(fileOption))
            {
                args.Add($"--{FileOption} {fileOption}");
            }

            return args.ToArray();
        }

        public static BlobPipelineOptions ToBlobPipelineOptions(int blockSize, int writers, int readers,
            bool skipMissingSources, int bufferCapacity, string apiVersion)
        {
            var options = new BlobPipelineOptions(
                BlockSizeBytes: blockSize,
                NumberOfWriters: writers,
                NumberOfReaders: readers,
                SkipMissingSources: skipMissingSources,
                ReadWriteBuffersCapacity: bufferCapacity,
                MemoryBufferCapacity: bufferCapacity,
                ApiVersion: apiVersion);

            options = PipelineOptionsOptimizer.OptimizeOptionsIfApplicable(options);

            return options;
        }
    }
}
