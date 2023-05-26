// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    public class BlobPipelineOptionsConverter
    {
        public const string FileOption = "file";
        public const string BlockSizeOption = "blockSize";
        public const string WritersOption = "writers";
        public const string ReadersOption = "readers";
        public const string BufferCapacityOption = "bufferCapacity";
        public const string ApiVersionOption = "apiVersion";

        public static string[] ToCommandArgs(string command, string fileOption, BlobPipelineOptions blobPipelineOptions, bool skipMissingSources)
        {
            ArgumentException.ThrowIfNullOrEmpty(command);
            ArgumentNullException.ThrowIfNull(blobPipelineOptions);

            var args = new List<string>()
            {
                command,
                $"--{CommandFactory.SkipMissingSources} {skipMissingSources}",
                $"--{BlockSizeOption} {blobPipelineOptions.BlockSizeBytes}",
                $"--{WritersOption} {blobPipelineOptions.NumberOfWriters}",
                $"--{ReadersOption} {blobPipelineOptions.NumberOfReaders}",
                $"--{BufferCapacityOption} {blobPipelineOptions.ReadWriteBuffersCapacity}",
                $"--{ApiVersionOption} {blobPipelineOptions.ApiVersion}"
            };

            if (!string.IsNullOrEmpty(fileOption))
            {
                args.Add($"--{FileOption} {fileOption}");
            }

            return args.ToArray();
        }

        public static BlobPipelineOptions ToBlobPipelineOptions(int blockSize, int writers, int readers,
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
