// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Models;

namespace Tes.Runner.Transfer
{
    /// <summary>
    /// Pipeline options optimizer
    /// </summary>
    public class PipelineOptionsOptimizer
    {
        private readonly ISystemInfoProvider systemInfoProvider;
        private readonly IFileInfoProvider fileInfoProvider;
        private const double MemoryBufferCapacityFactor = 0.4; // 40% of total memory
        private const long MaxMemoryBufferSizeInBytes = BlobSizeUtils.GiB * 2; // 2 GiB of total memory
        private const int MaxWorkingThreadsCount = 90;

        public PipelineOptionsOptimizer(ISystemInfoProvider systemInfoProvider) : this(systemInfoProvider, new DefaultFileInfoProvider())
        {

        }

        public PipelineOptionsOptimizer(ISystemInfoProvider systemInfoProvider, IFileInfoProvider fileInfoProvider)
        {
            ArgumentNullException.ThrowIfNull(systemInfoProvider, nameof(systemInfoProvider));
            ArgumentNullException.ThrowIfNull(fileInfoProvider, nameof(fileInfoProvider));
            this.systemInfoProvider = systemInfoProvider;
            this.fileInfoProvider = fileInfoProvider;
        }

        /// <summary>
        /// Creates new pipeline options with optimized values, if default values are provided
        /// </summary>
        /// <param name="options"><see cref="BlobPipelineOptions"/> to optimize</param>
        /// <param name="taskOutputs">If provided, it will adjust the block size to accomodate the maximum file size of the output</param>
        /// <returns>An optimized instance of <see cref="BlobPipelineOptions"/>. If default values are not provided, returns original options</returns>
        public BlobPipelineOptions Optimize(BlobPipelineOptions options, List<FileOutput>? taskOutputs = default)
        {

            //only optimize if the transfer options are the default ones
            if (IsDefaultCapacityAndWorkerTransferOptions(options))
            {
                var blockSize = options.BlockSizeBytes;

                if (taskOutputs?.Count > 0)
                {
                    blockSize = GetAdjustedBlockSizeInBytesForUploads(options.BlockSizeBytes, taskOutputs);
                }

                return CreateOptimizedThreadingAndCapacityOptions(options, blockSize);
            }

            return options;
        }

        public static BlobPipelineOptions OptimizeOptionsIfApplicable(BlobPipelineOptions blobPipelineOptions, List<FileOutput>? taskOutputs)
        {
            //optimization is only available for Linux
            //Windows supports an implementation of ISystemInfoProvider is required.
            if (!LinuxSystemInfoProvider.IsLinuxSystem())
            {
                return blobPipelineOptions;
            }

            var optimizer = new PipelineOptionsOptimizer(new LinuxSystemInfoProvider());

            return optimizer.Optimize(blobPipelineOptions, taskOutputs);
        }

        private int GetAdjustedBlockSizeInBytesForUploads(int currentBlockSizeInBytes, List<FileOutput> taskOutputs)
        {

            BlobSizeUtils.ValidateBlockSizeForUpload(currentBlockSizeInBytes);

            foreach (var taskOutput in taskOutputs)
            {
                var fileSize = fileInfoProvider.GetFileSize(taskOutput.Path!);

                if (fileSize / (double)currentBlockSizeInBytes > BlobSizeUtils.MaxBlobBlocksCount)
                {
                    var minIncrementUnits = Math.Ceiling((double)fileSize / (BlobSizeUtils.MaxBlobBlocksCount * (long)BlobSizeUtils.BlockSizeIncrementUnitInBytes));

                    var newBlockSizeInBytes = (((int)minIncrementUnits - (BlobSizeUtils.DefaultBlockSizeBytes / BlobSizeUtils.BlockSizeIncrementUnitInBytes)) * BlobSizeUtils.BlockSizeIncrementUnitInBytes) + BlobSizeUtils.DefaultBlockSizeBytes;

                    //try again with the new value and see if it works for all outputs. 
                    return GetAdjustedBlockSizeInBytesForUploads(newBlockSizeInBytes, taskOutputs);
                }
            }

            return currentBlockSizeInBytes;
        }

        private static bool IsDefaultCapacityAndWorkerTransferOptions(BlobPipelineOptions options)
        {
            return options is
            {
                ReadWriteBuffersCapacity: BlobPipelineOptions.DefaultReadWriteBuffersCapacity,
                NumberOfWriters: BlobPipelineOptions.DefaultNumberOfWriters,
                NumberOfReaders: BlobPipelineOptions.DefaultNumberOfReaders,
                MemoryBufferCapacity: BlobPipelineOptions.DefaultMemoryBufferCapacity
            };
        }

        private BlobPipelineOptions CreateOptimizedThreadingAndCapacityOptions(BlobPipelineOptions options, int blockSize)
        {
            var bufferCapacity = GetOptimizedMemoryBufferCapacity(blockSize);

            // for now, readers and writers are the same
            var readers = GetOptimizedWorkers(bufferCapacity);
            var writers = readers;

            // for now, memory buffer capacity is the same as the buffer capacity
            var memoryBufferCapacity = bufferCapacity;
            var readerWriteCapacity = memoryBufferCapacity;

            return new BlobPipelineOptions(
                BlockSizeBytes: blockSize,
                ReadWriteBuffersCapacity: readerWriteCapacity,
                NumberOfWriters: writers,
                NumberOfReaders: readers,
                FileHandlerPoolCapacity: options.FileHandlerPoolCapacity,
                ApiVersion: options.ApiVersion,
                MemoryBufferCapacity: memoryBufferCapacity);
        }

        private int GetOptimizedWorkers(int bufferCapacity)
        {
            if (bufferCapacity > MaxWorkingThreadsCount)
            {
                return MaxWorkingThreadsCount;
            }

            return bufferCapacity;
        }

        private int GetOptimizedMemoryBufferCapacity(int blockSize)
        {
            var memoryBuffer = systemInfoProvider.TotalMemory * MemoryBufferCapacityFactor;

            if (memoryBuffer > MaxMemoryBufferSizeInBytes)
            {
                return RoundDownToNearestTen((int)(MaxMemoryBufferSizeInBytes / blockSize));
            }

            return RoundDownToNearestTen((int)(Convert.ToInt64(memoryBuffer) / blockSize));
        }

        private static int RoundDownToNearestTen(int value)
        {
            return (int)Math.Floor((double)value / 10) * 10;
        }
    }
}
