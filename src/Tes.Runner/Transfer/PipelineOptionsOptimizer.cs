// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer
{
    /// <summary>
    /// Pipeline options optimizer
    /// </summary>
    public class PipelineOptionsOptimizer
    {
        private readonly ISystemInfoProvider systemInfoProvider;
        private const double MemoryBufferCapacityFactor = 0.4; // 40% of total memory
        private const long MaxMemoryBufferSizeInBytes = BlobSizeUtils.GiB * 2; // 2 GiB of total memory
        private const int MaxWorkingThreadsCount = 90;

        public PipelineOptionsOptimizer(ISystemInfoProvider systemInfoProvider)
        {
            ArgumentNullException.ThrowIfNull(systemInfoProvider);

            this.systemInfoProvider = systemInfoProvider;
        }

        /// <summary>
        /// Creates new pipeline options with optimized values, if default values are provided
        /// </summary>
        /// <param name="options"><see cref="BlobPipelineOptions"/> to optimize</param>
        /// <returns>An optimized instance of <see cref="BlobPipelineOptions"/>. If default values are not provided, returns original options</returns>
        public BlobPipelineOptions Optimize(BlobPipelineOptions options)
        {
            //only optimize if the transfer options are the default ones
            if (IsDefaultCapacityAndWorkerTransferOptions(options))
            {
                return CreateOptimizedOptions(options);
            }

            return options;
        }

        public static BlobPipelineOptions OptimizeOptionsIfApplicable(BlobPipelineOptions blobPipelineOptions)
        {
            //optimization is only available for Linux
            //Windows supports an implementation of ISystemInfoProvider is required.
            if (!LinuxSystemInfoProvider.IsLinuxSystem())
            {
                return blobPipelineOptions;
            }

            var optimizer = new PipelineOptionsOptimizer(new LinuxSystemInfoProvider());

            return optimizer.Optimize(blobPipelineOptions);
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

        private BlobPipelineOptions CreateOptimizedOptions(BlobPipelineOptions options)
        {
            var blockSize = GetBlockSize(options.BlockSizeBytes);
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
                SkipMissingSources: options.SkipMissingSources,
                FileHandlerPoolCapacity: options.FileHandlerPoolCapacity,
                ApiVersion: options.ApiVersion,
                MemoryBufferCapacity: memoryBufferCapacity);
        }

        private static int GetBlockSize(int optionsBlockSizeBytes)
        {
            if (optionsBlockSizeBytes <= 0)
            {
                throw new ArgumentException("Block size must be greater than 0.");
            }

            return optionsBlockSizeBytes;
        }

        private static int GetOptimizedWorkers(int bufferCapacity)
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
