// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer
{
    public static class BlobSizeUtils
    {
        public const int MaxBlobBlocksCount = 50000;
        public const int MiB = 1024 * 1024;
        public const long GiB = MiB * 1024;
        public const int DefaultBlockSizeBytes = MiB * 8; //8 MiB;
        public const int BlockSizeIncrementUnitInBytes = 4 * MiB; // 4 MiB

        public static int GetNumberOfParts(long length, int blockSize)
        {
            if (blockSize <= 0)
            {
                throw new Exception(
                    $"Invalid block size. The value must be greater than 0. Provided value: {blockSize}");
            }

            var numberOfParts = Convert.ToInt32(Math.Ceiling((double)(length) / blockSize));

            return numberOfParts;
        }

        public static double ToBandwidth(long length, double seconds)
        {
            return Math.Round(length / (1024d * 1024d) / seconds, 2);
        }

        public static void ValidateBlockSizeForUpload(int blockSizeBytes)
        {
            var minSizeInMiB = BlockSizeIncrementUnitInBytes / MiB;

            if (blockSizeBytes < minSizeInMiB)
            {
                throw new InvalidOperationException($"Invalid block size. The value must be greater or equal to {minSizeInMiB} MiB");
            }

            if (blockSizeBytes % BlockSizeIncrementUnitInBytes > 0)
            {
                throw new InvalidOperationException($"The provided block size: {blockSizeBytes:n:0} is not valid for the upload operation. The block size must be a multiple of {minSizeInMiB} MiB ({BlockSizeIncrementUnitInBytes:n:0} bytes)");
            }
        }
    }
}
