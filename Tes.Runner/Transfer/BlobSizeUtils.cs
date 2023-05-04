namespace Tes.Runner.Transfer
{
    public static class BlobSizeUtils
    {
        public const int MaxBlobPartsCount = 50000;
        public const int MiB = 1024 * 1024;
        public const int DefaultBlockSizeBytes = MiB * 10; //10 MiB;

        public static int GetNumberOfParts(long length, int blockSize)
        {
            if (blockSize <= 0)
            {
                throw new Exception(
                    $"Invalid block size. The value must be greater than 0. Provided value: {blockSize}");
            }

            var numberOfParts = Convert.ToInt32(Math.Ceiling((double)(length) / blockSize));

            if (numberOfParts > MaxBlobPartsCount)
            {
                throw new Exception(
                    $"The number of blocks exceeds the maximum allowed by the service of {MaxBlobPartsCount}. Try increasing the block size. Current block size: {blockSize}");
            }

            return numberOfParts;
        }

        public static double ToBandwidth(long length, double seconds)
        {
            return Math.Round(length / (1024d * 1024d) / seconds, 2);
        }
    }
}
