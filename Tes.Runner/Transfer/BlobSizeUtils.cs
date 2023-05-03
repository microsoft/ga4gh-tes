namespace Tes.Runner.Transfer
{
    public static class BlobSizeUtils
    {
        public const int MaxNumberBlobParts = 50000;
        public const int MiB = 1024 * 1024;
        public const int DefaultBlockSize = MiB * 10; //10 MiB;

        public static int GetNumberOfParts(long length, int blockSize)
        {
            if (blockSize <= 0)
            {
                throw new Exception(
                    $"Invalid block size. The value must be greater than 0. Provided value: {blockSize}");
            }

            var numberOfParts = (int)Math.Ceiling((double)(length) / blockSize);

            if (numberOfParts > MaxNumberBlobParts)
            {
                throw new Exception(
                    $"The number of blocks exceeds the maximum allowed by the service of {MaxNumberBlobParts}. Try increasing the block size. Current block size: {blockSize}");
            }

            return numberOfParts;
        }

        public static double ToBandwidth(long length, double seconds)
        {
            return Math.Round(length / (1024d * 1024d) / seconds, 2);
        }
    }
}
