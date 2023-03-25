// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;

namespace Tes.Runner.Transfer
{
    public class MemoryBufferPoolFactory
    {
        public static async ValueTask<Channel<byte[]>> CreateMemoryBufferPoolAsync(int capacity, int blockSize)
        {
            if (capacity <= 0)
            {
                throw new ArgumentException("Invalid capacity. Value must be greater than 0", nameof(capacity));
            }

            if (blockSize <= 0 || blockSize > 1024 * 1024 * 100)
            {
                throw new ArgumentException("Invalid blockSize. Value must be greater than 0 and less than 100 MiB", nameof(capacity));
            }

            var bufferPool = Channel.CreateBounded<byte[]>(capacity);
            for (var i = 0; i < capacity; i++)
            {
                await bufferPool.Writer.WriteAsync(new byte[blockSize]);
            }

            return bufferPool;
        }

    }
}
