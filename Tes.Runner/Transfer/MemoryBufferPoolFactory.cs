// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;

namespace Tes.Runner.Transfer
{
    public class MemoryBufferPoolFactory
    {
        const int MaxBufferSize = 1024 * 1024 * 100; //100 MiB

        public static async ValueTask<Channel<byte[]>> CreateMemoryBufferPoolAsync(int capacity, int bufferSize)
        {
            if (capacity <= 0)
            {
                throw new ArgumentException("Invalid capacity. Value must be greater than 0", nameof(capacity));
            }

            if (bufferSize <= 0 || bufferSize > MaxBufferSize)
            {
                throw new ArgumentException($"Invalid memory buffer size. Value must be greater than 0 and less than {MaxBufferSize/BlobSizeUtils.MiB} MiB", nameof(capacity));
            }

            var bufferPool = Channel.CreateBounded<byte[]>(capacity);
            for (var i = 0; i < capacity; i++)
            {
                await bufferPool.Writer.WriteAsync(new byte[bufferSize]);
            }

            return bufferPool;
        }
    }
}
