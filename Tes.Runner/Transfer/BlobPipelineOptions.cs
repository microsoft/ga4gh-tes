// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer
{
    public record BlobPipelineOptions(int BlockSize = BlobSizeUtils.DefaultBlockSize, int NumberOfBuffers = 10, int NumberOfWriters = 10, int NumberOfReaders = 10,
        int BufferCapacity = 10, int MemoryBufferCapacity = 10);
}
