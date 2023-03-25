// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer
{
    public record BlobPipelineOptions(int BlockSize, int NumberOfBuffers, int NumberOfWriters, int NumberOfReaders,
        int BufferCapacity = 10, int MemoryBufferCapacity = 10);
}
