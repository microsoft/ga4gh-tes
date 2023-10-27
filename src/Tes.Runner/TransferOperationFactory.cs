// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Transfer;

namespace Tes.Runner;

/// <summary>
/// Factory of the pipeline transfer operations.
/// </summary>
public class TransferOperationFactory : ITransferOperationFactory
{
    public async Task<BlobDownloader> CreateBlobDownloaderAsync(BlobPipelineOptions blobPipelineOptions)
    {
        var memoryBufferChannel = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(blobPipelineOptions.MemoryBufferCapacity, blobPipelineOptions.BlockSizeBytes);

        return new BlobDownloader(blobPipelineOptions, memoryBufferChannel);
    }

    public async Task<BlobUploader> CreateBlobUploaderAsync(BlobPipelineOptions blobPipelineOptions)
    {
        var memoryBufferChannel = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(blobPipelineOptions.MemoryBufferCapacity, blobPipelineOptions.BlockSizeBytes);

        return new BlobUploader(blobPipelineOptions, memoryBufferChannel);
    }
}
