// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Tes.Runner.Transfer;

namespace Tes.Runner;

/// <summary>
/// Factory of the pipeline transfer operations.
/// </summary>
public class TransferOperationFactory(
        Func<BlobPipelineOptions, Channel<byte[]>, BlobDownloader> downloaderFactory,
        Func<BlobPipelineOptions, Channel<byte[]>, BlobUploader> uploaderFactory
    ) : ITransferOperationFactory
{
    public async Task<BlobDownloader> CreateBlobDownloaderAsync(BlobPipelineOptions blobPipelineOptions)
    {
        var memoryBufferChannel = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(blobPipelineOptions.MemoryBufferCapacity, blobPipelineOptions.BlockSizeBytes);

        return downloaderFactory(blobPipelineOptions, memoryBufferChannel);
    }

    public async Task<BlobUploader> CreateBlobUploaderAsync(BlobPipelineOptions blobPipelineOptions)
    {
        var memoryBufferChannel = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(blobPipelineOptions.MemoryBufferCapacity, blobPipelineOptions.BlockSizeBytes);

        return uploaderFactory(blobPipelineOptions, memoryBufferChannel);
    }
}
