// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Transfer;

namespace Tes.Runner;

/// <summary>
/// Default implementation of the pipeline transfer operations.
/// </summary>
public interface ITransferOperationFactory
{
    Task<BlobDownloader> CreateBlobDownloaderAsync(BlobPipelineOptions blobPipelineOptions);
    Task<BlobUploader> CreateBlobUploaderAsync(BlobPipelineOptions blobPipelineOptions);
}
