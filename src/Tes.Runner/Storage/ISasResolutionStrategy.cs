// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Sas;

namespace Tes.Runner.Storage
{
    public interface ISasResolutionStrategy
    {
        Task<Uri> CreateSasTokenWithStrategyAsync(string sourceUrl, BlobSasPermissions blobSasPermissions);
    }
}
