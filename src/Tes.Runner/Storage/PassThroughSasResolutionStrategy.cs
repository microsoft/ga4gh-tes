// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Sas;

namespace Tes.Runner.Storage
{
    public class PassThroughSasResolutionStrategy : ISasResolutionStrategy
    {
        public Task<Uri> CreateSasTokenWithStrategyAsync(string sourceUrl, BlobSasPermissions blobSasPermissions)
        {
            ArgumentException.ThrowIfNullOrEmpty(sourceUrl);

            return Task.FromResult(new Uri(sourceUrl));
        }
    }
}
