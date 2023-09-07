// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Sas;

namespace Tes.Runner.Storage
{
    public interface IUrlTransformationStrategy
    {
        Task<Uri> TransformUrlWithStrategyAsync(string sourceUrl, BlobSasPermissions blobSasPermissions = default);
    }
}
