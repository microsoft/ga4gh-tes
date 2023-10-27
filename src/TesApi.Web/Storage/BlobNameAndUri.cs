// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace TesApi.Web.Storage
{
    /// <summary>
    /// A storage blob's BlobName and Uri without SAS token.
    /// </summary>
    /// <param name="BlobName">The "BlobName" (without container name or account name) of the storage blob.</param>
    /// <param name="BlobUri">The URL of the storage blob without any SAS token.</param>
    public record struct BlobNameAndUri(string BlobName, Uri BlobUri);
}
