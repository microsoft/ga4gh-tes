// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner
{
    /// <summary>
    /// Upload File Log Entry
    /// </summary>
    /// <param name="Length">Size of file in bytes.</param>
    /// <param name="BlobUrl">Target URL</param>
    /// <param name="FileName">Source Path</param>
    public record struct CompletedUploadFile(long Length, Uri? BlobUrl, string FileName);
}
