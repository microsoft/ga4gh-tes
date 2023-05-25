// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer
{
    public record BlobOperationInfo(Uri Url, String FileName, String SourceLocationForLength, bool ReadOnlyHandlerForExistingFile, bool SkipIfSourceMissing = false);
}
