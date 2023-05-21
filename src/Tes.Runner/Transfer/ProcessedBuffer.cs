// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;

namespace Tes.Runner.Transfer
{
    public record ProcessedBuffer(string FileName, Uri? BlobUrl, long FileSize, int Ordinal,
        int NumberOfParts, Channel<FileStream> FileHandlerPool, Uri? BlobPartUrl, int Length,
        Md5Processor? BufferMd5Processor);
}
