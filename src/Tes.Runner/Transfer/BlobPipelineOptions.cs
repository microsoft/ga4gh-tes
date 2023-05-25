﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer
{
    public record BlobPipelineOptions(int BlockSizeBytes = BlobSizeUtils.DefaultBlockSizeBytes, int ReadWriteBuffersCapacity = BlobPipelineOptions.DefaultReadWriteBuffersCapacity, int NumberOfWriters = BlobPipelineOptions.DefaultNumberOfWriters, int NumberOfReaders = BlobPipelineOptions.DefaultNumberOfReaders,
        bool SkipMissingSources = BlobPipelineOptions.DefaultSkipMissingSources, int FileHandlerPoolCapacity = BlobPipelineOptions.DefaultFileHandlerPoolCapacity, int MemoryBufferCapacity = BlobPipelineOptions.DefaultMemoryBufferCapacity, string ApiVersion = BlobPipelineOptions.DefaultApiVersion)
    {
        public const int DefaultNumberOfWriters = 10;
        public const int DefaultNumberOfReaders = 10;
        public const string DefaultApiVersion = "2020-10-02";
        public const int DefaultFileHandlerPoolCapacity = 20;
        public const int DefaultMemoryBufferCapacity = 10;
        public const bool DefaultSkipMissingSources = false;
        public const int DefaultReadWriteBuffersCapacity = 10;
    }
}
