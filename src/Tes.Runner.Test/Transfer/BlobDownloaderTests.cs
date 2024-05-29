// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Tes.Runner.Transfer.Tests
{
    [TestClass]
    [TestCategory("Unit")]
    public class BlobDownloaderTests
    {
        private BlobDownloader blobDownloader = null!;
        private readonly BlobPipelineOptions blobPipelineOptions = new();

        [TestInitialize]
        public async Task Init()
        {
            blobDownloader = new BlobDownloader(blobPipelineOptions,
                await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(10, blobPipelineOptions.BlockSizeBytes),
                pipeline => new(pipeline, NullLogger<ProcessedPartsProcessor>.Instance),
                (pipeline, options) => new(pipeline, options, NullLogger.Instance),
                (pipeline, options, channel, strategy) => new(pipeline, options, channel, strategy, NullLogger<PartsWriter>.Instance),
                (pipeline, options, channel, strategy) => new(pipeline, options, channel, strategy, NullLogger<PartsReader>.Instance),
                NullLogger<BlobDownloader>.Instance);
        }

        [TestMethod]
        public async Task ExecuteReadAsync_EmptyPartIsProvided_SucceedsWithoutMakingHttpRequest()
        {
            var part = new PipelineBuffer()
            {
                BlobPartUrl = new Uri("https://foo.com"), //This is invalid on purpose, as we don't want to make a real request
                FileName = "emptyFile",
                Length = 0,
                Offset = 0,
                Ordinal = 0,
                NumberOfParts = 1,
                FileSize = 0,
                Data = new byte[BlobSizeUtils.DefaultBlockSizeBytes]
            };

            var length = await blobDownloader.ExecuteReadAsync(part, CancellationToken.None);

            Assert.AreEqual(0, length);
        }
    }
}
