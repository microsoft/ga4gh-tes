// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Moq;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test
{
    [TestClass]
    [TestCategory("Unit")]
    public class PartReaderTests
    {
        private const int MemBuffersCapacity = 10;
        private PartsReader partsReader;
        private Mock<IBlobPipeline> pipeline;
        private Channel<byte[]> memoryBufferChannel;
        private readonly int blockSize = BlobSizeUtils.DefaultBlockSize;
        private BlobPipelineOptions options;
        private Channel<PipelineBuffer> readBufferChannel;
        private Channel<PipelineBuffer> writeBufferChannel;
        private readonly long fileSize = BlobSizeUtils.MiB * 100;
        private readonly string fileName = "tempFile";

        [TestInitialize]
        public async Task SetUp()
        {
            memoryBufferChannel = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(MemBuffersCapacity, blockSize);
            options = new BlobPipelineOptions();
            pipeline = new Mock<IBlobPipeline>();
            partsReader = new PartsReader(pipeline.Object, options, memoryBufferChannel);
            readBufferChannel = Channel.CreateBounded<PipelineBuffer>(10);
            writeBufferChannel = Channel.CreateBounded<PipelineBuffer>(10);
        }

        [TestMethod]
        public async Task StartPartsReaderAsync_CallsPipelineReadOperationExpectedNumberOfTimesAndWriterChannelContainsParts()
        {
            var numberOfParts = await PrepareReaderChannelAsync();

            await partsReader.StartPartsReaderAsync(readBufferChannel, writeBufferChannel);

            pipeline.Verify(p => p.ExecuteReadAsync(It.IsAny<PipelineBuffer>()), Times.Exactly(numberOfParts));
            Assert.AreEqual(numberOfParts, writeBufferChannel.Reader.Count);
        }
        [TestMethod]
        public async Task StartPartsReaderAsync_MemoryBuffersAreUsed()
        {
            var numberOfParts = await PrepareReaderChannelAsync();

            await partsReader.StartPartsReaderAsync(readBufferChannel, writeBufferChannel);

            //The reader reads from the memory buffer to create parts, the number of items in the memory
            //buffer must be the available must be the difference between the number of memory buffers and the number of parts to create
            Assert.AreEqual(MemBuffersCapacity - numberOfParts, memoryBufferChannel.Reader.Count);
        }

        private async Task<int> PrepareReaderChannelAsync()
        {
            var buffer = new PipelineBuffer();
            var numberOfParts = (int)(fileSize / blockSize);
            await RunnerTestUtils.AddPipelineBuffersAndCompleteChannelAsync(readBufferChannel, numberOfParts,
                new Uri("https://foo.bar/cont/blob"), blockSize, fileSize, fileName);
            return numberOfParts;
        }
    }
}
