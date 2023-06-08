﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Moq;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test
{
    [TestClass]
    [TestCategory("Unit")]
    public class PartsWriterTests
    {
        private const int MemBuffersCapacity = 10;
        private PartsWriter? partsWriter;
        private Mock<IBlobPipeline>? pipeline;
        private Channel<byte[]>? memoryBufferChannel;
        private readonly int blockSizeBytes = BlobSizeUtils.DefaultBlockSizeBytes;
        private BlobPipelineOptions? options;
        private Channel<ProcessedBuffer>? processedBufferChannel;
        private Channel<PipelineBuffer>? writeBufferChannel;
        private readonly long fileSize = BlobSizeUtils.MiB * 100;
        private readonly string fileName = "tempFile";

        [TestInitialize]
        public void SetUp()
        {
            //the memory pool must be empty as the writer will write to it
            memoryBufferChannel = Channel.CreateBounded<byte[]>(RunnerTestUtils.MemBuffersCapacity);
            options = new BlobPipelineOptions();
            pipeline = new Mock<IBlobPipeline>();
            partsWriter = new PartsWriter(pipeline.Object, options, memoryBufferChannel);
            processedBufferChannel = Channel.CreateBounded<ProcessedBuffer>(RunnerTestUtils.PipelineBufferCapacity);
            writeBufferChannel = Channel.CreateBounded<PipelineBuffer>(RunnerTestUtils.PipelineBufferCapacity);
        }

        [TestMethod]
        public async Task StartPartsWriterAsync_CallsPipelineWriteOperationExpectedNumberOfTimesAndProcessedChannelContainsParts()
        {
            var numberOfParts = await PrepareWriterChannelAsync();

            await partsWriter!.StartPartsWritersAsync(writeBufferChannel!, processedBufferChannel!);

            pipeline!.Verify(p => p.ExecuteWriteAsync(It.IsAny<PipelineBuffer>()), Times.Exactly(numberOfParts));
            Assert.AreEqual(numberOfParts, processedBufferChannel!.Reader.Count);
        }
        [TestMethod]
        public async Task StartPartsReaderAsync_MemoryBuffersAreReturned()
        {
            var numberOfParts = await PrepareWriterChannelAsync();

            await partsWriter!.StartPartsWritersAsync(writeBufferChannel!, processedBufferChannel!);

            //The writers write to the memory channel/pool after processing, the number of items in the memory
            //buffer must the number of parts to that read from the writer's channel.
            Assert.AreEqual(numberOfParts, memoryBufferChannel!.Reader.Count);
        }

        private async Task<int> PrepareWriterChannelAsync()
        {
            return await RunnerTestUtils.PreparePipelineChannelAsync(blockSizeBytes, fileSize, fileName, "https://foo.bar/cont/blob", writeBufferChannel);
        }
    }
}
