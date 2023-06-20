// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Moq;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test
{
    [TestClass]
    [TestCategory("Unit")]
    public class PartsProducerTests
    {
#pragma warning disable CS8618
        private Mock<IBlobPipeline> pipeline;
        private PartsProducer partsProducer;
        private Channel<PipelineBuffer> readBuffer;
#pragma warning restore CS8618

        [TestInitialize]
        public void SetUp()
        {
            pipeline = new Mock<IBlobPipeline>();
            readBuffer = Channel.CreateUnbounded<PipelineBuffer>();
        }

        [DataTestMethod]
        [DataRow(BlobSizeUtils.MiB, 10 * BlobSizeUtils.MiB, 10)]
        [DataRow(BlobSizeUtils.MiB, 2 * BlobSizeUtils.MiB, 2)]
        [DataRow(BlobSizeUtils.MiB, (2 * BlobSizeUtils.MiB) + 1, 3)]
        [DataRow(BlobSizeUtils.MiB, (2 * BlobSizeUtils.MiB) - 1, 2)]
        [DataRow(BlobSizeUtils.MiB, 0, 1)]
        public async Task StartPartsProducersAsync_ProducesTheExpectedNumberOfParts(int blockSizeBytes, long fileSize,
            int expectedParts)
        {
            var options = new BlobPipelineOptions(BlockSizeBytes: blockSizeBytes);
            partsProducer = new PartsProducer(pipeline.Object, options);
            pipeline.Setup(p => p.GetSourceLengthAsync(It.IsAny<string>())).ReturnsAsync(fileSize);

            var blobOp = new BlobOperationInfo(new Uri("https://foo.bar/con/blob"), "blob", "blob", false);
            var opsList = new List<BlobOperationInfo>() { blobOp };

            await partsProducer.StartPartsProducersAsync(opsList, readBuffer);

            readBuffer.Writer.Complete();

            var parts = await RunnerTestUtils.ReadAllPipelineBuffersAsync(readBuffer.Reader.ReadAllAsync());

            Assert.IsNotNull(parts);
            Assert.AreEqual(expectedParts, parts.Count);
        }

        [DataTestMethod]
        [DataRow(BlobSizeUtils.MiB, 5 * BlobSizeUtils.MiB, BlobSizeUtils.MiB, BlobSizeUtils.MiB, BlobSizeUtils.MiB, BlobSizeUtils.MiB, BlobSizeUtils.MiB)]
        [DataRow(BlobSizeUtils.MiB, (2 * BlobSizeUtils.MiB) + 1, BlobSizeUtils.MiB, BlobSizeUtils.MiB, 1)]
        [DataRow(BlobSizeUtils.MiB, (2 * BlobSizeUtils.MiB) - 1, BlobSizeUtils.MiB, BlobSizeUtils.MiB - 1)]
        [DataRow(BlobSizeUtils.MiB, 0, 0)]
        public async Task StartPartsProducersAsync_PartsAreProperSize(int blockSize, long fileSize,
            params int[] expectedPartSize)
        {
            var options = new BlobPipelineOptions(BlockSizeBytes: blockSize);
            partsProducer = new PartsProducer(pipeline.Object, options);
            pipeline.Setup(p => p.GetSourceLengthAsync(It.IsAny<string>())).ReturnsAsync(fileSize);

            var blobOp = new BlobOperationInfo(new Uri("https://foo.bar/con/blob"), "blob", "blob", false);
            var opsList = new List<BlobOperationInfo>() { blobOp };

            await partsProducer.StartPartsProducersAsync(opsList, readBuffer);

            readBuffer.Writer.Complete();

            var parts = await RunnerTestUtils.ReadAllPipelineBuffersAsync(readBuffer.Reader.ReadAllAsync());

            Assert.AreEqual(expectedPartSize.Length, parts.Count);

            for (var i = 0; i < parts.Count; i++)
            {
                Assert.AreEqual(expectedPartSize[i], parts[i].Length);
            }
        }

        [DataTestMethod]
        [DataRow(BlobSizeUtils.MiB * 10, (BlobSizeUtils.MiB * 100) + 1)]
        public async Task StartPartsProducersAsync_PartsHaveExpectedLengths(int blockSize, long fileSize)
        {
            var options = new BlobPipelineOptions(BlockSizeBytes: blockSize);
            partsProducer = new PartsProducer(pipeline.Object, options);
            pipeline.Setup(p => p.GetSourceLengthAsync(It.IsAny<string>())).ReturnsAsync(fileSize);

            var blobOp = new BlobOperationInfo(new Uri("https://foo.bar/con/blob"), "blob", "blob", false);
            var opsList = new List<BlobOperationInfo>() { blobOp };

            await partsProducer.StartPartsProducersAsync(opsList, readBuffer);

            readBuffer.Writer.Complete();

            var parts = await RunnerTestUtils.ReadAllPipelineBuffersAsync(readBuffer.Reader.ReadAllAsync());

            //Assert.AreEqual(expectedPartSize.Length, parts.Count);

            var partsFilesSize = parts.Sum(p => p.Length);

            var expectedOffset = 0;

            for (var i = 0; i < parts.Count; i++)
            {
                var part = parts[i];
                Assert.AreEqual(expectedOffset, part.Offset);

                expectedOffset += part.Length;
            }

            Assert.AreEqual(fileSize, partsFilesSize);
        }
    }
}
