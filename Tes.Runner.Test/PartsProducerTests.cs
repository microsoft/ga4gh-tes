using System.Threading.Channels;
using Moq;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test
{
    [TestClass]
    [TestCategory("Unit")]
    public class PartsProducerTests
    {
        private Mock<IBlobPipeline> pipeline;
        private PartsProducer partsProducer;
        private Channel<PipelineBuffer> readBuffer;

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
        public async Task StartPartsProducersAsync_ProducesTheExpectedNumberOfParts(int blockSize, long fileSize,
            int expectedParts)
        {
            var options = new BlobPipelineOptions(BlockSize: blockSize);
            partsProducer = new PartsProducer(pipeline.Object, options);
            pipeline.Setup(p => p.GetSourceLengthAsync(It.IsAny<string>())).ReturnsAsync(fileSize);

            var blobOp = new BlobOperationInfo(new Uri("https://foo.bar/con/blob"), "blob", "blob", false);

            await partsProducer.StartPartsProducersAsync(new List<BlobOperationInfo>() { blobOp }, readBuffer);

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
            var options = new BlobPipelineOptions(BlockSize: blockSize);
            partsProducer = new PartsProducer(pipeline.Object, options);
            pipeline.Setup(p => p.GetSourceLengthAsync(It.IsAny<string>())).ReturnsAsync(fileSize);

            var blobOp = new BlobOperationInfo(new Uri("https://foo.bar/con/blob"), "blob", "blob", false);

            await partsProducer.StartPartsProducersAsync(new List<BlobOperationInfo>() { blobOp }, readBuffer);

            readBuffer.Writer.Complete();

            var parts = await RunnerTestUtils.ReadAllPipelineBuffersAsync(readBuffer.Reader.ReadAllAsync());

            Assert.AreEqual(expectedPartSize.Length, parts.Count);

            for (int i = 0; i < parts.Count; i++)
            {
                Assert.AreEqual(expectedPartSize[i], parts[i].Length);
            }
        }
    }
}
