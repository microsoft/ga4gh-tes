using System.Threading.Channels;
using Moq;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test
{
    [TestClass]
    [TestCategory("unit")]
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
    }
}
