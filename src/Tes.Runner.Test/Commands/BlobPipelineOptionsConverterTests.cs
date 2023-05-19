using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands.Tests
{
    [TestClass]
    [TestCategory("Unit")]
    public class BlobPipelineOptionsConverterTests
    {
        [TestMethod]
        public void ToCommandArgs_SetsAllOptionsAsCliOptions()
        {
            var args = BlobPipelineOptionsConverter.ToCommandArgs("upload", "file", new BlobPipelineOptions());

            Assert.IsNotNull(args);
            Assert.AreEqual("upload", args[0]);
            Assert.AreEqual($"--blockSize {BlobSizeUtils.DefaultBlockSizeBytes}", args[1]);
            Assert.AreEqual($"--writers {BlobPipelineOptions.DefaultNumberOfWriters}", args[2]);
            Assert.AreEqual($"--readers {BlobPipelineOptions.DefaultNumberOfReaders}", args[3]);
            Assert.AreEqual($"--bufferCapacity {BlobPipelineOptions.DefaultReadWriteBuffersCapacity}", args[4]);
            Assert.AreEqual($"--apiVersion {BlobPipelineOptions.DefaultApiVersion}", args[5]);
            Assert.AreEqual("--file file", args[6]);
        }

        [TestMethod]
        public void ToBlobPipelineOptions_CreatesBlobPipelinesOptions()
        {

            var options = BlobPipelineOptionsConverter.ToBlobPipelineOptions(1, 2, 3, 4, "2010-01-01");
            Assert.IsNotNull(options);
            Assert.AreEqual(1, options.BlockSizeBytes);
            Assert.AreEqual(2, options.NumberOfWriters);
            Assert.AreEqual(3, options.NumberOfReaders);
            Assert.AreEqual(4, options.ReadWriteBuffersCapacity);
            Assert.AreEqual(4, options.MemoryBufferCapacity);
            Assert.AreEqual("2010-01-01", options.ApiVersion);
        }
    }
}
