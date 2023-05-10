using Moq;

namespace Tes.Runner.Transfer.Tests
{
    [TestClass]
    [TestCategory("Unit")]
    public class PipelineOptionsOptimizerTests
    {
        private PipelineOptionsOptimizer optimizer = null!;
        private Mock<ISystemInfoProvider> systemInfoProviderMock = null!;

        [TestInitialize]
        public void SetUp()
        {
            systemInfoProviderMock = new Mock<ISystemInfoProvider>();
            optimizer = new PipelineOptionsOptimizer(systemInfoProviderMock.Object);
        }

        /// <summary>
        /// The optimizer use ~40% of the available memory for the buffer size.
        /// If 40% of the of the available memory is greater than 2 GiB, the optimizer uses 2 GiB.
        /// This test verifies that the optimizer uses 2 GiB when the available memory is greater than 5 GiB.
        /// It assumes the default block size of 10 MiB.
        /// </summary>
        /// <param name="numberOfGigs"></param>
        /// <param name="expectedBufferSize"></param>
        [DataTestMethod]
        [DataRow(1, 40)]
        [DataRow(2, 80)]
        [DataRow(3, 120)]
        [DataRow(4, 160)]
        [DataRow(5, 200)]
        [DataRow(6, 200)]
        [DataRow(7, 200)]
        [DataRow(8, 200)]
        public void OptimizeOptionsIfApplicable_DefaultOptionsAreProvided_OptimizesMemoryBuffer(int numberOfGigs, int expectedBufferSize)
        {
            var options = new BlobPipelineOptions();

            systemInfoProviderMock.Setup(x => x.TotalMemory).Returns(numberOfGigs * BlobSizeUtils.GiB);

            var newOptions = optimizer.Optimize(options);

            Assert.IsNotNull(newOptions);
            Assert.AreEqual(expectedBufferSize, newOptions.ReadWriteBuffersCapacity);
            Assert.AreEqual(expectedBufferSize, newOptions.MemoryBufferCapacity);
        }

        [DataTestMethod]
        [DataRow(1, 40)]
        [DataRow(2, 80)]
        [DataRow(3, 90)]
        [DataRow(4, 90)]
        [DataRow(5, 90)]
        [DataRow(6, 90)]
        [DataRow(7, 90)]
        [DataRow(8, 90)]
        public void OptimizeOptionsIfApplicable_DefaultOptionsAreProvided_OptimizesReadersAndWriters(int numberOfGigs, int expectedReadersAndWriters)
        {
            var options = new BlobPipelineOptions();

            systemInfoProviderMock.Setup(x => x.TotalMemory).Returns(numberOfGigs * BlobSizeUtils.GiB);

            var newOptions = optimizer.Optimize(options);

            Assert.IsNotNull(newOptions);
            //to simplify the optimization, the number of readers and writers is always the same when optimized.
            Assert.AreEqual(expectedReadersAndWriters, newOptions.NumberOfWriters);
            Assert.AreEqual(expectedReadersAndWriters, newOptions.NumberOfReaders);
        }

        [TestMethod]
        public void OptimizeOptionsIfApplicable_TransferSettingsAreModified_NoOptimization()
        {
            systemInfoProviderMock.Setup(x => x.TotalMemory).Returns(1 * BlobSizeUtils.GiB);

            var options = new BlobPipelineOptions() { NumberOfReaders = 1 };
            var newOptions = optimizer.Optimize(options);

            Assert.IsNotNull(newOptions);
            Assert.AreEqual(newOptions, options);

            options = new BlobPipelineOptions() { NumberOfWriters = 1 };
            newOptions = optimizer.Optimize(options);

            Assert.IsNotNull(newOptions);
            Assert.AreEqual(newOptions, options);

            options = new BlobPipelineOptions() { MemoryBufferCapacity = 1 };
            newOptions = optimizer.Optimize(options);

            Assert.IsNotNull(newOptions);
            Assert.AreEqual(newOptions, options);

            options = new BlobPipelineOptions() { ReadWriteBuffersCapacity = 1 };
            newOptions = optimizer.Optimize(options);

            Assert.IsNotNull(newOptions);
            Assert.AreEqual(newOptions, options);
        }
    }
}
