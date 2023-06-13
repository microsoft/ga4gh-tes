// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Moq;
using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test.Transfer
{
    [TestClass]
    [TestCategory("Unit")]
    public class PipelineOptionsOptimizerTests
    {
        private PipelineOptionsOptimizer optimizer = null!;
        private Mock<ISystemInfoProvider> systemInfoProviderMock = null!;
        private Mock<IFileInfoProvider> fileInfoProviderMock = null!;

        [TestInitialize]
        public void SetUp()
        {
            fileInfoProviderMock = new Mock<IFileInfoProvider>();
            systemInfoProviderMock = new Mock<ISystemInfoProvider>();
            optimizer = new PipelineOptionsOptimizer(systemInfoProviderMock.Object, fileInfoProviderMock.Object);
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
        [DataRow(1, 50)]
        [DataRow(2, 100)]
        [DataRow(3, 150)]
        [DataRow(4, 200)]
        [DataRow(5, 250)]
        [DataRow(6, 250)]
        [DataRow(7, 250)]
        [DataRow(8, 250)]
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
        [DataRow(1, 10)]
        [DataRow(2, 20)]
        [DataRow(3, 30)]
        [DataRow(4, 40)]
        [DataRow(5, 50)]
        [DataRow(6, 60)]
        [DataRow(10, 90)]
        [DataRow(11, 90)]
        public void OptimizeOptionsIfApplicable_DefaultOptionsAreProvided_OptimizesReadersAndWriters(int numberOfCores, int expectedReadersAndWriters)
        {
            var options = new BlobPipelineOptions();

            systemInfoProviderMock.Setup(x => x.ProcessorCount).Returns(numberOfCores);

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

        [DataTestMethod]
        [DataRow(1, 8)]
        [DataRow(390, 8)]
        [DataRow(400, 12)]
        [DataRow(500, 12)]
        [DataRow(700, 16)]
        [DataRow(900, 20)]
        [DataRow(1100, 24)]
        public void OptimizeOptionsIfApplicable_LargeFileIsProvided_BlockSizeIsOptimized(int fileSizeInGiB, int expectedBlockSizeInMiB)
        {
            systemInfoProviderMock.Setup(x => x.TotalMemory).Returns(10 * BlobSizeUtils.GiB);
            fileInfoProviderMock.Setup(x => x.GetFileSize(It.IsAny<string>())).Returns(fileSizeInGiB * BlobSizeUtils.GiB);

            var outputs = new List<FileOutput>() { new FileOutput() { Path = "fileName", SasStrategy = SasResolutionStrategy.None, TargetUrl = "https://blob.foo/cont/blob" } };
            var options = new BlobPipelineOptions();

            var newOptions = optimizer.Optimize(options, outputs);

            Assert.IsNotNull(newOptions);
            Assert.AreEqual(expectedBlockSizeInMiB, newOptions.BlockSizeBytes / BlobSizeUtils.MiB);
        }
    }
}
