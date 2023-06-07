// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Transfer;

namespace Tes.Runner.Test.Transfer
{
    [TestClass]
    public class BlobSizeUtilsTests
    {
        [DataTestMethod]
        [DataRow(BlobSizeUtils.GiB, 8 * BlobSizeUtils.MiB)]
        [DataRow(500 * BlobSizeUtils.GiB, 8 * BlobSizeUtils.MiB)]
        [DataRow(500 * BlobSizeUtils.GiB, 24 * BlobSizeUtils.MiB)]
        public void GetNumberOfParts_VariousFileAndBlockSizes_ReturnsExpectedNumberOfParts(long fileSizeInBytes, int blockSizeInBytes)
        {
            var partNumbers = BlobSizeUtils.GetNumberOfParts(fileSizeInBytes, blockSizeInBytes);
            var expectedNumberOfParts = (fileSizeInBytes + blockSizeInBytes - 1) / blockSizeInBytes;
            Assert.AreEqual(expectedNumberOfParts, partNumbers);
        }
        
        [TestMethod]
        public void ValidateBlockSizeForUploadTest_BlockSizeIsLessThanIncrementUnit_ThrowsInvalidOperationException()
        {
            var blockSize = BlobSizeUtils.BlockSizeIncrementUnitInBytes - 1;

           Assert.ThrowsException<InvalidOperationException>(()=>BlobSizeUtils.ValidateBlockSizeForUpload(blockSize));
        }

        [TestMethod]
        public void ValidateBlockSizeForUploadTest_BlockSizeIsNotAMultipleOfTheIncrementUnit_ThrowsInvalidOperationException()
        {
            var blockSize = (2 * BlobSizeUtils.BlockSizeIncrementUnitInBytes) - 1;

            Assert.ThrowsException<InvalidOperationException>(() => BlobSizeUtils.ValidateBlockSizeForUpload(blockSize));
        }

        [TestMethod]
        public void ValidateBlockSizeForUploadTest_BlockSizeIsAMultipleOfTheIncrementUnit_NoExceptionThrown()
        {
            var blockSize = (2 * BlobSizeUtils.BlockSizeIncrementUnitInBytes);

             BlobSizeUtils.ValidateBlockSizeForUpload(blockSize);
        }
    }
}