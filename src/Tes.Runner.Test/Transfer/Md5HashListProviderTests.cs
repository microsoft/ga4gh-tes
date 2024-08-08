// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Transfer;

namespace Tes.Runner.Test.Transfer
{
    [TestClass]
    [TestCategory("Unit")]
    public class Md5HashListProviderTests
    {
        private Md5HashListProvider hashListProvider = null!;

        [TestInitialize]
        public void Setup()
        {
            hashListProvider = new Md5HashListProvider(Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);
        }

        [TestMethod]
        public void CalculateAndAddBlockHash_10Buffers_EqualNumberOfEntriesAreAddedToTheHashList()
        {
            var numberOfParts = 10;
            var buffers = RunnerTestUtils.CreatePipelineBuffers(numberOfParts, new Uri("https://foo.bar"),
                BlobSizeUtils.DefaultBlockSizeBytes, BlobSizeUtils.DefaultBlockSizeBytes * numberOfParts, "fooFile");

            foreach (var buffer in buffers)
            {
                hashListProvider.CalculateAndAddBlockHash(buffer);
            }

            Assert.AreEqual(numberOfParts, hashListProvider.HashList.Count);
        }


        [TestMethod]
        public void CalculateAndAddBlockHash_BufferSizeIsGreaterThanHashListItemBlockSizeInBytes_Md5HashListIsCalculatedForBuffer()
        {
            //this will create a buffer 10 x larger than the hash list item block size
            var buffer = RunnerTestUtils.CreateBufferWithRandomData(Md5HashListProvider.HashListItemBlockSizeInBytes * 10);

            hashListProvider.CalculateAndAddBlockHash(buffer);

            //create all hash items in the buffer independently
            var referenceHashes = new List<string>();
            for (var i = 0; i < 10; i++)
            {
                var start = i * Md5HashListProvider.HashListItemBlockSizeInBytes;
                var end = start + Md5HashListProvider.HashListItemBlockSizeInBytes;
                referenceHashes.Add(RunnerTestUtils.CalculateMd5Hash(buffer.Data[start..end]));
            }

            Assert.AreEqual(1, hashListProvider.HashList.Count);
            Assert.AreEqual(hashListProvider.HashList.Values.First(), string.Join("", referenceHashes));
        }

        [TestMethod]
        public void CalculateAndAddBlockHash_BufferSizeIsGreaterThanHashListItemBlockSizeInBytesAndNotAMultiple_Md5HashListIsCalculatedForBuffer()
        {
            //this will create a buffer 2 x larger than the hash list item block size with extra bytes
            var extraBytes = 5;
            var buffer = RunnerTestUtils.CreateBufferWithRandomData((Md5HashListProvider.HashListItemBlockSizeInBytes * 2) + extraBytes);

            hashListProvider.CalculateAndAddBlockHash(buffer);

            //create all hash items in the buffer independently
            var referenceHashes = new List<string>();
            for (var i = 0; i < 3; i++)
            {
                var start = i * Md5HashListProvider.HashListItemBlockSizeInBytes;
                var end = start + Md5HashListProvider.HashListItemBlockSizeInBytes;
                if (i == 2)
                {
                    end = start + extraBytes;
                }
                referenceHashes.Add(RunnerTestUtils.CalculateMd5Hash(buffer.Data[start..end]));
            }

            Assert.AreEqual(1, hashListProvider.HashList.Count);
            Assert.AreEqual(hashListProvider.HashList.Values.First(), string.Join("", referenceHashes));
        }

        [TestMethod]
        public void GetRootHashTest_RootHashIsCalculated()
        {
            var numberOfParts = 10;
            var buffers = RunnerTestUtils.CreatePipelineBuffers(numberOfParts, new Uri("https://foo.bar"),
                BlobSizeUtils.DefaultBlockSizeBytes, BlobSizeUtils.DefaultBlockSizeBytes * numberOfParts, "fooFile");

            var referenceHashes = new List<string>();

            foreach (var buffer in buffers)
            {
                //the default block size is 8MiB therefore each block generates two hash items
                var start = 0;
                var end = Md5HashListProvider.HashListItemBlockSizeInBytes;
                var referenceHash = RunnerTestUtils.CalculateMd5Hash(buffer.Data[start..end]);
                referenceHashes.Add(referenceHash);

                start = Md5HashListProvider.HashListItemBlockSizeInBytes;
                end = Md5HashListProvider.HashListItemBlockSizeInBytes * 2;
                referenceHash = RunnerTestUtils.CalculateMd5Hash(buffer.Data[start..end]);
                referenceHashes.Add(referenceHash);

                hashListProvider.CalculateAndAddBlockHash(buffer);
            }

            var rootHash = hashListProvider.GetRootHash();

            Assert.AreEqual(RunnerTestUtils.GetRootHashFromSortedHashList(referenceHashes), rootHash);
        }
    }
}
