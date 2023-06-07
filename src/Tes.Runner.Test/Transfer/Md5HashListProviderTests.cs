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
            hashListProvider = new Md5HashListProvider();
        }

        [TestMethod]
        public void CalculateAndAddBlockHash_HashesAreAddedToTheList()
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
        public void CalculateAndAddBlockHash_Md5HashIsCalculatedForEachBlock()
        {
            var numberOfParts = 10;
            var buffers = RunnerTestUtils.CreatePipelineBuffers(numberOfParts, new Uri("https://foo.bar"),
                BlobSizeUtils.DefaultBlockSizeBytes, BlobSizeUtils.DefaultBlockSizeBytes * numberOfParts, "fooFile");

            var referenceHashes = new List<string>();

            foreach (var buffer in buffers)
            {
                var referenceHash = RunnerTestUtils.AddRandomDataAndReturnMd5(buffer.Data);
                referenceHashes.Add(referenceHash);
                hashListProvider.CalculateAndAddBlockHash(buffer);
            }

            Assert.AreEqual(numberOfParts, hashListProvider.HashList.Count);
            Assert.IsTrue(hashListProvider.HashList.All(h => referenceHashes.Contains(h.Value)));
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
                var referenceHash = RunnerTestUtils.AddRandomDataAndReturnMd5(buffer.Data);
                referenceHashes.Add(referenceHash);
                hashListProvider.CalculateAndAddBlockHash(buffer);
            }

            var rootHash = hashListProvider.GetRootHash();

            Assert.AreEqual(RunnerTestUtils.GetRootHashFromSortedHashList(referenceHashes), rootHash);
        }
    }
}
