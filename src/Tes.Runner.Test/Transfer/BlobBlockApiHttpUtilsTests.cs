// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Xml;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test.Transfer
{

    [TestClass]
    [TestCategory("Unit")]
    public class BlobBlockApiHttpUtilsTests
    {
        private readonly string stubRootHash = "ab123456789012345678901234567890";
        private readonly string blobUrlWithSasToken = "https://blob.com/bar/blob?sasToken";
        [TestMethod]
        public void CreatePutBlockRequestAsyncTest_RootHashIsSet_IncludesMetadataHeader()
        {
            var request = BlobBlockApiHttpUtils.CreateBlobBlockListRequest(
                 BlobSizeUtils.MiB * 10, new Uri(blobUrlWithSasToken),
                 BlobSizeUtils.DefaultBlockSizeBytes, BlobPipelineOptions.DefaultApiVersion, stubRootHash);

            var headerName = $"x-ms-meta-{BlobBlockApiHttpUtils.RootHashMetadataName}";
            Assert.IsTrue(request.Headers.Contains(headerName));
            Assert.IsTrue(request.Headers.TryGetValues(headerName, out var values));
            Assert.IsTrue(values.Contains(stubRootHash));
        }

        [TestMethod]
        public void CreatePutBlockRequestAsyncTest_RootHashIsNull_NoMetadataHeader()
        {
            var request = BlobBlockApiHttpUtils.CreateBlobBlockListRequest(
                BlobSizeUtils.MiB * 10, new Uri(blobUrlWithSasToken),
                BlobSizeUtils.DefaultBlockSizeBytes, BlobPipelineOptions.DefaultApiVersion, null);

            var headerName = $"x-ms-meta-{BlobBlockApiHttpUtils.RootHashMetadataName}";
            Assert.IsFalse(request.Headers.Contains(headerName));
        }

        [TestMethod]
        public void ParsePutBlockUrlTest_BasedUrlWithSasToken_PutBlockUrlIsParsed()
        {
            var ordinal = 0;
            var blockId = BlobBlockApiHttpUtils.ToBlockId(ordinal);
            var expectedUri = new Uri($"{blobUrlWithSasToken}&comp=block&blockid={blockId}");

            var putBlockUrl = BlobBlockApiHttpUtils.ParsePutBlockUrl(new Uri(blobUrlWithSasToken), ordinal);

            Assert.AreEqual(expectedUri, putBlockUrl);
        }

        [TestMethod]
        public void ToBlockIdTest_BlockOrdinalIsProvided_BlockIsParsed()
        {
            var expectedBlockId = "YmxvY2swMDAwMQ==";  //block00001
            var blockId = BlobBlockApiHttpUtils.ToBlockId(1);

            Assert.AreEqual(expectedBlockId, blockId);
        }

        [TestMethod]
        public async Task CreateBlobBlockListRequest_10PartsBlobCreates10BlockList()
        {
            var fileSize = BlobSizeUtils.DefaultBlockSizeBytes * 10;
            var blobUrl = new Uri(blobUrlWithSasToken);
            var blockListRequest = BlobBlockApiHttpUtils.CreateBlobBlockListRequest(fileSize, blobUrl, BlobSizeUtils.DefaultBlockSizeBytes,
                BlobPipelineOptions.DefaultApiVersion, null);
            var xmlDoc = new XmlDocument();


            var xmlList = await blockListRequest.Content!.ReadAsStringAsync();
            Assert.IsNotNull(xmlList);

            xmlDoc.LoadXml(xmlList);
            var xmlBlockList = xmlDoc.GetElementsByTagName("Latest");

            Assert.IsNotNull(xmlBlockList);
            Assert.AreEqual(10, xmlBlockList.Count);

            for (var i = 0; i < 10; i++)
            {
                var blockId = BlobBlockApiHttpUtils.ToBlockId(i);
                Assert.AreEqual(blockId, xmlBlockList[i]!.InnerText);
            }
        }
    }
}
