// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net;
using System.Xml;
using Moq;
using Moq.Protected;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test.Transfer
{

    [TestClass]
    [TestCategory("Unit")]
    public class BlobBlockApiHttpUtilsTests
    {
        private readonly string stubRootHash = "ab123456789012345678901234567890";
        private readonly string blobUrlWithSasToken = "https://blob.com/bar/blob?sasToken";
        private Mock<HttpMessageHandler> mockHttpMessageHandler = null!;
        private BlobBlockApiHttpUtils blobBlockApiHttpUtils = null!;
        private const int MaxRetryCount = 3;

        [TestInitialize]
        public void SetUp()
        {
            mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            blobBlockApiHttpUtils = new BlobBlockApiHttpUtils(new HttpClient(mockHttpMessageHandler.Object),
                BlobBlockApiHttpUtils.DefaultAsyncRetryPolicy(MaxRetryCount));
        }

        [DataTestMethod]
        [DataRow(typeof(TimeoutException))]
        [DataRow(typeof(IOException))]
        public async Task ExecuteHttpRequestAndReadBodyResponseAsync_TaskIsCancelledWithRetriableException_RetriesAndSucceeds(Type innerExceptionType)
        {
            var retryCount = 0;
            var expectedRetryCount = MaxRetryCount;
            var expectedException = new TaskCanceledException("task cancelled", (Exception)Activator.CreateInstance(innerExceptionType)!);

            var buffer = new PipelineBuffer() { Data = Array.Empty<byte>() };
            mockHttpMessageHandler
                .Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((_, _) =>
                {
                    retryCount++;
                    if (retryCount < expectedRetryCount)
                    {
                        throw expectedException;
                    }
                })
                .Returns(Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)));

            await blobBlockApiHttpUtils.ExecuteHttpRequestAndReadBodyResponseAsync(buffer,
                () => new HttpRequestMessage(HttpMethod.Get, "https://foo.com"));

            Assert.AreEqual(expectedRetryCount, retryCount);
        }


        [DataTestMethod]
        [DataRow(typeof(ArgumentNullException))]
        [DataRow(typeof(InvalidOperationException))]
        [ExpectedException(typeof(TaskCanceledException))]
        public async Task ExecuteHttpRequestAndReadBodyResponseAsync_TaskIsCancelledWithNotRetriableException_NoRetriesAndFails(Type innerExceptionType)
        {
            var expectedException = new TaskCanceledException("task cancelled", (Exception)Activator.CreateInstance(innerExceptionType)!);

            var buffer = new PipelineBuffer() { Data = Array.Empty<byte>() };
            mockHttpMessageHandler
                .Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((_, _) => throw expectedException)
                .Returns(Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)));

            await blobBlockApiHttpUtils.ExecuteHttpRequestAndReadBodyResponseAsync(buffer,
                () => new HttpRequestMessage(HttpMethod.Get, "https://foo.com"));

        }

        [DataTestMethod]
        [DataRow(typeof(TimeoutException))]
        [DataRow(typeof(IOException))]
        public async Task ExecuteHttpRequestAsync_TaskIsCancelledWithRetriableException_RetriesAndSucceeds(
            Type innerExceptionType)
        {
            var retryCount = 0;
            var expectedRetryCount = MaxRetryCount;
            var expectedException = new TaskCanceledException("task cancelled",
                               (Exception)Activator.CreateInstance(innerExceptionType)!);
            mockHttpMessageHandler
                .Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(),
                                       ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((_, _) =>
                {
                    retryCount++;
                    if (retryCount < expectedRetryCount)
                    {
                        throw expectedException;
                    }
                })
                .Returns(Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)));
            await blobBlockApiHttpUtils.ExecuteHttpRequestAsync(() =>
                               new HttpRequestMessage(HttpMethod.Get, "https://foo.com"));
            Assert.AreEqual(expectedRetryCount, retryCount);
        }

        [DataTestMethod]
        [DataRow(typeof(ArgumentNullException))]
        [DataRow(typeof(InvalidOperationException))]
        [ExpectedException(typeof(TaskCanceledException))]
        public async Task ExecuteHttpRequestAsync_TaskIsCancelledWithNotRetriableException_NoRetriesAndFails(
            Type innerExceptionType)
        {
            var expectedException = new TaskCanceledException("task cancelled",
                                              (Exception)Activator.CreateInstance(innerExceptionType)!);
            mockHttpMessageHandler
                .Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(),
                                                          ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((_, _) => throw expectedException)
                .Returns(Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)));
            await blobBlockApiHttpUtils.ExecuteHttpRequestAsync(() =>
                                              new HttpRequestMessage(HttpMethod.Get, "https://foo.com"));
        }

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
