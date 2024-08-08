// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Xml;
using Moq;
using Moq.Protected;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test.Transfer
{

    [TestClass]
    [TestCategory("Unit")]
    public class BlobApiHttpUtilsTests
    {
        private readonly string stubRootHash = "ab123456789012345678901234567890";
        private readonly string blobUrlWithSasToken = "https://blob.com/bar/blob?sasToken";
        private Mock<HttpMessageHandler> mockHttpMessageHandler = null!;
        private BlobApiHttpUtils blobApiHttpUtils = null!;
        private const int MaxRetryCount = 3;

        [TestInitialize]
        public void SetUp()
        {
            mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            blobApiHttpUtils = new BlobApiHttpUtils(new HttpClient(mockHttpMessageHandler.Object),
                logger => HttpRetryPolicyDefinition.DefaultAsyncRetryPolicy(logger, MaxRetryCount),
                Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);
        }

        [DataTestMethod]
        [DataRow(typeof(TimeoutException))]
        [DataRow(typeof(IOException))]
        public async Task ExecuteHttpRequestAndReadBodyResponseAsync_TaskIsCancelledWithRetriableException_RetriesAndSucceeds(Type innerExceptionType)
        {
            var retryCount = 0;
            var expectedRetryCount = MaxRetryCount;
            var expectedException = new TaskCanceledException("task cancelled", (Exception)Activator.CreateInstance(innerExceptionType)!);

            var buffer = new PipelineBuffer() { Data = [] };
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

            await blobApiHttpUtils.ExecuteHttpRequestAndReadBodyResponseAsync(buffer,
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

            var buffer = new PipelineBuffer() { Data = [] };
            mockHttpMessageHandler
                .Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
                .Callback<HttpRequestMessage, CancellationToken>((_, _) => throw expectedException)
                .Returns(Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)));

            await blobApiHttpUtils.ExecuteHttpRequestAndReadBodyResponseAsync(buffer,
                () => new HttpRequestMessage(HttpMethod.Get, "https://foo.com"));

        }

        [DataTestMethod]
        [DataRow(typeof(TimeoutException))]
        [DataRow(typeof(IOException))]
        [DataRow(typeof(SocketException))]
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
            await blobApiHttpUtils.ExecuteHttpRequestAsync(() =>
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
            await blobApiHttpUtils.ExecuteHttpRequestAsync(() =>
                                              new HttpRequestMessage(HttpMethod.Get, "https://foo.com"));
        }

        [TestMethod]
        public void CreatePutBlockRequestAsyncTest_RootHashIsSet_IncludesMetadataHeader()
        {
            var request = BlobApiHttpUtils.CreateBlobBlockListRequest(
                 BlobSizeUtils.MiB * 10, new Uri(blobUrlWithSasToken),
                 BlobSizeUtils.DefaultBlockSizeBytes, BlobPipelineOptions.DefaultApiVersion, stubRootHash, default);

            var headerName = $"x-ms-meta-{BlobApiHttpUtils.RootHashMetadataName}";
            Assert.IsTrue(request.Headers.Contains(headerName));
            Assert.IsTrue(request.Headers.TryGetValues(headerName, out var values));
            Assert.IsTrue(values.Contains(stubRootHash));
        }

        [TestMethod]
        public void CreatePutBlockRequestAsyncTest_ContentMd5IsSet_IncludesContentMd5HeaderBase64Encoded()
        {
            var contentMd5 = "iXMWkpF2Rk68mtCF8x5yhA==";
            var request = BlobApiHttpUtils.CreateBlobBlockListRequest(
                BlobSizeUtils.MiB * 10, new Uri(blobUrlWithSasToken),
                BlobSizeUtils.DefaultBlockSizeBytes, BlobPipelineOptions.DefaultApiVersion, default, contentMd5);

            Assert.IsTrue(request.Headers.Contains("x-ms-blob-content-md5"));
            Assert.IsTrue(request.Headers.TryGetValues("x-ms-blob-content-md5", out var values));
            Assert.IsTrue(values.Contains(contentMd5));
        }

        [TestMethod]
        public void CreatePutBlockRequestAsyncTest_RootHashIsNull_NoMetadataHeader()
        {
            var request = BlobApiHttpUtils.CreateBlobBlockListRequest(
                BlobSizeUtils.MiB * 10, new Uri(blobUrlWithSasToken),
                BlobSizeUtils.DefaultBlockSizeBytes, BlobPipelineOptions.DefaultApiVersion, default, default);

            var headerName = $"x-ms-meta-{BlobApiHttpUtils.RootHashMetadataName}";
            Assert.IsFalse(request.Headers.Contains(headerName));
        }

        [TestMethod]
        public void ParsePutBlockUrlTest_BasedUrlWithSasToken_PutBlockUrlIsParsed()
        {
            var ordinal = 0;
            var blockId = BlobApiHttpUtils.ToBlockId(ordinal);
            var expectedUri = new Uri($"{blobUrlWithSasToken}&comp=block&blockid={blockId}");

            var putBlockUrl = BlobApiHttpUtils.ParsePutBlockUrl(new Uri(blobUrlWithSasToken), ordinal);

            Assert.AreEqual(expectedUri, putBlockUrl);
        }

        [TestMethod]
        public void ToBlockIdTest_BlockOrdinalIsProvided_BlockIsParsed()
        {
            var expectedBlockId = "YmxvY2swMDAwMQ==";  //block00001
            var blockId = BlobApiHttpUtils.ToBlockId(1);

            Assert.AreEqual(expectedBlockId, blockId);
        }

        [TestMethod]
        public async Task CreateBlobBlockListRequest_10PartsBlobCreates10BlockList()
        {
            var fileSize = BlobSizeUtils.DefaultBlockSizeBytes * 10;
            var blobUrl = new Uri(blobUrlWithSasToken);
            var blockListRequest = BlobApiHttpUtils.CreateBlobBlockListRequest(fileSize, blobUrl, BlobSizeUtils.DefaultBlockSizeBytes,
                BlobPipelineOptions.DefaultApiVersion, default, default);
            var xmlDoc = new XmlDocument();


            var xmlList = await blockListRequest.Content!.ReadAsStringAsync();
            Assert.IsNotNull(xmlList);

            xmlDoc.LoadXml(xmlList);
            var xmlBlockList = xmlDoc.GetElementsByTagName("Latest");

            Assert.IsNotNull(xmlBlockList);
            Assert.AreEqual(10, xmlBlockList.Count);

            for (var i = 0; i < 10; i++)
            {
                var blockId = BlobApiHttpUtils.ToBlockId(i);
                Assert.AreEqual(blockId, xmlBlockList[i]!.InnerText);
            }
        }

        [DataTestMethod]
        [DataRow("https://foo.com/cont", "https://foo.com/cont?comp=appendblock")]
        [DataRow("https://foo.com/cont/blob", "https://foo.com/cont/blob?comp=appendblock")]
        [DataRow("https://foo.com/cont/", "https://foo.com/cont/?comp=appendblock")]
        [DataRow("https://foo.com/cont/?sig=123", "https://foo.com/cont/?sig=123&comp=appendblock")]

        public void ParsePutAppendBlockUrl_ValidUrl_ExpectedResult(string baseUrl, string expectedResult)
        {
            var result = BlobApiHttpUtils.ParsePutAppendBlockUrl(new Uri(baseUrl));
            Assert.AreEqual(expectedResult, result.ToString());
        }



        [DataTestMethod]
        [DataRow("data", "https://foo.com/cont/blob?sg=signature")]
        [DataRow("", "https://foo.com/cont?sg=signature")]
        public async Task CreatePutAppendBlockRequestAsync_ValidInputs_ExpectedRequestIsCreated(string data, string url)
        {
            var request = BlobApiHttpUtils.CreatePutAppendBlockRequestAsync(data, new Uri(url), BlobApiHttpUtils.DefaultApiVersion);

            var expectedUrl = BlobApiHttpUtils.ParsePutAppendBlockUrl(new Uri(url));

            Assert.AreEqual(HttpMethod.Put, request.Method);
            Assert.AreEqual(expectedUrl.ToString(), request.RequestUri?.ToString());
            Assert.AreEqual(data, await request.Content?.ReadAsStringAsync()!);
            Assert.IsTrue(request.Headers.Contains("x-ms-version"));
            Assert.IsTrue(request.Headers.Contains("x-ms-date"));
        }

        [DataTestMethod]
        [DataRow("data", "data", "https://foo.com/cont/blob?sg=signature", BlobApiHttpUtils.BlockBlobType)]
        [DataRow("", null, "https://foo.com/cont/blob?sg=signature", BlobApiHttpUtils.AppendBlobType)]
        [DataRow("data", null, "https://foo.com/cont/blob?sg=signature", BlobApiHttpUtils.AppendBlobType)]
        public async Task CreatePutBlobRequestAsync_ValidInput_ExpectedRequestIsCreated(string data, string? expectedContent, string url, string blobType)
        {
            var request = BlobApiHttpUtils.CreatePutBlobRequestAsync(new Uri(url), data, BlobApiHttpUtils.DefaultApiVersion, tags: default, blobType);


            Assert.AreEqual(HttpMethod.Put, request.Method);
            Assert.AreEqual(url, request.RequestUri?.ToString());

            if (expectedContent is null)
            {
                Assert.IsNull(request.Content);
            }
            else
            {
                Assert.AreEqual(expectedContent, await request.Content?.ReadAsStringAsync()!);
            }


            Assert.IsTrue(request.Headers.Contains("x-ms-version"));
            Assert.IsTrue(request.Headers.Contains("x-ms-date"));

            Assert.IsTrue(request.Headers.Contains("x-ms-blob-type"));
            request.Headers.TryGetValues("x-ms-blob-type", out var actualBlobType);
            Assert.AreEqual(blobType, actualBlobType?.FirstOrDefault());
        }
    }
}
