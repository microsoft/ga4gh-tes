﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net;
using System.Web;
using Azure.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Polly;
using Tes.ApiClients.Models.Terra;
using TesApi.Web.Management.Models.Terra;
using static Tes.ApiClients.CachingRetryHandler;

namespace Tes.ApiClients.Tests
{
    [TestClass, TestCategory("Unit")]
    public class TerraWsmApiClientTests
    {
        private TerraWsmApiClient terraWsmApiClient = null!;
        private Mock<TokenCredential> tokenCredential = null!;
        private Mock<CachingRetryHandler> cacheAndRetryHandler = null!;
        private Lazy<Mock<ICachingAsyncPolicy>> asyncRetryPolicy = null!;
        private Lazy<Mock<ICachingAsyncPolicy<HttpResponseMessage>>> asyncResponseRetryPolicy = null!;
        private TerraApiStubData terraApiStubData = null!;

        [TestInitialize]
        public void SetUp()
        {
            terraApiStubData = new TerraApiStubData();
            tokenCredential = new Mock<TokenCredential>();
            cacheAndRetryHandler = new Mock<CachingRetryHandler>();
            var cache = new Mock<Microsoft.Extensions.Caching.Memory.IMemoryCache>();
            cache.Setup(c => c.CreateEntry(It.IsAny<object>())).Returns(new Mock<Microsoft.Extensions.Caching.Memory.ICacheEntry>().Object);
            cacheAndRetryHandler.SetupGet(c => c.AppCache).Returns(cache.Object);
            asyncResponseRetryPolicy = new(TestServices.RetryHandlersHelpers.GetCachingHttpResponseMessageAsyncRetryPolicyMock(cacheAndRetryHandler));
            asyncRetryPolicy = new(TestServices.RetryHandlersHelpers.GetCachingAsyncRetryPolicyMock(cacheAndRetryHandler));
            terraWsmApiClient = new TerraWsmApiClient(TerraApiStubData.WsmApiHost, tokenCredential.Object,
                cacheAndRetryHandler.Object, NullLogger<TerraWsmApiClient>.Instance);
        }

        [TestMethod]
        public void GetContainerSasTokenApiUri_NoSasParameters_ReturnsExpectedUriWithoutQueryString()
        {
            var uri = terraWsmApiClient.GetSasTokenApiUrl(terraApiStubData.WorkspaceId,
                terraApiStubData.ContainerResourceId, null!);

            var expectedUri =
                $"{TerraApiStubData.WsmApiHost}/api/workspaces/v1/{terraApiStubData.WorkspaceId}/resources/controlled/azure/storageContainer/{terraApiStubData.ContainerResourceId}/getSasToken";
            Assert.AreEqual(expectedUri, uri.ToString());
            Assert.AreEqual(string.Empty, uri.Query);
        }

        [TestMethod]
        public void GetContainerSasTokenApiUri_WithAllSasParameters_ReturnsExpectedUriWithQueryString()
        {
            var sasParams = new SasTokenApiParameters("ipRange", 10, "rwdl", "blobName");

            var uri = terraWsmApiClient.GetSasTokenApiUrl(terraApiStubData.WorkspaceId,
                terraApiStubData.ContainerResourceId, sasParams);

            var expectedUri =
                $"{TerraApiStubData.WsmApiHost}/api/workspaces/v1/{terraApiStubData.WorkspaceId}/resources/controlled/azure/storageContainer/{terraApiStubData.ContainerResourceId}/getSasToken"
                + $"?sasIpRange={sasParams.SasIpRange}&sasExpirationDuration={sasParams.SasExpirationInSeconds}&sasPermissions={sasParams.SasPermission}&sasBlobName={sasParams.SasBlobName}";

            var parsedQs = HttpUtility.ParseQueryString(uri.Query);

            Assert.AreEqual(expectedUri, uri.ToString());
            Assert.AreEqual(parsedQs["sasIpRange"], sasParams.SasIpRange);
            Assert.AreEqual(parsedQs["sasExpirationDuration"], sasParams.SasExpirationInSeconds.ToString());
            Assert.AreEqual(parsedQs["sasPermissions"], sasParams.SasPermission);
            Assert.AreEqual(parsedQs["sasBlobName"], sasParams.SasBlobName);
        }

        [TestMethod]
        public void GetContainerSasTokenApiUri_WithSomeSasParameters_ReturnsExpectedUriWithQueryString()
        {
            var sasParams = new SasTokenApiParameters("ipRange", 10, null!, null!);

            var uri = terraWsmApiClient.GetSasTokenApiUrl(terraApiStubData.WorkspaceId,
                terraApiStubData.ContainerResourceId, sasParams);

            var expectedUri =
                $"{TerraApiStubData.WsmApiHost}/api/workspaces/v1/{terraApiStubData.WorkspaceId}/resources/controlled/azure/storageContainer/{terraApiStubData.ContainerResourceId}/getSasToken"
                + $"?sasIpRange={sasParams.SasIpRange}&sasExpirationDuration={sasParams.SasExpirationInSeconds}";

            var parsedQs = HttpUtility.ParseQueryString(uri.Query);

            Assert.AreEqual(expectedUri, uri.ToString());
            Assert.AreEqual(parsedQs["sasIpRange"], sasParams.SasIpRange);
            Assert.AreEqual(parsedQs["sasExpirationDuration"], sasParams.SasExpirationInSeconds.ToString());
        }

        [TestMethod]
        public async Task GetSasTokenAsync_ValidRequest_ReturnsPayload()
        {
            var response = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(terraApiStubData.GetWsmSasTokenApiResponseInJson())
            };

            asyncResponseRetryPolicy.Value.Setup(c => c.ExecuteAsync(It.IsAny<Func<Polly.Context, CancellationToken, Task<HttpResponseMessage>>>(), It.IsAny<Polly.Context>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(response);

            asyncRetryPolicy.Value.Setup(c => c.ExecuteAsync(It.IsAny<Func<Polly.Context, CancellationToken, Task<string>>>(), It.IsAny<Polly.Context>(), It.IsAny<CancellationToken>()))
                .Returns((Func<Polly.Context, CancellationToken, Task<string>> action, Polly.Context context, CancellationToken cancellationToken) => action(context, cancellationToken));

            var apiResponse = await terraWsmApiClient.GetSasTokenAsync(terraApiStubData.WorkspaceId,
                terraApiStubData.ContainerResourceId, null!, CancellationToken.None);

            Assert.IsNotNull(apiResponse);
            Assert.IsTrue(!string.IsNullOrEmpty(apiResponse.Token));
            Assert.IsTrue(!string.IsNullOrEmpty(apiResponse.Url));
        }

        [TestMethod]
        public async Task GetContainerResourcesAsync_ValidRequest_ReturnsPayload()
        {
            var response = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(terraApiStubData.GetContainerResourcesApiResponseInJson())
            };

            asyncResponseRetryPolicy.Value
                .Setup(c => c.ExecuteAsync(It.IsAny<Func<Polly.Context, CancellationToken, Task<HttpResponseMessage>>>(), It.IsAny<Polly.Context>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(response);

            asyncRetryPolicy.Value
                .Setup(c => c.ExecuteAsync(It.IsAny<Func<Polly.Context, CancellationToken, Task<string>>>(), It.IsAny<Polly.Context>(), It.IsAny<CancellationToken>()))
                .Returns((Func<Polly.Context, CancellationToken, Task<string>> action, Polly.Context context, CancellationToken cancellationToken) => action(context, cancellationToken));

            var apiResponse = await terraWsmApiClient.GetContainerResourcesAsync(terraApiStubData.WorkspaceId,
                offset: 0, limit: 10, CancellationToken.None);

            Assert.IsNotNull(apiResponse);
            Assert.AreEqual(1, apiResponse.Resources.Count);
            Assert.IsTrue(apiResponse.Resources.Any(r => r.Metadata.ResourceId.ToString().Equals(terraApiStubData.ContainerResourceId.ToString(), StringComparison.OrdinalIgnoreCase)));
        }

        [TestMethod]
        public async Task DeleteBatchPoolAsync_204Response_Succeeds()
        {
            var wsmResourceId = Guid.NewGuid();
            var response = new HttpResponseMessage(HttpStatusCode.NoContent);

            asyncResponseRetryPolicy.Value
                .Setup(c => c.ExecuteAsync(It.IsAny<Func<Polly.Context, CancellationToken, Task<HttpResponseMessage>>>(), It.IsAny<Polly.Context>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(response);

            await terraWsmApiClient.DeleteBatchPoolAsync(terraApiStubData.WorkspaceId, wsmResourceId, CancellationToken.None);
        }

        [TestMethod]
        public void GetDeleteBatchPoolUrl_ValidWorkspaceAndResourceId_ValidWSMUrl()
        {
            var wsmResourceId = Guid.NewGuid();

            var url = terraWsmApiClient.GetDeleteBatchPoolUrl(terraApiStubData.WorkspaceId, wsmResourceId);

            Assert.IsNotNull(url);
            var expectedUrl = $"{TerraApiStubData.WsmApiHost}/api/workspaces/v1/{terraApiStubData.WorkspaceId}/resources/controlled/azure/batchpool/{wsmResourceId}";
            Assert.AreEqual(expectedUrl, url);
        }

    }
}
