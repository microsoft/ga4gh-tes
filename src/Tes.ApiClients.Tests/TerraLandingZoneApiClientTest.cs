// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Tes.ApiClients.Tests
{
    [TestClass, TestCategory("Unit")]
    public class TerraLandingZoneApiClientTest
    {
        private TerraLandingZoneApiClient terraLandingZoneApiClient = null!;
        private Mock<TokenCredential> tokenCredential = null!;
        private Mock<CachingRetryHandler> cacheAndRetryHandler = null!;
        private Lazy<Mock<CachingRetryHandler.ICachingAsyncPolicy>> asyncRetryPolicy = null!;
        private Lazy<Mock<CachingRetryHandler.ICachingAsyncPolicy<HttpResponseMessage>>> asyncResponseRetryPolicy = null!;
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
            terraLandingZoneApiClient = new TerraLandingZoneApiClient(TerraApiStubData.LandingZoneApiHost, tokenCredential.Object, cacheAndRetryHandler.Object, NullLogger<TerraLandingZoneApiClient>.Instance);
        }

        [TestMethod]
        public async Task GetResourceQuotaAsync_ValidResourceIdReturnsQuotaInformationAndGetsAuthToken()
        {
            var body = terraApiStubData.GetResourceQuotaApiResponseInJson();

            asyncResponseRetryPolicy.Value.Setup(c => c.ExecuteAsync(
                    It.IsAny<Func<Polly.Context, CancellationToken, Task<HttpResponseMessage>>>(), It.IsAny<Polly.Context>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new HttpResponseMessage());

            asyncRetryPolicy.Value.Setup(c => c.ExecuteAsync(
                    It.IsAny<Func<Polly.Context, CancellationToken, Task<string>>>(), It.IsAny<Polly.Context>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(body);

            var quota = await terraLandingZoneApiClient.GetResourceQuotaAsync(terraApiStubData.LandingZoneId, terraApiStubData.BatchAccountId, cacheResults: true, cancellationToken: CancellationToken.None);

            Assert.IsNotNull(quota);
            Assert.AreEqual(terraApiStubData.LandingZoneId, quota.LandingZoneId);
            Assert.AreEqual(terraApiStubData.BatchAccountId, quota.AzureResourceId);
            Assert.AreEqual("Microsoft.Batch/batchAccounts", quota.ResourceType);
            Assert.AreEqual(100, quota.QuotaValues.PoolQuota);
            Assert.IsTrue(quota.QuotaValues.DedicatedCoreQuotaPerVmFamilyEnforced);
            Assert.AreEqual(300, quota.QuotaValues.ActiveJobAndJobScheduleQuota);
            Assert.AreEqual(350, quota.QuotaValues.DedicatedCoreQuota);
            Assert.AreEqual(59, quota.QuotaValues.DedicatedCoreQuotaPerVmFamily.Count);
            Assert.AreEqual("standardLSv2Family", quota.QuotaValues.DedicatedCoreQuotaPerVmFamily.Keys.First());
            Assert.AreEqual(0, quota.QuotaValues.DedicatedCoreQuotaPerVmFamily.Values.First());
            tokenCredential.Verify(t => t.GetTokenAsync(It.IsAny<TokenRequestContext>(),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [TestMethod]
        public async Task GetLandingZoneResourcesAsync_ListOfLandingZoneResourcesAndGetsAuthToken()
        {
            var body = terraApiStubData.GetResourceApiResponseInJson();

            asyncResponseRetryPolicy.Value.Setup(c => c.ExecuteAsync(
                    It.IsAny<Func<Polly.Context, CancellationToken, Task<HttpResponseMessage>>>(), It.IsAny<Polly.Context>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new HttpResponseMessage());

            asyncRetryPolicy.Value.Setup(c => c.ExecuteAsync(
                    It.IsAny<Func<Polly.Context, CancellationToken, Task<string>>>(), It.IsAny<Polly.Context>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(body);

            var resources = await terraLandingZoneApiClient.GetLandingZoneResourcesAsync(terraApiStubData.LandingZoneId, CancellationToken.None);

            Assert.IsNotNull(resources);
            Assert.AreEqual(terraApiStubData.LandingZoneId, resources.Id);
            Assert.AreEqual(5, resources.Resources.Length);
            tokenCredential.Verify(t => t.GetTokenAsync(It.IsAny<TokenRequestContext>(),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [TestMethod]
        public void GetLandingZoneResourcesApiUrl_CorrectUrlIsParsed()
        {
            var url = terraLandingZoneApiClient.GetLandingZoneResourcesApiUrl(terraApiStubData.LandingZoneId);

            var expectedUrl = $"{TerraApiStubData.LandingZoneApiHost}/api/landingzones/v1/azure/{terraApiStubData.LandingZoneId}/resources";

            Assert.AreEqual(expectedUrl, url.ToString());

        }

        [TestMethod]
        public void GetQuotaApiUrl_CorrectUrlIsParsed()
        {
            var url = terraLandingZoneApiClient.GetQuotaApiUrl(terraApiStubData.LandingZoneId, terraApiStubData.BatchAccountId);

            var expectedUrl = $"{TerraApiStubData.LandingZoneApiHost}/api/landingzones/v1/azure/{terraApiStubData.LandingZoneId}/resource-quota?azureResourceId={Uri.EscapeDataString(terraApiStubData.BatchAccountId)}";

            Assert.AreEqual(expectedUrl, url.ToString());

        }
    }
}
