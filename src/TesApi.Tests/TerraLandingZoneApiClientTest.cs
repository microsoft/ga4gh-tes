using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using TesApi.Web.Management;
using TesApi.Web.Management.Clients;

namespace TesApi.Tests
{
    [TestClass]
    public class TerraLandingZoneApiClientTest
    {
        private TerraLandingZoneApiClient terraLandingZoneApiClient;
        private Mock<TokenCredential> tokenCredential;
        private Mock<CacheAndRetryHandler> cacheAndRetryHandler;
        private TerraApiStubData terraApiStubData;

        [TestInitialize]
        public void SetUp()
        {
            terraApiStubData = new TerraApiStubData();
            tokenCredential = new Mock<TokenCredential>();
            cacheAndRetryHandler = new Mock<CacheAndRetryHandler>();
            terraLandingZoneApiClient = new TerraLandingZoneApiClient(terraApiStubData.ApiHost, tokenCredential.Object, cacheAndRetryHandler.Object, NullLogger<TerraLandingZoneApiClient>.Instance);
        }

        [TestMethod]
        public async Task GetResourceQuotaAsync_ValidResourceIdReturnsQuotaInformationAndGetsAuthToken()
        {
            var body = terraApiStubData.GetResourceQuotaApiResponseInJson();
            cacheAndRetryHandler.Setup(c => c.ExecuteWithRetryAndCachingAsync(It.IsAny<string>(),
                    It.IsAny<Func<Task<string>>>()))
                .ReturnsAsync(body);

            var quota = await terraLandingZoneApiClient.GetResourceQuotaAsync(terraApiStubData.LandingZoneId, terraApiStubData.BatchAccountId, cacheResults: true);

            Assert.IsNotNull(quota);
            Assert.AreEqual(terraApiStubData.LandingZoneId, quota.LandingZoneId);
            Assert.AreEqual(terraApiStubData.BatchAccountId, quota.AzureResourceId);
            Assert.AreEqual("Microsoft.Batch/batchAccounts", quota.ResourceType);
            Assert.AreEqual(100, quota.QuotaValues.PoolQuota);
            Assert.IsTrue(quota.QuotaValues.DedicatedCoreQuotaPerVMFamilyEnforced);
            Assert.AreEqual(300, quota.QuotaValues.ActiveJobAndJobScheduleQuota);
            Assert.AreEqual(350, quota.QuotaValues.DedicatedCoreQuota);
            Assert.AreEqual(59, quota.QuotaValues.DedicatedCoreQuotaPerVMFamily.Count);
            Assert.AreEqual("standardLSv2Family", quota.QuotaValues.DedicatedCoreQuotaPerVMFamily.Keys.First());
            Assert.AreEqual(0, quota.QuotaValues.DedicatedCoreQuotaPerVMFamily.Values.First());
            tokenCredential.Verify(t => t.GetTokenAsync(It.IsAny<TokenRequestContext>(),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [TestMethod]
        public async Task GetLandingZoneResourcesAsync_ListOfLandingZoneResourcesAndGetsAuthToken()
        {
            var body = terraApiStubData.GetResourceApiResponseInJson();
            cacheAndRetryHandler.Setup(c => c.ExecuteWithRetryAndCachingAsync(It.IsAny<string>(),
                    It.IsAny<Func<Task<string>>>()))
                .ReturnsAsync(body);

            var resources = await terraLandingZoneApiClient.GetLandingZoneResourcesAsync(terraApiStubData.LandingZoneId);

            Assert.IsNotNull(resources);
            Assert.AreEqual(terraApiStubData.LandingZoneId, resources.Id);
            Assert.AreEqual(5, resources.Resources.Length);
            tokenCredential.Verify(t => t.GetTokenAsync(It.IsAny<TokenRequestContext>(),
                    It.IsAny<CancellationToken>()),
                Times.Once);

        }
    }
}
