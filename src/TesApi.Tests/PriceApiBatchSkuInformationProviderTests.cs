using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using LazyCache;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.Models;
using TesApi.Web.Management;

namespace TesApi.Tests
{
    [TestClass, TestCategory("Integration")]
    public class PriceApiBatchSkuInformationProviderTests
    {
        private PriceApiClient pricingApiClient;
        private Mock<ILogger<PriceApiBatchSkuInformationProvider>> loggerMock;
        private PriceApiBatchSkuInformationProvider provider;
        private PriceApiBatchSkuInformationProvider providerWithCache;
        private IAppCache appCache;

        [TestInitialize]
        public void Initialize()
        {
            pricingApiClient = new PriceApiClient();
            loggerMock = new Mock<ILogger<PriceApiBatchSkuInformationProvider>>();
            provider = new PriceApiBatchSkuInformationProvider(pricingApiClient,
                loggerMock.Object);
            appCache = new CachingService();
            providerWithCache = new PriceApiBatchSkuInformationProvider(appCache, pricingApiClient, loggerMock.Object);
        }

        [TestMethod]
        public async Task GetVmSizesAndPricesAsync_ReturnsVmsWithPricingInformation()
        {
            var results = await provider.GetVmSizesAndPricesAsync("eastus");

            Assert.IsTrue(results.Any(r => r.PricePerHour is not null && r.PricePerHour > 0));
        }

        [TestMethod]
        public async Task GetVmSizesAndPricesAsync_ReturnsLowAndNormalPriorityInformation()
        {
            var results = await provider.GetVmSizesAndPricesAsync("eastus");

            Assert.IsTrue(results.Any(r => r.LowPriority && r.PricePerHour is not null && r.PricePerHour > 0));
            Assert.IsTrue(results.Any(r => !r.LowPriority && r.PricePerHour is not null && r.PricePerHour > 0));
        }

        [TestMethod]
        public async Task GetVmSizesAndPricesAsync_WithCacheReturnsVmsWithPricingInformation()
        {
            var results = await providerWithCache.GetVmSizesAndPricesAsync("eastus");

            Assert.IsTrue(results.Any(r => r.PricePerHour is not null && r.PricePerHour > 0));
            //item was added to the cache.
            Assert.IsTrue(appCache.Get<List<VirtualMachineInformation>>("eastus")
                .Any(r => r.PricePerHour is not null && r.PricePerHour > 0));
        }
    }
}
