// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.ApiClients;
using Tes.ApiClients.Options;
using TesApi.Web.Management;

namespace TesApi.Tests
{
    [TestClass, TestCategory("Integration")]
    public class PriceApiBatchSkuInformationProviderTests
    {
        private PriceApiClient pricingApiClient;
        private PriceApiBatchSkuInformationProvider provider;
        private IMemoryCache appCache;
        private CacheAndRetryHandler cacheAndRetryHandler;
        private Mock<IOptions<RetryPolicyOptions>> mockRetryOptions;

        [TestInitialize]
        public void Initialize()
        {
            appCache = new MemoryCache(new MemoryCacheOptions());
            mockRetryOptions = new Mock<IOptions<RetryPolicyOptions>>();
            mockRetryOptions.Setup(m => m.Value).Returns(new RetryPolicyOptions());

            cacheAndRetryHandler = new CacheAndRetryHandler(appCache, mockRetryOptions.Object);
            pricingApiClient = new PriceApiClient(cacheAndRetryHandler, new NullLogger<PriceApiClient>());
            provider = new PriceApiBatchSkuInformationProvider(pricingApiClient, new NullLogger<PriceApiBatchSkuInformationProvider>());
        }

        [TestCleanup]
        public void Cleanup()
        {
            appCache?.Dispose();
        }

        [TestMethod]
        public async Task GetVmSizesAndPricesAsync_ReturnsVmsWithPricingInformation()
        {
            //using var serviceProvider = new TestServices.TestServiceProvider<PriceApiBatchSkuInformationProvider>();
            //var provider = serviceProvider.GetT();
            var results = await provider.GetVmSizesAndPricesAsync("eastus", System.Threading.CancellationToken.None);

            Assert.IsTrue(results.Any(r => r.PricePerHour is not null && r.PricePerHour > 0));
        }

        [TestMethod]
        public async Task GetVmSizesAndPricesAsync_ReturnsLowAndNormalPriorityInformation()
        {
            //provider = serviceProvider.GetT();
            var results = await provider.GetVmSizesAndPricesAsync("eastus", System.Threading.CancellationToken.None);

            Assert.IsTrue(results.Any(r => r.LowPriority && r.PricePerHour is not null && r.PricePerHour > 0));
            Assert.IsTrue(results.Any(r => !r.LowPriority && r.PricePerHour is not null && r.PricePerHour > 0));
        }
    }
}
