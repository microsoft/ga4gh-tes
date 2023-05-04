// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Linq;
using System.Threading.Tasks;
using LazyCache;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using TesApi.Web.Management;
using TesApi.Web.Management.Clients;
using TesApi.Web.Management.Configuration;

namespace TesApi.Tests
{
    [TestClass, TestCategory("Integration")]
    public class PriceApiBatchSkuInformationProviderTests
    {
        private PriceApiClient pricingApiClient;
        private PriceApiBatchSkuInformationProvider provider;
        private IAppCache appCache;
        private CacheAndRetryHandler cacheAndRetryHandler;
        private Mock<IOptions<RetryPolicyOptions>> mockRetryOptions;

        [TestInitialize]
        public void Initialize()
        {
            appCache = new CachingService();
            mockRetryOptions = new Mock<IOptions<RetryPolicyOptions>>();
            mockRetryOptions.Setup(m => m.Value).Returns(new RetryPolicyOptions());

            cacheAndRetryHandler = new CacheAndRetryHandler(appCache, mockRetryOptions.Object);
            pricingApiClient = new PriceApiClient(cacheAndRetryHandler, new NullLogger<PriceApiClient>());
            provider = new PriceApiBatchSkuInformationProvider(pricingApiClient, new NullLogger<PriceApiBatchSkuInformationProvider>());
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
            ///using var serviceProvider = new TestServices.TestServiceProvider<PriceApiBatchSkuInformationProvider>();
            //provider = serviceProvider.GetT();
            var results = await provider.GetVmSizesAndPricesAsync("eastus", System.Threading.CancellationToken.None);

            Assert.IsTrue(results.Any(r => r.LowPriority && r.PricePerHour is not null && r.PricePerHour > 0));
            Assert.IsTrue(results.Any(r => !r.LowPriority && r.PricePerHour is not null && r.PricePerHour > 0));
        }
    }
}
