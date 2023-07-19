// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using TesApi.Web.Management;
using TesApi.Web.Management.Clients;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Management.Models.Pricing;

namespace TesApi.Tests
{
    [TestClass, TestCategory("Integration")]
    public class PriceApiClientTests
    {
        private PriceApiClient pricingApiClient;
        private CacheAndRetryHandler cacheAndRetryHandler;
        private IMemoryCache appCache;

        [TestInitialize]
        public void Initialize()
        {
            appCache = new MemoryCache(new MemoryCacheOptions());
            var options = new Mock<IOptions<RetryPolicyOptions>>();
            options.Setup(o => o.Value).Returns(new RetryPolicyOptions());
            cacheAndRetryHandler = new CacheAndRetryHandler(appCache, options.Object);
            pricingApiClient = new PriceApiClient(cacheAndRetryHandler, new NullLogger<PriceApiClient>());
        }

        [TestCleanup]
        public void Cleanup()
        {
            appCache?.Dispose();
        }

        [TestMethod]
        public async Task GetPricingInformationPageAsync_ReturnsSinglePageWithItemsWithMaxPageSize()
        {
            var page = await pricingApiClient.GetPricingInformationPageAsync(0, "westus2", System.Threading.CancellationToken.None);

            Assert.IsNotNull(page);
            Assert.IsTrue(page.Items.Length == 100);
        }

        [TestMethod]
        public async Task GetPricingInformationPageAsync_ReturnsSinglePageAndCaches()
        {
            var page = await pricingApiClient.GetPricingInformationPageAsync(0, "westus2", System.Threading.CancellationToken.None, cacheResults: true);
            var cacheKey = await pricingApiClient.ToCacheKeyAsync(new Uri(page.RequestLink), false, System.Threading.CancellationToken.None);
            var cachedPage = JsonSerializer.Deserialize<RetailPricingData>(appCache.Get<string>(cacheKey));
            Assert.IsNotNull(page);
            Assert.IsTrue(page.Items.Length == 100);
            Assert.IsNotNull(cachedPage);
            Assert.IsTrue(cachedPage.Items.Length == 100);
        }

        [TestMethod]
        public async Task GetPricingInformationAsync_ReturnsMoreThan100Items()
        {
            var pages = await pricingApiClient.GetAllPricingInformationAsync("westus2", System.Threading.CancellationToken.None).ToListAsync();

            Assert.IsNotNull(pages);
            Assert.IsTrue(pages.Count > 100);
        }

        [TestMethod]
        public async Task GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync_ReturnsOnlyNonWindowsAndNonSpotInstances()
        {
            var pages = await pricingApiClient.GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync("westus2", System.Threading.CancellationToken.None).ToListAsync();

            Assert.IsTrue(pages.Count > 0);
            Assert.IsFalse(pages.Any(r => r.productName.Contains(" Windows")));
            Assert.IsFalse(pages.Any(r => r.productName.Contains(" Spot")));
        }
    }
}
