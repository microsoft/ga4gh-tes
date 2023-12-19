// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json;
using CommonUtilities.Options;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Tes.ApiClients.Models.Pricing;

namespace Tes.ApiClients.Tests
{
    [TestClass, TestCategory("Integration")]
    public class PriceApiClientTests
    {
        private PriceApiClient pricingApiClient = null!;
        private CachingRetryPolicyBuilder cachingRetryHandler = null!;
        private IMemoryCache appCache = null!;

        [TestInitialize]
        public void Initialize()
        {
            appCache = new MemoryCache(new MemoryCacheOptions());
            var options = new Mock<IOptions<RetryPolicyOptions>>();
            options.Setup(o => o.Value).Returns(new RetryPolicyOptions());
            cachingRetryHandler = new CachingRetryPolicyBuilder(appCache, options.Object);
            pricingApiClient = new PriceApiClient(cachingRetryHandler, new NullLogger<PriceApiClient>());
        }

        [TestCleanup]
        public void Cleanup()
        {
            appCache.Dispose();
        }

        [TestMethod]
        public async Task GetPricingInformationPageAsync_ReturnsSinglePageWithItemsWithMaxPageSize()
        {
            var page = await pricingApiClient.GetPricingInformationPageAsync(0, "westus2", CancellationToken.None);

            Assert.IsNotNull(page);
            Assert.IsTrue(page.Items.Length > 0);
        }

        [TestMethod]
        public async Task GetPricingInformationPageAsync_ReturnsSinglePageAndCaches()
        {
            var page = await pricingApiClient.GetPricingInformationPageAsync(0, "westus2", CancellationToken.None, cacheResults: true);
            var cacheKey = await pricingApiClient.ToCacheKeyAsync(new Uri(page.RequestLink), false, CancellationToken.None);
            var cachedPage = appCache.Get<RetailPricingData>(cacheKey);
            Assert.IsNotNull(page);
            Assert.IsTrue(page.Items.Length > 0);
            Assert.IsNotNull(cachedPage);
            Assert.IsTrue(Enumerable.SequenceEqual(cachedPage.Items, page.Items));
        }

        [TestMethod]
        public async Task GetPricingInformationAsync_ReturnsMoreThan100Items()
        {
            var pages = await pricingApiClient.GetAllPricingInformationAsync("westus2", CancellationToken.None).ToListAsync();

            Assert.IsNotNull(pages);
            Assert.IsTrue(pages.Count > 100);
        }

        [TestMethod]
        public async Task GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync_ReturnsOnlyNonWindowsAndNonSpotInstances()
        {
            var pages = await pricingApiClient.GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync("westus2", CancellationToken.None).ToListAsync();

            Assert.IsTrue(pages.Count > 0);
            Assert.IsFalse(pages.Any(r => r.productName.Contains(" Windows")));
            Assert.IsFalse(pages.Any(r => r.productName.Contains(" Spot")));
        }
    }
}
