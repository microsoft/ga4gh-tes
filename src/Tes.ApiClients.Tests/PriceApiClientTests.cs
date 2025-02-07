// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using CommonUtilities.Options;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

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
            Models.Pricing.RetailPricingData? page = default;

            try
            {
                page = await pricingApiClient.GetPricingInformationPageAsync(DateTime.UtcNow, 0, "Virtual Machines", "westus2", CancellationToken.None);
            }
            catch (HttpRequestException ex)
            {
                if (ex.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                {
                    Assert.Inconclusive();
                }
            }

            Assert.IsNotNull(page);
            Assert.IsTrue(page.Items.Length > 0);
        }

        [TestMethod]
        public async Task GetPricingInformationAsync_ReturnsMoreThan100Items()
        {
            List<Models.Pricing.PricingItem>? pages = default;

            try
            {
                pages = await pricingApiClient.GetAllPricingInformationAsync("Virtual Machines", "westus2", CancellationToken.None).ToListAsync();
            }
            catch (HttpRequestException ex)
            {
                if (ex.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                {
                    Assert.Inconclusive();
                }
            }

            Assert.IsNotNull(pages);
            Assert.IsTrue(pages.Count > 100);
        }

        [TestMethod]
        public async Task GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync_ReturnsOnlyNonWindowsAndNonSpotInstances()
        {
            List<Models.Pricing.PricingItem>? pages = default;

            try
            {
                pages = await pricingApiClient.GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync("westus2", CancellationToken.None).ToListAsync();
            }
            catch (HttpRequestException ex)
            {
                if (ex.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                {
                    Assert.Inconclusive();
                }
            }

            Assert.IsTrue(pages?.Count > 0);
            Assert.IsFalse(pages.Any(r => r.productName.Contains(" Windows")));
            Assert.IsFalse(pages.Any(r => r.productName.Contains(" Spot")));
        }
    }
}
