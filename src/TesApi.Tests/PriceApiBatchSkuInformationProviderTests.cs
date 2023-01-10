// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using LazyCache;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Tes.Models;
using TesApi.Web.Management;

namespace TesApi.Tests
{
    [TestClass, TestCategory("Integration")]
    public class PriceApiBatchSkuInformationProviderTests
    {
        [TestMethod]
        public async Task GetVmSizesAndPricesAsync_ReturnsVmsWithPricingInformation()
        {
            using var serviceProvider = new TestServices.TestServiceProvider<PriceApiBatchSkuInformationProvider>();
            var provider = serviceProvider.GetT();
            var results = await provider.GetVmSizesAndPricesAsync("eastus");

            Assert.IsTrue(results.Any(r => r.PricePerHour is not null && r.PricePerHour > 0));
        }

        [TestMethod]
        public async Task GetVmSizesAndPricesAsync_ReturnsLowAndNormalPriorityInformation()
        {
            using var serviceProvider = new TestServices.TestServiceProvider<PriceApiBatchSkuInformationProvider>();
            var provider = serviceProvider.GetT();
            var results = await provider.GetVmSizesAndPricesAsync("eastus");

            Assert.IsTrue(results.Any(r => r.LowPriority && r.PricePerHour is not null && r.PricePerHour > 0));
            Assert.IsTrue(results.Any(r => !r.LowPriority && r.PricePerHour is not null && r.PricePerHour > 0));
        }

        [TestMethod]
        public async Task GetVmSizesAndPricesAsync_WithCacheReturnsVmsWithPricingInformation()
        {
            using var serviceProvider = new TestServices.TestServiceProvider<IBatchSkuInformationProvider>();
            var appCache = serviceProvider.GetService<IAppCache>();
            var providerWithCache = serviceProvider.GetT();
            var results = await providerWithCache.GetVmSizesAndPricesAsync("eastus");

            Assert.IsTrue(results.Any(r => r.PricePerHour is not null && r.PricePerHour > 0));
            //item was added to the cache.
            Assert.IsTrue(appCache.Get<List<VirtualMachineInformation>>("eastus")
                .Any(r => r.PricePerHour is not null && r.PricePerHour > 0));
        }
    }
}
