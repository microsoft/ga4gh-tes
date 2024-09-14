// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Linq;
using System.Threading.Tasks;
using CommonUtilities;
using CommonUtilities.Options;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.ApiClients;
using TesApi.Web.Management;

namespace TesApi.Tests
{
    [TestClass, TestCategory("Integration")]
    public class PriceApiBatchSkuInformationProviderTests
    {
        private PriceApiClient pricingApiClient;
        private PriceApiBatchSkuInformationProvider provider;
        private IMemoryCache appCache;
        private CachingRetryPolicyBuilder cachingRetryHandler;
        private Mock<IOptions<RetryPolicyOptions>> mockRetryOptions;

        [TestInitialize]
        public void Initialize()
        {
            appCache = new MemoryCache(new MemoryCacheOptions());
            mockRetryOptions = new Mock<IOptions<RetryPolicyOptions>>();
            mockRetryOptions.Setup(m => m.Value).Returns(new RetryPolicyOptions());

            cachingRetryHandler = new CachingRetryPolicyBuilder(appCache, mockRetryOptions.Object);
            pricingApiClient = new PriceApiClient(cachingRetryHandler, new NullLogger<PriceApiClient>());
            var config = ExpensiveObjectTestUtility.AzureCloudConfig;
            provider = new PriceApiBatchSkuInformationProvider(pricingApiClient, config, new NullLogger<PriceApiBatchSkuInformationProvider>());
            //using var serviceProvider = new TestServices.TestServiceProvider<PriceApiBatchSkuInformationProvider>();
            //var provider = serviceProvider.GetT();
        }

        [TestCleanup]
        public void Cleanup()
        {
            appCache?.Dispose();
        }

        [TestMethod]
        public async Task GetVmSizesAndPricesAsync_ReturnsVmsWithPricingInformation()
        {
            var results = await provider.GetVmSizesAndPricesAsync("eastus", System.Threading.CancellationToken.None);

            Assert.IsTrue(results.Any(r => r.PricePerHour is not null && r.PricePerHour > 0));
        }

        [TestMethod]
        public async Task GetVmSizesAndPricesAsync_ReturnsLowAndNormalPriorityInformation()
        {
            var results = await provider.GetVmSizesAndPricesAsync("eastus", System.Threading.CancellationToken.None);

            Assert.IsTrue(results.Any(r => r.LowPriority && r.PricePerHour is not null && r.PricePerHour > 0));
            Assert.IsTrue(results.Any(r => !r.LowPriority && r.PricePerHour is not null && r.PricePerHour > 0));
        }

        [TestMethod]
        public async Task GetStorageDisksAndPricesAsync_ReturnsExpectedRecords()
        {
            var totalCapacityInGiB = 1024.0; // Ensure this == maxDataDiskCount * one of the capacities (middle number) in storagePricing
            var maxDataDiskCount = 4;
            var results = await provider.GetStorageDisksAndPricesAsync("eastus", totalCapacityInGiB, maxDataDiskCount, System.Threading.CancellationToken.None);

            Assert.AreEqual(4, results.Count);
            Assert.IsTrue(results.All(disk =>
                totalCapacityInGiB / maxDataDiskCount == disk.CapacityInGiB &&
                Azure.ResourceManager.Batch.Models.BatchDiskCachingType.ReadOnly.ToString().Equals(disk.Caching, System.StringComparison.OrdinalIgnoreCase) &&
                Azure.ResourceManager.Batch.Models.BatchStorageAccountType.StandardLrs.ToString().Equals(disk.StorageAccountType, System.StringComparison.OrdinalIgnoreCase)));
            Assert.AreEqual(results.Count, results.Select(disk => disk.Lun).Distinct().Count());
        }

        [DataTestMethod]
        [DataRow(1, 0, 0, 0)]
        [DataRow(32767.5, 1, 0, 0)]
        [DataRow(1, 1, 4, 1)]
        [DataRow(1, 2, 4, 1)]
        [DataRow(10, 2, 8, 2)]
        [DataRow(100, 2, 64, 2)]
        [DataRow(100, 10, 16, 7)]
        public void AdditionalDataDisksAreDeterminedCorrectly(double capacity, int maxDisks, int size, int quantity)
        {
            var results = PriceApiBatchSkuInformationProvider.DetermineDisks(storagePricing, capacity, maxDisks);

            Assert.AreEqual(quantity, results.Count);
            Assert.IsFalse(results.Any(disk => disk.CapacityInGiB != size));
        }

        private static readonly System.Collections.Generic.IEnumerable<Tes.ApiClients.Models.Pricing.StorageDiskPriceInformation> storagePricing =
        [
            new("E1 LRS Disk", 4, 0.000410686M),
            new("E2 LRS Disk", 8, 0.000821372M),
            new("E3 LRS Disk", 16, 0.00328549M),
            new("E4 LRS Disk", 32, 0.00328549M),
            new("E6 LRS Disk", 64, 0.00657098M),
            new("E10 LRS Disk", 128, 0.0131420M),
            new("E15 LRS Disk", 256, 0.0262839M),
            new("E20 LRS Disk", 512, 0.0525678M),
            new("E30 LRS Disk", 1024, 0.105136M),
            new("E40 LRS Disk", 2048, 0.210271M),
            new("E50 LRS Disk", 4096, 0.420543M),
            new("E60 LRS Disk", 8192, 0.841085M),
            new("E70 LRS Disk", 16384, 1.68217M),
            new("E80 LRS Disk", 32767, 3.36434M),
        ];
    }
}
