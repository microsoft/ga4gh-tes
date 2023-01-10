﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using LazyCache;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.Models;
using TesApi.Web;
using TesApi.Web.Management;

namespace TesApi.Tests
{
    [TestClass]
    public class ConfigurationUtilsTests
    {
        [TestMethod]
        public async Task ValidateSupportedVmSizesFileContent()
        {
            using var serviceProvider = new TestServices.TestServiceProvider<ConfigurationUtils>(
                configuration: GetInMemoryConfig(),
                azureProxy: PrepareAzureProxy,
                accountResourceInformation: GetResourceInformation(),
                batchSkuInformationProvider: GetMockSkuInformationProvider(),
                armBatchQuotaProvider: (GetMockQuotaProviderExpression, GetMockQuotaProvider()));
            var configurationUtils = serviceProvider.GetT();

            await configurationUtils.ProcessAllowedVmSizesConfigurationFileAsync();

            var expectedSupportedVmSizesFileContent =
                "VM Size Family       $/hour   $/hour  Memory  CPUs   Disk     Dedicated CPU\n" +
                "                  dedicated  low pri   (GiB)        (GiB)  quota (per fam.)\n" +
                "VmSize1 VmFamily1    11.000   22.000       3     2     20               100\n" +
                "VmSize2 VmFamily2    33.000   44.000       6     4     40                 0\n" +
                "VmSize3 VmFamily3    55.000      N/A      12     8     80               300";

            serviceProvider.AzureProxy.Verify(m => m.UploadBlobAsync(It.Is<Uri>(x => x.AbsoluteUri.Contains("supported-vm-sizes")), It.Is<string>(s => s.Equals(expectedSupportedVmSizesFileContent))), Times.Exactly(1));
        }

        private static BatchAccountResourceInformation GetResourceInformation()
            => new("batchAccount", "mrg", "sub-id", "eastus");

        private static System.Linq.Expressions.Expression<Func<ArmBatchQuotaProvider>> GetMockQuotaProviderExpression(IServiceProvider provider)
            => () => new ArmBatchQuotaProvider(
                provider.GetRequiredService<IAppCache>(),
                new AzureManagementClientsFactory(GetResourceInformation()),
                provider.GetRequiredService<ILogger<ArmBatchQuotaProvider>>());

        private static Action<Mock<ArmBatchQuotaProvider>> GetMockQuotaProvider()
            => new(mockArmQuotaProvider =>
                mockArmQuotaProvider.Setup(p => p.GetBatchAccountQuotasAsync())
                    .ReturnsAsync(GetNewAzureBatchAccountQuotas));

        private static Action<Mock<IBatchSkuInformationProvider>> GetMockSkuInformationProvider()
            => new(mockSkuProvider =>
                mockSkuProvider.Setup(p => p.GetVmSizesAndPricesAsync(It.IsAny<string>()))
                    .ReturnsAsync(GetNewVmSizeAndPricingList));

        [TestMethod]
        public async Task UnsupportedVmSizeInAllowedVmSizesFileIsIgnoredAndTaggedWithWarning()
        {
            using var serviceProvider = new TestServices.TestServiceProvider<ConfigurationUtils>(
                configuration: GetInMemoryConfig(),
                azureProxy: PrepareAzureProxy,
                accountResourceInformation: GetResourceInformation(),
                batchSkuInformationProvider: GetMockSkuInformationProvider(),
                armBatchQuotaProvider: (GetMockQuotaProviderExpression, GetMockQuotaProvider()));
            var configurationUtils = serviceProvider.GetT();

            await configurationUtils.ProcessAllowedVmSizesConfigurationFileAsync();

            Assert.AreEqual("VmSize1,VmSize2,VmFamily3", serviceProvider.Configuration["AllowedVmSizes"]);

            var expectedAllowedVmSizesFileContent =
                "VmSize1\n" +
                "#SomeComment\n" +
                "VmSize2\n" +
                "VmSizeNonExistent <-- WARNING: This VM size or family is either misspelled or not supported in your region. It will be ignored.\n" +
                "VmFamily3";

            serviceProvider.AzureProxy.Verify(m => m.UploadBlobAsync(It.Is<Uri>(x => x.AbsoluteUri.Contains("allowed-vm-sizes")), It.Is<string>(s => s.Equals(expectedAllowedVmSizesFileContent))), Times.Exactly(1));
        }

        private static IEnumerable<(string Key, string Value)> GetInMemoryConfig()
            => Enumerable.Repeat(("DefaultStorageAccountName", "defaultstorageaccount"), 1);

        private static void PrepareAzureProxy(Mock<IAzureProxy> azureProxy)
        {
            var allowedVmSizesFileContent = "VmSize1\n#SomeComment\nVmSize2\nVmSizeNonExistent\nVmFamily3";

            var storageAccountInfos = new Dictionary<string, StorageAccountInfo> {
                {
                    "defaultstorageaccount",
                    new StorageAccountInfo { Name = "defaultstorageaccount", Id = "Id", BlobEndpoint = "https://defaultstorageaccount.blob.core.windows.net/", SubscriptionId = "SubId" }
                }
             };

            azureProxy.Setup(a => a.DownloadBlobAsync(It.IsAny<Uri>())).Returns(Task.FromResult(allowedVmSizesFileContent));
            azureProxy.Setup(a => a.GetStorageAccountInfoAsync("defaultstorageaccount")).Returns(Task.FromResult(storageAccountInfos["defaultstorageaccount"]));
            azureProxy.Setup(a => a.GetStorageAccountKeyAsync(It.IsAny<StorageAccountInfo>())).Returns(Task.FromResult("Key1"));
        }

        private static List<VirtualMachineInformation> GetNewVmSizeAndPricingList()
        {
            return new List<VirtualMachineInformation>
            {
                new VirtualMachineInformation
                {
                    VmSize = "VmSize1", VmFamily = "VmFamily1", LowPriority = false, NumberOfCores = 2, MemoryInGB = 3,
                    ResourceDiskSizeInGB = 20, PricePerHour = 11
                },
                new VirtualMachineInformation
                {
                    VmSize = "VmSize1", VmFamily = "VmFamily1", LowPriority = true, NumberOfCores = 2, MemoryInGB = 3,
                    ResourceDiskSizeInGB = 20, PricePerHour = 22
                },
                new VirtualMachineInformation
                {
                    VmSize = "VmSize2", VmFamily = "VmFamily2", LowPriority = false, NumberOfCores = 4, MemoryInGB = 6,
                    ResourceDiskSizeInGB = 40, PricePerHour = 33
                },
                new VirtualMachineInformation
                {
                    VmSize = "VmSize2", VmFamily = "VmFamily2", LowPriority = true, NumberOfCores = 4, MemoryInGB = 6,
                    ResourceDiskSizeInGB = 40, PricePerHour = 44
                },
                new VirtualMachineInformation
                {
                    VmSize = "VmSize3", VmFamily = "VmFamily3", LowPriority = false, NumberOfCores = 8, MemoryInGB = 12,
                    ResourceDiskSizeInGB = 80, PricePerHour = 55
                }
            };
        }

        private static AzureBatchAccountQuotas GetNewAzureBatchAccountQuotas()
        {
            var dedicatedCoreQuotaPerVmFamily = new[]
            {
                new VirtualMachineFamilyCoreQuota("VmFamily1", 100), new VirtualMachineFamilyCoreQuota("VmFamily2", 0),
                new VirtualMachineFamilyCoreQuota("VmFamily3", 300)
            };

            var batchQuotas = new AzureBatchAccountQuotas
            {
                ActiveJobAndJobScheduleQuota = 1,
                PoolQuota = 1,
                DedicatedCoreQuota = 5,
                LowPriorityCoreQuota = 10,
                DedicatedCoreQuotaPerVMFamilyEnforced = true,
                DedicatedCoreQuotaPerVMFamily = dedicatedCoreQuotaPerVmFamily
            };
            return batchQuotas;
        }

        private static List<VirtualMachineInformation> GetNewVmSizeAndPricingList()
        {
            return new List<VirtualMachineInformation>
            {
                new VirtualMachineInformation
                {
                    VmSize = "VmSize1", VmFamily = "VmFamily1", LowPriority = false, NumberOfCores = 2, MemoryInGB = 3,
                    ResourceDiskSizeInGB = 20, PricePerHour = 11
                },
                new VirtualMachineInformation
                {
                    VmSize = "VmSize1", VmFamily = "VmFamily1", LowPriority = true, NumberOfCores = 2, MemoryInGB = 3,
                    ResourceDiskSizeInGB = 20, PricePerHour = 22
                },
                new VirtualMachineInformation
                {
                    VmSize = "VmSize2", VmFamily = "VmFamily2", LowPriority = false, NumberOfCores = 4, MemoryInGB = 6,
                    ResourceDiskSizeInGB = 40, PricePerHour = 33
                },
                new VirtualMachineInformation
                {
                    VmSize = "VmSize2", VmFamily = "VmFamily2", LowPriority = true, NumberOfCores = 4, MemoryInGB = 6,
                    ResourceDiskSizeInGB = 40, PricePerHour = 44
                },
                new VirtualMachineInformation
                {
                    VmSize = "VmSize3", VmFamily = "VmFamily3", LowPriority = false, NumberOfCores = 8, MemoryInGB = 12,
                    ResourceDiskSizeInGB = 80, PricePerHour = 55
                }
            };
        }

        private static AzureBatchAccountQuotas GetNewAzureBatchAccountQuotas()
        {
            var dedicatedCoreQuotaPerVmFamily = new[]
            {
                new VirtualMachineFamilyCoreQuota("VmFamily1", 100), new VirtualMachineFamilyCoreQuota("VmFamily2", 0),
                new VirtualMachineFamilyCoreQuota("VmFamily3", 300)
            };

            var batchQuotas = new AzureBatchAccountQuotas
            {
                ActiveJobAndJobScheduleQuota = 1,
                PoolQuota = 1,
                DedicatedCoreQuota = 5,
                LowPriorityCoreQuota = 10,
                DedicatedCoreQuotaPerVMFamilyEnforced = true,
                DedicatedCoreQuotaPerVMFamily = dedicatedCoreQuotaPerVmFamily
            };
            return batchQuotas;
        }
    }
}
