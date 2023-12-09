// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.Models;
using TesApi.Web;
using TesApi.Web.Management;
using TesApi.Web.Management.Models.Quotas;

namespace TesApi.Tests;

[TestClass]
public class BatchQuotaVerifierTests
{
    private BatchQuotaVerifier batchQuotaVerifier;
    private TestServices.TestServiceProvider<BatchQuotaVerifier> serviceProvider;
    private Mock<ILogger<BatchQuotaVerifier>> logger;
    private BatchVmFamilyQuotas batchVmFamilyQuotas = null;

    private const string Region = "eastus";
    private const string VmFamily = "StandardDSeries";

    [TestInitialize]
    public void BeforeEach()
    {
        logger = new Mock<ILogger<BatchQuotaVerifier>>();
        serviceProvider = new(
            accountResourceInformation: new("batchaccount", "mrg", "subid", "eastus", "batchAccount/endpoint"),
            batchSkuInformationProvider: ip => { },
            azureProxy: ap => ap.Setup(p => p.GetArmRegion()).Returns(Region),
            additionalActions: s => s.AddSingleton(logger.Object));
        batchQuotaVerifier = serviceProvider.GetT();
        serviceProvider.BatchQuotaProvider.Setup(p => p.GetQuotaForRequirementAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<int?>(), It.IsAny<System.Threading.CancellationToken>()))
            .ReturnsAsync(() => batchVmFamilyQuotas);
    }

    [TestCleanup]
    public void AfterEach()
    {
        serviceProvider.Dispose();
    }

    [TestMethod]
    [ExpectedException(typeof(InvalidOperationException))]
    public async Task CheckBatchAccountQuotasAsync_ProviderReturnsNull_ThrowsExceptionAndLogsException()
    {
        var vmInfo = new VirtualMachineInformation();

        await batchQuotaVerifier.CheckBatchAccountQuotasAsync(vmInfo, true, true, System.Threading.CancellationToken.None);

#pragma warning disable CA2254 // Template should be a static expression
        logger.Verify(l => l.LogError(It.IsAny<string>(), It.IsAny<Exception>()), Times.Once);
#pragma warning restore CA2254 // Template should be a static expression

    }

    [DataRow(10, 5, 10)] //not enough total quota
    [DataRow(10, 10, 5)] //not enough family quota
    [DataTestMethod]
    [ExpectedException(typeof(AzureBatchLowQuotaException))]
    public async Task CheckBatchAccountQuotasAsync_IsDedicatedNotEnoughCoreQuota_ThrowsAzureBatchLowQuotaException(int requestedNumberOfCores, int totalCoreQuota, int vmFamilyQuota)
        => await SetupAndCheckBatchAccountQuotasAsync(requestedNumberOfCores, totalCoreQuota, vmFamilyQuota, 0, 10, 5, 0);

    [DataRow(10, 100, 10, 10, 0, 100)] //too many active jobs
    [DataRow(10, 100, 10, 100, 100, 100)] //too many active pools
    [DataRow(10, 100, 10, 10, 0, 0)] //too total cores in use
    [DataTestMethod]
    [ExpectedException(typeof(AzureBatchQuotaMaxedOutException))]
    public async Task CheckBatchAccountQuotasAsync_IsDedicatedNotEnoughCoreQuota_ThrowsAzureBatchQuotaMaxedOutException(int requestedNumberOfCores, int totalCoreQuota, int activeJobCount, int activeJobAndJobScheduleQuota, int activePoolCount, int poolQuota)
        => await SetupAndCheckBatchAccountQuotasAsync(requestedNumberOfCores, totalCoreQuota, 100, activeJobCount, activeJobAndJobScheduleQuota, poolQuota, activePoolCount);

    public async Task SetupAndCheckBatchAccountQuotasAsync(int requestedNumberOfCores, int totalCoreQuota, int vmFamilyQuota, int activeJobCount, int activeJobAndJobScheduleQuota, int poolQuota, int activePoolCount)
    {
        var vmInfo = new VirtualMachineInformation
        {
            VCpusAvailable = requestedNumberOfCores
        };

        batchVmFamilyQuotas = new BatchVmFamilyQuotas(totalCoreQuota, vmFamilyQuota, poolQuota, activeJobAndJobScheduleQuota, true, VmFamily);

        serviceProvider.BatchSkuInformationProvider.Setup(p => p.GetVmSizesAndPricesAsync(It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>())).ReturnsAsync(CreateVmSkuList(10));
        serviceProvider.AzureProxy.Setup(p => p.GetBatchActiveJobCount()).Returns(activeJobCount);
        serviceProvider.AzureProxy.Setup(p => p.GetBatchActivePoolCount()).Returns(activePoolCount);
        serviceProvider.BatchSkuInformationProvider.Setup(p => p.GetVmSizesAndPricesAsync(Region, It.IsAny<System.Threading.CancellationToken>())).ReturnsAsync(CreateBatchSupportedVmSkuList(10));

        await batchQuotaVerifier.CheckBatchAccountQuotasAsync(vmInfo, true, true, System.Threading.CancellationToken.None);
    }

    private static List<VirtualMachineInformation> CreateBatchSupportedVmSkuList(int maxNumberOfCores)
    {
        return new()
        {
            new()
            {
                LowPriority = false,
                MaxDataDiskCount = 1,
                MemoryInGiB = 2.0d,
                VCpusAvailable = maxNumberOfCores / 2,
                PricePerHour = 1.0m,
                ResourceDiskSizeInGiB = 1d,
                VmFamily = VmFamily,
                VmSize = "D4"
            },
            new()
            {
                LowPriority = false,
                MaxDataDiskCount = 1,
                MemoryInGiB = 4.0d,
                VCpusAvailable = maxNumberOfCores / 2,
                PricePerHour = 1.0m,
                ResourceDiskSizeInGiB = 1d,
                VmFamily = VmFamily,
                VmSize = "D8"
            },
            new()
            {
                LowPriority = false,
                MaxDataDiskCount = 1,
                MemoryInGiB = 2.0d,
                VCpusAvailable = maxNumberOfCores / 2,
                PricePerHour = 1.0m,
                ResourceDiskSizeInGiB = 1d,
                VmFamily = VmFamily,
                VmSize = "D2"
            }
        };
    }

    private static List<VirtualMachineInformation> CreateVmSkuList(int maxNumberOfCores)
    {
        return new()
        {
            new()
            {
                LowPriority = false,
                MaxDataDiskCount = 1,
                MemoryInGiB = 2.0d,
                VCpusAvailable = maxNumberOfCores / 2,
                PricePerHour = 1.0m,
                ResourceDiskSizeInGiB = 1d,
                VmFamily = VmFamily,
                VmSize = "D4"
            },
            new()
            {
                LowPriority = false,
                MaxDataDiskCount = 1,
                MemoryInGiB = 2.0d,
                VCpusAvailable = maxNumberOfCores / 2,
                PricePerHour = 1.0m,
                ResourceDiskSizeInGiB = 1d,
                VmFamily = VmFamily,
                VmSize = "D2"
            }
        };
    }
}


