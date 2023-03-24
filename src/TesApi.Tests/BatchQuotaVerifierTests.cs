// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
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
    private Mock<IBatchQuotaProvider> quotaProvider;
    private Mock<IAzureProxy> azureProxy;
    private Mock<IBatchSkuInformationProvider> skuInfoProvider;
    private BatchAccountResourceInformation batchAccountResourceInformation;

    private const string Region = "eastus";
    private const string VmFamily = "StandardDSeries";

    [TestInitialize]
    public void BeforeEach()
    {
        azureProxy = new Mock<IAzureProxy>();
        azureProxy.Setup(p => p.GetArmRegion()).Returns(Region);
        quotaProvider = new Mock<IBatchQuotaProvider>();
        skuInfoProvider = new Mock<IBatchSkuInformationProvider>();
        batchAccountResourceInformation = new BatchAccountResourceInformation("batchaccount", "mrg", "subid", "eastus");

        new BatchQuotaVerifier(batchAccountResourceInformation, quotaProvider.Object, skuInfoProvider.Object, azureProxy.Object, new NullLogger<BatchQuotaVerifier>());
    }

    [TestMethod]
    [ExpectedException(typeof(InvalidOperationException))]
    public async Task CheckBatchAccountQuotasAsync_ProviderReturnsNull_ThrowsExceptionAndLogsException()
    {
        var logger = new Mock<ILogger<BatchQuotaVerifier>>();
        using var services = new TestServices.TestServiceProvider<BatchQuotaVerifier>(
            accountResourceInformation: new("batchaccount", "mrg", "subid", "eastus"),
            batchSkuInformationProvider: ip => { },
            azureProxy: ap => ap.Setup(p => p.GetArmRegion()).Returns(Region),
            additionalActions: s => s.AddSingleton(logger.Object));
        var batchQuotaVerifier = services.GetT();
        var vmInfo = new VirtualMachineInformation();

        BatchVmFamilyQuotas batchVmFamilyQuotas = null;
        services.BatchQuotaProvider.Setup(p => p.GetQuotaForRequirementAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<int?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(batchVmFamilyQuotas);

        await batchQuotaVerifier.CheckBatchAccountQuotasAsync(vmInfo, true, CancellationToken.None);

        logger.Verify(l => l.LogError(It.IsAny<string>(), It.IsAny<Exception>()), Times.Once);

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

    public static async Task SetupAndCheckBatchAccountQuotasAsync(int requestedNumberOfCores, int totalCoreQuota, int vmFamilyQuota, int activeJobCount, int activeJobAndJobScheduleQuota, int poolQuota, int activePoolCount)
    {
        using var services = new TestServices.TestServiceProvider<BatchQuotaVerifier>(
            accountResourceInformation: new("batchaccount", "mrg", "subid", "eastus"),
            batchSkuInformationProvider: ip => { },
            azureProxy: ap => ap.Setup(p => p.GetArmRegion()).Returns(Region));
        var batchQuotaVerifier = services.GetT();
        var vmInfo = new VirtualMachineInformation
        {
            VCpusAvailable = requestedNumberOfCores
        };

        var batchAccountQuotas = new BatchVmFamilyQuotas(totalCoreQuota, vmFamilyQuota, poolQuota, activeJobAndJobScheduleQuota, true, VmFamily);

        services.BatchQuotaProvider.Setup(p => p.GetQuotaForRequirementAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<int?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(batchAccountQuotas);

        services.BatchSkuInformationProvider.Setup(p => p.GetVmSizesAndPricesAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(CreateVmSkuList(10));
        services.AzureProxy.Setup(p => p.GetBatchActiveJobCount()).Returns(activeJobCount);
        services.AzureProxy.Setup(p => p.GetBatchActivePoolCount()).Returns(activePoolCount);
        services.BatchSkuInformationProvider.Setup(p => p.GetVmSizesAndPricesAsync(Region, It.IsAny<CancellationToken>())).ReturnsAsync(CreateBatchSupportedVmSkuList(10));

        await batchQuotaVerifier.CheckBatchAccountQuotasAsync(vmInfo, true, CancellationToken.None);
    }

    private static List<VirtualMachineInformation> CreateBatchSupportedVmSkuList(int maxNumberOfCores)
    {
        return new List<VirtualMachineInformation>()
        {
            new VirtualMachineInformation()
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
            new VirtualMachineInformation()
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
            new VirtualMachineInformation()
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
        return new List<VirtualMachineInformation>()
        {
            new VirtualMachineInformation()
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
            new VirtualMachineInformation()
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


