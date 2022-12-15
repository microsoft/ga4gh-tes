﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TesApi.Tests;

[TestClass]
public class BatchQuotaVerifierTests
{

    private BatchQuotaVerifier batchQuotaVerifier;
    private Mock<IBatchQuotaProvider> quotaProvider;
    private Mock<IAzureProxy> azureProxy;
    private Mock<ILogger<BatchQuotaVerifier>> logger;
    private Mock<IBatchSkuInformationProvider> skuInfoProvider;
    private BatchAccountResourceInformation batchAccountResourceInformation;

    private const string Region = "eastus";
    private const string VmFamily = "StandardDSeries";
    private BatchAccountOptions batchAccountOptions;
    private List<VirtualMachineInformation> vmSizeAndPriceList;

    public BatchQuotaVerifierTests() { }

    [TestInitialize]
    public void BeforeEach()
    {
        azureProxy = new Mock<IAzureProxy>();
        azureProxy.Setup(p => p.GetArmRegion()).Returns(Region);
        logger = new Mock<ILogger<BatchQuotaVerifier>>();
        quotaProvider = new Mock<IBatchQuotaProvider>();
        skuInfoProvider = new Mock<IBatchSkuInformationProvider>();
        batchAccountResourceInformation = new BatchAccountResourceInformation("batchaccount", "mrg", "subid", "eastus");

        batchQuotaVerifier = new BatchQuotaVerifier(batchAccountResourceInformation, quotaProvider.Object, skuInfoProvider.Object, azureProxy.Object, logger.Object);

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
        services.BatchQuotaProvider.Setup(p => p.GetBatchAccountQuotaForRequirementAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<int?>()))
            .ReturnsAsync(batchVmFamilyQuotas);

        await batchQuotaVerifier.CheckBatchAccountQuotasAsync(vmInfo);

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
            NumberOfCores = requestedNumberOfCores
        };

        var batchAccountQuotas = new BatchVmFamilyQuotas(totalCoreQuota, vmFamilyQuota, poolQuota, activeJobAndJobScheduleQuota, true, VmFamily);

        services.BatchQuotaProvider.Setup(p => p.GetBatchAccountQuotaForRequirementAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<int?>()))
            .ReturnsAsync(batchAccountQuotas);

        services.BatchSkuInformationProvider.Setup(p => p.GetVmSizesAndPricesAsync(It.IsAny<string>())).ReturnsAsync(CreateVmSkuList(10));
        services.AzureProxy.Setup(p => p.GetBatchActiveJobCount()).Returns(activeJobCount);
        services.AzureProxy.Setup(p => p.GetBatchActivePoolCount()).Returns(activePoolCount);
        services.BatchSkuInformationProvider.Setup(p => p.GetVmSizesAndPricesAsync(Region)).ReturnsAsync(CreateBatchSupportedVmSkuList(10));

        await batchQuotaVerifier.CheckBatchAccountQuotasAsync(vmInfo);

    }

    private static List<VirtualMachineInformation> CreateBatchSupportedVmSkuList(int maxNumberOfCores)
    {
        return new List<VirtualMachineInformation>()
        {
            new VirtualMachineInformation()
            {
                LowPriority = false,
                MaxDataDiskCount = 1,
                MemoryInGB = 2.0d,
                NumberOfCores = maxNumberOfCores / 2,
                PricePerHour = 1.0m,
                ResourceDiskSizeInGB = 1d,
                VmFamily = VmFamily,
                VmSize = "D4"
            },
            new VirtualMachineInformation()
            {
                LowPriority = false,
                MaxDataDiskCount = 1,
                MemoryInGB = 4.0d,
                NumberOfCores = maxNumberOfCores / 2,
                PricePerHour = 1.0m,
                ResourceDiskSizeInGB = 1d,
                VmFamily = VmFamily,
                VmSize = "D8"
            },
            new VirtualMachineInformation()
            {
                LowPriority = false,
                MaxDataDiskCount = 1,
                MemoryInGB = 2.0d,
                NumberOfCores = maxNumberOfCores / 2,
                PricePerHour = 1.0m,
                ResourceDiskSizeInGB = 1d,
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
                MemoryInGB = 2.0d,
                NumberOfCores = maxNumberOfCores / 2,
                PricePerHour = 1.0m,
                ResourceDiskSizeInGB = 1d,
                VmFamily = VmFamily,
                VmSize = "D4"
            },
            new VirtualMachineInformation()
            {
                LowPriority = false,
                MaxDataDiskCount = 1,
                MemoryInGB = 2.0d,
                NumberOfCores = maxNumberOfCores / 2,
                PricePerHour = 1.0m,
                ResourceDiskSizeInGB = 1d,
                VmFamily = VmFamily,
                VmSize = "D2"
            }
        };
    }
}


