// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using TesApi.Web.Management.Models.Quotas;

namespace TesApi.Web.Management;

/// <summary>
/// Quota provider that uses the ARM API. 
/// </summary>
public class ArmBatchQuotaProvider : IBatchQuotaProvider
{
    /// <summary>
    /// Azure proxy instance
    /// </summary>
    private readonly IAzureProxy azureProxy;
    /// <summary>
    /// Logger instance.
    /// </summary>
    private readonly ILogger logger;


    /// <summary>
    /// Constructor of ArmResourceQuotaVerifier
    /// </summary>
    /// <param name="azureProxy"></param>
    /// <param name="logger"></param>
    public ArmBatchQuotaProvider(IAzureProxy azureProxy, ILogger<ArmBatchQuotaProvider> logger)
    {
        this.azureProxy = azureProxy;
        this.logger = logger;
    }

    private BatchVmFamilyQuotas ToVmFamilyBatchAccountQuotas(AzureBatchAccountQuotas batchAccountQuotas, string vmFamily, bool lowPriority, int? coresRequirement)
    {

        var isDedicated = !lowPriority;
        var totalCoreQuota = isDedicated ? batchAccountQuotas.DedicatedCoreQuota : batchAccountQuotas.LowPriorityCoreQuota;
        var isDedicatedAndPerVmFamilyCoreQuotaEnforced =
            isDedicated && batchAccountQuotas.DedicatedCoreQuotaPerVMFamilyEnforced;

        var vmFamilyCoreQuota = isDedicatedAndPerVmFamilyCoreQuotaEnforced
            ? batchAccountQuotas.DedicatedCoreQuotaPerVMFamily.FirstOrDefault(q => q.Name.Equals(vmFamily,
                      StringComparison.OrdinalIgnoreCase))
                  ?.CoreQuota ??
              0
            : coresRequirement ?? 0;

        return new BatchVmFamilyQuotas(totalCoreQuota, vmFamilyCoreQuota, batchAccountQuotas.PoolQuota,
            batchAccountQuotas.ActiveJobAndJobScheduleQuota, batchAccountQuotas.DedicatedCoreQuotaPerVMFamilyEnforced, vmFamily);
    }

    /// <inheritdoc />
    public async Task<BatchVmFamilyQuotas> GetBatchAccountQuotaForRequirementAsync(string vmFamily, bool lowPriority,
        int? coresRequirement)
    {
        return ToVmFamilyBatchAccountQuotas(await azureProxy.GetBatchAccountQuotasAsync(), vmFamily, lowPriority, coresRequirement);
    }

    /// <inheritdoc />
    public async Task<BatchVmCoreQuota> GetVmCoresPerFamilyAsync(bool lowPriority)
    {
        var isDedicated = !lowPriority;
        var batchQuota = await azureProxy.GetBatchAccountQuotasAsync();
        var isDedicatedAndPerVmFamilyCoreQuotaEnforced =
            isDedicated && batchQuota.DedicatedCoreQuotaPerVMFamilyEnforced;
        var numberOfCores = lowPriority ? batchQuota.LowPriorityCoreQuota : batchQuota.DedicatedCoreQuota;

        List<BatchVmCoresPerFamily> dedicatedCoresPerFamilies = null;
        if (isDedicatedAndPerVmFamilyCoreQuotaEnforced)
        {
            dedicatedCoresPerFamilies = batchQuota.DedicatedCoreQuotaPerVMFamily
                         .Select(r => new BatchVmCoresPerFamily(r.Name, r.CoreQuota))
                         .ToList();
        }

        return new BatchVmCoreQuota(numberOfCores, lowPriority, isDedicatedAndPerVmFamilyCoreQuotaEnforced, dedicatedCoresPerFamilies);
    }
}
