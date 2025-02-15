// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using TesApi.Web.Management.Models.Quotas;

namespace TesApi.Web.Management;

/// <summary>
/// Quota provider that uses the ARM API. 
/// </summary>
public class ArmBatchQuotaProvider : IBatchQuotaProvider
{
    /// <summary>
    /// Logger instance.
    /// </summary>
    private readonly ILogger logger;

    private readonly IMemoryCache appCache;
    private readonly AzureManagementClientsFactory clientsFactory;


    /// <summary>
    /// Constructor of ArmResourceQuotaVerifier
    /// </summary>
    /// <param name="logger"></param>
    /// <param name="appCache"></param>
    /// <param name="clientsFactory"></param>
    public ArmBatchQuotaProvider(IMemoryCache appCache, AzureManagementClientsFactory clientsFactory,
        ILogger<ArmBatchQuotaProvider> logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(appCache);
        ArgumentNullException.ThrowIfNull(clientsFactory);

        this.logger = logger;
        this.clientsFactory = clientsFactory;
        this.appCache = appCache;
    }

    /// <inheritdoc />
    public async Task<BatchVmFamilyQuotas> GetQuotaForRequirementAsync(string vmFamily, bool lowPriority,
            int? coresRequirement, CancellationToken cancellationToken)
        => ToVmFamilyBatchAccountQuotas(await GetBatchAccountQuotasAsync(cancellationToken), vmFamily, lowPriority, coresRequirement);

    /// <inheritdoc />
    public async Task<BatchVmCoreQuota> GetVmCoreQuotaAsync(bool lowPriority, CancellationToken cancellationToken)
    {
        var isDedicated = !lowPriority;
        var batchQuota = await GetBatchAccountQuotasAsync(cancellationToken);
        var isDedicatedAndPerVmFamilyCoreQuotaEnforced =
            isDedicated && batchQuota.DedicatedCoreQuotaPerVMFamilyEnforced;
        var numberOfCores = lowPriority ? batchQuota.LowPriorityCoreQuota : batchQuota.DedicatedCoreQuota;

        List<BatchVmCoresPerFamily> dedicatedCoresPerFamilies = null;
        if (isDedicatedAndPerVmFamilyCoreQuotaEnforced)
        {
            dedicatedCoresPerFamilies = batchQuota.DedicatedCoreQuotaPerVMFamily
                         .Select(r => new BatchVmCoresPerFamily(r.Name, r.CoreQuota ?? 0))
                         .ToList();
        }

        return new BatchVmCoreQuota(numberOfCores,
            lowPriority,
            isDedicatedAndPerVmFamilyCoreQuotaEnforced,
            dedicatedCoresPerFamilies,
            new AccountQuota(batchQuota.ActiveJobAndJobScheduleQuota, batchQuota.PoolQuota, batchQuota.DedicatedCoreQuota, batchQuota.LowPriorityCoreQuota));
    }

    /// <inheritdoc />
    public async Task<PoolAndJobQuota> GetPoolAndJobQuotaAsync(CancellationToken cancellationToken)
    {
        var quotas = await GetBatchAccountQuotasAsync(cancellationToken);
        return new(quotas.PoolQuota, quotas.ActiveJobAndJobScheduleQuota);
    }

    /// <summary>
    /// Getting the batch account quota.
    /// </summary>
    /// <returns></returns>
    public virtual async Task<AzureBatchAccountQuotas> GetBatchAccountQuotasAsync(CancellationToken cancellationToken)
        => await appCache.GetOrCreateAsync(clientsFactory.BatchAccountInformation.ToString(), entry =>
        {
            entry.AbsoluteExpirationRelativeToNow = TimeSpan.FromDays(1);
            return GetBatchAccountQuotasImplAsync(cancellationToken);
        });

    private async Task<AzureBatchAccountQuotas> GetBatchAccountQuotasImplAsync(CancellationToken cancellationToken)
    {
        try
        {
            logger.LogDebug($"Getting quota information for Batch Account: {clientsFactory.BatchAccountInformation.Name} calling ARM API");

            var managementClient = clientsFactory.CreateBatchAccountManagementClient();
            var batchAccount = (await managementClient.GetAsync(cancellationToken: cancellationToken)).Value.Data;

            return batchAccount is null
                ? throw new InvalidOperationException(
                    $"Batch Account was not found. Account name:{clientsFactory.BatchAccountInformation.Name}.  Resource group:{clientsFactory.BatchAccountInformation.ResourceGroupName}")
                : new AzureBatchAccountQuotas
                {
                    ActiveJobAndJobScheduleQuota = batchAccount.ActiveJobAndJobScheduleQuota ?? 0,
                    DedicatedCoreQuota = batchAccount.DedicatedCoreQuota ?? 0,
                    DedicatedCoreQuotaPerVMFamily = batchAccount.DedicatedCoreQuotaPerVmFamily,
                    DedicatedCoreQuotaPerVMFamilyEnforced = batchAccount.IsDedicatedCoreQuotaPerVmFamilyEnforced ?? false,
                    LowPriorityCoreQuota = batchAccount.LowPriorityCoreQuota ?? 0,
                    PoolQuota = batchAccount.PoolQuota ?? 0,
                };
        }
        catch (Exception ex)
        {
            logger.LogError(ex, $"An exception occurred when getting the batch account.");
            throw;
        }
    }

    private static BatchVmFamilyQuotas ToVmFamilyBatchAccountQuotas(AzureBatchAccountQuotas batchAccountQuotas, string vmFamily, bool lowPriority, int? coresRequirement)
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
}
