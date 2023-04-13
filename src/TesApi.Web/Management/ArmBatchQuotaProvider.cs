// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LazyCache;
using Microsoft.Azure.Management.Batch;
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

    private readonly IAppCache appCache;
    private readonly AzureManagementClientsFactory clientsFactory;


    /// <summary>
    /// Constructor of ArmResourceQuotaVerifier
    /// </summary>
    /// <param name="logger"></param>
    /// <param name="appCache"></param>
    /// <param name="clientsFactory"></param>
    public ArmBatchQuotaProvider(IAppCache appCache, AzureManagementClientsFactory clientsFactory,
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
                         .Select(r => new BatchVmCoresPerFamily(r.Name, r.CoreQuota))
                         .ToList();
        }

        return new BatchVmCoreQuota(numberOfCores,
            lowPriority,
            isDedicatedAndPerVmFamilyCoreQuotaEnforced,
            dedicatedCoresPerFamilies,
            new AccountQuota(batchQuota.ActiveJobAndJobScheduleQuota, batchQuota.PoolQuota, batchQuota.DedicatedCoreQuota, batchQuota.LowPriorityCoreQuota));
    }

    /// <summary>
    /// Getting the batch account quota.
    /// </summary>
    /// <returns></returns>
    public virtual async Task<AzureBatchAccountQuotas> GetBatchAccountQuotasAsync(CancellationToken cancellationToken)
        => await appCache.GetOrAddAsync(clientsFactory.BatchAccountInformation.ToString(), async () => await GetBatchAccountQuotasImplAsync(cancellationToken));

    private async Task<AzureBatchAccountQuotas> GetBatchAccountQuotasImplAsync(CancellationToken cancellationToken)
    {
        try
        {
            logger.LogInformation($"Getting quota information for Batch Account: {clientsFactory.BatchAccountInformation.Name} calling ARM API");

            var batchAccount = await (await clientsFactory.CreateBatchAccountManagementClient(cancellationToken)).BatchAccount.GetAsync(clientsFactory.BatchAccountInformation.ResourceGroupName, clientsFactory.BatchAccountInformation.Name, cancellationToken);

            if (batchAccount == null)
            {
                throw new InvalidOperationException(
                    $"Batch Account was not found. Account name:{clientsFactory.BatchAccountInformation.Name}.  Resource group:{clientsFactory.BatchAccountInformation.ResourceGroupName}");
            }

            return new AzureBatchAccountQuotas
            {
                ActiveJobAndJobScheduleQuota = batchAccount.ActiveJobAndJobScheduleQuota,
                DedicatedCoreQuota = batchAccount.DedicatedCoreQuota ?? 0,
                DedicatedCoreQuotaPerVMFamily = batchAccount.DedicatedCoreQuotaPerVMFamily,
                DedicatedCoreQuotaPerVMFamilyEnforced = batchAccount.DedicatedCoreQuotaPerVMFamilyEnforced,
                LowPriorityCoreQuota = batchAccount.LowPriorityCoreQuota ?? 0,
                PoolQuota = batchAccount.PoolQuota,

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
