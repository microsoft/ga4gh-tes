// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tes.Models;
using TesApi.Web.Management.Models.Quotas;

namespace TesApi.Web.Management;


/// <summary>
/// Contains logic that verifies if the batch account can fulfill the compute requirements using quota and sizing information.
/// </summary>
public class BatchQuotaVerifier : IBatchQuotaVerifier
{
    private const string AzureSupportUrl = "https://portal.azure.com/#blade/Microsoft_Azure_Support/HelpAndSupportBlade/newsupportrequest";
    private readonly IAzureProxy azureProxy;
    private readonly ILogger logger;
    private readonly IBatchQuotaProvider batchQuotaProvider;
    private readonly IBatchSkuInformationProvider batchSkuInformationProvider;
    private readonly BatchAccountResourceInformation batchAccountInformation;


    /// <summary>
    /// Constructor of BatchQuotaVerifier
    /// </summary>
    /// <param name="batchQuotaProvider"><see cref="IBatchQuotaProvider"/></param>
    /// <param name="batchSkuInformationProvider"><see cref="IBatchSkuInformationProvider"/></param>
    /// <param name="batchAccountInformation"><see cref="BatchAccountResourceInformation"/></param>
    /// <param name="azureProxy"><see cref="IAzureProxy"/></param>
    /// <param name="logger"><see cref="ILogger"/></param>
    public BatchQuotaVerifier(BatchAccountResourceInformation batchAccountInformation,
        IBatchQuotaProvider batchQuotaProvider,
        IBatchSkuInformationProvider batchSkuInformationProvider,
        IAzureProxy azureProxy,
        ILogger<BatchQuotaVerifier> logger)
    {
        ArgumentNullException.ThrowIfNull(azureProxy);
        ArgumentNullException.ThrowIfNull(batchQuotaProvider);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(batchSkuInformationProvider);
        ArgumentNullException.ThrowIfNull(batchAccountInformation);

        if (string.IsNullOrEmpty(batchAccountInformation.Region))
        {
            throw new ArgumentException($"The batch account information does not include the region. Batch information provided:{batchAccountInformation}", nameof(batchAccountInformation));
        }

        ArgumentNullException.ThrowIfNull(azureProxy);

        this.azureProxy = azureProxy;
        this.logger = logger;
        this.batchAccountInformation = batchAccountInformation;
        this.batchSkuInformationProvider = batchSkuInformationProvider;
        this.batchQuotaProvider = batchQuotaProvider;
        this.azureProxy = azureProxy;
    }

    /// <inheritdoc cref="IBatchQuotaProvider"/>
    public async Task CheckBatchAccountQuotasAsync(VirtualMachineInformation virtualMachineInformation, bool needPoolOrJobQuotaCheck, CancellationToken cancellationToken)
    {
        var workflowCoresRequirement = virtualMachineInformation.VCpusAvailable ?? 0;
        var isDedicated = !virtualMachineInformation.LowPriority;
        var vmFamily = virtualMachineInformation.VmFamily;
        BatchVmFamilyQuotas batchVmFamilyBatchQuotas;

        try
        {
            batchVmFamilyBatchQuotas = await batchQuotaProvider.GetQuotaForRequirementAsync(
                virtualMachineInformation.VmFamily,
                virtualMachineInformation.LowPriority,
                virtualMachineInformation.VCpusAvailable,
                cancellationToken);

            if (batchVmFamilyBatchQuotas is null)
            {
                throw new InvalidOperationException(
                    "Could not obtain quota information from the management service. The return value is null");
            }
        }
        catch (Exception e)
        {
            logger.LogError(e, "Failed to retrieve quota information for the management provider");
            throw;
        }

        var isDedicatedAndPerVmFamilyCoreQuotaEnforced = isDedicated && batchVmFamilyBatchQuotas.DedicatedCoreQuotaPerVmFamilyEnforced;

        if (workflowCoresRequirement > batchVmFamilyBatchQuotas.TotalCoreQuota)
        {
            // The workflow task requires more cores than the total Batch account's cores quota - FAIL
            throw new AzureBatchLowQuotaException($"Azure Batch Account does not have enough {(isDedicated ? "dedicated" : "low priority")} cores quota to run a workflow with cpu core requirement of {workflowCoresRequirement}. Please submit an Azure Support request to increase your quota: {AzureSupportUrl}");
        }

        if (isDedicatedAndPerVmFamilyCoreQuotaEnforced && workflowCoresRequirement > batchVmFamilyBatchQuotas.VmFamilyQuota)
        {
            // The workflow task requires more cores than the total Batch account's dedicated family quota - FAIL
            throw new AzureBatchLowQuotaException($"Azure Batch Account does not have enough dedicated {vmFamily} cores quota to run a workflow with cpu core requirement of {workflowCoresRequirement}. Please submit an Azure Support request to increase your quota: {AzureSupportUrl}");
        }

        if (needPoolOrJobQuotaCheck)
        {
            var batchUtilization = GetBatchAccountUtilization();

            if (batchUtilization.ActiveJobsCount + 1 > batchVmFamilyBatchQuotas.ActiveJobAndJobScheduleQuota)
            {
                throw new AzureBatchQuotaMaxedOutException($"No remaining active jobs quota available. There are {batchUtilization.ActiveJobsCount} active jobs out of {batchVmFamilyBatchQuotas.ActiveJobAndJobScheduleQuota}.");
            }

            if (batchUtilization.ActivePoolsCount + 1 > batchVmFamilyBatchQuotas.PoolQuota)
            {
                throw new AzureBatchQuotaMaxedOutException($"No remaining pool quota available. There are {batchUtilization.ActivePoolsCount} pools in use out of {batchVmFamilyBatchQuotas.PoolQuota}.");
            }
        }
    }

    /// <inheritdoc cref="IBatchQuotaProvider"/>
    public IBatchQuotaProvider GetBatchQuotaProvider()
        => batchQuotaProvider;

    private BatchAccountUtilization GetBatchAccountUtilization()
    {
        // TODO: make these async
        var activeJobsCount = azureProxy.GetBatchActiveJobCount();
        var activePoolsCount = azureProxy.GetBatchActivePoolCount();

        return new(activeJobsCount, activePoolsCount);
    }
}
