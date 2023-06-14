// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading;
using System.Threading.Tasks;
using Tes.Models;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Provides the ability to verify if the quota is available for a given SKU
    /// </summary>
    public interface IBatchQuotaVerifier
    {
        /// <summary>
        /// Checks if the current quota allows fulfillment of the requested VM SKU.
        /// </summary>
        /// <param name="virtualMachineInformation"></param>
        /// <param name="needPoolOrJobQuotaCheck">Flag to enable checking pool and job quotas.</param>
        /// <param name="needCoresUtilizationQuotaCheck">Flag to enable checking core quotas against current utilization. Zero quota checks are always performed.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        /// <exception cref="AzureBatchLowQuotaException">Thrown when a task requires more cores than total quota available</exception>
        /// <exception cref="AzureBatchQuotaMaxedOutException">Thrown when a max quota condition was identified</exception>
        Task CheckBatchAccountQuotasAsync(VirtualMachineInformation virtualMachineInformation, bool needPoolOrJobQuotaCheck, bool needCoresUtilizationQuotaCheck, CancellationToken cancellationToken);

        /// <summary>
        /// Gets the instance of the batch quota provider.
        /// </summary>
        /// <returns>Batch quota provider <see cref="IBatchQuotaProvider"/></returns>
        IBatchQuotaProvider GetBatchQuotaProvider();
    }
}
