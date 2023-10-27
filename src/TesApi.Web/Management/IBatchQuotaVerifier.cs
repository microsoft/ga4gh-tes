// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
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
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        /// <exception cref="AzureBatchLowQuotaException">Thrown when a task requires more cores than total quota available</exception>
        /// <exception cref="AzureBatchQuotaMaxedOutException">Thrown when a max quota condition was identified</exception>
        Task CheckBatchAccountQuotasAsync(VirtualMachineInformation virtualMachineInformation, bool needPoolOrJobQuotaCheck, CancellationToken cancellationToken);

        /// <summary>
        /// Checks if the current quota allows creation of the requested quantity of new Pools and Jobs.
        /// </summary>
        /// <param name="required">The quantity of new pools and jobs that need to be accomodated for success.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>A <see cref="CheckGroupPoolAndJobQuotaResult"/> that returns the size of the portion of <paramref name="required"/> that would've resulted in the provided <see cref="AzureBatchQuotaMaxedOutException"/>.</returns>
        Task<CheckGroupPoolAndJobQuotaResult> CheckBatchAccountPoolAndJobQuotasAsync(int required, CancellationToken cancellationToken);

        /// <summary>
        /// Gets the instance of the batch quota provider.
        /// </summary>
        /// <returns>Batch quota provider <see cref="IBatchQuotaProvider"/></returns>
        IBatchQuotaProvider GetBatchQuotaProvider();

        /// <summary>
        /// Result of group checking quota for pools and jobs.
        /// </summary>
        /// <param name="Exceeded">The number of pools or jobs above the "required" request that exceeded the available quota.</param>
        /// <param name="Exception">The <see cref="Exception"/> to return to the tasks that could not be accomodated.</param>
        public record struct CheckGroupPoolAndJobQuotaResult(int Exceeded, Exception Exception);
    }
}
