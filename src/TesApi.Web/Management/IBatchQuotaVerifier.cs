﻿// Copyright (c) Microsoft Corporation.
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
        /// <param name="required"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>The size of the portion of <paramref name="required"/> that would've resulted in the returned <see cref="AzureBatchQuotaMaxedOutException"/>.</returns>
        Task<(int exceeded, Exception exception)> CheckBatchAccountPoolOrJobQuotasAsync(int required, CancellationToken cancellationToken);

        /// <summary>
        /// Gets the instance of the batch quota provider.
        /// </summary>
        /// <returns>Batch quota provider <see cref="IBatchQuotaProvider"/></returns>
        IBatchQuotaProvider GetBatchQuotaProvider();
    }
}
