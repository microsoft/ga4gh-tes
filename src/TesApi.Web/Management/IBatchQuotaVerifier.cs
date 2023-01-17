// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
        /// <returns></returns>
        /// <exception cref="AzureBatchQuotaMaxedOutException">Thrown when a max quota condition was identified</exception>
        Task CheckBatchAccountQuotasAsync(VirtualMachineInformation virtualMachineInformation);

        /// <summary>
        /// Gets the instance of the batch quota provider. 
        /// </summary>
        /// <returns>Batch quota provider <see cref="IBatchQuotaProvider"/></returns>
        IBatchQuotaProvider GetBatchQuotaProvider();
    }
}
