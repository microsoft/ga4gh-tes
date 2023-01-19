// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Tasks;
using TesApi.Web.Management.Models.Quotas;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Provides quota information of Batch VM resources. 
    /// </summary>
    public interface IBatchQuotaProvider
    {
        /// <summary>
        /// Returns the quota information for the VM requirements. 
        /// </summary>
        /// <param name="vmFamily"></param>
        /// <param name="lowPriority"></param>
        /// <param name="coresRequirement"></param>
        /// <returns></returns>
        Task<BatchVmFamilyQuotas> GetQuotaForRequirementAsync(
            string vmFamily,
            bool lowPriority,
            int? coresRequirement);

        /// <summary>
        /// Returns a list of vm core quota per family.
        /// If quota per family is not enforced, returns the total core quota for low or normal priority vms.
        /// </summary>
        /// <param name="lowPriority"></param>
        /// <returns></returns>
        Task<BatchVmCoreQuota> GetVmCoreQuotaAsync(bool lowPriority);
    }
}
