// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Management.Models.Quotas
{
    /// <summary>
    /// Record representing the batch account quotas for a vm family. 
    /// </summary>
    /// <param name="TotalCoreQuota">Total core quota</param>
    /// <param name="VmFamilyQuota">Vm Family quota</param>
    /// <param name="PoolQuota">Pool quota</param>
    /// <param name="ActiveJobAndJobScheduleQuota">Job and job schedule quota</param>
    /// <param name="DedicatedCoreQuotaPerVmFamilyEnforced">if quota per vm family is enforced for a vm family</param>
    /// <param name="VmFamily">Vm family</param>
    public record BatchVmFamilyQuotas(
        int TotalCoreQuota,
        int VmFamilyQuota,
        int PoolQuota,
        int ActiveJobAndJobScheduleQuota,
        bool DedicatedCoreQuotaPerVmFamilyEnforced,
        string VmFamily);
}
