// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;

namespace TesApi.Web.Management.Models.Quotas
{
    /// <summary>
    /// Core quota information of a batch account.
    /// </summary>
    /// <param name="NumberOfCores">Number of cores.</param>
    /// <param name="IsLowPriority">If the cores apply to low priority</param>
    /// <param name="IsDedicatedAndPerVmFamilyCoreQuotaEnforced">If dedicate core quota is enforced</param>
    /// <param name="DedicatedCoreQuotas">Dedicated core quota list</param>
    /// <param name="AccountQuota">Additional quota information for the account</param>
    public record BatchVmCoreQuota(
        int NumberOfCores,
        bool IsLowPriority,
        bool IsDedicatedAndPerVmFamilyCoreQuotaEnforced,
        List<BatchVmCoresPerFamily> DedicatedCoreQuotas,
        AccountQuota AccountQuota = default);
}
