// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Management.Models.Quotas
{
    /// <summary>
    /// Pool and Active Job and JobSchedule quotas.
    /// </summary>
    /// <param name="PoolQuota">Pool quota.</param>
    /// <param name="ActiveJobAndJobScheduleQuota">Active Job and JobSchedule quota.</param>
    public record struct PoolAndJobQuota(int PoolQuota, int ActiveJobAndJobScheduleQuota);
}
