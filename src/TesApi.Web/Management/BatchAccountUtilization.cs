// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Management
{
    /// <summary>
    /// A record containing utilization information for a batch account
    /// </summary>
    /// <param name="ActiveJobsCount">Active job counts</param>
    /// <param name="ActivePoolsCount">Active pool count</param>
    public record BatchAccountUtilization(
        int ActiveJobsCount,
        int ActivePoolsCount);

}
