// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace TesApi.Web.Management.Models.Quotas
{
    /// <summary>
    /// Result of group checking quota for pools and jobs.
    /// </summary>
    /// <param name="ExceededQuantity">The number of pools or jobs above the "required" request that exceeded the available quota.</param>
    /// <param name="Exception">The <see cref="Exception"/> to return to the tasks that could not be accomodated.</param>
    public record struct CheckGroupPoolAndJobQuotaResult(int ExceededQuantity, Exception Exception);
}
