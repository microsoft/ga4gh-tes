// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.Azure.Batch.Common;

namespace TesApi.Web
{
    /// <summary>
    /// The allocation state for a <see cref="Microsoft.Azure.Batch.CloudPool"/>.
    /// </summary>
    /// <param name="AllocationState">Gets an <see cref="Microsoft.Azure.Batch.Common.AllocationState"/> which indicates what node allocation activity is occurring on the pool.</param>
    /// <param name="AllocationStateTransitionTime">Gets the time at which the pool entered its current <see cref="AllocationState"/>.</param>
    /// <param name="AutoScaleEnabled">Gets whether the pool size should automatically adjust according to the pool's AutoScaleFormula.</param>
    /// <param name="TargetLowPriority">Gets the desired number of low-priority compute nodes in the pool.</param>
    /// <param name="CurrentLowPriority">Gets the number of low-priority compute nodes currently in the pool.</param>
    /// <param name="TargetDedicated">Gets the desired number of dedicated compute nodes in the pool.</param>
    /// <param name="CurrentDedicated">Gets the number of dedicated compute nodes currently in the pool.</param>
    public record FullBatchPoolAllocationState(AllocationState? AllocationState, DateTime? AllocationStateTransitionTime, bool? AutoScaleEnabled, int? TargetLowPriority, int? CurrentLowPriority, int? TargetDedicated, int? CurrentDedicated)
    {
    }
}
