// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.Azure.Batch.Common;

namespace TesApi.Web
{
    /// <summary>
    /// Contains all needed Azure Batch pool allocation state needed by <see cref="BatchPool"/>.
    /// </summary>
    /// <param name="AllocationStateTransitionTime">Gets the time at which the pool entered its current <see cref="AllocationState"/>.</param>
    /// <param name="AutoScaleEnabled">Gets whether the pool size should automatically adjust according to a configured autoscale formula.</param>
    /// <param name="TargetLowPriority">Gets the desired number of low-priority compute nodes in the pool.</param>
    /// <param name="CurrentLowPriority">Gets the number of low-priority compute nodes currently in the pool.</param>
    /// <param name="TargetDedicated">Gets the desired number of dedicated compute nodes in the pool.</param>
    /// <param name="CurrentDedicated">Gets the number of dedicated compute nodes currently in the pool.</param>
    public record AzureBatchPoolAllocationState(DateTime? AllocationStateTransitionTime, bool? AutoScaleEnabled, int? TargetLowPriority, int? CurrentLowPriority, int? TargetDedicated, int? CurrentDedicated)
    {
        /// <summary>
        /// Gets an <see cref="AllocationState"/> which indicates what node allocation activity is occurring on the pool.
        /// </summary>
        public AllocationState? AllocationState { get; set; }

        /// <summary>
        /// Deconstruction method for <see cref="AzureBatchPoolAllocationState"/>.
        /// </summary>
        /// <param name="AllocationState">Gets an <see cref="AllocationState"/> which indicates what node allocation activity is occurring on the pool.</param>
        /// <param name="AllocationStateTransitionTime">Gets the time at which the pool entered its current <see cref="AllocationState"/>.</param>
        /// <param name="AutoScaleEnabled">Gets whether the pool size should automatically adjust according to a configured autoscale formula.</param>
        /// <param name="TargetLowPriority">Gets the desired number of low-priority compute nodes in the pool.</param>
        /// <param name="CurrentLowPriority">Gets the number of low-priority compute nodes currently in the pool.</param>
        /// <param name="TargetDedicated">Gets the desired number of dedicated compute nodes in the pool.</param>
        /// <param name="CurrentDedicated">Gets the number of dedicated compute nodes currently in the pool.</param>
        public void Deconstruct(out AllocationState? AllocationState, out DateTime? AllocationStateTransitionTime, out bool? AutoScaleEnabled, out int? TargetLowPriority, out int? CurrentLowPriority, out int? TargetDedicated, out int? CurrentDedicated)
        {
            AllocationState = this.AllocationState;
            AllocationStateTransitionTime = this.AllocationStateTransitionTime;
            AutoScaleEnabled = this.AutoScaleEnabled;
            TargetLowPriority = this.TargetLowPriority;
            CurrentLowPriority = this.CurrentLowPriority;
            TargetDedicated = this.TargetDedicated;
            CurrentDedicated = this.CurrentDedicated;
        }
    }
}
