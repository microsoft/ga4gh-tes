// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text.RegularExpressions;
using Tes.Converters;
using Tes.Repository;
using Tes.TaskSubmitters;

namespace Tes.Models
{
    public partial class TesTask : RepositoryItem<TesTask>
    {
        public static readonly List<TesState> ActiveStates =
        [
            TesState.QUEUEDEnum,
            TesState.RUNNINGEnum,
            TesState.PAUSEDEnum,
            TesState.INITIALIZINGEnum
        ];

        /// <summary>
        /// Number of retries attempted
        /// </summary>
        [DataMember(Name = "error_count")]
        public int ErrorCount { get; set; }

        /// <summary>
        /// Boolean of whether cancellation was requested
        /// </summary>
        [DataMember(Name = "is_cancel_requested")]
        public bool IsCancelRequested { get; set; }

        /// <summary>
        /// Date + time the task was completed, in RFC 3339 format. This is set by the system, not the client.
        /// </summary>
        /// <value>Date + time the task was completed, in RFC 3339 format. This is set by the system, not the client.</value>
        [System.Text.Json.Serialization.JsonConverter(typeof(JsonValueConverterDateTimeOffsetRFC3339))]
        [DataMember(Name = "end_time")]
        public DateTimeOffset? EndTime { get; set; }

        /// <summary>
        /// Top-most parent workflow ID from the workflow engine
        /// </summary>
        [IgnoreDataMember]
        public string WorkflowId => TaskSubmitter?.WorkflowId;

        /// <summary>
        /// Workflow engine task metadata
        /// </summary>
        [DataMember(Name = "task_submitter")]
        public TaskSubmitter TaskSubmitter { get; set; }

        /// <summary>
        /// Assigned Azure Batch PoolId
        /// </summary>
        [DataMember(Name = "pool_id")]
        public string PoolId { get; set; }

        /// <summary>
        /// Overall reason of the task failure, populated when task execution ends in EXECUTOR_ERROR or SYSTEM_ERROR, for example "DiskFull".
        /// </summary>
        [IgnoreDataMember]
        public string FailureReason => this.Logs?.LastOrDefault()?.FailureReason;

        /// <summary>
        /// Warning that gets populated if task encounters an issue that needs user's attention.
        /// </summary>
        [IgnoreDataMember]
        public string Warning => this.Logs?.LastOrDefault()?.Warning;

        /// <summary>
        /// Cromwell-specific result code, populated when Batch task execution ends in COMPLETED, containing the exit code of the inner Cromwell script.
        /// </summary>
        [IgnoreDataMember]
        public int? CromwellResultCode => this.Logs?.LastOrDefault()?.CromwellResultCode;

        /// <summary>
        /// Cromwell task description without shard and attempt numbers
        /// </summary>
        [IgnoreDataMember]
        public string CromwellTaskInstanceName => (TaskSubmitter as CromwellTaskSubmitter)?.CromwellTaskInstanceName;

        /// <summary>
        /// Cromwell shard number
        /// </summary>
        [IgnoreDataMember]
        public int? CromwellShard => (TaskSubmitter as CromwellTaskSubmitter)?.CromwellShard;

        /// <summary>
        /// Cromwell attempt number
        /// </summary>
        [IgnoreDataMember]
        public int? CromwellAttempt => (TaskSubmitter as CromwellTaskSubmitter)?.CromwellAttempt;

        public bool IsActiveState()
        {
            return ActiveStates.Contains(this.State);
        }
    }
}
