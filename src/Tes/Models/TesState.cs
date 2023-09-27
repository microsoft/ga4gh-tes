﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

/*
 * Task Execution Service
 *
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * OpenAPI spec version: 0.3.0
 *
 * Generated by: https://openapi-generator.tech
 */

using System.Runtime.Serialization;
using Newtonsoft.Json;

namespace Tes.Models
{
    /// <summary>
    /// Task states.
    /// </summary>
    /// <value>
    ///   - UNKNOWN: The state of the task is unknown.  This provides a safe default for messages where this field is missing, for example, so that a missing field does not accidentally imply that the state is QUEUED.
    ///   - QUEUED: The task is queued.
    ///   - INITIALIZING: The task has been assigned to a worker and is currently preparing to run. For example, the worker may be turning on, downloading input files, etc.
    ///   - RUNNING: The task is running. Input files are downloaded and the first Executor has been started.
    ///   - PAUSED: The task is paused.  An implementation may have the ability to pause a task, but this is not required.
    ///   - COMPLETE: The task has completed running. Executors have exited without error and output files have been successfully uploaded.
    ///   - EXECUTOR_ERROR: The task encountered an error in one of the Executor processes. Generally, this means that an Executor exited with a non-zero exit code.
    ///   - SYSTEM_ERROR: The task was stopped due to a system error, but not from an Executor, for example an upload failed due to network issues, the worker's ran out of disk space, etc.
    ///   - CANCELED: The task was canceled by the user.
    ///   - PREEMPTED: The task is stopped (preempted) by the system. The reasons for this would be tied to the specific system running the job. Generally, this means that the system reclaimed the compute capacity for reallocation.
    ///   - CANCELING: The task was canceled by the user, but the downstream resources are still awaiting deletion.
    /// </value>
    [JsonConverter(typeof(Newtonsoft.Json.Converters.StringEnumConverter))]
    public enum TesState
    {
        /// <summary>
        /// Enum UNKNOWNEnum for UNKNOWN
        /// </summary>
        [EnumMember(Value = "UNKNOWN")]
        UNKNOWNEnum = 1,

        /// <summary>
        /// Enum QUEUEDEnum for QUEUED
        /// </summary>
        [EnumMember(Value = "QUEUED")]
        QUEUEDEnum = 2,

        /// <summary>
        /// Enum INITIALIZINGEnum for INITIALIZING
        /// </summary>
        [EnumMember(Value = "INITIALIZING")]
        INITIALIZINGEnum = 3,

        /// <summary>
        /// Enum RUNNINGEnum for RUNNING
        /// </summary>
        [EnumMember(Value = "RUNNING")]
        RUNNINGEnum = 4,

        /// <summary>
        /// Enum PAUSEDEnum for PAUSED
        /// </summary>
        [EnumMember(Value = "PAUSED")]
        PAUSEDEnum = 5,

        /// <summary>
        /// Enum COMPLETEEnum for COMPLETE
        /// </summary>
        [EnumMember(Value = "COMPLETE")]
        COMPLETEEnum = 6,

        /// <summary>
        /// Enum EXECUTORERROREnum for EXECUTOR_ERROR
        /// </summary>
        [EnumMember(Value = "EXECUTOR_ERROR")]
        EXECUTORERROREnum = 7,

        /// <summary>
        /// Enum SYSTEMERROREnum for SYSTEM_ERROR
        /// </summary>
        [EnumMember(Value = "SYSTEM_ERROR")]
        SYSTEMERROREnum = 8,

        /// <summary>
        /// Enum CANCELEDEnum for CANCELED
        /// </summary>
        [EnumMember(Value = "CANCELED")]
        CANCELEDEnum = 9,

        /// <summary>
        /// Enum PREEMPTEDEnum for PREEMPTED
        /// </summary>
        [EnumMember(Value = "PREEMPTED")]
        PREEMPTEDEnum = 10,

        /// <summary>
        /// Enum CANCELINGEnum for CANCELING
        /// </summary>
        [EnumMember(Value = "CANCELING")]
        CANCELINGEnum = 11
    }
}
