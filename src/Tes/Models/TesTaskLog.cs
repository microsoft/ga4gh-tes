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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using Newtonsoft.Json;
using Tes.Utilities;

namespace Tes.Models
{
    /// <summary>
    /// TaskLog describes logging information related to a Task.
    /// </summary>
    [DataContract]
    public partial class TesTaskLog : IEquatable<TesTaskLog>
    {
        public TesTaskLog()
            => NewtonsoftJsonSafeInit.SetDefaultSettings();

        /// <summary>
        /// Logs for each executor
        /// </summary>
        /// <value>Logs for each executor</value>
        [DataMember(Name = "logs")]
        public List<TesExecutorLog> Logs { get; set; }

        /// <summary>
        /// Arbitrary logging metadata included by the implementation.
        /// </summary>
        /// <value>Arbitrary logging metadata included by the implementation.</value>
        [DataMember(Name = "metadata")]
        public Dictionary<string, string> Metadata { get; set; }

        /// <summary>
        /// When the task started, in RFC 3339 format.
        /// </summary>
        /// <value>When the task started, in RFC 3339 format.</value>
        [DataMember(Name = "start_time")]
        public DateTimeOffset? StartTime { get; set; }

        /// <summary>
        /// When the task ended, in RFC 3339 format.
        /// </summary>
        /// <value>When the task ended, in RFC 3339 format.</value>
        [DataMember(Name = "end_time")]
        public DateTimeOffset? EndTime { get; set; }

        /// <summary>
        /// Information about all output files. Directory outputs are flattened into separate items.
        /// </summary>
        /// <value>Information about all output files. Directory outputs are flattened into separate items.</value>
        [DataMember(Name = "outputs")]
        public List<TesOutputFileLog> Outputs { get; set; }

        /// <summary>
        /// System logs are any logs the system decides are relevant, which are not tied directly to an Executor process. Content is implementation specific: format, size, etc.  System logs may be collected here to provide convenient access.  For example, the system may include the name of the host where the task is executing, an error message that caused a SYSTEM_ERROR state (e.g. disk is full), etc.  System logs are only included in the FULL task view.
        /// </summary>
        /// <value>System logs are any logs the system decides are relevant, which are not tied directly to an Executor process. Content is implementation specific: format, size, etc.  System logs may be collected here to provide convenient access.  For example, the system may include the name of the host where the task is executing, an error message that caused a SYSTEM_ERROR state (e.g. disk is full), etc.  System logs are only included in the FULL task view.</value>
        [DataMember(Name = "system_logs")]
        public List<string> SystemLogs { get; set; }

        /// <summary>
        /// Returns the string presentation of the object
        /// </summary>
        /// <returns>String presentation of the object</returns>
        public override string ToString()
            => new StringBuilder()
                .Append("class TesTaskLog {\n")
                .Append("  Logs: ").Append(Logs).Append('\n')
                .Append("  Metadata: ").Append(Metadata).Append('\n')
                .Append("  StartTime: ").Append(StartTime).Append('\n')
                .Append("  EndTime: ").Append(EndTime).Append('\n')
                .Append("  Outputs: ").Append(Outputs).Append('\n')
                .Append("  SystemLogs: ").Append(SystemLogs).Append('\n')
                .Append("}\n")
                .ToString();

        /// <summary>
        /// Returns the JSON string presentation of the object
        /// </summary>
        /// <returns>JSON string presentation of the object</returns>
        public string ToJson()
            => JsonConvert.SerializeObject(this, Formatting.Indented);

        /// <summary>
        /// Returns true if objects are equal
        /// </summary>
        /// <param name="obj">Object to be compared</param>
        /// <returns>Boolean</returns>
        public override bool Equals(object obj)
            => obj switch
            {
                var x when x is null => false,
                var x when ReferenceEquals(this, x) => true,
                _ => obj.GetType() == GetType() && Equals((TesTaskLog)obj),
            };

        /// <summary>
        /// Returns true if TesTaskLog instances are equal
        /// </summary>
        /// <param name="other">Instance of TesTaskLog to be compared</param>
        /// <returns>Boolean</returns>
        public bool Equals(TesTaskLog other)
            => other switch
            {
                var x when x is null => false,
                var x when ReferenceEquals(this, x) => true,
                _ =>
                (
                    Logs == other.Logs ||
                    Logs is not null &&
                    Logs.SequenceEqual(other.Logs)
                ) &&
                (
                    Metadata == other.Metadata ||
                    Metadata is not null &&
                    Metadata.SequenceEqual(other.Metadata)
                ) &&
                (
                    StartTime == other.StartTime ||
                    StartTime is not null &&
                    StartTime.Equals(other.StartTime)
                ) &&
                (
                    EndTime == other.EndTime ||
                    EndTime is not null &&
                    EndTime.Equals(other.EndTime)
                ) &&
                (
                    Outputs == other.Outputs ||
                    Outputs is not null &&
                    Outputs.SequenceEqual(other.Outputs)
                ) &&
                (
                    SystemLogs == other.SystemLogs ||
                    SystemLogs is not null &&
                    SystemLogs.SequenceEqual(other.SystemLogs)
                ),
            };

        /// <summary>
        /// Gets the hash code
        /// </summary>
        /// <returns>Hash code</returns>
        public override int GetHashCode()
        {
            unchecked // Overflow is fine, just wrap
            {
                var hashCode = 41;
                // Suitable nullity checks etc, of course :)
                if (Logs is not null)
                {
                    hashCode = hashCode * 59 + Logs.GetHashCode();
                }

                if (Metadata is not null)
                {
                    hashCode = hashCode * 59 + Metadata.GetHashCode();
                }

                if (StartTime is not null)
                {
                    hashCode = hashCode * 59 + StartTime.GetHashCode();
                }

                if (EndTime is not null)
                {
                    hashCode = hashCode * 59 + EndTime.GetHashCode();
                }

                if (Outputs is not null)
                {
                    hashCode = hashCode * 59 + Outputs.GetHashCode();
                }

                if (SystemLogs is not null)
                {
                    hashCode = hashCode * 59 + SystemLogs.GetHashCode();
                }

                return hashCode;
            }
        }

        #region Operators
#pragma warning disable 1591

        public static bool operator ==(TesTaskLog left, TesTaskLog right)
            => Equals(left, right);

        public static bool operator !=(TesTaskLog left, TesTaskLog right)
            => !Equals(left, right);

#pragma warning restore 1591
        #endregion Operators
    }
}
