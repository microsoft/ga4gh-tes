// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using Tes.Models;
using Tes.TaskSubmitters;

namespace Tes.Extensions
{
    /// <summary>
    /// <see cref="TesTask"/> extensions
    /// </summary>
    public static class TesTaskExtensions
    {
        /// <summary>
        /// Reports if task was submitted by Cromwell.
        /// </summary>
        /// <param name="tesTask"><see cref="TesTask"/>.</param>
        /// <returns><see cref="true"/> if task is from Cromwell, false otherwise.</returns>
        public static bool IsCromwell(this TesTask tesTask)
        {
            return tesTask.TaskSubmitter is CromwellTaskSubmitter;
        }

        /// <summary>
        /// Gets Cromwell task metadata
        /// </summary>
        /// <param name="tesTask"><see cref="TesTask"/>.</param>
        /// <returns><see cref="CromwellTaskSubmitter"/>.</returns>
        public static CromwellTaskSubmitter GetCromwellMetadata(this TesTask tesTask)
        {
            return tesTask.TaskSubmitter as CromwellTaskSubmitter;
        }

        /// <summary>
        /// Visits each value in an enumeration with an action.
        /// </summary>
        /// <typeparam name="T">Type of enumerated items.</typeparam>
        /// <param name="values">Enumeration on which to visit each item.</param>
        /// <param name="action">Action to invoke with each item.</param>
        public static void ForEach<T>(this IEnumerable<T> values, Action<T> action)
        {
            foreach (var value in values)
            {
                action(value);
            }
        }

        /// <summary>
        /// Adds a range of items to an <see cref="IList{T}"/>.
        /// </summary>
        /// <typeparam name="T">Type of enumerated items.</typeparam>
        /// <param name="list">List to add <paramref name="items"/> to.</param>
        /// <param name="items">Items to add to <paramref name="list"/>.</param>
        public static void AddRange<T>(this IList<T> list, IEnumerable<T> items)
        {
            items.ForEach(list.Add);
        }

        /// <summary>
        /// Writes to <see cref="TesTask"/> system log.
        /// </summary>
        /// <param name="tesTask"><see cref="TesTask"/></param>
        /// <param name="logEntries">List of strings to write to the log.</param>
        public static void AddToSystemLog(this TesTask tesTask, IEnumerable<string> logEntries)
        {
            if (logEntries is not null && logEntries.Any(e => !string.IsNullOrEmpty(e)))
            {
                var tesTaskLog = tesTask.GetOrAddTesTaskLog();
                tesTaskLog.SystemLogs ??= [];
                tesTaskLog.SystemLogs.AddRange(logEntries);
            }
        }

        /// <summary>
        /// Sets the failure reason for <see cref="TesTask"/> and optionally adds additional system log items
        /// </summary>
        /// <param name="tesTask"><see cref="TesTask"/></param>
        /// <param name="failureReason">Failure reason code</param>
        /// <param name="additionalSystemLogItems">Additional system log entries</param>
        public static void SetFailureReason(this TesTask tesTask, string failureReason, params string[] additionalSystemLogItems)
        {
            tesTask.GetOrAddTesTaskLog().FailureReason = failureReason;
            tesTask.AddToSystemLog([failureReason]);
            tesTask.AddToSystemLog(additionalSystemLogItems.Where(i => !string.IsNullOrEmpty(i)));
        }

        /// <summary>
        /// Sets the failure reason for <see cref="TesTask"/> using values from <see cref="TesException"/>
        /// </summary>
        /// <param name="tesTask"><see cref="TesTask"/></param>
        /// <param name="tesException"><see cref="TesException"/></param>
        public static void SetFailureReason(this TesTask tesTask, TesException tesException)
            => tesTask.SetFailureReason(tesException.FailureReason, tesException.Message, tesException.StackTrace);

        /// <summary>
        /// Sets the warning for <see cref="TesTask"/> and optionally adds additional system log items
        /// </summary>
        /// <param name="tesTask"><see cref="TesTask"/></param>
        /// <param name="warning">Warning code</param>
        /// <param name="additionalSystemLogItems">Additional system log entries</param>
        public static void SetWarning(this TesTask tesTask, string warning, params string[] additionalSystemLogItems)
        {
            tesTask.GetOrAddTesTaskLog().Warning = warning;
            tesTask.AddToSystemLog([warning]);
            tesTask.AddToSystemLog(additionalSystemLogItems.Where(i => !string.IsNullOrEmpty(i)));
        }

        /// <summary>
        /// Returns the last <see cref="TesTaskLog"/>. Adds it if none exist.
        /// </summary>
        /// <param name="tesTask"><see cref="TesTask"/></param>
        /// <returns>Last <see cref="TesTaskLog"/></returns>
        public static TesTaskLog GetOrAddTesTaskLog(this TesTask tesTask)
        {
            if (tesTask.Logs is null || !tesTask.Logs.Any())
            {
                tesTask.Logs = [new()];
            }

            return tesTask.Logs.Last();
        }

        /// <summary>
        /// Adds a new <see cref="TesTaskLog"/> to <see cref="TesTask"/>
        /// </summary>
        /// <param name="tesTask"><see cref="TesTask"/></param>
        /// <returns>Last <see cref="TesTaskLog"/></returns>
        public static TesTaskLog AddTesTaskLog(this TesTask tesTask)
        {
            tesTask.Logs ??= [];
            tesTask.Logs.Add(new());

            return tesTask.Logs.Last();
        }

        /// <summary>
        /// Returns the <see cref="BatchNodeMetrics"/>. Adds it if it doesn't exist.
        /// </summary>
        /// <param name="tesTaskLog"><see cref="TesTaskLog"/></param>
        /// <returns>Initialized <see cref="BatchNodeMetrics"/></returns>
        public static BatchNodeMetrics GetOrAddBatchNodeMetrics(this TesTaskLog tesTaskLog)
            => tesTaskLog.BatchNodeMetrics ??= new();

        /// <summary>
        /// Returns the Metadata property of <see cref="TesTaskLog"/>. Adds it if it doesn't exist.
        /// </summary>
        /// <param name="tesTaskLog"><see cref="TesTaskLog"/></param>
        /// <returns>Initialized Metadata property</returns>
        public static Dictionary<string, string> GetOrAddMetadata(this TesTaskLog tesTaskLog)
            => tesTaskLog.Metadata ??= new();

        /// <summary>
        /// Returns the last <see cref="TesExecutorLog"/>. Adds it if none exist.
        /// </summary>
        /// <param name="tesTaskLog"><see cref="TesTaskLog"/></param>
        /// <returns>Initialized <see cref="TesExecutorLog"/></returns>
        public static TesExecutorLog GetOrAddExecutorLog(this TesTaskLog tesTaskLog)
        {
            if (tesTaskLog.Logs is null || !tesTaskLog.Logs.Any())
            {
                tesTaskLog.Logs = [new()];
            }

            return tesTaskLog.Logs.Last();
        }

        /// <summary>
        /// Get the backend parameter value for the specified parameter
        /// </summary>
        /// <returns>The value if it exists; null otherwise</returns>
        public static string GetBackendParameterValue(this TesResources resources, TesResources.SupportedBackendParameters parameter)
            => resources.BackendParameters?.TryGetValue(parameter.ToString(), out var backendParameterValue) ?? false ? backendParameterValue : null;

        /// <summary>
        /// Checks if a backend parameter was present
        /// </summary>
        /// <returns>True if the parameter value is not null or whitespace; false otherwise</returns>
        public static bool ContainsBackendParameterValue(this TesResources resources, TesResources.SupportedBackendParameters parameter)
            => !string.IsNullOrWhiteSpace(resources.GetBackendParameterValue(parameter));
    }
}
