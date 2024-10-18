// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
            if ((tesTask.Logs?.Count ?? 0) == 0)
            {
                TesTaskLog log = new();
                tesTask.Logs = [log];
                return log;
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
            TesTaskLog log = new();
            tesTask.Logs ??= [];
            tesTask.Logs.Add(log);

            return log;
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
            => tesTaskLog.Metadata ??= [];

        /// <summary>
        /// Returns the last <see cref="TesExecutorLog"/>. Adds it if none exist.
        /// </summary>
        /// <param name="tesTaskLog"><see cref="TesTaskLog"/></param>
        /// <returns>Initialized <see cref="TesExecutorLog"/></returns>
        public static TesExecutorLog GetOrAddExecutorLog(this TesTaskLog tesTaskLog)
        {
            if ((tesTaskLog.Logs?.Count ?? 0) == 0)
            {
                TesExecutorLog log = new();
                tesTaskLog.Logs = [log];
                return log;
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
