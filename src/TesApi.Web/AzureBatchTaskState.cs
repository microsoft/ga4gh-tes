// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using Tes.Models;
using static TesApi.Web.AzureBatchTaskState;

namespace TesApi.Web
{
    /// <summary>
    /// Snapshot state of the progression of an attempt to run a <see cref="TesTask"/>
    /// </summary>
    /// <param name="State">Task state. Also see <seealso cref="TesState"/>.</param>
    /// <param name="OutputFileLogs">File details after the task has completed successfully, for logging purposes.</param>
    /// <param name="Failure">Failure information.</param>
    /// <param name="CloudTaskCreationTime"><see cref="Microsoft.Azure.Batch.CloudTask.CreationTime"/>.</param>
    /// <param name="BatchTaskStartTime"><see cref="Microsoft.Azure.Batch.TaskExecutionInformation.StartTime"/>.</param>
    /// <param name="ExecutorStartTime"></param>
    /// <param name="ExecutorEndTime"></param>
    /// <param name="ExecutorExitCode"></param>
    /// <param name="BatchTaskEndTime"><see cref="Microsoft.Azure.Batch.TaskExecutionInformation.EndTime"/>.</param>
    /// <param name="BatchTaskExitCode"><see cref="Microsoft.Azure.Batch.TaskExecutionInformation.ExitCode"/>.</param>
    /// <param name="Warning">Warning. First item in enumeration is the Warning code, rest of items are additional system log entries.</param>
    public record class AzureBatchTaskState(
        TaskState State,
        IEnumerable<OutputFileLog> OutputFileLogs = default,
        FailureInformation? Failure = default,
        DateTimeOffset? CloudTaskCreationTime = default,
        DateTimeOffset? BatchTaskStartTime = default,
        DateTimeOffset? ExecutorStartTime = default,
        DateTimeOffset? ExecutorEndTime = default,
        int? ExecutorExitCode = default,
        DateTimeOffset? BatchTaskEndTime = default,
        int? BatchTaskExitCode = default,
        IEnumerable<string> Warning = default)
    {
        /// <summary>
        /// Unknown error. Either an exception or a missing reason.
        /// </summary>
        public const string UnknownError = @"UnknownError";

        /// <summary>
        /// <see cref="TesState.EXECUTOR_ERROR"/>.
        /// </summary>
        public const string ExecutorError = @"ExecutorError";


        /// <summary>
        /// <see cref="TesState.SYSTEM_ERROR"/>.
        /// </summary>
        public const string SystemError = @"SystemError";

        /// <summary>
        /// TesTask's state
        /// </summary>
        public enum TaskState
        {
            /// <summary>
            /// The event does not represent any change in the task's state.
            /// </summary>
            NoChange = 1,

            /// <summary>
            /// The event provides task state without changing the task's <see cref="TesTask.State"/>.
            /// </summary>
            InfoUpdate,

            /// <summary>
            /// A request has been made for the task's cancellation.
            /// </summary>
            CancellationRequested,

            /// <summary>
            /// The task has been assigned to a compute node, but is waiting for a
            /// required Job Preparation task to complete on the node.
            /// </summary>
            Initializing,

            /// <summary>
            /// The task is running on a compute node.
            /// </summary>
            Running,

            /// <summary>
            /// The task is no longer eligible to run, usually because the task has
            /// finished successfully, or the task has finished unsuccessfully and
            /// has exhausted its retry limit.  A task is also marked as completed
            /// if an error occurred launching the task, or when the task has been
            /// terminated.
            /// </summary>
            CompletedSuccessfully,

            /// <summary>
            /// The task has completed, but it finished unsuccessfully or the executor had an error
            /// </summary>
            CompletedWithErrors,

            /// <summary>
            /// Azure Batch was unable to allocate a machine for the job.  This could be due to either a temporary or permanent unavailability of the given VM SKU
            /// </summary>
            NodeAllocationFailed,

            /// <summary>
            /// Azure Batch preempted the execution of this task while running on a low-priority node
            /// </summary>
            NodePreempted,

            /// <summary>
            /// Node failed during the startup task
            /// </summary>
            NodeStartTaskFailed,

            /// <summary>
            /// Node failed during startup or task execution (for example, ContainerInvalidImage, DiskFull)
            /// </summary>
            NodeFailedDuringStartupOrExecution,

            /// <summary>
            /// Node failed during upload or download
            /// </summary>
            NodeFilesUploadOrDownloadFailed,
        }

        /// <summary>
        /// OutputFileLog describes a single output file. This describes file details after the task has completed successfully, for logging purposes.
        /// </summary>
        /// <param name="Url">URL of the file in storage, e.g. s3://bucket/file.txt</param>
        /// <param name="Path">Path of the file inside the container. Must be an absolute path.</param>
        /// <param name="Size">Size of the file in bytes.</param>
        public record struct OutputFileLog(Uri Url, string Path, long Size);

        /// <summary>
        /// TesTask's failure information
        /// </summary>
        /// <param name="Reason">Failure code. Intended to be machine readable. See <see cref="TesTaskLog.FailureReason"/>.</param>
        /// <param name="SystemLogs">Failure details to be added to <see cref="TesTaskLog.SystemLogs"/>.</param>
        public record struct FailureInformation(string Reason, IEnumerable<string> SystemLogs);

        /// <summary>
        /// SystemLog-appending copy constructor
        /// </summary>
        /// <param name="original"><see cref="AzureBatchTaskState"/> to copy</param>
        /// <param name="appendToSystemLog">Text to add to the SystemLog in the copy</param>
        protected AzureBatchTaskState(AzureBatchTaskState original, string appendToSystemLog)
            : this(original)
        {
            Failure = original.Failure switch
            {
                null => new(UnknownError, Enumerable.Empty<string>().Append(appendToSystemLog)),
                { SystemLogs: null } => new(original.Failure.Value.Reason ?? UnknownError, Enumerable.Empty<string>().Append(appendToSystemLog)),
                _ => new(original.Failure.Value.Reason ?? UnknownError, original.Failure.Value.SystemLogs.Append(appendToSystemLog)),
            };
        }
    }
}
