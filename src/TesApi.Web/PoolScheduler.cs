// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CommonUtilities;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Logging;
using Tes.Models;
using Tes.Repository;
using CloudTaskWithPreviousComputeNodeId = TesApi.Web.IBatchPool.CloudTaskWithPreviousComputeNodeId;

namespace TesApi.Web
{
    /// <summary>
    /// A background service that monitors <see cref="CloudPool"/>s in the batch system, orchestrates their lifecycle, and updates their state.
    /// This should only be used as a system-wide singleton service.  This class does not support scale-out on multiple machines,
    /// nor does it implement a leasing mechanism.  In the future, consider using the Lease Blob operation.
    /// </summary>
    /// <param name="hostApplicationLifetime">Used for requesting termination of the current application during initialization.</param>
    /// <param name="repository">The main TES task database repository implementation.</param>
    /// <param name="batchScheduler">The batch scheduler implementation.</param>
    /// <param name="taskScheduler">The batch scheduler implementation.</param>
    /// <param name="logger">The logger instance.</param>
    /// <exception cref="ArgumentNullException"></exception>
    internal class PoolScheduler(Microsoft.Extensions.Hosting.IHostApplicationLifetime hostApplicationLifetime, IRepository<TesTask> repository, IBatchScheduler batchScheduler, ITaskScheduler taskScheduler, ILogger<PoolScheduler> logger)
        : OrchestrateOnBatchSchedulerServiceBase(hostApplicationLifetime, repository, batchScheduler, logger)
    {
        private readonly ITaskScheduler TaskScheduler = taskScheduler;

        /// <summary>
        /// Interval between each call to <see cref="IBatchPool.ServicePoolAsync(CancellationToken)"/>.
        /// </summary>
        public static readonly TimeSpan RunInterval = TimeSpan.FromSeconds(30); // The very fastest process inside of Azure Batch accessing anything within pools or jobs uses a 30 second polling interval

        private static readonly TimeSpan StateTransitionTimeForDeletionTimeSpan = 0.75 * Web.BatchScheduler.BatchDeleteNewTaskWorkaroundTimeSpan;
        private static readonly TimeSpan CompletedTaskListTimeSpan = 0.5 * Web.BatchScheduler.BatchDeleteNewTaskWorkaroundTimeSpan;

        /// <summary>
        /// Predicate to obtain <see cref="CloudTask"/>s (recently) running on <see cref="ComputeNode"/>s. Used to connect tasks and nodes together.
        /// </summary>
        internal static bool TaskListWithComputeNodeInfoPredicate(CloudTask task) => !TaskState.Completed.Equals(task.State) && !string.IsNullOrEmpty(task.ComputeNodeInformation?.ComputeNodeId);

        /// <summary>
        /// Predicate to obtain <see cref="CloudTask"/>s pending in Azure Batch.
        /// </summary>
        private static bool ActiveTaskListPredicate(CloudTask task) => TaskState.Active.Equals(task.State);

        /// <summary>
        /// Predicate used to obtain <see cref="CloudTask"/>s to backstop completing <see cref="TesTask"/>s in case of problems with the <see cref="Tes.Runner.Events.EventsPublisher.TaskCompletionEvent"/>.
        /// </summary>
        private static bool CompletedTaskListPredicate(CloudTask task, DateTime now) => TaskState.Completed.Equals(task.State) && task.StateTransitionTime < now - CompletedTaskListTimeSpan;

        /// <inheritdoc />
        protected override void ExecuteSetup(CancellationToken cancellationToken)
        {
            BatchScheduler.LoadExistingPoolsAsync(cancellationToken).Wait(cancellationToken); // Delay starting PoolScheduler until this completes to finish initializing the shared parts of BatchScheduler.
        }

        /// <inheritdoc />
        protected override async ValueTask ExecuteCoreAsync(CancellationToken cancellationToken)
        {
            await ExecuteActionOnIntervalAsync(
                RunInterval,
                async token => await ExecuteActionOnPoolsAsync(
                    async (pool, token) =>
                    {
                        await pool.ServicePoolAsync(token);
                        await ProcessTasksAsync(pool, DateTime.UtcNow, pool.ListCloudTasksAsync(), token);
                    },
                    token),
                cancellationToken);
        }

        /// <summary>
        /// Performs an action on each batch pool.
        /// </summary>
        /// <param name="action">Method performing operations on a <see cref="IBatchPool"/>.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private async ValueTask ExecuteActionOnPoolsAsync(Func<IBatchPool, CancellationToken, ValueTask> action, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(action);

            var pools = BatchScheduler.GetPools().ToList();

            if (0 == pools.Count)
            {
                return;
            }

            var startTime = DateTime.UtcNow;

            await Parallel.ForEachAsync(pools, cancellationToken, async (pool, token) =>
            {
                try
                {
                    await action(pool, token);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exc)
                {
                    Logger.LogError(exc, @"Batch pool {PoolId} threw an exception when serviced.", pool.PoolId);
                }
            });

            Logger.LogDebug(@"Service Batch Pools for {PoolsCount} pools completed in {TotalSeconds} seconds.", pools.Count, DateTime.UtcNow.Subtract(startTime).TotalSeconds);
        }

        /// <summary>
        /// Processes tasks connected to a pool to manage state.
        /// </summary>
        /// <param name="pool">The <see cref="IBatchPool"/> associated with <paramref name="tasks"/>.</param>
        /// <param name="now">Reference time.</param>
        /// <param name="tasks"><see cref="CloudTask"/>s requiring attention.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private async ValueTask ProcessTasksAsync(IBatchPool pool, DateTime now, IEnumerable<CloudTaskWithPreviousComputeNodeId> tasks, CancellationToken cancellationToken)
        {
            var batchStateCandidateTasks = Enumerable.Empty<CloudTaskWithPreviousComputeNodeId>();
            var deletionCandidateTasks = AsyncEnumerable.Empty<IBatchScheduler.CloudTaskId>();

            var deletionCandidateCreationCutoff = now - Web.BatchScheduler.BatchDeleteNewTaskWorkaroundTimeSpan;
            var stateTransitionTimeCutoffForDeletions = now - StateTransitionTimeForDeletionTimeSpan;

            foreach (var taskWithNodeId in tasks)
            {
                var (task, computeNodeId) = taskWithNodeId;

                if (!string.IsNullOrWhiteSpace(computeNodeId) || ActiveTaskListPredicate(task) || CompletedTaskListPredicate(task, now))
                {
                    batchStateCandidateTasks = batchStateCandidateTasks.Append(taskWithNodeId);
                }

                if (TaskState.Completed.Equals(task.State) && task.CreationTime < deletionCandidateCreationCutoff && task.StateTransitionTime < stateTransitionTimeCutoffForDeletions)
                {
                    deletionCandidateTasks = deletionCandidateTasks.Append(new IBatchScheduler.CloudTaskId(pool.PoolId, task.Id, task.CreationTime.Value));
                }
            }

            await ProcessCloudTaskStatesAsync(pool.PoolId, GetCloudTaskStatesAsync(pool, now, batchStateCandidateTasks, cancellationToken), cancellationToken);

            await ProcessTasksToDelete(deletionCandidateTasks, cancellationToken);
        }

        /// <summary>
        /// Updates each task based on the provided states.
        /// </summary>
        /// <param name="poolId">The batch pool from which the state was obtained.</param>
        /// <param name="states">The states with which to update the associated <see cref="TesTask"/>s.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private async ValueTask ProcessCloudTaskStatesAsync(string poolId, IAsyncEnumerable<CloudTaskBatchTaskState> states, CancellationToken cancellationToken)
        {
            ConcurrentBag<(TesTask TesTask, AzureBatchTaskState State)> tasksAndStates = [];

            await Parallel.ForEachAsync(states, cancellationToken, async (state, token) =>
            {
                TesTask tesTask = default;
                if (await Repository.TryGetItemAsync(BatchScheduler.GetTesTaskIdFromCloudTaskId(state.CloudTaskId), token, task => tesTask = task) && tesTask is not null)
                {
                    tasksAndStates.Add((tesTask, state.TaskState));
                }
                else
                {
                    Logger.LogError(@"Unable to locate TesTask for CloudTask '{CloudTask}' with action state {ActionState}.", state.CloudTaskId, state.TaskState.State);
                }
            });

            if (!tasksAndStates.IsEmpty)
            {
                ConcurrentBag<string> requeues = [];
                Dictionary<string, AzureBatchTaskState> statesByTask = new(StringComparer.Ordinal);
                List<TesTask> tasks = [];

                tasksAndStates.ForEach(t =>
                {
                    tasks.Add(t.TesTask);
                    statesByTask.Add(t.TesTask.Id, t.State);
                });

                do
                {
                    requeues.Clear();
                    await OrchestrateTesTasksOnBatchAsync(
                        $"NodeState ({poolId})",
                        _ => ValueTask.FromResult(tasks.ToAsyncEnumerable()),
                        (tesTasks, token) => TaskScheduler.ProcessTesTaskBatchStatesAsync(tesTasks, tesTasks.Select(task => statesByTask[task.Id]).ToArray(), token),
                        ex => { requeues.Add(ex.RepositoryItem.Id); return ValueTask.CompletedTask; }, cancellationToken);

                    // Fetch updated TesTasks from the repository
                    ConcurrentBag<TesTask> requeuedTasks = [];
                    await Parallel.ForEachAsync(requeues, cancellationToken, async (id, token) =>
                    {
                        TesTask tesTask = default;

                        if (await Repository.TryGetItemAsync(id, token, task => tesTask = task))
                        {
                            requeuedTasks.Add(tesTask);
                        }
                    });

                    // Stage next loop
                    tasks.Clear();
                    requeuedTasks.ForEach(tasks.Add);
                }
                while (!requeues.IsEmpty);
            }
            else
            {
                Logger.LogTrace("No task state changes from pool/node information this time: PoolId: {PoolId}.", poolId);
            }
        }

        /// <summary>
        /// Deletes <see cref="CloudTask"/>s.
        /// </summary>
        /// <param name="tasks">Tasks to delete.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private async ValueTask ProcessTasksToDelete(IAsyncEnumerable<IBatchScheduler.CloudTaskId> tasks, CancellationToken cancellationToken)
        {
            await foreach (var taskResult in BatchScheduler.DeleteCloudTasksAsync(tasks, cancellationToken).WithCancellation(cancellationToken))
            {
                try
                {
                    switch (await taskResult)
                    {
                        case true:
                            Logger.LogTrace(@"Azure task {CloudTask} was deleted.", taskResult.Related.TaskId);
                            break;

                        case false:
                            Logger.LogTrace(@"Azure task {CloudTask} was NOT deleted.", taskResult.Related.TaskId);
                            break;
                    }
                }
                catch (Exception exc)
                {
                    Logger.LogError(exc, @"Failed to delete azure task '{CloudTask}': '{ExceptionType}': '{ExceptionMessage}'", taskResult.Related.TaskId, exc.GetType().FullName, exc.Message);
                }
            }
        }

        /// <summary>
        /// Obtains <see cref="CloudTaskBatchTaskState"/>s for updating <see cref="TesTask"/>s.
        /// </summary>
        /// <param name="pool">The <see cref="IBatchPool"/> associated with <paramref name="tasks"/>.</param>
        /// <param name="now">Reference time.</param>
        /// <param name="tasks"><see cref="CloudTask"/>s which need <see cref="CloudTaskBatchTaskState"/>s for further processing.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns><see cref="CloudTaskBatchTaskState"/> for each <paramref name="tasks"/> that are associated with or assigned errors reported by batch.</returns>
        /// <remarks>If a task was running on a compute node reported as in a fatal state, that state will be reported for the task. Otherwise, pending tasks will be assigned resize and starttask failures and completed tasks will be reported as complete.</remarks>
        private async IAsyncEnumerable<CloudTaskBatchTaskState> GetCloudTaskStatesAsync(IBatchPool pool, DateTime now, IEnumerable<CloudTaskWithPreviousComputeNodeId> tasks, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            List<CloudTaskWithPreviousComputeNodeId> taskListWithComputeNodeInfo; // To check if the task was running when its node became preempted or unusable
            List<CloudTask> activeTaskList; // These are candidates to be the victim of resizes or starttask failures
            List<CloudTask> completedTaskList; // Backstop if events don't provide timely task completion information in a timely manner

            {
                var tasksWithNodeIds = tasks.ToList();
                taskListWithComputeNodeInfo = tasksWithNodeIds.Where(task => !string.IsNullOrWhiteSpace(task.PreviousComputeNodeId)).ToList();
                var taskList = tasksWithNodeIds.Select(task => task.CloudTask).ToList();
                activeTaskList = [.. taskList.Where(ActiveTaskListPredicate).OrderByDescending(task => task.StateTransitionTime?.ToUniversalTime())];
                completedTaskList = taskList.Where(task => CompletedTaskListPredicate(task, now)).ToList();
            }

            if (taskListWithComputeNodeInfo.Count > 0)
            {
                Logger.LogDebug("{PoolId} reported nodes that will be removed. There are {tasksWithComputeNodeInfo} tasks that might be impacted.", pool.PoolId, taskListWithComputeNodeInfo.Count);

                await foreach (var node in (await pool.ListEjectableComputeNodesAsync()).WithCancellation(cancellationToken))
                {
                    foreach (var task in taskListWithComputeNodeInfo
                        .Where(task => node.Id.Equals(task.PreviousComputeNodeId, StringComparison.InvariantCultureIgnoreCase))
                        .Select(task => task.CloudTask))
                    {
                        Logger.LogTrace("{TaskId} connected to node {NodeId} in state {NodeState}.", task.Id, node.Id, node.State);

                        yield return new(task.Id, node.State switch
                        {
                            ComputeNodeState.Preempted => new(AzureBatchTaskState.TaskState.NodePreempted),
                            ComputeNodeState.Unusable => new(AzureBatchTaskState.TaskState.NodeFailedDuringStartupOrExecution, Failure: ParseComputeNodeErrors(node.Errors)),
                            _ => throw new System.Diagnostics.UnreachableException(),
                        });

                        Logger.LogTrace("Removing {TaskId} from consideration for other errors.", task.Id);
                        _ = activeTaskList.Remove(task);
                    }
                }
            }

            foreach (var state in activeTaskList.Zip(GetFailures(), (cloud, state) => new CloudTaskBatchTaskState(cloud.Id, state)))
            {
                yield return state;
            }

            foreach (var task in completedTaskList)
            {
                yield return new(task.Id, GetCompletedBatchState(task));
            }

            yield break;

            static AzureBatchTaskState.FailureInformation ParseComputeNodeErrors(IReadOnlyList<ComputeNodeError> nodeErrors)
            {
                var totalList = nodeErrors.Select(nodeError => Enumerable.Empty<string>()
                    .Append(nodeError.Code).Append(nodeError.Message)
                    .Concat(nodeError.ErrorDetails.Select(FormatNameValuePair)))
                    .SelectMany(s => s).ToList();

                if (totalList.Contains(TaskFailureInformationCodes.DiskFull))
                {
                    return new(TaskFailureInformationCodes.DiskFull, totalList);
                }
                else
                {
                    return new(BatchErrorCodeStrings.NodeStateUnusable, totalList);
                }
            }

            IEnumerable<AzureBatchTaskState> GetFailures()
            {
                foreach (var failure in RepeatUntil(PopNextStartTaskFailure, failure => failure is null))
                {
                    yield return ConvertFromStartTask(failure);
                    cancellationToken.ThrowIfCancellationRequested();
                }

                foreach (var failure in RepeatUntil(PopNextResizeError, failure => failure is null))
                {
                    yield return ConvertFromResize(failure);
                    cancellationToken.ThrowIfCancellationRequested();
                }

                yield break;
            }

            AzureBatchTaskState ConvertFromResize(ResizeError failure)
                => new(AzureBatchTaskState.TaskState.NodeAllocationFailed, Failure: new(failure.Code, Enumerable.Empty<string>()
                    .Append(failure.Message)
                    .Concat(failure.Values.Select(FormatNameValuePair))));

            AzureBatchTaskState ConvertFromStartTask(IBatchPool.StartTaskFailureInformation failure)
                => new(AzureBatchTaskState.TaskState.NodeStartTaskFailed, Failure: new(failure.TaskFailureInformation.Code, Enumerable.Empty<string>()
                    .Append($"Start task failed ({failure.TaskFailureInformation.Category}): {failure.TaskFailureInformation.Message}")
                    .Concat(failure.TaskFailureInformation.Details?.Select(FormatNameValuePair) ?? [])
                    .Append(failure.NodeId)));

            ResizeError PopNextResizeError()
                => pool.ResizeErrors.TryDequeue(out var resizeError) ? resizeError : default;

            IBatchPool.StartTaskFailureInformation PopNextStartTaskFailure()
                => pool.StartTaskFailures.TryDequeue(out var failure) ? failure : default;

            AzureBatchTaskState GetCompletedBatchState(CloudTask task)
            {
                Logger.LogTrace("Getting batch task state from completed task {TesTask}.", BatchScheduler.GetTesTaskIdFromCloudTaskId(task.Id));
                return task.ExecutionInformation.Result switch
                {
                    TaskExecutionResult.Success => new(
                        AzureBatchTaskState.TaskState.CompletedSuccessfully,
                        BatchTaskStartTime: task.ExecutionInformation.StartTime,
                        BatchTaskEndTime: task.ExecutionInformation.EndTime,
                        BatchTaskExitCode: task.ExecutionInformation.ExitCode),

                    TaskExecutionResult.Failure => new(
                        AzureBatchTaskState.TaskState.CompletedWithErrors,
                        Failure: new(task.ExecutionInformation.FailureInformation.Code,
                        Enumerable.Empty<string>()
                            .Append(task.ExecutionInformation.FailureInformation.Message)
                            .Append($"Batch task ExitCode: {task.ExecutionInformation?.ExitCode}, Failure message: {task.ExecutionInformation?.FailureInformation?.Message}")
                            .Concat(task.ExecutionInformation.FailureInformation.Details?.Select(FormatNameValuePair) ?? [])),
                        BatchTaskStartTime: task.ExecutionInformation.StartTime,
                        BatchTaskEndTime: task.ExecutionInformation.EndTime,
                        BatchTaskExitCode: task.ExecutionInformation.ExitCode),

                    _ => throw new System.Diagnostics.UnreachableException(),
                };
            }

            static string FormatNameValuePair(NameValuePair pair)
                => $"{pair.Name}: {pair.Value}";

            static IEnumerable<T> RepeatUntil<T>(Func<T> func, Predicate<T> stop)
            {
                while (true)
                {
                    var t = func();

                    if (stop(t))
                    {
                        yield break;
                    }

                    yield return t;
                }
            }
        }

        /// <summary>
        /// A <see cref="CloudTask"/> associated with a pool resize or node error.
        /// </summary>
        /// <param name="CloudTaskId"><see cref="CloudTask.Id"/>.</param>
        /// <param name="TaskState">A compute node and/or pool resize error.</param>
        private record CloudTaskBatchTaskState(string CloudTaskId, AzureBatchTaskState TaskState);
    }
}
