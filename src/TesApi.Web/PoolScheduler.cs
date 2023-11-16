// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Logging;
using Tes.Models;
using Tes.Repository;
using CloudTaskBatchTaskState = TesApi.Web.IBatchPool.CloudTaskBatchTaskState;

namespace TesApi.Web
{
    /// <summary>
    /// A background service that montitors CloudPools in the batch system, orchestrates their lifecycle, and updates their state.
    /// This should only be used as a system-wide singleton service.  This class does not support scale-out on multiple machines,
    /// nor does it implement a leasing mechanism.  In the future, consider using the Lease Blob operation.
    /// </summary>
    internal class PoolScheduler : OrchestrateOnBatchSchedulerServiceBase
    {
        /// <summary>
        /// Interval between each call to <see cref="IBatchPool.ServicePoolAsync(CancellationToken)"/>.
        /// </summary>
        public static readonly TimeSpan RunInterval = TimeSpan.FromSeconds(30); // The very fastest process inside of Azure Batch accessing anything within pools or jobs uses a 30 second polling interval

        private static readonly TimeSpan StateTransitionTimeForDeletionTimeSpan = 0.75 * BatchScheduler.BatchDeleteNewTaskWorkaroundTimeSpan;
        private static readonly TimeSpan CompletedTaskListTimeSpan = 0.5 * BatchScheduler.BatchDeleteNewTaskWorkaroundTimeSpan;

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="hostApplicationLifetime">Used for requesting termination of the current application during initialization.</param>
        /// <param name="repository">The main TES task database repository implementation</param>
        /// <param name="batchScheduler"></param>
        /// <param name="logger"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public PoolScheduler(Microsoft.Extensions.Hosting.IHostApplicationLifetime hostApplicationLifetime, IRepository<TesTask> repository, IBatchScheduler batchScheduler, ILogger<PoolScheduler> logger)
            : base(hostApplicationLifetime, repository, batchScheduler, logger) { }

        /// <inheritdoc />
        protected override void ExecuteSetup(CancellationToken stoppingToken)
        {
            batchScheduler.LoadExistingPoolsAsync(stoppingToken).Wait(stoppingToken); // Delay starting Scheduler until this completes to finish initializing BatchScheduler.
        }

        /// <inheritdoc />
        protected override Task ExecuteCoreAsync(CancellationToken stoppingToken)
        {
            return ServiceBatchPoolsAsync(stoppingToken);
        }

        /// <summary>
        /// Performs an action on each batch pool.
        /// </summary>
        /// <param name="pollName"></param>
        /// <param name="action"></param>
        /// <param name="stoppingToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private async ValueTask ExecuteActionOnPoolsAsync(string pollName, Func<IBatchPool, CancellationToken, ValueTask> action, CancellationToken stoppingToken)
        {
            ArgumentNullException.ThrowIfNull(action);

            var pools = batchScheduler.GetPools().ToList();

            if (0 == pools.Count)
            {
                return;
            }

            var startTime = DateTime.UtcNow;

            await Parallel.ForEachAsync(pools, stoppingToken, async (pool, token) =>
            {
                try
                {
                    await action(pool, token);
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, @"Batch pool {PoolId} threw an exception in {Poll}.", pool.Id, pollName);
                }
            });

            logger.LogDebug(@"{Poll} for {PoolsCount} pools completed in {TotalSeconds} seconds.", pollName, pools.Count, DateTime.UtcNow.Subtract(startTime).TotalSeconds);
        }

        /// <summary>
        /// Repeatedly services all batch pools associated with this TES instance, including updating tasks.
        /// </summary>
        /// <param name="stoppingToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private Task ServiceBatchPoolsAsync(CancellationToken stoppingToken)
        {
            return ExecuteActionOnIntervalAsync(
                RunInterval,
                async token =>
                await ExecuteActionOnPoolsAsync(
                    "Service Batch Pools",
                    async (pool, cancellationToken) =>
                    {
                        await pool.ServicePoolAsync(cancellationToken);
                        await ProcessTasksAsync(pool, DateTime.UtcNow, pool.ListCloudTasksAsync(), cancellationToken);
                    },
                    token),
                stoppingToken);
        }

        /// <summary>
        /// Processes tasks to manage state.
        /// </summary>
        /// <param name="pool"></param>
        /// <param name="now">Reference time.</param>
        /// <param name="tasks"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async ValueTask ProcessTasksAsync(IBatchPool pool, DateTime now, IAsyncEnumerable<CloudTask> tasks, CancellationToken cancellationToken)
        {
            var batchStateCandidateTasks = AsyncEnumerable.Empty<CloudTask>();
            var deletionCandidateTasks = AsyncEnumerable.Empty<IBatchScheduler.CloudTaskId>();

            var deletionCandidateCreationCutoff = now - BatchScheduler.BatchDeleteNewTaskWorkaroundTimeSpan;
            var stateTransitionTimeCutoffForDeletions = now - StateTransitionTimeForDeletionTimeSpan;

            await foreach (var task in tasks.WithCancellation(cancellationToken))
            {

                if (TaskListWithComputeNodeInfoPredicate(task) || ActiveTaskListPredicate(task) || CompletedTaskListPredicate(task, now))
                {
                    batchStateCandidateTasks = batchStateCandidateTasks.Append(task);
                }

                if (TaskState.Completed.Equals(task.State) && task.CreationTime < deletionCandidateCreationCutoff && task.StateTransitionTime < stateTransitionTimeCutoffForDeletions)
                {
                    deletionCandidateTasks = deletionCandidateTasks.Append(new IBatchScheduler.CloudTaskId(pool.Id, task.Id, task.CreationTime.Value));
                }
            }

            await ProcessCloudTaskStatesAsync(pool.Id, GetCloudTaskStatesAsync(pool, now, batchStateCandidateTasks, cancellationToken), cancellationToken);

            await ProcessDeletedTasks(deletionCandidateTasks, cancellationToken);
        }

        /// <summary>
        /// Shared between <see cref="ProcessTasksAsync"/> and <see cref="GetCloudTaskStatesAsync"/>.
        /// </summary>
        internal static bool TaskListWithComputeNodeInfoPredicate(CloudTask task) => !TaskState.Completed.Equals(task.State) && !string.IsNullOrEmpty(task.ComputeNodeInformation?.ComputeNodeId);

        /// <summary>
        /// Shared between <see cref="ProcessTasksAsync"/> and <see cref="GetCloudTaskStatesAsync"/>.
        /// </summary>
        private static bool ActiveTaskListPredicate(CloudTask task) => TaskState.Active.Equals(task.State);

        /// <summary>
        /// Shared between <see cref="ProcessTasksAsync"/> and <see cref="GetCloudTaskStatesAsync"/>.
        /// </summary>
        private static bool CompletedTaskListPredicate(CloudTask task, DateTime now) => TaskState.Completed.Equals(task.State) && task.StateTransitionTime < now - CompletedTaskListTimeSpan;

        /// <summary>
        /// Updates each task based on the provided states.
        /// </summary>
        /// <param name="poolId">The batch pool/job from which the state was obtained.</param>
        /// <param name="states">The states with which to update the associated tes tasks.</param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private async ValueTask ProcessCloudTaskStatesAsync(string poolId, IAsyncEnumerable<CloudTaskBatchTaskState> states, CancellationToken cancellationToken)
        {
            var list = new ConcurrentBag<(TesTask TesTask, AzureBatchTaskState State)>();

            await Parallel.ForEachAsync(states, cancellationToken, async (state, token) =>
            {
                TesTask tesTask = default;
                if (await repository.TryGetItemAsync(batchScheduler.GetTesTaskIdFromCloudTaskId(state.CloudTaskId), token, task => tesTask = task) && tesTask is not null)
                {
                    list.Add((tesTask, state.TaskState));
                }
                else
                {
                    logger.LogError(@"Unable to locate TesTask for CloudTask '{CloudTask}' with action state {ActionState}.", state.CloudTaskId, state.TaskState.State);
                }
            });

            if (!list.IsEmpty)
            {
                await OrchestrateTesTasksOnBatchAsync(
                    $"NodeState ({poolId})",
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
                    async _ => list.Select(t => t.TesTask).ToAsyncEnumerable(),
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
                    (tesTasks, token) => batchScheduler.ProcessTesTaskBatchStatesAsync(tesTasks, list.Select(t => t.State).ToArray(), token),
                    cancellationToken);
            }
            else
            {
                logger.LogDebug("No task state changes from pool/node information this time: PoolId: {PoolId}.", poolId);
            }
        }

        /// <summary>
        /// Deletes cloud tasks.
        /// </summary>
        /// <param name="tasks">Tasks to delete.</param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private async ValueTask ProcessDeletedTasks(IAsyncEnumerable<IBatchScheduler.CloudTaskId> tasks, CancellationToken cancellationToken)
        {
            await foreach (var taskResult in batchScheduler.DeleteCloudTasksAsync(tasks, cancellationToken).WithCancellation(cancellationToken))
            {
                try
                {
                    switch (await taskResult)
                    {
                        case true:
                            logger.LogDebug(@"Azure task {CloudTask} was deleted.", taskResult.Related.TaskId);
                            break;

                        case false:
                            logger.LogDebug(@"Azure task {CloudTask} was NOT deleted.", taskResult.Related.TaskId);
                            break;
                    }
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, @"Failed to delete azure task '{CloudTask}': '{ExceptionType}': '{ExceptionMessage}'", taskResult.Related.TaskId, exc.GetType().FullName, exc.Message);
                }
            }
        }

        /// <summary>
        /// Obtains <see cref="CloudTaskBatchTaskState"/> for tasks in specific conditions.
        /// </summary>
        /// <param name="pool"></param>
        /// <param name="now"></param>
        /// <param name="tasks"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async IAsyncEnumerable<CloudTaskBatchTaskState> GetCloudTaskStatesAsync(IBatchPool pool, DateTime now, IAsyncEnumerable<CloudTask> tasks, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            List<CloudTask> taskListWithComputeNodeInfo; // To check if the task was running when its node became preempted or unusable
            List<CloudTask> activeTaskList; // These are candidates to be the victim of resizes or starttask failures
            List<CloudTask> completedTaskList; // Backstop if events don't provide timely task completion information in a timely manner

            {
                var taskList = await tasks.ToListAsync(cancellationToken);
                taskListWithComputeNodeInfo = taskList.Where(TaskListWithComputeNodeInfoPredicate).ToList();
                activeTaskList = taskList.Where(ActiveTaskListPredicate).OrderByDescending(task => task.StateTransitionTime).ToList();
                completedTaskList = taskList.Where(task => CompletedTaskListPredicate(task, now)).ToList();
            }

            if (taskListWithComputeNodeInfo.Count > 0)
            {
                await foreach (var node in pool.ListLostComputeNodesAsync().WithCancellation(cancellationToken))
                {
                    foreach (var task in taskListWithComputeNodeInfo.Where(task => node.Id.Equals(task.ComputeNodeInformation.ComputeNodeId, StringComparison.InvariantCultureIgnoreCase)))
                    {
                        yield return new(task.Id, node.State switch
                        {
                            ComputeNodeState.Preempted => new(AzureBatchTaskState.TaskState.NodePreempted),
                            ComputeNodeState.Unusable => new(AzureBatchTaskState.TaskState.NodeFailedDuringStartupOrExecution, Failure: ParseComputeNodeErrors(node.Errors)),
                            _ => throw new System.Diagnostics.UnreachableException(),
                        });

                        _ = activeTaskList.Remove(task);
                    }
                }
            }

            await foreach (var state in activeTaskList.ToAsyncEnumerable().Zip(GetFailures(cancellationToken),
                    (cloud, state) => new CloudTaskBatchTaskState(cloud.Id, state))
                .WithCancellation(cancellationToken))
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
                var totalList = nodeErrors.Select(nodeError => Enumerable.Empty<string>().Append(nodeError.Code).Append(nodeError.Message)
                    .Concat(nodeError.ErrorDetails.Select(errorDetail => Enumerable.Empty<string>().Append(errorDetail.Name).Append(errorDetail.Value)).SelectMany(s => s)))
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

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
            async IAsyncEnumerable<AzureBatchTaskState> GetFailures([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
            {
                for (var failure = PopNextStartTaskFailure(); failure is not null; failure = PopNextStartTaskFailure())
                {
                    yield return ConvertFromStartTask(failure);
                    cancellationToken.ThrowIfCancellationRequested();
                }

                for (var failure = PopNextResizeError(); failure is not null; failure = PopNextResizeError())
                {
                    yield return ConvertFromResize(failure);
                    cancellationToken.ThrowIfCancellationRequested();
                }

                yield break;
            }

            AzureBatchTaskState ConvertFromResize(ResizeError failure)
                => new(AzureBatchTaskState.TaskState.NodeAllocationFailed, Failure: new(failure.Code, Enumerable.Empty<string>()
                    .Append(failure.Message)
                    .Concat(failure.Values.Select(t => t.Value))));

            AzureBatchTaskState ConvertFromStartTask(TaskFailureInformation failure)
                => new(AzureBatchTaskState.TaskState.NodeStartTaskFailed, Failure: new(failure.Code, Enumerable.Empty<string>()
                    .Append(failure.Message)
                    .Append($"Start task failed ({failure.Category})")
                    .Concat(failure.Details.Select(t => t.Value))));

            ResizeError PopNextResizeError()
                => pool.ResizeErrors.TryDequeue(out var resizeError) ? resizeError : default;

            TaskFailureInformation PopNextStartTaskFailure()
                => pool.StartTaskFailures.TryDequeue(out var failure) ? failure : default;

            AzureBatchTaskState GetCompletedBatchState(CloudTask task)
            {
                logger.LogDebug("Getting batch task state from completed task {TesTask}.", batchScheduler.GetTesTaskIdFromCloudTaskId(task.Id));
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
                            .Concat(task.ExecutionInformation.FailureInformation.Details.Select(pair => pair.Value))),
                        BatchTaskStartTime: task.ExecutionInformation.StartTime,
                        BatchTaskEndTime: task.ExecutionInformation.EndTime,
                        BatchTaskExitCode: task.ExecutionInformation.ExitCode),

                    _ => throw new System.Diagnostics.UnreachableException(),
                };
            }
        }
    }
}
