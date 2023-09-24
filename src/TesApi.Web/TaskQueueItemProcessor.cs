// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using Tes.Models;
using Tes.Repository;
using TesApi.Controllers;

namespace TesApi.Web
{
    public interface ITaskQueueItemProcessor
    {
        Task UpdateTaskQueueItemAsync(TaskQueueItemBasic item, CancellationToken cancellationToken);
        bool TryDequeue(string poolId, out TaskQueueItemFull item);
        bool ContainsKey(string id);
    }

    public class TaskQueueItemProcessor : BackgroundService, ITaskQueueItemProcessor
    {
        private readonly TimeSpan runInterval = TimeSpan.FromSeconds(4);
        private readonly ILogger<TaskQueueItemsApiController> logger;
        private readonly IRetryPolicyProvider retryPolicyProvider;
        private readonly TaskCompletionSource<bool> initializationTcs = new TaskCompletionSource<bool>();
        private readonly AsyncRetryPolicy initializationRetryPolicy;
        private readonly IRepository<TesTask> repository;
        private ConcurrentDictionary<string, ConcurrentQueue<TaskQueueItemFull>> taskQueues;  // Created once during initialization
        private ConcurrentDictionary<string, TaskQueueItemFull> taskItemDictionary = new ConcurrentDictionary<string, TaskQueueItemFull>();

        public TaskQueueItemProcessor(IRepository<TesTask> repository, ILogger<TaskQueueItemsApiController> logger, IRetryPolicyProvider retryPolicyProvider)
        {
            this.repository = repository;
            this.logger = logger;
            this.retryPolicyProvider = retryPolicyProvider;
            this.initializationRetryPolicy = retryPolicyProvider.CreateDefaultCriticalServiceRetryPolicy();
        }

        public async Task InitializeAsync(CancellationToken stoppingToken)
        {
            var tesTasks = (await repository.GetItemsAsync(
                    predicate: t => t.State == TesState.QUEUEDEnum
                        || t.State == TesState.INITIALIZINGEnum
                        || t.State == TesState.RUNNINGEnum
                        || (t.State == TesState.CANCELEDEnum && t.IsCancelRequested),
                    cancellationToken: stoppingToken))
                .OrderBy(t => t.CreationTime)
                .ToList();

            // 1.  Create new TaskQueueItems for each TesTask that is QUEUED
            var newTaskQueueItems = new List<TaskQueueItemFull>();
            var queuedTesTasks = new List<TesTask>();

            foreach (var task in tesTasks.Where(t => t.State == TesState.QUEUEDEnum))
            {
                var taskQueueItem = TaskQueueItemFull.CreateNew(task);
                task.TaskQueueItemId = taskQueueItem.Id;
                queuedTesTasks.Add(task);
            }

            // 2 Update database first for atomicity
            // TODO implement
            // Only need to update the TaskQueueItemId of each TesTask, not a full JSON update
            await repository.UpdateItemsAsync(queuedTesTasks, stoppingToken);

            // 3. Create ConcurrentQueue and ConcurrentDictionary
            foreach (var poolId in tesTasks.Select(t => t.PoolId).Distinct())
            {
                taskQueues.TryAdd(poolId, new ConcurrentQueue<TaskQueueItemFull>(newTaskQueueItems));
            }
            
            // 3a.  Add new TaskQueueItems to the ConcurrentDictionary
            foreach (var taskQueueItem in newTaskQueueItems)
            {
                taskItemDictionary.TryAdd(taskQueueItem.Id, taskQueueItem);
            }

            // 3b.  Add existing TaskQueueItems to the ConcurrentDictionary
            foreach (var task in tesTasks.Where(t => t.State != TesState.QUEUEDEnum))
            {
                TaskQueueItemFull taskQueueItem;

                if (!string.IsNullOrWhiteSpace(task.TaskQueueItemId))
                {
                    taskQueueItem = TaskQueueItemFull.CreateFromExisting(task);
                }
                else
                {
                    // Happens when upgrading TES from an earlier version during task runtime
                    logger.LogWarning("TaskQueueItem ID is null or whitespace for TesTask {TesTaskId}. This is only expected immediately after an upgrade if tasks were running.", task.Id);
                    taskQueueItem = TaskQueueItemFull.CreateNew(task);
                }

                taskItemDictionary.TryAdd(taskQueueItem.Id, taskQueueItem);
            }

            initializationTcs.SetResult(true);
        }

        public Task EnsureInitializedAsync()
        {
            return initializationTcs.Task;
        }

        public async Task UpdateTaskQueueItemAsync(TaskQueueItemBasic item, CancellationToken cancellationToken)
        {
            if (!initializationTcs.Task.IsCompletedSuccessfully)
            {
                throw new InvalidOperationException("TaskQueueItemProcessor is not initialized.");
            }

            if (taskItemDictionary.TryGetValue(item.Id, out TaskQueueItemFull existingItem))
            {
                switch (item.State)
                {
                    case TaskQueueItemState.Running:
                        {
                            existingItem.State = TaskQueueItemState.Running;
                            existingItem.LastPing = DateTime.UtcNow;
                            break;
                        }
                    case TaskQueueItemState.Complete:
                    case TaskQueueItemState.Failed:
                        // TODO - only update state
                        await repository.UpdateItemAsync(existingItem.TesTask, cancellationToken);
                        taskItemDictionary.TryRemove(item.Id, out _);
                        break;
                    default:
                        throw new Exception($"Unexpected TaskQueueItemState {item.State}");
                }
            }
        }

        public bool TryDequeue(string poolId, out TaskQueueItemFull item)
        {
            if (!initializationTcs.Task.IsCompletedSuccessfully)
            {
                throw new InvalidOperationException("TaskQueueItemProcessor is not initialized.");
            }

            ConcurrentQueue<TaskQueueItemFull> taskQueue;

            if (!taskQueues.TryGetValue(poolId, out taskQueue))
            {
                string msg = $"Task queue with poolId {poolId} not found";
                logger.LogError(msg);
                throw new Exception(msg);
            }

            return taskQueue.TryDequeue(out item);
        }

        public bool ContainsKey(string id)
        {
            if (!initializationTcs.Task.IsCompletedSuccessfully)
            {
                throw new InvalidOperationException("TaskQueueItemProcessor is not initialized.");
            }

            return taskItemDictionary.ContainsKey(id);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // 1.  Attempt to initialize; if it fails, restart the application
            var initializeResult = await initializationRetryPolicy.ExecuteAndCaptureAsync(t => InitializeAsync(stoppingToken), stoppingToken);

            if (initializeResult.Outcome == OutcomeType.Failure)
            {
                const string msg = "Could not initialize the TaskQueueItemProcessor. Is the database available?";
                logger.LogCritical(msg, initializeResult.FinalException);
                initializationTcs.SetException(initializeResult.FinalException);
                throw new Exception(msg, initializeResult.FinalException);
            }

            // (note: clients are pinging every 30s)
            // 2.  Give clients 90s to ping before requeuing tasks
            var pingTimeout = TimeSpan.FromMinutes(1);
            await Task.Delay(TimeSpan.FromSeconds(90), stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // 3.  Requeue dead tasks (i.e. a VM disappeared/crashed)
                    var utcNow = DateTime.UtcNow;
                    var unresponsiveNodes = taskItemDictionary.Where((x, y) => (utcNow - x.Value.LastPing) > pingTimeout).ToList();

                    foreach (var node in unresponsiveNodes)
                    {
                        // TODO check Storage Account to see if it exited gracefully with executor error (but TES was down)
                        // TODO if it did NOT exit gracefully, requeue it, but after 3 tries, permanently fail with SYSTEM ERROR
                        logger.LogError("Task {TesTaskId} either unresponsive or failed with SYSTEM_ERROR", node.Value.TesTask.Id);
                        taskItemDictionary.TryRemove(node.Key, out _);
                        // Queue a new item
                        var taskQueueItem = TaskQueueItemFull.CreateNew(node.Value.TesTask);
                        taskQueueItem.TesTask.TaskQueueItemId = taskQueueItem.Id;

                        // TODO - update the TaskQueueItemId of the TesTask and State back to QUEUED, not a full JSON update
                        await repository.UpdateStateAndTaskQueueItemIdAsync(node.Value.TesTask.Id, TesState.QUEUEDEnum, taskQueueItem.Id, stoppingToken);

                        ConcurrentQueue<TaskQueueItemFull> taskQueue;

                        if (!taskQueues.TryGetValue(taskQueueItem.TesTask.PoolId, out taskQueue))
                        {
                            logger.LogError($"Pool with ID {taskQueueItem.TesTask.PoolId} not found");
                            continue;
                        }

                        taskQueue.Enqueue(taskQueueItem);
                    }

                    // Process failed tasks (reported by clients as EXECUTORERROREnum)
                    var completeTasks = taskItemDictionary.Where((x, y) => x.Value.State == TaskQueueItemState.Failed || x.Value.State == TaskQueueItemState.Complete).ToList();

                    foreach (var task in completeTasks)
                    {
                        switch (task.Value.State)
                        {
                            case TaskQueueItemState.Complete:
                                logger.LogInformation("Task {TesTaskId} completed successfully", task.Value.TesTask.Id);
                                await repository.UpdateStateAndTaskQueueItemIdAsync(task.Value.TesTask.Id, TesState.COMPLETEEnum, null, stoppingToken);
                                break;
                            case TaskQueueItemState.Failed:
                                logger.LogError("Task {TesTaskId} failed", task.Value.TesTask.Id);
                                // TODO add more details to update as needed
                                await repository.UpdateStateAndTaskQueueItemIdAsync(task.Value.TesTask.Id, TesState.EXECUTORERROREnum, null, stoppingToken);
                                break;
                            default:
                                throw new Exception($"Unexpected TaskQueueItemState {task.Value.State}");
                        }

                        taskItemDictionary.TryRemove(task.Key, out _);
                    }

                    var queuedTasks = taskItemDictionary.Where((x, y) => x.Value.State == TaskQueueItemState.Queued).ToList();
                    // 4.  TODO Get PoolID from?  Add CloudTasks as needed to each pool (including creating pools and jobs)
                    //  TODOSet to INITIALIZED for each task
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, "Error in TaskQueueItemProcessor");
                }

                await Task.Delay(runInterval, stoppingToken);
            }
        }
    }
}
