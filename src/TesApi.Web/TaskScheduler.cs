// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tes.Models;
using Tes.Repository;
using TesApi.Web.Events;
using TesApi.Web.Extensions;

namespace TesApi.Web
{
    /// <summary>
    /// An interface for scheduling <see cref="TesTask"/>s.
    /// </summary>
    public interface ITaskScheduler
    {

        /// <summary>
        /// Schedules a <see cref="TesTask"/>
        /// </summary>
        /// <param name="tesTask">A <see cref="TesTask"/> to schedule on the batch system.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        Task ProcessQueuedTesTaskAsync(TesTask tesTask, CancellationToken cancellationToken);

        /// <summary>
        /// Updates <see cref="TesTask"/>s with task-related state
        /// </summary>
        /// <param name="tesTasks"><see cref="TesTask"/>s to schedule on the batch system.</param>
        /// <param name="taskStates"><see cref="AzureBatchTaskState"/>s corresponding to each <seealso cref="TesTask"/>.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>True for each corresponding <see cref="TesTask"/> that needs to be persisted.</returns>
        IAsyncEnumerable<RelatedTask<TesTask, bool>> ProcessTesTaskBatchStatesAsync(IEnumerable<TesTask> tesTasks, AzureBatchTaskState[] taskStates, CancellationToken cancellationToken);
    }

    /// <summary>
    /// A background service that schedules <see cref="TesTask"/>s in the batch system, orchestrates their lifecycle, and updates their state.
    /// This should only be used as a system-wide singleton service.  This class does not support scale-out on multiple machines,
    /// nor does it implement a leasing mechanism.  In the future, consider using the Lease Blob operation.
    /// </summary>
    /// <param name="nodeEventProcessor">The task node event processor.</param>
    /// <param name="hostApplicationLifetime">Used for requesting termination of the current application during initialization.</param>
    /// <param name="repository">The main TES task database repository implementation.</param>
    /// <param name="batchScheduler">The batch scheduler implementation.</param>
    /// <param name="taskSchedulerLogger">The logger instance.</param>
    internal class TaskScheduler(RunnerEventsProcessor nodeEventProcessor, Microsoft.Extensions.Hosting.IHostApplicationLifetime hostApplicationLifetime, IRepository<TesTask> repository, IBatchScheduler batchScheduler, ILogger<TaskScheduler> taskSchedulerLogger)
        : OrchestrateOnBatchSchedulerServiceBase(hostApplicationLifetime, repository, batchScheduler, taskSchedulerLogger)
        , ITaskScheduler
    {
        private static readonly TimeSpan blobRunInterval = TimeSpan.FromSeconds(15);
        internal static readonly TimeSpan BatchRunInterval = TimeSpan.FromSeconds(30); // The very fastest process inside of Azure Batch accessing anything within pools or jobs appears to use a 30 second polling interval
        private static readonly TimeSpan shortBackgroundRunInterval = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan longBackgroundRunInterval = TimeSpan.FromSeconds(1);
        private readonly RunnerEventsProcessor nodeEventProcessor = nodeEventProcessor;

        /// <summary>
        /// Checks to see if the hosted service is running.
        /// </summary>
        /// <value>False if the service hasn't started up yet, True if it has started, throws TaskCanceledException if service is/has shutdown.</value>
        private bool IsRunning => stoppingToken is not null && (stoppingToken.Value.IsCancellationRequested ? throw new TaskCanceledException() : true);

        private CancellationToken? stoppingToken = null;
        private readonly ConcurrentQueue<TesTask> queuedTesTasks = [];
        private readonly ConcurrentQueue<(TesTask[] TesTasks, AzureBatchTaskState[] TaskStates, ChannelWriter<RelatedTask<TesTask, bool>> Channel)> tesTaskBatchStates = [];

        /// <inheritdoc />
        protected override async ValueTask ExecuteSetupAsync(CancellationToken cancellationToken)
        {
            try
            {
                // Delay "starting" TaskScheduler until this completes to finish initializing BatchScheduler.
                await BatchScheduler.UploadTaskRunnerIfNeededAsync(cancellationToken);
                // Ensure BatchScheduler has loaded existing pools before "starting".
                //await BatchScheduler.LoadExistingPoolsAsync(cancellationToken);
            }
            catch (Exception exc)
            {
                Logger.LogError(exc, @"Checking/storing the node task runner binary failed with {Message}", exc.Message);
                throw;
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            Logger.LogDebug(@"Querying active tasks");

            foreach (var tesTask in
                (await Repository.GetItemsAsync(
                    predicate: t => t.IsActiveState(false), // TODO: preemptedIsTerminal
                    cancellationToken: cancellationToken))
                .OrderBy(t => t.CreationTime))
            {
                try
                {
                    if (TesState.QUEUED.Equals(tesTask.State) && string.IsNullOrWhiteSpace(tesTask.PoolId))
                    {
                        Logger.LogDebug(@"Adding queued task from repository");
                        queuedTesTasks.Enqueue(tesTask);
                    }
                    else
                    {
                        var pool = BatchScheduler.GetPools().SingleOrDefault(pool => tesTask.PoolId.Equals(pool.PoolId, StringComparison.OrdinalIgnoreCase));

                        if (pool is null)
                        {
                            Logger.LogDebug(@"Adding task w/o pool id from repository");
                            queuedTesTasks.Enqueue(tesTask); // TODO: is there a better way to treat tasks that are not "queued" that are also not associated with any known pool?
                        }
                        else
                        {
                            Logger.LogDebug(@"Adding task to pool w/o cloudtask");
                            _ = pool.AssociatedTesTasks.AddOrUpdate(tesTask.Id, key => null, (key, value) => value);
                        }
                    }
                }
                catch (Exception ex)
                {
                    await ProcessOrchestratedTesTaskAsync("Initialization", new(Task.FromException<bool>(ex), tesTask), cancellationToken);
                }
            }

            Logger.LogDebug(@"Active tasks processed");
        }

        /// <inheritdoc />
        protected override async ValueTask ExecuteCoreAsync(CancellationToken cancellationToken)
        {
            stoppingToken = cancellationToken;
            List<Task> queuedTasks = [];

            while (!cancellationToken.IsCancellationRequested && queuedTesTasks.TryDequeue(out var tesTask))
            {
                queuedTasks.Add(((ITaskScheduler)this).ProcessQueuedTesTaskAsync(tesTask, cancellationToken));
            }

            while (!cancellationToken.IsCancellationRequested && tesTaskBatchStates.TryDequeue(out var result))
            {
                queuedTasks.Add(ProcessQueuedTesTaskStatesRequestAsync(result.TesTasks, result.TaskStates, result.Channel, cancellationToken));
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            queuedTasks.Add(ExecuteShortBackgroundTasksAsync(cancellationToken));
            queuedTasks.Add(ExecuteLongBackgroundTasksAsync(cancellationToken));
            queuedTasks.Add(ExecuteCancelledTesTasksOnBatchAsync(cancellationToken));
            queuedTasks.Add(ExecuteUpdateTesTaskFromEventBlobAsync(cancellationToken));

            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            Logger.LogDebug(@"Task load: {TaskCount}", queuedTasks.Count);
            await Task.WhenAll(queuedTasks);
        }

        private async Task ProcessQueuedTesTaskStatesRequestAsync(TesTask[] tesTasks, AzureBatchTaskState[] taskStates, ChannelWriter<RelatedTask<TesTask, bool>> channel, CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var relatedTask in ((ITaskScheduler)this).ProcessTesTaskBatchStatesAsync(tesTasks, taskStates, cancellationToken))
                {
                    await channel.WriteAsync(relatedTask, cancellationToken);
                }

                channel.Complete();
            }
            catch (Exception ex)
            {
                channel.Complete(ex);
            }
        }

        /// <summary>
        /// Retrieves all event blobs from storage and updates the resultant state.
        /// </summary>
        /// <param name="cancellationToken">Triggered when Microsoft.Extensions.Hosting.IHostedService.StopAsync(System.Threading.CancellationToken) is called.</param>
        /// <returns></returns>
        private Task ExecuteShortBackgroundTasksAsync(CancellationToken cancellationToken)
        {
            return ExecuteActionOnIntervalAsync(shortBackgroundRunInterval, BatchScheduler.PerformShortBackgroundTasksAsync, cancellationToken);
        }

        /// <summary>
        /// Retrieves all event blobs from storage and updates the resultant state.
        /// </summary>
        /// <param name="cancellationToken">Triggered when Microsoft.Extensions.Hosting.IHostedService.StopAsync(System.Threading.CancellationToken) is called.</param>
        /// <returns></returns>
        private async Task ExecuteLongBackgroundTasksAsync(CancellationToken cancellationToken)
        {
            await ExecuteActionOnIntervalAsync(longBackgroundRunInterval,
                async token => await Task.WhenAll(BatchScheduler.PerformLongBackgroundTasksAsync(token).ToBlockingEnumerable(token)),
                cancellationToken);
        }

        /// <summary>
        /// Retrieves all cancelled TES tasks from the database, performs an action in the batch system, and updates the resultant state
        /// </summary>
        /// <param name="cancellationToken">Triggered when Microsoft.Extensions.Hosting.IHostedService.StopAsync(System.Threading.CancellationToken) is called.</param>
        /// <returns></returns>
        private Task ExecuteCancelledTesTasksOnBatchAsync(CancellationToken cancellationToken)
        {
            Func<CancellationToken, ValueTask<IAsyncEnumerable<TesTask>>> query = new(
                async token => (await Repository.GetItemsAsync(
                    predicate: t => t.State == TesState.CANCELING,
                    cancellationToken: token))
                .OrderByDescending(t => t.CreationTime)
                .ToAsyncEnumerable());

            return ExecuteActionOnIntervalAsync(BatchRunInterval,
                token => OrchestrateTesTasksOnBatchAsync(
                    "Cancelled",
                    query,
                    (tasks, ct) => ((ITaskScheduler)this).ProcessTesTaskBatchStatesAsync(
                        tasks,
                        Enumerable.Repeat<AzureBatchTaskState>(new(AzureBatchTaskState.TaskState.CancellationRequested), tasks.Length).ToArray(),
                        ct),
                    token),
                cancellationToken);
        }

        /// <summary>
        /// Retrieves all event blobs from storage and updates the resultant state.
        /// </summary>
        /// <param name="cancellationToken">Triggered when Microsoft.Extensions.Hosting.IHostedService.StopAsync(System.Threading.CancellationToken) is called.</param>
        /// <returns></returns>
        private Task ExecuteUpdateTesTaskFromEventBlobAsync(CancellationToken cancellationToken)
        {
            return ExecuteActionOnIntervalAsync(blobRunInterval,
                async token =>
                    await UpdateTesTasksFromAvailableEventsAsync(
                        await ParseAvailableEvents(token),
                        token),
                cancellationToken);
        }

        /// <summary>
        /// Determines the <see cref="AzureBatchTaskState"/>s from each event available for processing and their associated <see cref="TesTask"/>s.
        /// </summary>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns><see cref="TesTask"/>s and <see cref="AzureBatchTaskState"/>s from all events.</returns>
        private async ValueTask<IEnumerable<(TesTask Task, AzureBatchTaskState State, Func<CancellationToken, Task> MarkProcessedAsync)>> ParseAvailableEvents(CancellationToken cancellationToken)
        {
            var messages = new ConcurrentBag<(RunnerEventsMessage Message, TesTask Task, AzureBatchTaskState State, Func<CancellationToken, Task> MarkProcessedAsync)>();

            // Get and parse event blobs
            await Parallel.ForEachAsync(BatchScheduler.GetEventMessagesAsync(cancellationToken), cancellationToken, async (eventMessage, token) =>
            {
                var tesTask = await GetTesTaskAsync(eventMessage.Tags["task-id"], eventMessage.Tags["event-name"]);

                if (tesTask is null)
                {
                    return;
                }

                try
                {
                    nodeEventProcessor.ValidateMessageMetadata(eventMessage);
                    eventMessage = await nodeEventProcessor.DownloadAndValidateMessageContentAsync(eventMessage, token);
                    var state = await nodeEventProcessor.GetMessageBatchStateAsync(eventMessage, tesTask, token);
                    messages.Add((eventMessage, tesTask, state, ct => nodeEventProcessor.MarkMessageProcessedAsync(eventMessage, ct)));
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    throw;
                }
                catch (ArgumentException ex)
                {
                    Logger.LogError(ex, @"Verifying event metadata failed: {ErrorMessage}", ex.Message);

                    messages.Add((
                        eventMessage,
                        tesTask,
                        new(AzureBatchTaskState.TaskState.InfoUpdate, Warning:
                        [
                            "EventParsingFailed",
                            $"{ex.GetType().FullName}: {ex.Message}"
                        ]),
                        ct => nodeEventProcessor.RemoveMessageFromReattemptsAsync(eventMessage, ct)));
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, @"Downloading and parsing event failed: {ErrorMessage}", ex.Message);

                    messages.Add((
                        eventMessage,
                        tesTask,
                        new(AzureBatchTaskState.TaskState.InfoUpdate, Warning:
                        [
                            "EventParsingFailed",
                            $"{ex.GetType().FullName}: {ex.Message}"
                        ]),
                        (ex is System.Diagnostics.UnreachableException || ex is RunnerEventsProcessor.DownloadOrParseException)
                            ? ct => nodeEventProcessor.MarkMessageProcessedAsync(eventMessage, ct) // Mark event processed to prevent retries
                            : default));  // Retry this event.
                }

                // Helpers
                async ValueTask<TesTask> GetTesTaskAsync(string id, string @event)
                {
                    TesTask tesTask = default;
                    if (await Repository.TryGetItemAsync(id, token, task => tesTask = task) && tesTask is not null)
                    {
                        Logger.LogDebug("Completing event '{TaskEvent}' for task {TesTask}.", @event, tesTask.Id);
                        return tesTask;
                    }
                    else
                    {
                        Logger.LogDebug("Could not find task {TesTask} for event '{TaskEvent}'.", id, @event);
                        return null;
                    }
                }
            });

            return nodeEventProcessor.OrderProcessedByExecutorSequence(messages, @event => @event.Message).Select(@event => (@event.Task, @event.State, @event.MarkProcessedAsync));
        }

        /// <summary>
        /// Updates each task based on the provided state.
        /// </summary>
        /// <param name="eventStates">A collection of associated <see cref="TesTask"/>s, <see cref="AzureBatchTaskState"/>s, and a method to mark the source event processed.</param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private async ValueTask UpdateTesTasksFromAvailableEventsAsync(IEnumerable<(TesTask Task, AzureBatchTaskState State, Func<CancellationToken, Task> MarkProcessedAsync)> eventStates, CancellationToken cancellationToken)
        {
            eventStates = eventStates.ToList();

            if (!eventStates.Any())
            {
                return;
            }

            // Update TesTasks
            await OrchestrateTesTasksOnBatchAsync(
                "NodeEvent",
                _ => ValueTask.FromResult(eventStates.Select(@event => @event.Task).ToAsyncEnumerable()),
                (tesTasks, token) => ((ITaskScheduler)this).ProcessTesTaskBatchStatesAsync(tesTasks, eventStates.Select(@event => @event.State).ToArray(), token),
                cancellationToken,
                "events");

            await Parallel.ForEachAsync(eventStates.Select(@event => @event.MarkProcessedAsync).Where(func => func is not null), cancellationToken, async (markEventProcessed, token) =>
            {
                try
                {
                    await markEventProcessed(token);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, @"Failed to tag event as processed.");
                }
            });
        }

        /// <inheritdoc/>
        async Task ITaskScheduler.ProcessQueuedTesTaskAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            if (IsRunning)
            {
                await ProcessOrchestratedTesTaskAsync("Queued", new(BatchScheduler.ProcessQueuedTesTaskAsync(tesTask, cancellationToken), tesTask), cancellationToken);
            }
            else
            {
                queuedTesTasks.Enqueue(tesTask);
            }
        }

        /// <inheritdoc/>
        IAsyncEnumerable<RelatedTask<TesTask, bool>> ITaskScheduler.ProcessTesTaskBatchStatesAsync(IEnumerable<TesTask> tesTasks, AzureBatchTaskState[] taskStates, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(tesTasks);
            ArgumentNullException.ThrowIfNull(taskStates);

            if (IsRunning)
            {
                return taskStates.Zip(tesTasks, (TaskState, TesTask) => (TaskState, TesTask))
                    .Select(entry => new RelatedTask<TesTask, bool>(entry.TesTask?.IsActiveState() ?? false // Removes already terminal (and null) TesTasks from being further processed.
                        ? WrapHandleTesTaskTransitionAsync(entry.TesTask, entry.TaskState, cancellationToken)
                        : Task.FromResult(false), entry.TesTask))
                    .WhenEach(cancellationToken, tesTaskTask => tesTaskTask.Task);

                async Task<bool> WrapHandleTesTaskTransitionAsync(TesTask tesTask, AzureBatchTaskState azureBatchTaskState, CancellationToken cancellationToken)
                    => await BatchScheduler.ProcessTesTaskBatchStateAsync(tesTask, azureBatchTaskState, cancellationToken);
            }
            else
            {
                var channel = Channel.CreateBounded<RelatedTask<TesTask, bool>>(new BoundedChannelOptions(taskStates.Length) { SingleReader = true, SingleWriter = true });
                tesTaskBatchStates.Enqueue((tesTasks.ToArray(), taskStates, channel.Writer));
                return channel.Reader.ReadAllAsync(cancellationToken);
            }
        }
    }
}
