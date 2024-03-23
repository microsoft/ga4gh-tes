// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tes.Models;
using Tes.Repository;
using TesApi.Web.Events;

namespace TesApi.Web
{
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
    {
        private readonly TimeSpan blobRunInterval = TimeSpan.FromSeconds(5);
        private readonly TimeSpan batchRunInterval = TimeSpan.FromSeconds(30); // The very fastest process inside of Azure Batch accessing anything within pools or jobs uses a 30 second polling interval
        private readonly RunnerEventsProcessor nodeEventProcessor = nodeEventProcessor;

        /// <inheritdoc />
        protected override async ValueTask ExecuteSetupAsync(CancellationToken cancellationToken)
        {
            try
            {
                // Delay "starting" TaskScheduler until this completes to finish initializing BatchScheduler.
                await BatchScheduler.UploadTaskRunnerIfNeededAsync(cancellationToken);
            }
            catch (Exception exc)
            {
                Logger.LogError(exc, @"Checking/storing the node task runner binary failed with {Message}", exc.Message);
                throw;
            }
        }

        /// <inheritdoc />
        protected override async ValueTask ExecuteCoreAsync(CancellationToken cancellationToken)
        {
            await Task.WhenAll(
                ExecuteCancelledTesTasksOnBatchAsync(cancellationToken),
                ExecuteQueuedTesTasksOnBatchAsync(cancellationToken),
                ExecuteUpdateTesTaskFromEventBlobAsync(cancellationToken));
        }

        /// <summary>
        /// Retrieves all queued TES tasks from the database, performs an action in the batch system, and updates the resultant state
        /// </summary>
        /// <returns></returns>
        private async Task ExecuteQueuedTesTasksOnBatchAsync(CancellationToken cancellationToken)
        {
            var query = new Func<CancellationToken, ValueTask<IAsyncEnumerable<TesTask>>>(
                async token => (await Repository.GetItemsAsync(
                    predicate: t => t.State == TesState.QUEUEDEnum,
                    cancellationToken: token))
                .OrderBy(t => t.CreationTime)
                .ToAsyncEnumerable());

            await ExecuteActionOnIntervalAsync(batchRunInterval,
                token => OrchestrateTesTasksOnBatchAsync("Queued", query, BatchScheduler.ProcessQueuedTesTasksAsync, token),
                cancellationToken);
        }

        /// <summary>
        /// Retrieves all cancelled TES tasks from the database, performs an action in the batch system, and updates the resultant state
        /// </summary>
        /// <param name="cancellationToken">Triggered when Microsoft.Extensions.Hosting.IHostedService.StopAsync(System.Threading.CancellationToken) is called.</param>
        /// <returns></returns>
        private async Task ExecuteCancelledTesTasksOnBatchAsync(CancellationToken cancellationToken)
        {
            var query = new Func<CancellationToken, ValueTask<IAsyncEnumerable<TesTask>>>(
                async token => (await Repository.GetItemsAsync(
                    predicate: t => t.State == TesState.CANCELINGEnum,
                    cancellationToken: token))
                .OrderByDescending(t => t.CreationTime)
                .ToAsyncEnumerable());

            await ExecuteActionOnIntervalAsync(batchRunInterval,
                token => OrchestrateTesTasksOnBatchAsync(
                    "Cancelled",
                    query,
                    (tasks, ct) => BatchScheduler.ProcessTesTaskBatchStatesAsync(
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
        private async Task ExecuteUpdateTesTaskFromEventBlobAsync(CancellationToken cancellationToken)
        {
            await ExecuteActionOnIntervalAsync(blobRunInterval,
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
                (tesTasks, token) => BatchScheduler.ProcessTesTaskBatchStatesAsync(tesTasks, eventStates.Select(@event => @event.State).ToArray(), token),
                cancellationToken,
                "events");

            await Parallel.ForEachAsync(eventStates.Select(@event => @event.MarkProcessedAsync).Where(func => func is not null), cancellationToken, async (markEventProcessed, token) =>
            {
                try
                {
                    await markEventProcessed(token);
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, @"Failed to tag event as processed.");
                }
            });
        }
    }
}
