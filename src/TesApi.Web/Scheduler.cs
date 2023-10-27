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
    /// A background service that schedules TES tasks in the batch system, orchestrates their lifecycle, and updates their state.
    /// This should only be used as a system-wide singleton service.  This class does not support scale-out on multiple machines,
    /// nor does it implement a leasing mechanism.  In the future, consider using the Lease Blob operation.
    /// </summary>
    internal class Scheduler : OrchestrateOnBatchSchedulerServiceBase
    {
        private readonly TimeSpan blobRunInterval = TimeSpan.FromSeconds(5);
        private readonly TimeSpan batchRunInterval = TimeSpan.FromSeconds(30); // The very fastest process inside of Azure Batch accessing anything within pools or jobs uses a 30 second polling interval
        private readonly RunnerEventsProcessor nodeEventProcessor;

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="nodeEventProcessor">The task node event processor.</param>
        /// <param name="hostApplicationLifetime">Used for requesting termination of the current application during initialization.</param>
        /// <param name="repository">The main TES task database repository implementation</param>
        /// <param name="batchScheduler">The batch scheduler implementation</param>
        /// <param name="logger">The logger instance</param>
        public Scheduler(RunnerEventsProcessor nodeEventProcessor, Microsoft.Extensions.Hosting.IHostApplicationLifetime hostApplicationLifetime, IRepository<TesTask> repository, IBatchScheduler batchScheduler, ILogger<Scheduler> logger)
            : base(hostApplicationLifetime, repository, batchScheduler, logger)
        {
            this.nodeEventProcessor = nodeEventProcessor;
        }


        /// <summary>
        /// The main thread that continuously schedules TES tasks in the batch system
        /// </summary>
        /// <param name="stoppingToken">Triggered when Microsoft.Extensions.Hosting.IHostedService.StopAsync(System.Threading.CancellationToken) is called.</param>
        /// <returns>A System.Threading.Tasks.Task that represents the long running operations.</returns>
        protected override async Task ExecuteSetupAsync(CancellationToken stoppingToken)
        {
            try
            {
                // Delay "starting" Scheduler until this completes to finish initializing BatchScheduler.
                await batchScheduler.UploadTaskRunnerIfNeeded(stoppingToken);
            }
            catch (Exception exc)
            {
                logger.LogError(exc, @"Checking/storing the node task runner binary failed with {Message}", exc.Message);
                throw;
            }
        }

        /// <summary>
        /// The main thread that continuously schedules TES tasks in the batch system
        /// </summary>
        /// <param name="stoppingToken">Triggered when Microsoft.Extensions.Hosting.IHostedService.StopAsync(System.Threading.CancellationToken) is called.</param>
        /// <returns>A System.Threading.Tasks.Task that represents the long running operations.</returns>
        protected override Task ExecuteCoreAsync(CancellationToken stoppingToken)
        {
            return Task.WhenAll(
                ExecuteCancelledTesTasksOnBatchAsync(stoppingToken),
                ExecuteQueuedTesTasksOnBatchAsync(stoppingToken),
                ExecuteTerminatedTesTasksOnBatchAsync(stoppingToken),
                ExecuteUpdateTesTaskFromEventBlobAsync(stoppingToken));
        }

        /// <summary>
        /// Retrieves all queued TES tasks from the database, performs an action in the batch system, and updates the resultant state
        /// </summary>
        /// <returns></returns>
        private Task ExecuteQueuedTesTasksOnBatchAsync(CancellationToken stoppingToken)
        {
            var query = new Func<CancellationToken, ValueTask<IAsyncEnumerable<TesTask>>>(
                async cancellationToken => (await repository.GetItemsAsync(
                    predicate: t => t.State == TesState.QUEUEDEnum,
                    cancellationToken: cancellationToken))
                .OrderBy(t => t.CreationTime)
                .ToAsyncEnumerable());

            return ExecuteActionOnIntervalAsync(batchRunInterval,
                cancellationToken => OrchestrateTesTasksOnBatchAsync("Queued", query, batchScheduler.ProcessQueuedTesTasksAsync, cancellationToken),
                stoppingToken);
        }

        /// <summary>
        /// Retrieves all cancelled TES tasks from the database, performs an action in the batch system, and updates the resultant state
        /// </summary>
        /// <param name="stoppingToken">Triggered when Microsoft.Extensions.Hosting.IHostedService.StopAsync(System.Threading.CancellationToken) is called.</param>
        /// <returns></returns>
        private Task ExecuteCancelledTesTasksOnBatchAsync(CancellationToken stoppingToken)
        {
            var query = new Func<CancellationToken, ValueTask<IAsyncEnumerable<TesTask>>>(
                async cancellationToken => (await repository.GetItemsAsync(
                    predicate: t => t.State == TesState.CANCELINGEnum,
                    cancellationToken: cancellationToken))
                .OrderBy(t => t.CreationTime)
                .ToAsyncEnumerable());

            return ExecuteActionOnIntervalAsync(batchRunInterval,
                cancellationToken => OrchestrateTesTasksOnBatchAsync(
                    "Cancelled",
                    query,
                    (tasks, cancellationToken) => batchScheduler.ProcessTesTaskBatchStatesAsync(
                        tasks,
                        Enumerable.Repeat<AzureBatchTaskState>(new(AzureBatchTaskState.TaskState.CancellationRequested), tasks.Length).ToArray(),
                        cancellationToken),
                    cancellationToken),
                stoppingToken);
        }

        /// <summary>
        /// Retrieves all terminated TES tasks from the database, performs an action in the batch system, and updates the resultant state
        /// </summary>
        /// <param name="stoppingToken">Triggered when Microsoft.Extensions.Hosting.IHostedService.StopAsync(System.Threading.CancellationToken) is called.</param>
        /// <returns></returns>
        private Task ExecuteTerminatedTesTasksOnBatchAsync(CancellationToken stoppingToken)
        {
            var query = new Func<CancellationToken, ValueTask<IAsyncEnumerable<TesTask>>>(
                async cancellationToken => (await repository.GetItemsAsync(
                    predicate: t => t.IsTaskDeletionRequired,
                    cancellationToken: cancellationToken))
                .OrderBy(t => t.CreationTime)
                .ToAsyncEnumerable());

            return ExecuteActionOnIntervalAsync(batchRunInterval,
                cancellationToken => OrchestrateTesTasksOnBatchAsync(
                    "Terminated",
                    query,
                    (tasks, cancellationToken) => batchScheduler.ProcessTesTaskBatchStatesAsync(
                        tasks,
                        Enumerable.Repeat<AzureBatchTaskState>(new(AzureBatchTaskState.TaskState.CancellationRequested), tasks.Length).ToArray(),
                        cancellationToken),
                    cancellationToken),
                stoppingToken);
        }

        /// <summary>
        /// Retrieves all event blobs from storage and updates the resultant state.
        /// </summary>
        /// <param name="stoppingToken">Triggered when Microsoft.Extensions.Hosting.IHostedService.StopAsync(System.Threading.CancellationToken) is called.</param>
        /// <returns></returns>
        private Task ExecuteUpdateTesTaskFromEventBlobAsync(CancellationToken stoppingToken)
        {
            return ExecuteActionOnIntervalAsync(blobRunInterval,
                UpdateTesTasksFromEventBlobsAsync,
                stoppingToken);
        }

        /// <summary>
        /// Retrieves all event blobs from storage and updates the resultant state.
        /// </summary>
        /// <param name="stoppingToken">Triggered when Microsoft.Extensions.Hosting.IHostedService.StopAsync(System.Threading.CancellationToken) is called.</param>
        /// <returns></returns>
        async ValueTask UpdateTesTasksFromEventBlobsAsync(CancellationToken stoppingToken)
        {
            var markEventsProcessedList = new ConcurrentBag<Func<CancellationToken, Task>>();
            Func<IEnumerable<(RunnerEventsMessage Message, AzureBatchTaskState State)>> getEventsInOrder;

            {
                var messages = new ConcurrentBag<(RunnerEventsMessage Message, AzureBatchTaskState State)>();

                // Get and parse event blobs
                await Parallel.ForEachAsync(batchScheduler.GetEventMessagesAsync(stoppingToken), stoppingToken, async (eventMessage, cancellationToken) =>
                {
                    try
                    {
                        nodeEventProcessor.ValidateMessageMetadata(eventMessage);
                        await nodeEventProcessor.DownloadAndValidateMessageContentAsync(eventMessage, cancellationToken);
                        messages.Add((eventMessage, nodeEventProcessor.GetMessageBatchState(eventMessage)));
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, @"");
                        messages.Add((eventMessage, new(AzureBatchTaskState.TaskState.InfoUpdate, Warning: new List<string>
                        {
                            "EventParsingFailed",
                            $"{ex.GetType().FullName}: {ex.Message}",
                        })));
                        return;
                    }

                    markEventsProcessedList.Add(token => nodeEventProcessor.MarkMessageProcessedAsync(eventMessage, token));
                });

                getEventsInOrder = () => nodeEventProcessor.OrderProcessedByExecutorSequence(messages, item => item.Message);
            }

            var orderedMessageList = getEventsInOrder().ToList();

            if (!orderedMessageList.Any())
            {
                return;
            }

            // Update TesTasks
            await OrchestrateTesTasksOnBatchAsync(
                "NodeEvent",
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
                async token => GetTesTasks(token),
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
                (tesTasks, token) => batchScheduler.ProcessTesTaskBatchStatesAsync(tesTasks, orderedMessageList.Select(t => t.State).ToArray(), token),
                stoppingToken,
                "events");

            await Parallel.ForEachAsync(markEventsProcessedList, stoppingToken, async (markEventProcessed, cancellationToken) =>
                {
                    try
                    {
                        await markEventProcessed(cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, @"");
                    }
                });

            // Helpers
            async IAsyncEnumerable<TesTask> GetTesTasks([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
            {
                foreach (var (id, @event) in orderedMessageList.Select(t => t.Message).Select(m => (m.TesTaskId, m.RunnerEventMessage.Name)))
                {
                    TesTask tesTask = default;
                    if (await repository.TryGetItemAsync(id, cancellationToken, task => tesTask = task) && tesTask is not null)
                    {
                        logger.LogDebug("Completing event '{TaskEvent}' for task {TesTask}.", @event, tesTask.Id);
                        yield return tesTask;
                    }
                    else
                    {
                        logger.LogDebug("Could not find task {TesTask} for event '{TaskEvent}'.", id, @event);
                        yield return null;
                    }
                }
            }
        }
    }
}
