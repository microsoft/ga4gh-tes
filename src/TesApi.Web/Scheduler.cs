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

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="hostApplicationLifetime">Used for requesting termination of the current application during initialization.</param>
        /// <param name="repository">The main TES task database repository implementation</param>
        /// <param name="batchScheduler">The batch scheduler implementation</param>
        /// <param name="logger">The logger instance</param>
        public Scheduler(Microsoft.Extensions.Hosting.IHostApplicationLifetime hostApplicationLifetime, IRepository<TesTask> repository, IBatchScheduler batchScheduler, ILogger<Scheduler> logger)
            : base(hostApplicationLifetime, repository, batchScheduler, logger) { }


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

        // TODO: Implement this
        /// <summary>
        /// Retrieves all event blobs from storage and updates the resultant state.
        /// </summary>
        /// <param name="stoppingToken">Triggered when Microsoft.Extensions.Hosting.IHostedService.StopAsync(System.Threading.CancellationToken) is called.</param>
        /// <returns></returns>
        async ValueTask UpdateTesTasksFromEventBlobsAsync(CancellationToken stoppingToken)
        {
            var messageInfos = new ConcurrentBag<NodeEventMessage>();
            var messages = new ConcurrentBag<(string Id, AzureBatchTaskState State)>();

            // Get and parse event blobs
            await foreach (var message in batchScheduler.GetEventMessagesAsync(stoppingToken)
                .WithCancellation(stoppingToken))
            {
                messageInfos.Add(message);
            }

            try
            {
                await Parallel.ForEachAsync(messageInfos, ProcessMessage);
            }
            catch { } // TODO: identify exceptions

            // Update TesTasks
            await OrchestrateTesTasksOnBatchAsync(
                "NodeEvent",
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
                async token => GetTesTasks(token),
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
                (tesTasks, token) => batchScheduler.ProcessTesTaskBatchStatesAsync(tesTasks, messages.Select(t => t.State).ToArray(), token),
                stoppingToken);

            // Helpers
            async ValueTask ProcessMessage(NodeEventMessage messageInfo, CancellationToken cancellationToken)
            {
                messages.Add(await messageInfo.GetMessageBatchStateAsync(cancellationToken));
                await messageInfo.MarkMessageProcessed(cancellationToken);
            }

            async IAsyncEnumerable<TesTask> GetTesTasks([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
            {
                foreach (var id in messages.Select(t => batchScheduler.GetTesTaskIdFromCloudTaskId(t.Id)))
                {
                    TesTask tesTask = default;
                    if (await repository.TryGetItemAsync(id, cancellationToken, task => tesTask = task) && tesTask is not null)
                    {
                        logger.LogDebug("Completing task {TesTask}.", tesTask.Id);
                        yield return tesTask;
                    }
                    else
                    {
                        logger.LogDebug("Could not find task {TesTask}.", id);
                        yield return null;
                    }
                }
            }
        }
    }
}
