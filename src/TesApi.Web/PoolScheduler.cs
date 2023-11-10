// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Extensions.Logging;
using Tes.Models;
using Tes.Repository;
using static TesApi.Web.IBatchPool;

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
            return ExecuteActionOnIntervalAsync(RunInterval, async cancellationToken =>
            {
                await ExecuteActionOnPoolsAsync(
                    "Service Batch Pools",
                    async (pool, token) =>
                    {
                        await pool.ServicePoolAsync(token);
                        await ProcessCloudTaskStatesAsync(pool.Id, pool.GetCloudTaskStatesAsync(token), token);
                        await ProcessDeletedTasks(pool.GetTasksToDelete(token), token);
                    },
                    cancellationToken);
            }, stoppingToken);

            async ValueTask ProcessCloudTaskStatesAsync(string poolId, IAsyncEnumerable<CloudTaskBatchTaskState> states, CancellationToken cancellationToken)
            {
                var list = new List<(TesTask TesTask, AzureBatchTaskState State)>();

                await foreach (var (cloudTaskId, state) in states.WithCancellation(cancellationToken))
                {
                    TesTask tesTask = default;
                    if (await repository.TryGetItemAsync(batchScheduler.GetTesTaskIdFromCloudTaskId(cloudTaskId), cancellationToken, task => tesTask = task) && tesTask is not null)
                    {
                        list.Add((tesTask, state));
                    }
                    else
                    {
                        logger.LogDebug(@"Unable to locate TesTask for CloudTask '{CloudTask}' with action state {ActionState}.", cloudTaskId, state.State);
                    }
                }

                if (list.Count != 0)
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

            async ValueTask ProcessDeletedTasks(IAsyncEnumerable<IBatchScheduler.CloudTaskId> tasks, CancellationToken cancellationToken)
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
        }
    }
}
