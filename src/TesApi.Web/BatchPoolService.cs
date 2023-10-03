// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Extensions.Logging;
using Tes.Models;
using Tes.Repository;

namespace TesApi.Web
{
    /// <summary>
    /// A background service that montitors CloudPools in the batch system, orchestrates their lifecycle, and updates their state.
    /// This should only be used as a system-wide singleton service.  This class does not support scale-out on multiple machines,
    /// nor does it implement a leasing mechanism.  In the future, consider using the Lease Blob operation.
    /// </summary>
    internal class BatchPoolService : OrchestrateOnBatchSchedulerService
    {
        /// <summary>
        /// Interval between each call to <see cref="IBatchPool.ServicePoolAsync(CancellationToken)"/>.
        /// </summary>
        public static readonly TimeSpan RunInterval = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="repository">The main TES task database repository implementation</param>
        /// <param name="batchScheduler"></param>
        /// <param name="logger"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public BatchPoolService(IRepository<TesTask> repository, IBatchScheduler batchScheduler, ILogger<BatchPoolService> logger)
            : base(repository, batchScheduler, logger) { }

        /// <inheritdoc />
        protected override void ExecuteSetup(CancellationToken stoppingToken)
        {
            batchScheduler.LoadExistingPoolsAsync(stoppingToken).Wait(stoppingToken); // Delay starting Scheduler until this completes to finish initializing BatchScheduler.
        }

        /// <inheritdoc />
        protected override Task ExecuteCoreAsync(CancellationToken stoppingToken)
        {
            return Task.WhenAll(ServiceBatchPoolsAsync(stoppingToken), ExecuteCompletedTesTasksOnBatchAsync(stoppingToken));
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

            foreach (var pool in pools)
            {
                try
                {
                    await action(pool, stoppingToken);
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, @"Batch pool {PoolId} threw an exception in {Poll}.", pool.Pool?.PoolId, pollName);
                }
            }

            logger.LogDebug(@"{Poll} for {PoolsCount} pools completed in {TotalSeconds} seconds.", pollName, pools.Count, DateTime.UtcNow.Subtract(startTime).TotalSeconds);
        }

        /// <summary>
        /// Calls <see cref="ExecuteServiceBatchPoolsAsync(CancellationToken)"/> repeatedly.
        /// </summary>
        /// <param name="stoppingToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private Task ServiceBatchPoolsAsync(CancellationToken stoppingToken)
        {
            return ExecuteActionOnIntervalAsync(RunInterval, ExecuteServiceBatchPoolsAsync, stoppingToken);
        }

        /// <summary>
        /// Retrieves all batch pools from the database and affords an opportunity to react to changes.
        /// </summary>
        /// <param name="stoppingToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private ValueTask ExecuteServiceBatchPoolsAsync(CancellationToken stoppingToken)
        {
            return ExecuteActionOnPoolsAsync("ServiceBatchPools", (pool, token) => pool.ServicePoolAsync(token), stoppingToken);
        }

        /// <summary>
        /// Calls <see cref="ProcessCompletedCloudTasksAsync(CancellationToken)"/> repeatedly.
        /// </summary>
        /// <param name="stoppingToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private Task ExecuteCompletedTesTasksOnBatchAsync(CancellationToken stoppingToken)
        {
            return ExecuteActionOnIntervalAsync(RunInterval, ProcessCompletedCloudTasksAsync, stoppingToken);
        }

        /// <summary>
        /// Retrieves all completed tasks from every batch pools from the database and affords an opportunity to react to changes.
        /// </summary>
        /// <param name="stoppingToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private async ValueTask ProcessCompletedCloudTasksAsync(CancellationToken stoppingToken)
        {
            var tasks = new List<CloudTask>();
            await ExecuteActionOnPoolsAsync("ServiceBatchTasks", async (pool, token) => tasks.AddRange(await pool.GetCompletedTasks(token).ToListAsync(token)), stoppingToken);

            await OrchestrateTesTasksOnBatchAsync(
                "Completed",
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
                async token => GetTesTasks(token),
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
                (tesTasks, token) => batchScheduler.ProcessTesTaskBatchStatesAsync(tesTasks, tasks.Select(GetBatchState).ToArray(), token),
                stoppingToken);

            async IAsyncEnumerable<TesTask> GetTesTasks([EnumeratorCancellation] CancellationToken cancellationToken)
            {
                foreach (var id in tasks.Select(t => t.Id))
                {
                    TesTask tesTask = default;
                    if (await repository.TryGetItemAsync(id, cancellationToken, task => tesTask = task) && tesTask is not null)
                    {
                        yield return tesTask;
                    }
                }
            }

            AzureBatchTaskState GetBatchState(CloudTask task)
            {
                if (task.ExecutionInformation.ExitCode != 0 || task.ExecutionInformation.FailureInformation is not null)
                {
                    return new(AzureBatchTaskState.TaskState.CompletedWithErrors,
                        Failure: new(task.ExecutionInformation.FailureInformation.Code,
                        Enumerable.Empty<string>()
                            .Append(task.ExecutionInformation.FailureInformation.Message)
                            .Append($"Batch task ExitCode: {task.ExecutionInformation?.ExitCode}, Failure message: {task.ExecutionInformation?.FailureInformation?.Message}")
                            .Concat(task.ExecutionInformation.FailureInformation.Details.Select(pair => pair.Value))),
                        BatchTaskStartTime: task.ExecutionInformation.StartTime,
                        BatchTaskEndTime: task.ExecutionInformation.EndTime,
                        BatchTaskExitCode: task.ExecutionInformation.ExitCode);
                }
                else
                {
                    return new(AzureBatchTaskState.TaskState.CompletedSuccessfully,
                        BatchTaskStartTime: task.ExecutionInformation.StartTime,
                        BatchTaskEndTime: task.ExecutionInformation.EndTime,
                        BatchTaskExitCode: task.ExecutionInformation.ExitCode);
                }
            }
        }
    }
}
