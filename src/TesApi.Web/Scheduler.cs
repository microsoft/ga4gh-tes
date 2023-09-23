// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using Tes.Extensions;
using CommonUtilities;
using Tes.Models;
using Tes.Repository;

namespace TesApi.Web
{
    /// <summary>
    /// A background service that schedules TES tasks in the batch system, orchestrates their lifecycle, and updates their state.
    /// This should only be used as a system-wide singleton service.  This class does not support scale-out on multiple machines,
    /// nor does it implement a leasing mechanism.  In the future, consider using the Lease Blob operation.
    /// </summary>
    public class Scheduler : BackgroundService
    {
        private readonly IRepository<TesTask> repository;
        private readonly IBatchScheduler batchScheduler;
        private readonly ILogger<Scheduler> logger;
        private readonly TimeSpan runInterval = TimeSpan.FromSeconds(4);
        private readonly SemaphoreSlim processNewTasksLock = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim processExistingTasksLock = new SemaphoreSlim(1, 1);
        private readonly Random random = new Random(Guid.NewGuid().GetHashCode());
        private readonly AsyncRetryPolicy tesRunnerUploadRetryPolicy;
        private readonly IHostApplicationLifetime applicationLifetime;

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="repository">The main TES task database repository implementation</param>
        /// <param name="batchScheduler">The batch scheduler implementation</param>
        /// <param name="logger">The logger instance</param>
        /// <param name="retryPolicyProvider">The retry policy provider</param>
        /// <param name="applicationLifetime">The application lifetime instance</param>
        public Scheduler(IRepository<TesTask> repository, IBatchScheduler batchScheduler, ILogger<Scheduler> logger, IRetryPolicyProvider retryPolicyProvider, IHostApplicationLifetime applicationLifetime)
        {
            this.repository = repository;
            this.batchScheduler = batchScheduler;
            this.logger = logger;
            this.tesRunnerUploadRetryPolicy = retryPolicyProvider.CreateDefaultCriticalServiceRetryPolicy();
            this.applicationLifetime = applicationLifetime;
        }

        /// <inheritdoc />
        public override Task StopAsync(CancellationToken cancellationToken)
        {
            logger.LogInformation("Scheduler stopping...");
            return base.StopAsync(cancellationToken);
        }

        /// <summary>
        /// The main thread that continuously schedules TES tasks in the batch system
        /// </summary>
        /// <param name="stoppingToken">Triggered when Microsoft.Extensions.Hosting.IHostedService.StopAsync(System.Threading.CancellationToken) is called.</param>
        /// <returns>A System.Threading.Tasks.Task that represents the long running operations.</returns>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var uploadTesRunnerResult = await tesRunnerUploadRetryPolicy.ExecuteAndCaptureAsync(t => UploadTesRunnerAsync(stoppingToken), stoppingToken);

            if (uploadTesRunnerResult.Outcome == OutcomeType.Failure)
            {
                const string msg = "Could not upload the TES runner to Azure Storage. Does this container have access to the default storage account or workspace storage account?";
                logger.LogCritical(msg, uploadTesRunnerResult.FinalException);
                applicationLifetime.StopApplication();
                throw new Exception(msg, uploadTesRunnerResult.FinalException);
            }

            logger.LogInformation("Scheduler started.");

            await Task.WhenAll(
                Task.Run(async () =>
                {
                    await ProcessTesTasksContinuouslyAsync(t => t.State == TesState.QUEUEDEnum, areExistingTesTasks: false, stoppingToken);
                }),
                Task.Run(async () =>
                {
                    await ProcessTesTasksContinuouslyAsync(t =>
                        t.State == TesState.INITIALIZINGEnum
                        || t.State == TesState.RUNNINGEnum
                        || (t.State == TesState.CANCELEDEnum && t.IsCancelRequested), areExistingTesTasks: true, stoppingToken);
                }));

            logger.LogInformation("Scheduler gracefully stopped.");
        }

        private async Task UploadTesRunnerAsync(CancellationToken stoppingToken)
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

        private async Task ProcessTesTasksContinuouslyAsync(Expression<Func<TesTask, bool>> predicate, bool areExistingTesTasks, CancellationToken stoppingToken)
        {
            var tesTasks = new List<TesTask>();
            var sw = Stopwatch.StartNew();

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (areExistingTesTasks)
                    {
                        // New tasks are being processed on the other thread, wait til they are done then acquire a lock
                        await processExistingTasksLock.WaitAsync(stoppingToken);
                    }

                    sw.Restart();

                    tesTasks = (await repository.GetItemsAsync(
                        predicate: predicate,
                        cancellationToken: stoppingToken))
                        .OrderBy(t => t.CreationTime)
                        .ToList();

                    if (tesTasks.Count == 0)
                    {
                        if (areExistingTesTasks)
                        {
                            batchScheduler.ClearBatchLogState();

                            // Release early; the Try/Finally block will have no effect
                            processExistingTasksLock.TryRelease();
                        }

                        await Task.Delay(runInterval, stoppingToken);
                        continue;
                    }

                    if (!areExistingTesTasks)
                    {
                        // New tasks
                        try
                        {
                            // Signal to existing tasks to pause processing
                            await processNewTasksLock.WaitAsync(stoppingToken);

                            // Wait until existing tasks are done processing and block them
                            await processExistingTasksLock.WaitAsync(stoppingToken);

                            await ProcessTesTasks(tesTasks, areExistingTesTasks, stoppingToken);
                        }
                        finally
                        {
                            // Allow existing tasks to resume
                            processNewTasksLock.TryRelease();
                            processExistingTasksLock.TryRelease();
                        }
                    }
                    else
                    {
                        // Existing tasks
                        await ProcessTesTasks(tesTasks, areExistingTesTasks, stoppingToken);
                    }

                    logger.LogInformation($"Processed {tesTasks.Count} {(areExistingTesTasks ? "existing" : "new")} TES tasks in {sw.Elapsed.TotalSeconds:n1}s; {sw.Elapsed.TotalSeconds / tesTasks.Count:n1} seconds per task");
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    logger.LogInformation($"ProcessTesTasksContinuouslyAsync for {(areExistingTesTasks ? "existing" : "new")} tasks cancelled.");
                    break;
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, exc.Message);
                    await Task.Delay(runInterval, stoppingToken);
                }
                finally
                {
                    if (areExistingTesTasks)
                    {
                        processExistingTasksLock.TryRelease();
                    }
                }

                tesTasks.Clear();
            }
        }

        private async Task ProcessTesTasks(List<TesTask> tesTasks, bool areExistingTesTasks, CancellationToken stoppingToken)
        {
            var pools = new HashSet<string>();

            // Prioritize processing terminal state tasks, then randomize order within each state to prevent head-of-line blocking
            foreach (var tesTask in tesTasks.OrderByDescending(t => t.State).ThenBy(t => random.Next()))
            {
                if (areExistingTesTasks && processNewTasksLock.CurrentCount == 0)
                {
                    // New tasks found, stop processing existing tasks
                    break;
                }

                try
                {
                    await ProcessTesTask(tesTask, stoppingToken);
                }
                catch (RepositoryCollisionException exc)
                {
                    logger.LogError(exc, $"RepositoryCollisionException in OrchestrateTesTasksOnBatch");
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, "Updating TES Task '{TesTask}' threw {ExceptionType}: '{ExceptionMessage}'. Stack trace: {ExceptionStackTrace}", tesTask.Id, exc.GetType().FullName, exc.Message, exc.StackTrace);
                }

                if (!string.IsNullOrWhiteSpace(tesTask.PoolId) && (TesState.QUEUEDEnum == tesTask.State || TesState.RUNNINGEnum == tesTask.State))
                {
                    pools.Add(tesTask.PoolId);
                }
            }

            if (areExistingTesTasks && batchScheduler.NeedPoolFlush)
            {
                await batchScheduler.FlushPoolsAsync(pools, stoppingToken);
            }
        }

        private async Task ProcessTesTask(TesTask tesTask, CancellationToken stoppingToken)
        {
            var isModified = false;

            try
            {
                isModified = await batchScheduler.ProcessTesTaskAsync(tesTask, stoppingToken);
            }
            catch (Exception exc)
            {
                if (++tesTask.ErrorCount > 3) // TODO: Should we increment this for exceptions here (current behaviour) or the attempted executions on the batch?
                {
                    tesTask.State = TesState.SYSTEMERROREnum;
                    tesTask.EndTime = DateTimeOffset.UtcNow;
                    tesTask.SetFailureReason("UnknownError", exc.Message, exc.StackTrace);
                }

                if (exc is Microsoft.Azure.Batch.Common.BatchException batchException)
                {
                    var requestInfo = batchException.RequestInformation;
                    //var requestId = batchException.RequestInformation?.ServiceRequestId;
                    var reason = (batchException.InnerException as Microsoft.Azure.Batch.Protocol.Models.BatchErrorException)?.Response?.ReasonPhrase;
                    var logs = new List<string>();

                    if (requestInfo?.ServiceRequestId is not null)
                    {
                        logs.Add($"Azure batch ServiceRequestId: {requestInfo.ServiceRequestId}");
                    }

                    if (requestInfo?.BatchError is not null)
                    {
                        logs.Add($"BatchErrorCode: {requestInfo.BatchError.Code}");
                        logs.Add($"BatchErrorMessage: {requestInfo.BatchError.Message}");
                        foreach (var detail in requestInfo.BatchError.Values?.Select(d => $"{d.Key}={d.Value}") ?? Enumerable.Empty<string>())
                        {
                            logs.Add(detail);
                        }
                    }

                    tesTask.AddToSystemLog(logs);
                }

                logger.LogError(exc, "TES task: {TesTask} threw an exception in OrchestrateTesTasksOnBatch().", tesTask.Id);
                await repository.UpdateItemAsync(tesTask, stoppingToken);
            }

            if (isModified)
            {
                var hasErrored = false;
                var hasEnded = false;

                switch (tesTask.State)
                {
                    case TesState.CANCELEDEnum:
                    case TesState.COMPLETEEnum:
                        hasEnded = true;
                        break;

                    case TesState.EXECUTORERROREnum:
                    case TesState.SYSTEMERROREnum:
                        hasErrored = true;
                        hasEnded = true;
                        break;

                    default:
                        break;
                }

                if (hasEnded)
                {
                    tesTask.EndTime = DateTimeOffset.UtcNow;
                }

                if (hasErrored)
                {
                    logger.LogDebug("{TesTask} failed, state: {TesTaskState}, reason: {TesTaskFailureReason}", tesTask.Id, tesTask.State, tesTask.FailureReason);
                }

                await repository.UpdateItemAsync(tesTask, stoppingToken);
            }
        }
    }
}
