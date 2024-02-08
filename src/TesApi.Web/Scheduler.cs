// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Tes.Extensions;
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
        private readonly TimeSpan runInterval = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="repository">The main TES task database repository implementation</param>
        /// <param name="batchScheduler">The batch scheduler implementation</param>
        /// <param name="logger">The logger instance</param>
        public Scheduler(IRepository<TesTask> repository, IBatchScheduler batchScheduler, ILogger<Scheduler> logger)
        {
            this.repository = repository;
            this.batchScheduler = batchScheduler;
            this.logger = logger;
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

            logger.LogInformation("Scheduler started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await OrchestrateTesTasksOnBatch(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, exc.Message);
                }

                try
                {
                    await Task.Delay(runInterval, stoppingToken);
                }
                catch (TaskCanceledException)
                {
                    break;
                }
            }

            logger.LogInformation("Scheduler gracefully stopped.");
        }

        /// <summary>
        /// Retrieves all actionable TES tasks from the database, performs an action in the batch system, and updates the resultant state
        /// </summary>
        /// <returns></returns>
        private async ValueTask OrchestrateTesTasksOnBatch(CancellationToken stoppingToken)
        {
            var pools = new HashSet<string>();

            var tesTasks = (await repository.GetItemsAsync(
                    predicate: t => t.State == TesState.QUEUEDEnum
                        || t.State == TesState.INITIALIZINGEnum
                        || t.State == TesState.RUNNINGEnum
                        || (t.State == TesState.CANCELEDEnum && t.IsCancelRequested),
                    cancellationToken: stoppingToken))
                .OrderBy(t => t.CreationTime)
                .ToList();

            if (0 == tesTasks.Count)
            {
                batchScheduler.ClearBatchLogState();
                return;
            }

            var startTime = DateTime.UtcNow;

            foreach (var tesTask in tesTasks)
            {
                try
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

                    logger.LogDebug("{TesTask} {WAS}, state: {TesTaskState}", tesTask.Id, "was " + (isModified ? "not " : string.Empty) + "modified", tesTask.State);

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
                catch (RepositoryCollisionException exc)
                {
                    logger.LogError(exc, $"RepositoryCollisionException in OrchestrateTesTasksOnBatch");
                }
                // TODO catch EF / postgres exception?
                //catch (Microsoft.Azure.Cosmos.CosmosException exc)
                //{
                //    TesTask currentTesTask = default;
                //    _ = await repository.TryGetItemAsync(tesTask.Id, t => currentTesTask = t);

                //    if (exc.StatusCode == System.Net.HttpStatusCode.PreconditionFailed)
                //    {
                //        logger.LogError(exc, $"Updating TES Task '{tesTask.Id}' threw an exception attempting to set state: {tesTask.State}. Another actor set state: {currentTesTask?.State}");
                //        currentTesTask?.SetWarning("ConcurrencyWriteFailure", tesTask.State.ToString(), exc.Message, exc.StackTrace);
                //    }
                //    else
                //    {
                //        logger.LogError(exc, $"Updating TES Task '{tesTask.Id}' threw {exc.GetType().FullName}: '{exc.Message}'. Stack trace: {exc.StackTrace}");
                //        currentTesTask?.SetWarning("UnknownError", exc.Message, exc.StackTrace);
                //    }

                //    if (currentTesTask is not null)
                //    {
                //        await repository.UpdateItemAsync(currentTesTask);
                //    }
                //}
                catch (Exception exc)
                {
                    logger.LogError(exc, "Updating TES Task '{TesTask}' threw {ExceptionType}: '{ExceptionMessage}'. Stack trace: {ExceptionStackTrace}", tesTask.Id, exc.GetType().FullName, exc.Message, exc.StackTrace);
                }

                if (!string.IsNullOrWhiteSpace(tesTask.PoolId) && (TesState.QUEUEDEnum == tesTask.State || TesState.RUNNINGEnum == tesTask.State))
                {
                    pools.Add(tesTask.PoolId);
                }
            }

            if (batchScheduler.NeedPoolFlush)
            {
                await batchScheduler.FlushPoolsAsync(pools, stoppingToken);
            }

            logger.LogDebug("OrchestrateTesTasksOnBatch for {TaskCount} tasks completed in {TotalSeconds} seconds.", tesTasks.Count, DateTime.UtcNow.Subtract(startTime).TotalSeconds);
        }
    }
}
