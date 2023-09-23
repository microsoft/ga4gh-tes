// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using CommonUtilities;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;
using Tes.Extensions;
using Tes.Models;
using Tes.Repository;
using TesApi.Web.Storage;

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
        private readonly IStorageAccessProvider storageAccessProvider;
        private List<string> completedTesTaskIds = new List<string>();

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="repository">The main TES task database repository implementation</param>
        /// <param name="batchScheduler">The batch scheduler implementation</param>
        /// <param name="logger">The logger instance</param>
        /// <param name="retryPolicyProvider">The retry policy provider</param>
        /// <param name="applicationLifetime">The application lifetime instance</param>
        public Scheduler(IRepository<TesTask> repository, IBatchScheduler batchScheduler, ILogger<Scheduler> logger, IStorageAccessProvider storageAccessProvider, IRetryPolicyProvider retryPolicyProvider, IHostApplicationLifetime applicationLifetime)
        {
            this.repository = repository;
            this.batchScheduler = batchScheduler;
            this.logger = logger;
            this.storageAccessProvider = storageAccessProvider;
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
        /// Helper method for tests to execute the main thread
        /// </summary>
        /// <param name="ct">Cancellation token</param>
        /// <returns>A task</returns>
        [Obsolete("This method is for testing purposes only and should not be used in production code.")]
        internal Task ExecuteForTestAsync(CancellationToken ct)
        {
            return ExecuteAsync(ct);
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
                    await ProcessTesTaskCompletionsContinuouslyAsync(stoppingToken);
                }),
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

                    var tesTasks = (await repository.GetItemsAsync(
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
                            _ = processExistingTasksLock.TryReleaseIfInUse();
                        }

                        await Task.Delay(runInterval, stoppingToken);
                        continue;
                    }

                    if (!areExistingTesTasks)
                    {
                        // New tasks
                        try
                        {
                            // Signal to existing task thread to pause processing
                            await processNewTasksLock.WaitAsync(stoppingToken);

                            // Wait until existing tasks thread is done processing, then block it from resuming
                            await processExistingTasksLock.WaitAsync(stoppingToken);

                            await ProcessTesTasks(tesTasks, areExistingTesTasks, stoppingToken);
                        }
                        finally
                        {
                            // Allow existing tasks thead to resume processing
                            _ = processNewTasksLock.TryReleaseIfInUse();
                            _ = processExistingTasksLock.TryReleaseIfInUse();
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
                        _ = processExistingTasksLock.TryReleaseIfInUse();
                    }
                }
            }
        }

        private async Task DeleteTesTaskCompletionByIdIfExistsAsync(string tesTaskId, CancellationToken stoppingToken)
        {
            var blobUri = new Uri(await storageAccessProvider.GetInternalTesBlobUrlAsync("task-completions", stoppingToken));
            var storageSegments = StorageAccountUrlSegments.Create(blobUri.ToString());
            var blobServiceClient = new BlobServiceClient(blobUri);
            var container = blobServiceClient.GetBlobContainerClient(storageSegments.ContainerName);
            var virtualDirectory = storageSegments.BlobName + "/";
            var blobName = $"{virtualDirectory}{tesTaskId}.json";
            await container.DeleteBlobIfExistsAsync(blobName, default, default, stoppingToken);
        }

        private async Task ProcessTesTaskCompletionsContinuouslyAsync(CancellationToken stoppingToken)
        {
            var sw = Stopwatch.StartNew();

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    sw.Restart();

                    // TODO - this should return a container token
                    var blobUri = new Uri(await storageAccessProvider.GetInternalTesBlobUrlAsync("task-completions", stoppingToken));
                    var storageSegments = StorageAccountUrlSegments.Create(blobUri.ToString());
                    var blobServiceClient = new BlobServiceClient(blobUri);
                    var container = blobServiceClient.GetBlobContainerClient(storageSegments.ContainerName);
                    var virtualDirectory = storageSegments.BlobName + "/";
                    var enumerator = container.GetBlobsAsync(prefix: virtualDirectory).GetAsyncEnumerator();
                    var completedTesTaskIds = new List<string>();

                    while (await enumerator.MoveNextAsync())
                    {
                        // example: tasks-completions/12345678_12345678123456781234567812345678.json
                        var blobName = enumerator.Current.Name;
                        var json = (await container.GetBlobClient(blobName).DownloadContentAsync(stoppingToken)).Value.Content.ToString();
                        var tesTaskCompletionMessage = JsonConvert.DeserializeObject<TesTaskCompletionMessage>(json);
                        completedTesTaskIds.Add(tesTaskCompletionMessage.Id);
                    }

                    // Set class-level object so that it can be used by the existing task processor thread
                    this.completedTesTaskIds = completedTesTaskIds;

                    if (completedTesTaskIds.Count > 0)
                    {
                        logger.LogInformation($"{completedTesTaskIds.Count} TES task completions downloaded in {sw.Elapsed.TotalSeconds:n1}s; {sw.Elapsed.TotalSeconds / completedTesTaskIds.Count:n1} seconds per task completion");
                    }

                    await Task.Delay(runInterval, stoppingToken);                    
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    logger.LogInformation($"ProcessTesTaskCompletionsContinuouslyAsync tasks cancelled.");
                    break;
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, exc.Message);
                    await Task.Delay(runInterval, stoppingToken);
                }
            }
        }

        private async Task ProcessTesTasks(List<TesTask> tesTasks, bool areExistingTesTasks, CancellationToken stoppingToken)
        {
            var pools = new HashSet<string>();
            int completedTesTaskIdsCount = completedTesTaskIds.Count;

            while (!stoppingToken.IsCancellationRequested)
            {
                bool newTaskCompletionsFound = false;

                // Prioritize processing completed tasks, then terminal state tasks, then randomize order within each state to prevent head-of-line blocking
                foreach (var tesTask in tesTasks.OrderByDescending(t => completedTesTaskIds.Contains(t.Id)).ThenBy(t => t.State).ThenBy(t => random.Next()))
                {
                    if (areExistingTesTasks && processNewTasksLock.CurrentCount == 0)
                    {
                        // New tasks found, stop processing existing tasks
                        return;
                    }

                    if (completedTesTaskIds.Count > completedTesTaskIdsCount)
                    {
                        // New task completions found, stop processing existing tasks
                        // Force loop to restart so that new task completions are processed
                        newTaskCompletionsFound = true;
                        break;
                    }

                    try
                    {
                        await ProcessTesTask(tesTask, stoppingToken);

                        if (completedTesTaskIds.Contains(tesTask.Id))
                        {
                            await DeleteTesTaskCompletionByIdIfExistsAsync(tesTask.Id, stoppingToken);
                        }
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

                if (newTaskCompletionsFound)
                {
                    // Force loop to restart so that new task completions are processed
                    continue;
                }

                // The while loop is only to handle the task completion case
                break;
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
