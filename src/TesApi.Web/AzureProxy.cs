// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Management.ApplicationInsights.Management;
using Microsoft.Azure.Management.Batch;
using Microsoft.Azure.Management.ContainerRegistry.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Rest;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;
using Tes.Models;
using TesApi.Web.Management.Batch;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Storage;
using BatchModels = Microsoft.Azure.Management.Batch.Models;
using CloudTask = Microsoft.Azure.Batch.CloudTask;
using ComputeNodeState = Microsoft.Azure.Batch.Common.ComputeNodeState;
using FluentAzure = Microsoft.Azure.Management.Fluent.Azure;
using JobState = Microsoft.Azure.Batch.Common.JobState;
using OnAllTasksComplete = Microsoft.Azure.Batch.Common.OnAllTasksComplete;
using PoolInformation = Microsoft.Azure.Batch.PoolInformation;
using TaskExecutionInformation = Microsoft.Azure.Batch.TaskExecutionInformation;
using TaskState = Microsoft.Azure.Batch.Common.TaskState;

namespace TesApi.Web
{
    /// <summary>
    /// Wrapper for Azure APIs
    /// </summary>
    public partial class AzureProxy : IAzureProxy
    {
        private const char BatchJobAttemptSeparator = '-';
        private static readonly AsyncRetryPolicy batchRaceConditionJobNotFoundRetryPolicy = Policy
            .Handle<BatchException>(ex => ex.RequestInformation.BatchError.Code == BatchErrorCodeStrings.JobNotFound)
            .WaitAndRetryAsync(5, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));

        private readonly ILogger logger;
        private readonly BatchClient batchClient;
        private readonly string subscriptionId;
        private readonly string location;
        private readonly string batchResourceGroupName;
        private readonly string batchAccountName;
        //TODO: This dependency should be injected at a higher level (e.g. scheduler), but that requires significant refactoring that should be done separately.
        private readonly IBatchPoolManager batchPoolManager;


        /// <summary>
        /// Constructor of AzureProxy
        /// </summary>
        /// <param name="batchAccountOptions">The Azure Batch Account options</param>
        /// <param name="batchPoolManager"><inheritdoc cref="IBatchPoolManager"/></param>
        /// <param name="logger">The logger</param>
        /// <exception cref="InvalidOperationException"></exception>
        public AzureProxy(IOptions<BatchAccountOptions> batchAccountOptions, IBatchPoolManager batchPoolManager, ILogger<AzureProxy> logger)
        {
            ArgumentNullException.ThrowIfNull(batchAccountOptions);
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(batchPoolManager);

            this.batchPoolManager = batchPoolManager;

            if (string.IsNullOrWhiteSpace(batchAccountOptions.Value.AccountName))
            {
                //TODO: check if there's a better exception for this scenario or we need to create a custom one.
                throw new InvalidOperationException("The batch account name is missing from the the configuration.");
            }

            this.logger = logger;

            if (!string.IsNullOrWhiteSpace(batchAccountOptions.Value.AppKey))
            {
                //If the key is provided assume we won't use ARM and the information will be provided via config
                batchClient = BatchClient.Open(new BatchSharedKeyCredentials(batchAccountOptions.Value.BaseUrl,
                    batchAccountOptions.Value.AccountName, batchAccountOptions.Value.AppKey));
                location = batchAccountOptions.Value.Region;
                subscriptionId = batchAccountOptions.Value.SubscriptionId;
                batchResourceGroupName = batchAccountOptions.Value.ResourceGroup;

            }
            else
            {
                batchAccountName = batchAccountOptions.Value.AccountName;
                var (SubscriptionId, ResourceGroupName, Location, BatchAccountEndpoint) = FindBatchAccountAsync(batchAccountName).Result;
                batchResourceGroupName = ResourceGroupName;
                subscriptionId = SubscriptionId;
                location = Location;
                batchClient = BatchClient.Open(new BatchTokenCredentials($"https://{BatchAccountEndpoint}", () => GetAzureAccessTokenAsync("https://batch.core.windows.net/")));
            }

            //azureOfferDurableId = batchAccountOptions.Value.AzureOfferDurableId;

            //if (!AzureRegionUtils.TryGetBillingRegionName(location, out billingRegionName))
            //{
            //    logger.LogWarning($"Azure ARM location '{location}' does not have a corresponding Azure Billing Region.  Prices from the fallback billing region '{DefaultAzureBillingRegionName}' will be used instead.");
            //    billingRegionName = DefaultAzureBillingRegionName;
            //}
        }

        // TODO: Static method because the instrumentation key is needed in both Program.cs and Startup.cs and we wanted to avoid intializing the batch client twice.
        // Can we skip initializing app insights with a instrumentation key in Program.cs? If yes, change this to an instance method.
        /// <summary>
        /// Gets the Application Insights instrumentation key
        /// </summary>
        /// <param name="appInsightsApplicationId">Application Insights application id</param>
        /// <returns>Application Insights instrumentation key</returns>
        public static async Task<string> GetAppInsightsInstrumentationKeyAsync(string appInsightsApplicationId)
        {
            var azureClient = await GetAzureManagementClientAsync();
            var subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);

            var credentials = new TokenCredentials(await GetAzureAccessTokenAsync());

            foreach (var subscriptionId in subscriptionIds)
            {
                try
                {
                    var app = (await new ApplicationInsightsManagementClient(credentials) { SubscriptionId = subscriptionId }.Components.ListAsync())
                        .FirstOrDefault(a => a.ApplicationId.Equals(appInsightsApplicationId, StringComparison.OrdinalIgnoreCase));

                    if (app is not null)
                    {
                        return app.InstrumentationKey;
                    }
                }
                catch
                {
                }
            }

            return null;
        }

        /// <inheritdoc/>
        public async Task<string> GetNextBatchJobIdAsync(string tesTaskId)
        {
            var jobFilter = new ODATADetailLevel
            {
                FilterClause = $"startswith(id,'{tesTaskId}{BatchJobAttemptSeparator}')",
                SelectClause = "id"
            };

            var lastAttemptNumber = (await batchClient.JobOperations.ListJobs(jobFilter).ToListAsync())
                .Select(j => int.Parse(j.Id.Split(BatchJobAttemptSeparator)[1]))
                .OrderBy(a => a)
                .LastOrDefault();

            return $"{tesTaskId}{BatchJobAttemptSeparator}{lastAttemptNumber + 1}";
        }

        /// <inheritdoc/>
        public IEnumerable<AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize()
            => batchClient.PoolOperations.ListPools()
                .Select(p => new
                {
                    p.VirtualMachineSize,
                    DedicatedNodeCount = Math.Max(p.TargetDedicatedComputeNodes ?? 0, p.CurrentDedicatedComputeNodes ?? 0),
                    LowPriorityNodeCount = Math.Max(p.TargetLowPriorityComputeNodes ?? 0, p.CurrentLowPriorityComputeNodes ?? 0)
                })
                .GroupBy(x => x.VirtualMachineSize)
                .Select(grp => new AzureBatchNodeCount { VirtualMachineSize = grp.Key, DedicatedNodeCount = grp.Sum(x => x.DedicatedNodeCount), LowPriorityNodeCount = grp.Sum(x => x.LowPriorityNodeCount) });

        /// <inheritdoc/>
        public int GetBatchActivePoolCount()
        {
            var activePoolsFilter = new ODATADetailLevel
            {
                FilterClause = "state eq 'active' or state eq 'deleting'",
                SelectClause = "id"
            };

            return batchClient.PoolOperations.ListPools(activePoolsFilter).Count();
        }

        /// <inheritdoc/>
        public int GetBatchActiveJobCount()
        {
            var activeJobsFilter = new ODATADetailLevel
            {
                FilterClause = "state eq 'active' or state eq 'disabling' or state eq 'terminating' or state eq 'deleting'",
                SelectClause = "id"
            };

            return batchClient.JobOperations.ListJobs(activeJobsFilter).Count();
        }

        /// <inheritdoc/>
        public async Task CreateAutoPoolModeBatchJobAsync(string jobId, CloudTask cloudTask, PoolInformation poolInformation, CancellationToken cancellationToken)
        {
            logger.LogInformation($"TES task: {cloudTask.Id} - creating Batch job");
            var job = batchClient.JobOperations.CreateJob(jobId, poolInformation);
            job.OnAllTasksComplete = OnAllTasksComplete.TerminateJob;
            await job.CommitAsync(cancellationToken: cancellationToken);
            logger.LogInformation($"TES task: {cloudTask.Id} - Batch job committed successfully.");
            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);

            try
            {
                logger.LogInformation($"TES task: {cloudTask.Id} adding task to job.");
                job = await batchRaceConditionJobNotFoundRetryPolicy.ExecuteAsync(ct =>
                    batchClient.JobOperations.GetJobAsync(job.Id, cancellationToken: ct),
                    cancellationToken);

                await job.AddTaskAsync(cloudTask, cancellationToken: cancellationToken);
                logger.LogInformation($"TES task: {cloudTask.Id} added task successfully.");
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                var batchError = JsonConvert.SerializeObject((ex as BatchException)?.RequestInformation?.BatchError);
                logger.LogError(ex, $"TES task: {cloudTask.Id} deleting {job.Id} because adding task to it failed. Batch error: {batchError}");

                try
                {
                    await batchClient.JobOperations.DeleteJobAsync(job.Id, cancellationToken: CancellationToken.None);
                }
                catch (Exception e)
                {
                    logger.LogError(e, $"TES task: {cloudTask.Id} deleting {job.Id} failed.");
                }

                throw;
            }
        }

        /// <inheritdoc/>
        public async Task CreateBatchJobAsync(PoolInformation poolInformation, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrEmpty(poolInformation?.PoolId, nameof(poolInformation));

            logger.LogInformation("TES: Creating Batch job {BatchJob}", poolInformation.PoolId);
            var job = batchClient.JobOperations.CreateJob(poolInformation.PoolId, poolInformation);
            job.OnAllTasksComplete = OnAllTasksComplete.NoAction;
            job.OnTaskFailure = OnTaskFailure.NoAction;

            await job.CommitAsync(cancellationToken: cancellationToken);
            logger.LogInformation("TES: Batch job {BatchJob} committed successfully", poolInformation.PoolId);
            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
        }

        /// <inheritdoc/>
        public async Task AddBatchTaskAsync(string tesTaskId, CloudTask cloudTask, PoolInformation poolInformation, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrEmpty(poolInformation?.PoolId, nameof(poolInformation));

            logger.LogInformation("TES task: {TesTask} - Adding task to job {BatchJob}", tesTaskId, poolInformation.PoolId);
            var job = await batchRaceConditionJobNotFoundRetryPolicy.ExecuteAsync(ct =>
                    batchClient.JobOperations.GetJobAsync(poolInformation.PoolId, cancellationToken: ct),
                    cancellationToken);

            await job.AddTaskAsync(cloudTask, cancellationToken: cancellationToken);
            logger.LogInformation("TES task: {TesTask} - Added task successfully", tesTaskId);
        }

        /// <inheritdoc/>
        public async Task DeleteBatchJobAsync(PoolInformation poolInformation, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrEmpty(poolInformation?.PoolId, nameof(poolInformation));
            logger.LogInformation("Deleting job {BatchJob}", poolInformation.PoolId);
            await batchClient.JobOperations.DeleteJobAsync(poolInformation.PoolId, cancellationToken: cancellationToken);
        }

        /// <inheritdoc/>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1826:Do not use Enumerable methods on indexable collections", Justification = "FirstOrDefault() is straightforward, the alternative is less clear.")]
        public async Task<AzureBatchJobAndTaskState> GetBatchJobAndTaskStateAsync(TesTask tesTask, bool usingAutoPools)
        {
            try
            {
                var nodeAllocationFailed = false;
                string nodeErrorCode = null;
                IEnumerable<string> nodeErrorDetails = null;
                var activeJobWithMissingAutoPool = false;
                ComputeNodeState? nodeState = null;
                TaskState? taskState = null;
                string poolId = null;
                TaskExecutionInformation taskExecutionInformation = null;
                CloudJob job = null;
                var attemptNumber = 0;
                CloudTask batchTask = null;

                var jobOrTaskFilter = new ODATADetailLevel
                {
                    FilterClause = $"startswith(id,'{tesTask.Id}{BatchJobAttemptSeparator}')",
                    SelectClause = "*"
                };

                if (usingAutoPools)
                {
                    // Normally, we will only find one job. If we find more, we always want the latest one. Thus, we use ListJobs()
                    var jobInfos = await batchClient.JobOperations.ListJobs(jobOrTaskFilter).ToAsyncEnumerable()
                        .Select(j => new { Job = j, AttemptNumber = int.Parse(j.Id.Split(BatchJobAttemptSeparator)[1]) })
                        .ToListAsync();

                    if (!jobInfos.Any())
                    {
                        return new AzureBatchJobAndTaskState { JobState = null };
                    }

                    if (jobInfos.Count(j => j.Job.State == JobState.Active) > 1)
                    {
                        return new AzureBatchJobAndTaskState { MoreThanOneActiveJobOrTaskFound = true };
                    }

                    var lastJobInfo = jobInfos.OrderBy(j => j.AttemptNumber).Last();

                    job = lastJobInfo.Job;
                    attemptNumber = lastJobInfo.AttemptNumber;

                    try
                    {
                        batchTask = await batchClient.JobOperations.GetTaskAsync(job.Id, tesTask.Id);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, @"Failed to get task for TesTask {TesTask}", tesTask.Id);
                    }
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(tesTask.PoolId))
                    {
                        return new AzureBatchJobAndTaskState { JobState = null };
                    }

                    try
                    {
                        job = await batchClient.JobOperations.GetJobAsync(tesTask.PoolId);
                    }
                    catch (BatchException ex) when (ex.InnerException is Microsoft.Azure.Batch.Protocol.Models.BatchErrorException e && e.Response.StatusCode == System.Net.HttpStatusCode.NotFound)
                    {
                        logger.LogError(ex, @"Failed to get job for TesTask {TesTask}", tesTask.Id);
                        return new AzureBatchJobAndTaskState { JobState = null };
                    }

                    var taskInfos = await batchClient.JobOperations.ListTasks(tesTask.PoolId, jobOrTaskFilter).ToAsyncEnumerable()
                        .Select(t => new { Task = t, AttemptNumber = int.Parse(t.Id.Split(BatchJobAttemptSeparator)[1]) })
                        .ToListAsync();

                    if (!taskInfos.Any())
                    {
                        logger.LogError(@"Failed to get task for TesTask {TesTask}", tesTask.Id);
                    }
                    else
                    {
                        if (taskInfos.Count(t => t.Task.State != TaskState.Completed) > 1)
                        {
                            return new AzureBatchJobAndTaskState { MoreThanOneActiveJobOrTaskFound = true };
                        }

                        var lastTaskInfo = taskInfos.OrderBy(t => t.AttemptNumber).Last();
                        batchTask = lastTaskInfo.Task;
                        attemptNumber = lastTaskInfo.AttemptNumber;
                    }
                }

                poolId = job.ExecutionInformation?.PoolId;

                Func<ComputeNode, bool> computeNodePredicate = usingAutoPools
                    ? n => (n.RecentTasks?.Select(t => t.JobId) ?? Enumerable.Empty<string>()).Contains(job.Id)
                    : n => (n.RecentTasks?.Select(t => t.TaskId) ?? Enumerable.Empty<string>()).Contains(batchTask?.Id);

                if (job.State == JobState.Active && poolId is not null)
                {
                    var poolFilter = new ODATADetailLevel
                    {
                        SelectClause = "*"
                    };

                    CloudPool pool;

                    try
                    {
                        pool = await batchClient.PoolOperations.GetPoolAsync(poolId, poolFilter);
                    }
                    catch (BatchException ex) when (ex.InnerException is Microsoft.Azure.Batch.Protocol.Models.BatchErrorException e && e.Response?.StatusCode == System.Net.HttpStatusCode.NotFound)
                    {
                        pool = default;
                    }

                    if (pool is not null)
                    {
                        nodeAllocationFailed = usingAutoPools && pool.ResizeErrors?.Count > 0; // When not using autopools, NodeAllocationFailed will be determined in BatchScheduler.GetBatchTaskStateAsync()

                        var node = await pool.ListComputeNodes().ToAsyncEnumerable().FirstOrDefaultAsync(computeNodePredicate);

                        if (node is not null)
                        {
                            nodeState = node.State;
                            var nodeError = node.Errors?.FirstOrDefault();
                            nodeErrorCode = nodeError?.Code;
                            nodeErrorDetails = nodeError?.ErrorDetails?.Select(e => e.Value);
                        }
                    }
                    else
                    {
                        if (job.CreationTime.HasValue && DateTime.UtcNow.Subtract(job.CreationTime.Value) > TimeSpan.FromMinutes(30))
                        {
                            activeJobWithMissingAutoPool = true;
                        }
                    }
                }

                if (batchTask is not null)
                {
                    taskState = batchTask.State;
                    taskExecutionInformation = batchTask.ExecutionInformation;
                }

                return new AzureBatchJobAndTaskState
                {
                    MoreThanOneActiveJobOrTaskFound = false,
                    ActiveJobWithMissingAutoPool = activeJobWithMissingAutoPool,
                    AttemptNumber = attemptNumber,
                    NodeAllocationFailed = nodeAllocationFailed,
                    NodeErrorCode = nodeErrorCode,
                    NodeErrorDetails = nodeErrorDetails,
                    NodeState = nodeState,
                    JobState = job.State,
                    TaskState = taskState,
                    Pool = new() { PoolId = poolId },
                    TaskExecutionResult = taskExecutionInformation?.Result,
                    TaskStartTime = taskExecutionInformation?.StartTime,
                    TaskEndTime = taskExecutionInformation?.EndTime,
                    TaskExitCode = taskExecutionInformation?.ExitCode,
                    TaskFailureInformation = taskExecutionInformation?.FailureInformation,
                    TaskContainerState = taskExecutionInformation?.ContainerInformation?.State,
                    TaskContainerError = taskExecutionInformation?.ContainerInformation?.Error
                };
            }
            catch (Exception ex)
            {
                logger.LogError(ex, @"GetBatchJobAndTaskStateAsync failed for TesTask {TesTask}", tesTask.Id);
                throw;
            }
        }

        /// <inheritdoc/>
        public async Task DeleteBatchJobAsync(string tesTaskId, CancellationToken cancellationToken = default)
        {
            var jobFilter = new ODATADetailLevel
            {
                FilterClause = $"startswith(id,'{tesTaskId}{BatchJobAttemptSeparator}') and state ne 'deleting'",
                SelectClause = "id"
            };

            var batchJobsToDelete = await batchClient.JobOperations.ListJobs(jobFilter).ToListAsync(cancellationToken);

            if (batchJobsToDelete.Count > 1)
            {
                logger.LogWarning($"Found more than one active job for TES task {tesTaskId}");
            }

            foreach (var job in batchJobsToDelete)
            {
                logger.LogInformation($"Deleting job {job.Id}");
                await job.DeleteAsync(cancellationToken: cancellationToken);
            }
        }

        /// <inheritdoc/>
        public async Task DeleteBatchTaskAsync(string tesTaskId, PoolInformation pool, CancellationToken cancellationToken)
        {
            var jobFilter = new ODATADetailLevel
            {
                FilterClause = $"startswith(id,'{tesTaskId}{BatchJobAttemptSeparator}')",
                SelectClause = "id"
            };

            var batchTasksToDelete = await batchClient.JobOperations.ListTasks(pool.PoolId, jobFilter).ToListAsync(cancellationToken);

            if (batchTasksToDelete.Count > 1)
            {
                logger.LogWarning("Found more than one active task for TES task {TesTask}", tesTaskId);
            }

            foreach (var task in batchTasksToDelete)
            {
                logger.LogInformation("Deleting task {BatchTask}", task.Id);
                await task.DeleteAsync(cancellationToken: cancellationToken);
            }
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> ListOldJobsToDeleteAsync(TimeSpan oldestJobAge)
        {
            var filter = new ODATADetailLevel
            {
                FilterClause = $"state eq 'completed' and executionInfo/endTime lt DateTime'{DateTime.Today.Subtract(oldestJobAge):yyyy-MM-ddTHH:mm:ssZ}'",
                SelectClause = "id"
            };

            return (await batchClient.JobOperations.ListJobs(filter).ToListAsync()).Select(c => c.Id);
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> ListOrphanedJobsToDeleteAsync(TimeSpan minJobAge, CancellationToken cancellationToken = default)
        {
            var filter = new ODATADetailLevel
            {
                FilterClause = $"state eq 'active' and creationTime lt DateTime'{DateTime.UtcNow.Subtract(minJobAge):yyyy-MM-ddTHH:mm:ssZ}'",
                SelectClause = "id,poolInfo,onAllTasksComplete"
            };

            var noActionTesjobs = (await batchClient.JobOperations.ListJobs(filter).ToListAsync(cancellationToken))
                .Where(j => j.PoolInformation?.AutoPoolSpecification?.AutoPoolIdPrefix == "TES" && j.OnAllTasksComplete == OnAllTasksComplete.NoAction);

            var noActionTesjobsWithNoTasks = await noActionTesjobs.ToAsyncEnumerable().WhereAwait(async j => !(await j.ListTasks().ToListAsync(cancellationToken)).Any()).ToListAsync(cancellationToken);

            return noActionTesjobsWithNoTasks.Select(j => j.Id);
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> GetActivePoolIdsAsync(string prefix, TimeSpan minAge, CancellationToken cancellationToken = default)
        {
            var activePoolsFilter = new ODATADetailLevel
            {
                FilterClause = $"state eq 'active' and startswith(id, '{prefix}') and creationTime lt DateTime'{DateTime.UtcNow.Subtract(minAge):yyyy-MM-ddTHH:mm:ssZ}'",
                SelectClause = "id"
            };

            return (await batchClient.PoolOperations.ListPools(activePoolsFilter).ToListAsync(cancellationToken)).Select(p => p.Id);
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudPool> GetActivePoolsAsync(string hostName)
        {
            var activePoolsFilter = new ODATADetailLevel
            {
                FilterClause = "state eq 'active'",
                SelectClause = BatchPool.CloudPoolSelectClause
            };

            return batchClient.PoolOperations.ListPools(activePoolsFilter).ToAsyncEnumerable()
                .Where(p => hostName.Equals(p.Metadata?.FirstOrDefault(m => BatchScheduler.PoolHostName.Equals(m.Name, StringComparison.Ordinal))?.Value, StringComparison.OrdinalIgnoreCase));
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> GetPoolIdsReferencedByJobsAsync(CancellationToken cancellationToken = default)
            => (await batchClient.JobOperations.ListJobs(new ODATADetailLevel(selectClause: "executionInfo")).ToListAsync(cancellationToken))
                .Where(j => !string.IsNullOrEmpty(j.ExecutionInformation?.PoolId))
                .Select(j => j.ExecutionInformation.PoolId);

        /// <inheritdoc/>
        public Task DeleteBatchComputeNodesAsync(string poolId, IEnumerable<ComputeNode> computeNodes, CancellationToken cancellationToken = default)
            => batchClient.PoolOperations.RemoveFromPoolAsync(poolId, computeNodes, deallocationOption: ComputeNodeDeallocationOption.TaskCompletion, cancellationToken: cancellationToken);

        /// <inheritdoc/>
        public Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken = default)
            => batchPoolManager.DeleteBatchPoolAsync(poolId, cancellationToken: cancellationToken);

        /// <inheritdoc/>
        public async Task DeleteBatchPoolIfExistsAsync(string poolId, CancellationToken cancellationToken = default)
        {
            try
            {
                var poolFilter = new ODATADetailLevel
                {
                    FilterClause = $"startswith(id,'{poolId}') and state ne 'deleting'",
                    SelectClause = "id"
                };

                var poolsToDelete = await batchClient.PoolOperations.ListPools(poolFilter).ToListAsync(cancellationToken);

                foreach (var pool in poolsToDelete)
                {
                    logger.LogInformation($"Pool ID: {pool.Id} Pool State: {pool?.State} deleting...");
                    await batchClient.PoolOperations.DeletePoolAsync(pool.Id, cancellationToken: cancellationToken);
                }
            }
            catch (Exception exc)
            {
                var batchErrorCode = (exc as BatchException)?.RequestInformation?.BatchError?.Code;

                if (batchErrorCode?.Trim().Equals("PoolBeingDeleted", StringComparison.OrdinalIgnoreCase) == true)
                {
                    // Do not throw if it's a deletion race condition
                    // Docs: https://learn.microsoft.com/en-us/rest/api/batchservice/Pool/Delete?tabs=HTTP

                    return;
                }

                logger.LogError(exc, $"Pool ID: {poolId} exception while attempting to delete the pool.  Batch error code: {batchErrorCode}");
                throw;
            }
        }

        /// <inheritdoc/>
        public Task<CloudPool> GetBatchPoolAsync(string poolId, DetailLevel detailLevel = default, CancellationToken cancellationToken = default)
            => batchClient.PoolOperations.GetPoolAsync(poolId, detailLevel: detailLevel, cancellationToken: cancellationToken);

        /// <inheritdoc/>
        public Task<CloudJob> GetBatchJobAsync(string jobId, DetailLevel detailLevel, CancellationToken cancellationToken)
            => batchClient.JobOperations.GetJobAsync(jobId, detailLevel, cancellationToken: cancellationToken);

        /// <inheritdoc/>
        public async Task<(AllocationState? AllocationState, bool? AutoScaleEnabled, int? TargetLowPriority, int? CurrentLowPriority, int? TargetDedicated, int? CurrentDedicated)> GetFullAllocationStateAsync(string poolId, CancellationToken cancellationToken = default)
        {
            var pool = await batchClient.PoolOperations.GetPoolAsync(poolId, detailLevel: new ODATADetailLevel(selectClause: "allocationState,enableAutoScale,targetLowPriorityNodes,currentLowPriorityNodes,targetDedicatedNodes,currentDedicatedNodes"), cancellationToken: cancellationToken);
            return (pool.AllocationState, pool.AutoScaleEnabled, pool.TargetLowPriorityComputeNodes, pool.CurrentLowPriorityComputeNodes, pool.TargetDedicatedComputeNodes, pool.CurrentDedicatedComputeNodes);
        }

        private static async Task<IEnumerable<StorageAccountInfo>> GetAccessibleStorageAccountsAsync()
        {
            var azureClient = await GetAzureManagementClientAsync();

            var subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);

            return (await Task.WhenAll(
                subscriptionIds.Select(async subId =>
                    (await azureClient.WithSubscription(subId).StorageAccounts.ListAsync())
                        .Select(a => new StorageAccountInfo { Id = a.Id, Name = a.Name, SubscriptionId = subId, BlobEndpoint = a.EndPoints.Primary.Blob }))))
                .SelectMany(a => a)
                .ToList();
        }

        /// <inheritdoc/>
        public async Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo)
        {
            try
            {
                var azureClient = await GetAzureManagementClientAsync();
                var storageAccount = await azureClient.WithSubscription(storageAccountInfo.SubscriptionId).StorageAccounts.GetByIdAsync(storageAccountInfo.Id);

                return (await storageAccount.GetKeysAsync())[0].Value;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"An exception occurred when getting the storage account key for account {storageAccountInfo.Name}.");
                throw;
            }
        }

        /// <inheritdoc/>
        public Task UploadBlobAsync(Uri blobAbsoluteUri, string content)
            => new CloudBlockBlob(blobAbsoluteUri).UploadTextAsync(content);

        /// <inheritdoc/>
        public Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath)
            => new CloudBlockBlob(blobAbsoluteUri).UploadFromFileAsync(filePath);

        /// <inheritdoc/>
        public Task<string> DownloadBlobAsync(Uri blobAbsoluteUri)
            => new CloudBlockBlob(blobAbsoluteUri).DownloadTextAsync();

        /// <inheritdoc/>
        public Task<bool> BlobExistsAsync(Uri blobAbsoluteUri)
            => new CloudBlockBlob(blobAbsoluteUri).ExistsAsync();

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> ListBlobsAsync(Uri directoryUri)
        {
            var blob = new CloudBlockBlob(directoryUri);
            var directory = blob.Container.GetDirectoryReference(blob.Name);

            BlobContinuationToken continuationToken = null;
            var results = new List<string>();

            do
            {
                var response = await directory.ListBlobsSegmentedAsync(useFlatBlobListing: true, blobListingDetails: BlobListingDetails.None, maxResults: null, currentToken: continuationToken, options: null, operationContext: null);
                continuationToken = response.ContinuationToken;
                results.AddRange(response.Results.Cast<CloudBlob>().Select(b => b.Name));
            }
            while (continuationToken is not null);

            return results;
        }

        /// <inheritdoc/>
        public bool LocalFileExists(string path)
            => File.Exists(path);

        /// <inheritdoc/>
        public bool TryReadCwlFile(string workflowId, out string content)
        {
            var fileName = $"cwl_temp_file_{workflowId}.cwl";

            try
            {
                var filePath = Directory.GetFiles("/cromwell-tmp", fileName, SearchOption.AllDirectories).FirstOrDefault();

                if (filePath is not null)
                {
                    content = File.ReadAllText(filePath);
                    return true;
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"Error looking up or retrieving contents of CWL file '{fileName}'");
            }

            content = null;
            return false;
        }

        /// <inheritdoc />
        public string GetArmRegion()
            => location;

        private static Task<string> GetAzureAccessTokenAsync(string resource = "https://management.azure.com/")
            => new AzureServiceTokenProvider().GetAccessTokenAsync(resource);

        /// <summary>
        /// Gets an authenticated Azure Client instance
        /// </summary>
        /// <returns>An authenticated Azure Client instance</returns>
        private static async Task<FluentAzure.IAuthenticated> GetAzureManagementClientAsync()
        {
            var accessToken = await GetAzureAccessTokenAsync();
            var azureCredentials = new AzureCredentials(new TokenCredentials(accessToken), null, null, AzureEnvironment.AzureGlobalCloud);
            var azureClient = FluentAzure.Authenticate(azureCredentials);

            return azureClient;
        }

        /// <inheritdoc/>
        public async Task<PoolInformation> CreateBatchPoolAsync(BatchModels.Pool poolInfo, bool isPreemptable)
            => await batchPoolManager.CreateBatchPoolAsync(poolInfo, isPreemptable);

        // https://learn.microsoft.com/azure/azure-resource-manager/management/move-resource-group-and-subscription#changed-resource-id
        [GeneratedRegex("/*/resourceGroups/([^/]*)/*")]
        private static partial Regex GetResourceGroupRegex();

        private static async Task<(string SubscriptionId, string ResourceGroupName, string Location, string BatchAccountEndpoint)> FindBatchAccountAsync(string batchAccountName)
        {
            var resourceGroupRegex = GetResourceGroupRegex();
            var tokenCredentials = new TokenCredentials(await GetAzureAccessTokenAsync());
            var azureClient = await GetAzureManagementClientAsync();

            var subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);

            foreach (var subId in subscriptionIds)
            {
                var batchAccount = (await new BatchManagementClient(tokenCredentials) { SubscriptionId = subId }.BatchAccount.ListAsync())
                    .FirstOrDefault(a => a.Name.Equals(batchAccountName, StringComparison.OrdinalIgnoreCase));

                if (batchAccount is not null)
                {
                    var resourceGroupName = resourceGroupRegex.Match(batchAccount.Id).Groups[1].Value;

                    return (subId, resourceGroupName, batchAccount.Location, batchAccount.AccountEndpoint);
                }
            }

            throw new Exception($"Batch account '{batchAccountName}' does not exist or the TES app service does not have Contributor role on the account.");
        }

        /// <inheritdoc/>
        public async Task<StorageAccountInfo> GetStorageAccountInfoAsync(string storageAccountName)
            => (await GetAccessibleStorageAccountsAsync())
                .FirstOrDefault(storageAccount => storageAccount.Name.Equals(storageAccountName, StringComparison.OrdinalIgnoreCase));

        /// <inheritdoc/>
        public IAsyncEnumerable<ComputeNode> ListComputeNodesAsync(string poolId, DetailLevel detailLevel = null)
            => batchClient.PoolOperations.ListComputeNodes(poolId, detailLevel: detailLevel).ToAsyncEnumerable();

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudTask> ListTasksAsync(string jobId, DetailLevel detailLevel = null)
            => batchClient.JobOperations.ListTasks(jobId, detailLevel: detailLevel).ToAsyncEnumerable();

        /// <inheritdoc/>
        public async Task DisableBatchPoolAutoScaleAsync(string poolId, CancellationToken cancellationToken)
            => await batchClient.PoolOperations.DisableAutoScaleAsync(poolId, cancellationToken: cancellationToken);

        /// <inheritdoc/>
        public async Task EnableBatchPoolAutoScaleAsync(string poolId, bool preemptable, TimeSpan interval, IAzureProxy.BatchPoolAutoScaleFormulaFactory formulaFactory, CancellationToken cancellationToken)
        {
            var (allocationState, _, _, currentLowPriority, _, currentDedicated) = await GetFullAllocationStateAsync(poolId, cancellationToken);

            if (allocationState != AllocationState.Steady)
            {
                throw new InvalidOperationException();
            }

            await batchClient.PoolOperations.EnableAutoScaleAsync(poolId, formulaFactory(preemptable, preemptable ? currentLowPriority ?? 0 : currentDedicated ?? 0), interval, cancellationToken: cancellationToken);
        }
    }
}
