// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Tes.Models;
using TesApi.Web.Storage;
using BatchModels = Microsoft.Azure.Management.Batch.Models;

namespace TesApi.Web
{
    /// <summary>
    /// Interface for the Azure API wrapper
    /// </summary>
    public interface IAzureProxy
    {
        /// <summary>
        /// Gets a new Azure Batch job id to schedule another task
        /// </summary>
        /// <param name="tesTaskId">The unique TES task ID</param>
        /// <returns>The next logical, new Azure Batch job ID</returns>
        Task<string> GetNextBatchJobIdAsync(string tesTaskId);

        /// <summary>
        /// Creates a new Azure Batch job for Autopools
        /// </summary>
        /// <param name="jobId"></param>
        /// <param name="cloudTask"></param>
        /// <param name="poolInformation"></param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        Task CreateAutoPoolModeBatchJobAsync(string jobId, CloudTask cloudTask, PoolInformation poolInformation, CancellationToken cancellationToken = default);

        /// <summary>
        /// Creates a new Azure Batch job for <see cref="IBatchPool"/>
        /// </summary>
        /// <param name="poolInformation"></param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        Task CreateBatchJobAsync(PoolInformation poolInformation, CancellationToken cancellationToken = default);

        /// <summary>
        /// Adds a task to the batch job paired to the <paramref name="poolInformation"/>."/>
        /// </summary>
        /// <param name="tesTaskId"></param>
        /// <param name="cloudTask"></param>
        /// <param name="poolInformation"></param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        Task AddBatchTaskAsync(string tesTaskId, CloudTask cloudTask, PoolInformation poolInformation, CancellationToken cancellationToken = default);

        /// <summary>
        /// Terminates and deletes an Azure Batch job for <see cref="IBatchPool"/>
        /// </summary>
        /// <param name="poolInformation"></param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        Task DeleteBatchJobAsync(PoolInformation poolInformation, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the <see cref="StorageAccountInfo"/> for the given storage account name
        /// </summary>
        /// <param name="storageAccountName">Storage account name</param>
        /// <returns><see cref="StorageAccountInfo"/></returns>
        Task<StorageAccountInfo> GetStorageAccountInfoAsync(string storageAccountName);

        /// <summary>
        /// Creates an Azure Batch pool who's lifecycle must be manually managed
        /// </summary>
        /// <param name="poolInfo">Contains information about the pool. <see cref="BatchModels.ProxyResource.Name"/> becomes the <see cref="Microsoft.Azure.Batch.Protocol.Models.CloudPool.Id"/></param>
        /// <param name="isPreemptable">True if nodes in this pool will all be preemptable. False if nodes will all be dedicated.</param>
        Task<PoolInformation> CreateBatchPoolAsync(BatchModels.Pool poolInfo, bool isPreemptable);

        /// <summary>
        /// Gets the combined state of Azure Batch job, task and pool that corresponds to the given TES task
        /// </summary>
        /// <param name="tesTask">The TES task</param>
        /// <param name="usingAutoPools"></param>
        /// <returns>Job state information</returns>
        Task<AzureBatchJobAndTaskState> GetBatchJobAndTaskStateAsync(TesTask tesTask, bool usingAutoPools);

        /// <summary>
        /// Deletes an Azure Batch job for Autopools
        /// </summary>
        /// <param name="taskId">The unique TES task ID</param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        Task DeleteBatchJobAsync(string taskId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes an Azure Batch task
        /// </summary>
        /// <param name="taskId">The unique TES task ID</param>
        /// <param name="poolInformation"></param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        Task DeleteBatchTaskAsync(string taskId, PoolInformation poolInformation, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the counts of active batch nodes, grouped by VmSize
        /// </summary>
        /// <returns>Batch node counts</returns>
        IEnumerable<AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize();

        /// <summary>
        /// Gets the count of active batch pools
        /// </summary>
        /// <returns>Count of active batch pools</returns>
        int GetBatchActivePoolCount();

        /// <summary>
        /// Gets the count of active batch jobs
        /// </summary>
        /// <returns>Count of active batch jobs</returns>
        int GetBatchActiveJobCount();

        /// <summary>
        /// Gets the primary key of the given storage account.
        /// </summary>
        /// <param name="storageAccountInfo">Storage account info</param>
        /// <returns>The primary key</returns>
        Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo);

        /// <summary>
        /// Uploads the text content to a blob
        /// </summary>
        /// <param name="blobAbsoluteUri">Absolute Blob URI</param>
        /// <param name="content">Blob content</param>
        /// <returns>A task to await</returns>
        Task UploadBlobAsync(Uri blobAbsoluteUri, string content);

        /// <summary>
        /// Uploads the file content to a blob
        /// </summary>
        /// <param name="blobAbsoluteUri">Absolute Blob URI</param>
        /// <param name="filePath">File path</param>
        /// <returns>A task to await</returns>
        Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath);

        /// <summary>
        /// Downloads a blob
        /// </summary>
        /// <param name="blobAbsoluteUri">Absolute Blob URI</param>
        /// <returns>Blob content</returns>
        Task<string> DownloadBlobAsync(Uri blobAbsoluteUri);

        /// <summary>
        /// Check if a blob exists.
        /// </summary>
        /// <param name="blobAbsoluteUri">Absolute Blob URI</param>
        /// <returns>Blob exists boolean.</returns>
        Task<bool> BlobExistsAsync(Uri blobAbsoluteUri);

        /// <summary>
        /// Gets the list of blobs in the given directory
        /// </summary>
        /// <param name="directoryUri">Directory Uri</param>
        /// <returns>List of blob paths</returns>
        Task<IEnumerable<string>> ListBlobsAsync(Uri directoryUri);

        /// <summary>
        /// Gets the ids of completed Batch jobs older than specified timespan
        /// </summary>
        /// <param name="oldestJobAge"></param>
        /// <returns>List of Batch job ids</returns>
        Task<IEnumerable<string>> ListOldJobsToDeleteAsync(TimeSpan oldestJobAge);

        /// <summary>
        /// Gets the ids of orphaned Batch jobs older than specified timespan
        /// These jobs are active for prolonged period of time, have auto pool, NoAction termination option, and no tasks
        /// </summary>
        /// <param name="minJobAge"></param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>List of Batch job ids</returns>
        Task<IEnumerable<string>> ListOrphanedJobsToDeleteAsync(TimeSpan minJobAge, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the list of active pool ids matching the prefix and with creation time older than the minAge
        /// </summary>
        /// <param name="prefix"></param>
        /// <param name="minAge"></param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>Active pool ids</returns>
        Task<IEnumerable<string>> GetActivePoolIdsAsync(string prefix, TimeSpan minAge, CancellationToken cancellationToken);

        /// <summary>
        /// Gets the list of active pools matching the hostname in the metadata
        /// </summary>
        /// <param name="hostName"></param>
        /// <returns>List of <see cref="Microsoft.Azure.Batch.Protocol.Models.CloudPool"/> managed by the host.</returns>
        IAsyncEnumerable<CloudPool> GetActivePoolsAsync(string hostName);

        /// <summary>
        /// Gets the list of pool ids referenced by the jobs
        /// </summary>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>Pool ids</returns>
        Task<IEnumerable<string>> GetPoolIdsReferencedByJobsAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Deletes the specified pool
        /// </summary>
        /// <param name="poolId">The id of the pool.</param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes the specified pool if it exists
        /// </summary>
        Task DeleteBatchPoolIfExistsAsync(string poolId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Retrieves the specified pool
        /// </summary>
        /// <param name="poolId">The <see cref="CloudPool.Id"/> of the pool to retrieve.</param>
        /// <param name="detailLevel">A Microsoft.Azure.Batch.DetailLevel used for controlling which properties are retrieved from the service.</param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns><see cref="CloudPool"/></returns>
        Task<CloudPool> GetBatchPoolAsync(string poolId, DetailLevel detailLevel = default, CancellationToken cancellationToken = default);

        /// <summary>
        /// Retrieves the specified batch job.
        /// </summary>
        /// <param name="jobId">The <see cref="Microsoft.Azure.Batch.Protocol.Models.CloudJob"/> of the job to retrieve.</param>
        /// <param name="detailLevel">A Microsoft.Azure.Batch.DetailLevel used for controlling which properties are retrieved from the service.</param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        Task<CloudJob> GetBatchJobAsync(string jobId, DetailLevel detailLevel = default, CancellationToken cancellationToken = default);

        /// <summary>
        /// Commits all pending changes to this Microsoft.Azure.Batch.CloudPool to the Azure Batch service.
        /// </summary>
        /// <param name="pool">The <see cref="CloudPool"/> to change.</param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        Task CommitBatchPoolChangesAsync(CloudPool pool, CancellationToken cancellationToken = default);

        /// <summary>
        /// Lists compute nodes in batch pool <paramref name="poolId"/>
        /// </summary>
        /// <param name="poolId">The id of the pool.</param>
        /// <param name="detailLevel">A Microsoft.Azure.Batch.DetailLevel used for filtering the list and for controlling which properties are retrieved from the service.</param>
        /// <returns></returns>
        IAsyncEnumerable<ComputeNode> ListComputeNodesAsync(string poolId, DetailLevel detailLevel = null);

        /// <summary>
        /// Lists jobs in the batch account
        /// </summary>
        /// <param name="jobId">The job id (which is the pool id)</param>
        /// <param name="detailLevel">A Microsoft.Azure.Batch.DetailLevel used for filtering the list and for controlling which properties are retrieved from the service.</param>
        /// <returns></returns>
        IAsyncEnumerable<CloudTask> ListTasksAsync(string jobId, DetailLevel detailLevel = null);

        /// <summary>
        /// Deletes the specified ComputeNodes
        /// </summary>
        /// <param name="poolId">The id of the pool.</param>
        /// <param name="computeNodes">Enumerable list of <see cref="ComputeNode"/>s to delete.</param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        Task DeleteBatchComputeNodesAsync(string poolId, IEnumerable<ComputeNode> computeNodes, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the allocation state and numbers of targeted and current compute nodes
        /// </summary>
        /// <param name="poolId">The id of the pool.</param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        Task<(AllocationState? AllocationState, bool? AutoScaleEnabled, int? TargetLowPriority, int? CurrentLowPriority, int? TargetDedicated, int? CurrentDedicated)> GetFullAllocationStateAsync(string poolId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Checks if a local file exists
        /// </summary>
        /// <param name="path"></param>
        /// <returns>True if file was found</returns>
        bool LocalFileExists(string path);

        /// <summary>
        /// Reads the content of the Common Workflow Language (CWL) file associated with the parent workflow of the TES task
        /// </summary>
        /// <param name="workflowId">Parent workflow</param>
        /// <param name="content">Content of the file</param>
        /// <returns>True if file was found</returns>
        bool TryReadCwlFile(string workflowId, out string content);

        /// <summary>
        /// Gets the configured arm region.
        /// </summary>
        /// <returns>arm region</returns>
        string GetArmRegion();

        /// <summary>
        /// Disables AutoScale in a Batch Pool
        /// </summary>
        /// <param name="poolId">The id of the pool.</param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        Task DisableBatchPoolAutoScaleAsync(string poolId, CancellationToken cancellationToken);

        /// <summary>
        /// Enables AutoScale in a Batch Pool
        /// </summary>
        /// <param name="poolId">The id of the pool.</param>
        /// <param name="preemptable">Type of compute nodes: false if dedicated, otherwise true.</param>
        /// <param name="interval">The interval for periodic reevaluation of the formula.</param>
        /// <param name="formulaFactory">A factory function that generates an auto-scale formula.</param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        Task EnableBatchPoolAutoScaleAsync(string poolId, bool preemptable, TimeSpan interval, BatchPoolAutoScaleFormulaFactory formulaFactory, CancellationToken cancellationToken);

        /// <summary>
        /// Describes a function to generate autoscale formulas
        /// </summary>
        /// <param name="preemptable">Type of compute nodes: false if dedicated, otherwise true.</param>
        /// <param name="currentTarget">Current number of compute nodes.</param>
        /// <returns></returns>
        delegate string BatchPoolAutoScaleFormulaFactory(bool preemptable, int currentTarget);
    }
}
