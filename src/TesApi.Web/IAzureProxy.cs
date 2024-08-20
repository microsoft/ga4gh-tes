﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Tes.Models;
using TesApi.Web.Storage;
using BlobModels = Azure.Storage.Blobs.Models;

namespace TesApi.Web
{
    /// <summary>
    /// Interface for the Azure API wrapper
    /// </summary>
    public interface IAzureProxy
    {
        /// <summary>
        /// Creates a new Azure Batch job for <see cref="IBatchPool"/>
        /// </summary>
        /// <param name="jobId"></param>
        /// <param name="poolId"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        Task CreateBatchJobAsync(string jobId, string poolId, CancellationToken cancellationToken);

        /// <summary>
        /// Adds a task to the batch job <paramref name="jobId"/>."/>
        /// </summary>
        /// <param name="tesTaskId"></param>
        /// <param name="cloudTask"></param>
        /// <param name="jobId"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        Task AddBatchTaskAsync(string tesTaskId, CloudTask cloudTask, string jobId, CancellationToken cancellationToken);

        /// <summary>
        /// Terminates and deletes an Azure Batch job for <see cref="IBatchPool"/>
        /// </summary>
        /// <param name="jobId"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        Task DeleteBatchJobAsync(string jobId, CancellationToken cancellationToken);

        /// <summary>
        /// Gets the <see cref="StorageAccountInfo"/> for the given storage account name
        /// </summary>
        /// <param name="storageAccountName">Storage account name</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns><see cref="StorageAccountInfo"/></returns>
        Task<StorageAccountInfo> GetStorageAccountInfoAsync(string storageAccountName, CancellationToken cancellationToken);

        /// <summary>
        /// Gets the combined state of Azure Batch job, task and pool that corresponds to the given TES task
        /// </summary>
        /// <param name="tesTask">The TES task</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>Job state information</returns>
        Task<AzureBatchJobAndTaskState> GetBatchJobAndTaskStateAsync(TesTask tesTask, CancellationToken cancellationToken);

        /// <summary>
        /// Deletes an Azure Batch task
        /// </summary>
        /// <param name="taskId">The unique TES task ID</param>
        /// <param name="jobId"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        Task DeleteBatchTaskAsync(string taskId, string jobId, CancellationToken cancellationToken);

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
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>The primary key</returns>
        Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo, CancellationToken cancellationToken);

        /// <summary>
        /// Uploads the text content to a blob
        /// </summary>
        /// <param name="blobAbsoluteUri">Absolute Blob URI</param>
        /// <param name="content">Blob content</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>A task to await</returns>
        Task UploadBlobAsync(Uri blobAbsoluteUri, string content, CancellationToken cancellationToken);

        /// <summary>
        /// Uploads the file content to a blob
        /// </summary>
        /// <param name="blobAbsoluteUri">Absolute Blob URI</param>
        /// <param name="filePath">File path</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>A task to await</returns>
        Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath, CancellationToken cancellationToken);

        /// <summary>
        /// Downloads a blob
        /// </summary>
        /// <param name="blobAbsoluteUri">Absolute Blob URI</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>Blob content</returns>
        Task<string> DownloadBlobAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken);

        /// <summary>
        /// Check if a blob exists.
        /// </summary>
        /// <param name="blobAbsoluteUri">Absolute Blob URI</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>Blob exists boolean.</returns>
        Task<bool> BlobExistsAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken);

        /// <summary>
        /// Gets the list of blobs in the given directory
        /// </summary>
        /// <param name="directoryUri">Directory Uri</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>List of blob paths</returns>
        // TODO: change return to IAsyncEnumerable
        Task<IEnumerable<BlobModels.BlobItem>> ListBlobsAsync(Uri directoryUri, CancellationToken cancellationToken);

        /// <summary>
        /// Fetches the blobs properties
        /// </summary>
        /// <param name="blobAbsoluteUri">Absolute Blob URI</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        Task<BlobModels.BlobProperties> GetBlobPropertiesAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken);

        /// <summary>
        /// Gets the list of active pool ids matching the prefix and with creation time older than the minAge
        /// </summary>
        /// <param name="prefix"></param>
        /// <param name="minAge"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
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
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>Pool ids</returns>
        Task<IEnumerable<string>> GetPoolIdsReferencedByJobsAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Retrieves the specified pool
        /// </summary>
        /// <param name="poolId">The <see cref="CloudPool.Id"/> of the pool to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="detailLevel">A Microsoft.Azure.Batch.DetailLevel used for controlling which properties are retrieved from the service.</param>
        /// <returns><see cref="CloudPool"/></returns>
        Task<CloudPool> GetBatchPoolAsync(string poolId, CancellationToken cancellationToken, DetailLevel detailLevel = default);

        /// <summary>
        /// Retrieves the specified batch job.
        /// </summary>
        /// <param name="jobId">The <see cref="Microsoft.Azure.Batch.Protocol.Models.CloudJob"/> of the job to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="detailLevel">A Microsoft.Azure.Batch.DetailLevel used for controlling which properties are retrieved from the service.</param>
        /// <returns></returns>
        Task<CloudJob> GetBatchJobAsync(string jobId, CancellationToken cancellationToken, DetailLevel detailLevel = default);

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
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        Task DeleteBatchComputeNodesAsync(string poolId, IEnumerable<ComputeNode> computeNodes, CancellationToken cancellationToken);

        /// <summary>
        /// Gets the allocation state and numbers of targeted and current compute nodes
        /// </summary>
        /// <param name="poolId">The id of the pool.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        Task<FullBatchPoolAllocationState> GetFullAllocationStateAsync(string poolId, CancellationToken cancellationToken);

        /// <summary>
        /// Gets the configured arm region.
        /// </summary>
        /// <returns>arm region</returns>
        string GetArmRegion();

        /// <summary>
        /// Gets the managed identity in batch account resource group.
        /// </summary>
        /// <param name="identityName">Name of the identity.</param>
        /// <returns>Resource Id of the purported managed identity.</returns>
        string GetManagedIdentityInBatchAccountResourceGroup(string identityName);

        /// <summary>
        /// Disables AutoScale in a Batch Pool
        /// </summary>
        /// <param name="poolId">The id of the pool.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        Task DisableBatchPoolAutoScaleAsync(string poolId, CancellationToken cancellationToken);

        /// <summary>
        /// Enables AutoScale in a Batch Pool
        /// </summary>
        /// <param name="poolId">The id of the pool.</param>
        /// <param name="preemptable">Type of compute nodes: false if dedicated, otherwise true.</param>
        /// <param name="interval">The interval for periodic reevaluation of the formula.</param>
        /// <param name="formulaFactory">A factory function that generates an auto-scale formula.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
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
