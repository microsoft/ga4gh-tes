// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TesApi.Web.Management.Clients;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Management.Models.Terra;

namespace TesApi.Web.Management.Batch
{
    /// <summary>
    /// 
    /// </summary>
    public class TerraBatchPoolManager : IBatchPoolManager
    {
        /// <summary>
        /// Metadata key for the entry that contains the terra wsm resource id. 
        /// </summary>
        public const string TerraResourceIdMetadataKey = "TerraBatchPoolResourceId";

        private const string CloningInstructionsCloneNothing = "COPY_NOTHING";
        private const string AccessScopeSharedAccess = "SHARED_ACCESS";
        private const string UserManaged = "USER";

        private readonly TerraWsmApiClient terraWsmApiClient;
        private readonly IMapper mapper;
        private readonly ILogger<TerraBatchPoolManager> logger;
        private readonly TerraOptions terraOptions;
        private readonly BatchAccountOptions batchAccountOptions;

        /// <summary>
        /// Provides batch pool created and delete operations via the Terra api. 
        /// </summary>
        /// <param name="terraWsmApiClient"></param>
        /// <param name="mapper"></param>
        /// <param name="batchAccountOptions"></param>
        /// <param name="logger"></param>
        /// <param name="terraOptions"></param>
        public TerraBatchPoolManager(TerraWsmApiClient terraWsmApiClient, IMapper mapper, IOptions<TerraOptions> terraOptions, IOptions<BatchAccountOptions> batchAccountOptions, ILogger<TerraBatchPoolManager> logger)
        {
            ArgumentNullException.ThrowIfNull(terraWsmApiClient);
            ArgumentNullException.ThrowIfNull(mapper);
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(terraOptions);
            ArgumentNullException.ThrowIfNull(batchAccountOptions);

            this.terraWsmApiClient = terraWsmApiClient;
            this.mapper = mapper;
            this.logger = logger;
            this.batchAccountOptions = batchAccountOptions.Value;
            this.terraOptions = terraOptions.Value;

            ValidateOptions();
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="poolInfo"></param>
        /// <param name="isPreemptable"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public async Task<PoolInformation> CreateBatchPoolAsync(Pool poolInfo, bool isPreemptable, CancellationToken cancellationToken)
        {
            var resourceId = Guid.NewGuid();
            var resourceName = $"TES-{resourceId}";

            var apiRequest = new ApiCreateBatchPoolRequest()
            {
                Common = new ApiCommon
                {
                    Name = resourceName,
                    Description = poolInfo.DisplayName,
                    CloningInstructions = CloningInstructionsCloneNothing,
                    AccessScope = AccessScopeSharedAccess,
                    ManagedBy = UserManaged,
                    ResourceId = resourceId
                },
                AzureBatchPool = mapper.Map<ApiAzureBatchPool>(poolInfo),
            };

            apiRequest.AzureBatchPool.Id = poolInfo.Name;

            AddResourceIdToPoolMetadata(apiRequest, resourceId);

            var response = await terraWsmApiClient.CreateBatchPool(Guid.Parse(terraOptions.WorkspaceId), apiRequest, cancellationToken);

            return new PoolInformation() { PoolId = response.AzureBatchPool.Attributes.Id };
        }

        private static void AddResourceIdToPoolMetadata(ApiCreateBatchPoolRequest apiRequest, Guid resourceId)
        {
            var resourceIdMetadataItem =
                new ApiBatchPoolMetadataItem() { Name = TerraResourceIdMetadataKey, Value = resourceId.ToString() };
            if (apiRequest.AzureBatchPool.Metadata is null)
            {
                apiRequest.AzureBatchPool.Metadata = new ApiBatchPoolMetadataItem[] { resourceIdMetadataItem };
                return;
            }

            var metadataList = apiRequest.AzureBatchPool.Metadata.ToList();

            metadataList.Add(resourceIdMetadataItem);

            apiRequest.AzureBatchPool.Metadata = metadataList.ToArray();
        }

        /// <summary>
        /// Deletes batch pool 
        /// </summary>
        /// <param name="poolId"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        public async Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrEmpty(poolId);

            try
            {
                logger.LogInformation(
                    $"Deleting pool with the ID/name: {poolId}");

                var wsmResourceId = await GetWsmResourceIdFromBatchPoolMetadataAsync(poolId, cancellationToken);

                await terraWsmApiClient.DeleteBatchPoolAsync(Guid.Parse(terraOptions.WorkspaceId), wsmResourceId, cancellationToken);

                logger.LogInformation(
                    $"Successfully deleted pool with the ID/name via WSM: {poolId}");
            }
            catch (Exception e)
            {
                logger.LogError(e, $"Error trying to delete pool named {poolId}");

                throw;
            }
        }

        private async Task<Guid> GetWsmResourceIdFromBatchPoolMetadataAsync(string poolId, CancellationToken cancellationToken)
        {
            var batchClient = CreateBatchClientFromOptions();

            var pool = await batchClient.PoolOperations.GetPoolAsync(poolId, cancellationToken: cancellationToken);

            if (pool is null)
            {
                throw new InvalidOperationException($"The Batch pool was not found. Pool ID: {poolId}");
            }

            var metadataItem = pool.Metadata.SingleOrDefault(m => m.Name.Equals(TerraResourceIdMetadataKey));

            if (string.IsNullOrEmpty(metadataItem?.Value))
            {
                throw new InvalidOperationException("The WSM resource ID was not found in the pool's metadata.");
            }

            var wsmResourceId = Guid.Parse(metadataItem.Value);
            return wsmResourceId;
        }

        private BatchClient CreateBatchClientFromOptions()
            => BatchClient.Open(new BatchSharedKeyCredentials(batchAccountOptions.BaseUrl,
                batchAccountOptions.AccountName, batchAccountOptions.AppKey));

        private void ValidateOptions()
        {
            ArgumentException.ThrowIfNullOrEmpty(terraOptions.WorkspaceId, nameof(terraOptions.WorkspaceId));
            ArgumentException.ThrowIfNullOrEmpty(batchAccountOptions.AccountName, nameof(batchAccountOptions.AccountName));
            ArgumentException.ThrowIfNullOrEmpty(batchAccountOptions.AppKey, nameof(batchAccountOptions.AppKey));
            ArgumentException.ThrowIfNullOrEmpty(batchAccountOptions.BaseUrl, nameof(batchAccountOptions.BaseUrl));
            ArgumentException.ThrowIfNullOrEmpty(batchAccountOptions.ResourceGroup, nameof(batchAccountOptions.ResourceGroup));
        }
    }
}
