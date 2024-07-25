// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using Azure.ResourceManager.Batch;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tes.ApiClients;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Management.Models.Terra;

namespace TesApi.Web.Management.Batch
{
    /// <summary>
    /// Provides management plane operations for Azure Batch Pools using Terra
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

        private readonly Lazy<TerraWsmApiClient> terraWsmApiClient;
        private readonly IMapper mapper;
        private readonly ILogger<TerraBatchPoolManager> logger;
        private readonly TerraOptions terraOptions;
        private readonly PoolMetadataReader poolMetadataReader;

        /// <summary>
        /// Provides batch pool created and delete operations via the Terra api. 
        /// </summary>
        /// <param name="terraWsmApiClient"></param>
        /// <param name="mapper"></param>
        /// <param name="poolMetadata"></param>
        /// <param name="logger"></param>
        /// <param name="terraOptions"></param>
        public TerraBatchPoolManager(Lazy<TerraWsmApiClient> terraWsmApiClient, IMapper mapper, PoolMetadataReader poolMetadata, IOptions<TerraOptions> terraOptions, ILogger<TerraBatchPoolManager> logger)
        {
            ArgumentNullException.ThrowIfNull(terraWsmApiClient);
            ArgumentNullException.ThrowIfNull(mapper);
            ArgumentNullException.ThrowIfNull(poolMetadata);
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(terraOptions);

            this.terraWsmApiClient = terraWsmApiClient;
            this.mapper = mapper;
            this.poolMetadataReader = poolMetadata;
            this.logger = logger;
            this.terraOptions = terraOptions.Value;

            ValidateOptions();
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="poolSpec"></param>
        /// <param name="isPreemptable"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public async Task<string> CreateBatchPoolAsync(BatchAccountPoolData poolSpec, bool isPreemptable, CancellationToken cancellationToken)
        {
            var resourceId = Guid.NewGuid();
            var resourceName = $"TES-{resourceId}";
            var nameItem = poolSpec.Metadata.Single(i => string.IsNullOrEmpty(i.Name));
            poolSpec.Metadata.Remove(nameItem);

            // Workaround: WSM requires inboundNatPools to be set if networkConfiguration somehow included endpointConfiguration
            if (poolSpec.NetworkConfiguration is not null)
            {
                poolSpec.NetworkConfiguration.EndpointInboundNatPools ??= [];
            }

            ApiCreateBatchPoolRequest apiRequest = new()
            {
                Common = new ApiCommon
                {
                    Name = resourceName,
                    Description = poolSpec.DisplayName,
                    CloningInstructions = CloningInstructionsCloneNothing,
                    AccessScope = AccessScopeSharedAccess,
                    ManagedBy = UserManaged,
                    ResourceId = resourceId
                },
                AzureBatchPool = mapper.Map<ApiAzureBatchPool>(poolSpec),
            };

            apiRequest.AzureBatchPool.Id = nameItem.Value;

            AddResourceIdToPoolMetadata(apiRequest, resourceId);

            var response = await terraWsmApiClient.Value.CreateBatchPool(Guid.Parse(terraOptions.WorkspaceId), apiRequest, cancellationToken);

            return response.AzureBatchPool.Attributes.Id;
        }

        private static void AddResourceIdToPoolMetadata(ApiCreateBatchPoolRequest apiRequest, Guid resourceId)
        {
            ApiBatchPoolMetadataItem resourceIdMetadataItem = new()
            { Name = TerraResourceIdMetadataKey, Value = resourceId.ToString() };

            if (apiRequest.AzureBatchPool.Metadata is null)
            {
                apiRequest.AzureBatchPool.Metadata = [resourceIdMetadataItem];
                return;
            }

            var metadataList = apiRequest.AzureBatchPool.Metadata.ToList();

            metadataList.Add(resourceIdMetadataItem);

            apiRequest.AzureBatchPool.Metadata = [.. metadataList];
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
                    "Deleting pool with the ID/name: {PoolId}", poolId);

                var wsmResourceId = await GetWsmResourceIdFromBatchPoolMetadataAsync(poolId, cancellationToken);

                await terraWsmApiClient.Value.DeleteBatchPoolAsync(Guid.Parse(terraOptions.WorkspaceId), wsmResourceId, cancellationToken);

                logger.LogInformation(
                    "Successfully deleted pool with the ID/name via WSM: {PoolId}", poolId);
            }
            catch (Exception e)
            {
                logger.LogError(e, "Error trying to delete pool named {PoolId}", poolId);

                throw;
            }
        }

        private async Task<Guid> GetWsmResourceIdFromBatchPoolMetadataAsync(string poolId, CancellationToken cancellationToken)
        {
            var metadataItem = await poolMetadataReader.GetMetadataValueAsync(poolId, TerraResourceIdMetadataKey, cancellationToken);

            if (string.IsNullOrEmpty(metadataItem))
            {
                throw new InvalidOperationException("The WSM resource ID was not found in the pool's metadata.");
            }

            var wsmResourceId = Guid.Parse(metadataItem);
            return wsmResourceId;
        }

        private void ValidateOptions()
        {
            ArgumentException.ThrowIfNullOrEmpty(terraOptions.WorkspaceId, nameof(terraOptions.WorkspaceId));
        }
    }
}
