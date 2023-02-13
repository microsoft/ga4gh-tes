// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
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
        private const string CloningInstructionsCloneNothing = "COPY_NOTHING";
        private const string AccessScopeSharedAccess = "SHARED_ACCESS";
        private const string UserManaged = "USER";
        private readonly string tesBatchPoolName = Guid.NewGuid().ToString();

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
        /// <returns></returns>
        public async Task<PoolInformation> CreateBatchPoolAsync(Pool poolInfo, bool isPreemptable)
        {
            var apiRequest = new ApiCreateBatchPoolRequest()
            {
                Common = new ApiCommon
                {
                    Name = tesBatchPoolName,
                    Description = poolInfo.DisplayName,
                    CloningInstructions = CloningInstructionsCloneNothing,
                    AccessScope = AccessScopeSharedAccess,
                    ManagedBy = UserManaged,
                    ResourceId = Guid.NewGuid()
                },
                AzureBatchPool = mapper.Map<ApiAzureBatchPool>(poolInfo),
            };

            apiRequest.AzureBatchPool.Id = poolInfo.Name;

            var response = await terraWsmApiClient.CreateBatchPool(Guid.Parse(terraOptions.WorkspaceId), apiRequest);

            return new PoolInformation() { PoolId = response.AzureBatchPool.Attributes.Id };
        }

        /// <summary>
        /// Deletes batch pool 
        /// </summary>
        /// <param name="poolId"></param>
        /// <param name="cancellationToken"></param>
        public async Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrEmpty(poolId);

            //TODO: This is a temporary implementation. It must be changed once WSM supports deleting batch pools
            try
            {
                var batchClient = CreateBatchClientFromOptions();

                logger.LogInformation(
                    $"Deleting pool with the id/name:{poolId}");

                await batchClient.PoolOperations.DeletePoolAsync(poolId, cancellationToken: cancellationToken);

                logger.LogInformation(
                    $"Successfully deleted pool with the id/name:{poolId}");

            }
            catch (Exception e)
            {
                logger.LogError(e, $"Error trying to delete pool named {poolId}");

                throw;
            }
        }

        private BatchClient CreateBatchClientFromOptions()
        {
            return BatchClient.Open(new BatchSharedKeyCredentials(batchAccountOptions.BaseUrl,
                batchAccountOptions.AccountName, batchAccountOptions.AppKey));
        }

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
