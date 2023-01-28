// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Management.Batch;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Extensions.Logging;

namespace TesApi.Web.Management.Batch
{
    /// <summary>
    /// 
    /// </summary>
    public class ArmBatchPoolManager : IBatchPoolManager
    {

        private readonly ILogger<ArmBatchPoolManager> logger;
        private readonly AzureManagementClientsFactory azureClientsFactory;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="azureClientsFactory"></param>
        /// <param name="logger"></param>
        public ArmBatchPoolManager(AzureManagementClientsFactory azureClientsFactory,
            ILogger<ArmBatchPoolManager> logger)
        {
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(azureClientsFactory);

            this.logger = logger;
            this.azureClientsFactory = azureClientsFactory;
        }

        /// <inheritdoc />
        public async Task<PoolInformation> CreateBatchPoolAsync(Pool poolInfo, bool isPreemptable)
        {
            try
            {
                var batchManagementClient = await azureClientsFactory.CreateBatchAccountManagementClient();

                logger.LogInformation($"Creating manual batch pool named {poolInfo.Name} with vmSize {poolInfo.VmSize} and low priority {isPreemptable}");

                var pool = await batchManagementClient.Pool.CreateAsync(azureClientsFactory.BatchAccountInformation.ResourceGroupName, azureClientsFactory.BatchAccountInformation.Name, poolInfo.Name, poolInfo);

                logger.LogInformation($"Successfully created manual batch pool named {poolInfo.Name} with vmSize {poolInfo.VmSize} and low priority {isPreemptable}");

                return new PoolInformation() { PoolId = pool.Name };
            }
            catch (Exception exc)
            {
                logger.LogError(exc, $"Error trying to create manual batch pool named {poolInfo.Name} with vmSize {poolInfo.VmSize} and low priority {isPreemptable}");
                throw;
            }
        }

        /// <inheritdoc />
        public async Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken = default)
        {
            try
            {
                var batchManagementClient = await azureClientsFactory.CreateBatchAccountManagementClient();

                logger.LogInformation(
                    $"Deleting pool with the id/name:{poolId} in Batch account:{azureClientsFactory.BatchAccountInformation.Name}");

                await batchManagementClient.Pool.DeleteWithHttpMessagesAsync(
                    azureClientsFactory.BatchAccountInformation.ResourceGroupName,
                    azureClientsFactory.BatchAccountInformation.Name, poolId, cancellationToken: cancellationToken);

                logger.LogInformation(
                    $"Successfully deleted pool with the id/name:{poolId} in Batch account:{azureClientsFactory.BatchAccountInformation.Name}");

            }
            catch (Exception e)
            {
                logger.LogError(e, $"Error trying to delete pool named {poolId}");

                throw;
            }
        }
    }
}
