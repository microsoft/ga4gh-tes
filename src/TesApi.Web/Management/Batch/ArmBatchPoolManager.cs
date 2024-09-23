// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.ResourceManager.Batch;
using Microsoft.Extensions.Logging;

namespace TesApi.Web.Management.Batch
{
    /// <summary>
    /// Provides management plane operations for Azure Batch Pools using ARM
    /// </summary>
    public class ArmBatchPoolManager : IBatchPoolManager
    {

        private readonly ILogger logger;
        private readonly AzureManagementClientsFactory azureClientsFactory;

        /// <summary>
        /// Constructor of ArmBatchPoolManager
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
        public async Task<string> CreateBatchPoolAsync(BatchAccountPoolData poolSpec, bool isPreemptable, CancellationToken cancellationToken)
        {
            var nameItem = poolSpec.Metadata.Single(i => string.IsNullOrEmpty(i.Name));

            try
            {
                poolSpec.Metadata.Remove(nameItem);

                var batchManagementClient = azureClientsFactory.CreateBatchAccountManagementClient();

                logger.LogInformation("Creating manual batch pool named {PoolName} with vmSize {PoolVmSize} and low priority {IsPreemptable}", nameItem.Value, poolSpec.VmSize, isPreemptable);

                _ = await batchManagementClient.GetBatchAccountPools().CreateOrUpdateAsync(Azure.WaitUntil.Completed, nameItem.Value, poolSpec, cancellationToken: cancellationToken);

                logger.LogInformation("Successfully created manual batch pool named {PoolName} with vmSize {PoolVmSize} and low priority {IsPreemptable}", nameItem.Value, poolSpec.VmSize, isPreemptable);

                return nameItem.Value;
            }
            catch (Exception exc)
            {
                var batchError = Newtonsoft.Json.JsonConvert.SerializeObject((exc as Microsoft.Azure.Batch.Common.BatchException)?.RequestInformation?.BatchError);
                logger.LogError(exc, "Error trying to create manual batch pool named {PoolName} with vmSize {PoolVmSize} and low priority {IsPreemptable}. Batch error: {BatchError}", nameItem.Value, poolSpec.VmSize, isPreemptable, batchError);
                throw;
            }
        }

        /// <inheritdoc />
        public async Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken = default)
        {
            try
            {
                var batchManagementClient = azureClientsFactory.CreateBatchAccountManagementClient();

                logger.LogInformation(
                    @"Deleting pool with the id/name:{PoolName} in Batch account:{BatchAccountName}", poolId, azureClientsFactory.BatchAccountInformation.Name);

                _ = await batchManagementClient.GetBatchAccountPools().Get(BatchAccountPoolResource.CreateResourceIdentifier(
                        azureClientsFactory.BatchAccountInformation.SubscriptionId, azureClientsFactory.BatchAccountInformation.ResourceGroupName,
                        azureClientsFactory.BatchAccountInformation.Name, poolId), cancellationToken: cancellationToken).Value
                    .DeleteAsync(Azure.WaitUntil.Completed, cancellationToken);

                logger.LogInformation(
                    @"Successfully deleted pool with the id/name:{PoolName} in Batch account:{BatchAccountName}", poolId, azureClientsFactory.BatchAccountInformation.Name);
            }
            catch (Exception e)
            {
                logger.LogError(e, @"Error trying to delete pool named {PoolName}", poolId);

                throw;
            }
        }
    }
}
