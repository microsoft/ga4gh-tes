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
        public async Task<string> CreateBatchPoolAsync(Pool poolSpec, bool isPreemptable, CancellationToken cancellationToken)
        {
            try
            {
                var batchManagementClient = await azureClientsFactory.CreateBatchAccountManagementClient(cancellationToken);

                logger.LogInformation("Creating manual batch pool named {PoolName} with vmSize {PoolVmSize} and low priority {IsPreemptable}", poolSpec.Name, poolSpec.VmSize, isPreemptable);

                var pool = await batchManagementClient.Pool.CreateAsync(azureClientsFactory.BatchAccountInformation.ResourceGroupName, azureClientsFactory.BatchAccountInformation.Name, poolSpec.Name, poolSpec, cancellationToken: cancellationToken);

                logger.LogInformation("Successfully created manual batch pool named {PoolName} with vmSize {PoolVmSize} and low priority {IsPreemptable}", poolSpec.Name, poolSpec.VmSize, isPreemptable);

                return pool.Name;
            }
            catch (Exception exc)
            {
                var batchError = Newtonsoft.Json.JsonConvert.SerializeObject((exc as Microsoft.Azure.Batch.Common.BatchException)?.RequestInformation?.BatchError);
                logger.LogError(exc, "Error trying to create manual batch pool named {PoolName} with vmSize {PoolVmSize} and low priority {IsPreemptable}. Batch error: {BatchError}", poolSpec.Name, poolSpec.VmSize, isPreemptable, batchError);
                throw;
            }
        }

        /// <inheritdoc />
        public async Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken = default)
        {
            try
            {
                var batchManagementClient = await azureClientsFactory.CreateBatchAccountManagementClient(cancellationToken);

                logger.LogInformation(
                    @"Deleting pool with the id/name:{PoolName} in Batch account:{BatchAccountName}", poolId, azureClientsFactory.BatchAccountInformation.Name);

                _ = await batchManagementClient.Pool.DeleteAsync(
                    azureClientsFactory.BatchAccountInformation.ResourceGroupName,
                    azureClientsFactory.BatchAccountInformation.Name, poolId, cancellationToken: cancellationToken);

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
