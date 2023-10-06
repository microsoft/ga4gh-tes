// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
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

        private readonly ILogger<ArmBatchPoolManager> logger;
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
        public async Task<string> CreateBatchPoolAsync(Pool poolInfo, bool isPreemptable, CancellationToken cancellationToken)
        {
            try
            {
                var batchManagementClient = await azureClientsFactory.CreateBatchAccountManagementClient(cancellationToken);

                var pool = await batchManagementClient.Pool.CreateAsync(azureClientsFactory.BatchAccountInformation.ResourceGroupName, azureClientsFactory.BatchAccountInformation.Name, poolInfo.Name, poolInfo, cancellationToken: cancellationToken);

                return pool.Name;
            }
            catch (Exception exc)
            {
                var batchError = Newtonsoft.Json.JsonConvert.SerializeObject((exc as Microsoft.Azure.Batch.Common.BatchException)?.RequestInformation?.BatchError);
                logger.LogError(exc, "Error trying to create batch pool named {PoolName} with vmSize {PoolVmSize} and low priority {IsPreemptable}. Batch error: {BatchError}", poolInfo.Name, poolInfo.VmSize, isPreemptable, batchError);
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
                    $"Deleting pool with the id/name:{poolId} in Batch account:{azureClientsFactory.BatchAccountInformation.Name}");

                await batchManagementClient.Pool.DeleteWithHttpMessagesAsync(
                    azureClientsFactory.BatchAccountInformation.ResourceGroupName,
                    azureClientsFactory.BatchAccountInformation.Name, poolId, cancellationToken: cancellationToken);

                logger.LogInformation(
                    $"Successfully deleted pool with the id/name:{poolId} in Batch account:{azureClientsFactory.BatchAccountInformation.Name}");

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

                logger.LogError(exc, "Error trying to delete pool named {PoolId}", poolId);

                throw;
            }
        }
    }
}
