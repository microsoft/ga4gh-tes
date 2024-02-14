// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TesApi.Web.Management.Configuration;

namespace TesApi.Web.Management.Batch
{

    /// <summary>
    /// Reads data from the pool metadata
    /// </summary>
    public class PoolMetadataReader
    {
        private readonly ILogger logger;
        private readonly TerraOptions terraOptions;
        private readonly IAzureProxy azureProxy;

        /// <summary>
        /// Parameter-less constructor of PoolMetadataReader
        /// </summary>
        protected PoolMetadataReader() { }

        /// <summary>
        /// Constructor of PoolMetadataReader
        /// </summary>
        /// <param name="terraOptions"><see cref="TerraOptions"/></param>
        /// <param name="azureProxy"></param>
        /// <param name="logger"><see cref="ILogger{TCategoryName}"/>></param>
        public PoolMetadataReader(IOptions<TerraOptions> terraOptions, IAzureProxy azureProxy, ILogger<PoolMetadataReader> logger)
        {
            ArgumentNullException.ThrowIfNull(azureProxy);
            ArgumentNullException.ThrowIfNull(terraOptions);
            ArgumentNullException.ThrowIfNull(logger);


            this.terraOptions = terraOptions.Value;
            this.azureProxy = azureProxy;
            this.logger = logger;

            ValidateOptions();
        }

        /// <summary>
        /// Gets a value from the Pool metadata.
        /// </summary>
        /// <param name="poolId">Pool id</param>
        /// <param name="key">Metadata key</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException">When pool is not found</exception>
        public virtual async ValueTask<string> GetMetadataValueAsync(string poolId, string key, CancellationToken cancellationToken)
        {
            logger.LogInformation(@"Getting metadata from pool {PoolId}. Key {MetadataKey}", poolId, key);

            var poolMetadata = (await azureProxy.GetBatchPoolAsync(poolId, cancellationToken: cancellationToken, new ODATADetailLevel { SelectClause = "metadata" }))?.Metadata;

            if (poolMetadata is null)
            {
                throw new InvalidOperationException($"Could not find pool with the id: {poolId}");
            }

            return poolMetadata.SingleOrDefault(m => m.Name.Equals(key))?.Value;
        }

        private void ValidateOptions()
        {
            ArgumentException.ThrowIfNullOrEmpty(terraOptions.WorkspaceId, nameof(terraOptions.WorkspaceId));
        }
    }
}
