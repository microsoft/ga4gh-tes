using System;
using System.Linq;
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
        private readonly ILogger<PoolMetadataReader> logger;
        private readonly TerraOptions terraOptions;
        private readonly BatchAccountOptions batchAccountOptions;
        private readonly BatchClient batchClient;

        /// <summary>
        /// Parameter-less constructor of PoolMetadataReader
        /// </summary>
        protected PoolMetadataReader() { }

        /// <summary>
        /// Constructor of PoolMetadataReader
        /// </summary>
        /// <param name="batchAccountOptions"><see cref="BatchAccountOptions"/></param>
        /// <param name="terraOptions"><see cref="TerraOptions"/></param>
        /// <param name="logger"><see cref="ILogger{TCategoryName}"/>></param>
        public PoolMetadataReader(IOptions<BatchAccountOptions> batchAccountOptions, IOptions<TerraOptions> terraOptions, ILogger<PoolMetadataReader> logger)
        {
            ArgumentNullException.ThrowIfNull(batchAccountOptions);
            ArgumentNullException.ThrowIfNull(terraOptions);
            ArgumentNullException.ThrowIfNull(logger);


            this.batchAccountOptions = batchAccountOptions.Value;
            this.terraOptions = terraOptions.Value;
            this.logger = logger;

            ValidateOptions();

            batchClient = CreateBatchClientFromOptions();
        }

        /// <summary>
        /// Gets a value from the Pool metadata.
        /// </summary>
        /// <param name="poolId">Pool id</param>
        /// <param name="key">Metadata key</param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException">When pool is not found</exception>
        public virtual string GetMetadataValue(string poolId, string key)
        {

            logger.LogInformation($"Getting metadata from pool {poolId}. Key {key}");

            var poolMetadata = batchClient.PoolOperations.GetPool(poolId)?.Metadata;

            if (poolMetadata is null)
            {
                throw new InvalidOperationException($"Could not find pool with the id: {poolId}");
            }

            return poolMetadata.SingleOrDefault(m => m.Name.Equals(key))?.Value;
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
