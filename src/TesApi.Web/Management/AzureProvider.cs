using System;
using Microsoft.Extensions.Logging;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Base class of providers that require access to Azure management API with retry an caching capabilities 
    /// </summary>
    public abstract class AzureProvider
    {
        private protected readonly CacheAndRetryHandler CacheAndRetryHandler;
        private protected readonly AzureManagementClientsFactory ManagementClientsFactory;
        private protected readonly ILogger Logger;

        /// <summary>
        /// Protected constructor AzureProvider
        /// </summary>
        /// <param name="cacheAndRetryHandler"><see cref="CacheAndRetryHandler"/></param>
        /// <param name="managementClientsFactory"><see cref="ManagementClientsFactory"/></param>
        /// <param name="logger"><see cref="ILogger{TCategoryName}"/>></param>
        protected AzureProvider(CacheAndRetryHandler cacheAndRetryHandler, AzureManagementClientsFactory managementClientsFactory, ILogger logger)
        {
            ArgumentNullException.ThrowIfNull(cacheAndRetryHandler);
            ArgumentNullException.ThrowIfNull(managementClientsFactory);
            ArgumentNullException.ThrowIfNull(logger);

            this.CacheAndRetryHandler = cacheAndRetryHandler;
            this.ManagementClientsFactory = managementClientsFactory;
            this.Logger = logger;
        }

        /// <summary>
        /// Protected parameter-less constructor 
        /// </summary>
        protected AzureProvider() { }
    }
}
