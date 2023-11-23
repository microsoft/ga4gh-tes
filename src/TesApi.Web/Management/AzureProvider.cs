// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.Extensions.Logging;
using Tes.ApiClients;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Base class of providers that require access to Azure management API with retry an caching capabilities
    /// </summary>
    public abstract class AzureProvider
    {
        private protected readonly CachingRetryHandler.ICachingAsyncPolicy CachingAsyncRetryPolicy;
        private protected readonly AzureManagementClientsFactory ManagementClientsFactory;
        private protected readonly ILogger Logger;

        /// <summary>
        /// Protected constructor AzureProvider
        /// </summary>
        /// <param name="cachingRetryHandler"><see cref="CachingAsyncRetryPolicy"/></param>
        /// <param name="managementClientsFactory"><see cref="ManagementClientsFactory"/></param>
        /// <param name="logger"><see cref="ILogger{TCategoryName}"/>></param>
        protected AzureProvider(CachingRetryHandler cachingRetryHandler, AzureManagementClientsFactory managementClientsFactory, ILogger logger)
        {
            ArgumentNullException.ThrowIfNull(cachingRetryHandler);
            ArgumentNullException.ThrowIfNull(managementClientsFactory);
            ArgumentNullException.ThrowIfNull(logger);

            this.ManagementClientsFactory = managementClientsFactory;
            this.Logger = logger;
            this.CachingAsyncRetryPolicy = cachingRetryHandler
                .RetryDefaultPolicyBuilder()
                .SetOnRetryBehavior(this.Logger)
                .AddCaching()
                .BuildAsync();
        }

        /// <summary>
        /// Protected parameter-less constructor
        /// </summary>
        protected AzureProvider() { }
    }
}
