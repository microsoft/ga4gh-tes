﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Management.ContainerRegistry.Fluent;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TesApi.Web.Management.Configuration;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Provides container registry information
    /// </summary>
    public class ContainerRegistryProvider : AzureProvider
    {
        private readonly ContainerRegistryOptions options;

        /// <summary>
        /// If true the auto-discovery is enabled.
        /// </summary>
        public bool IsAutoDiscoveryEnabled => options.AutoDiscoveryEnabled;

        /// <summary>
        /// Provides resource information about the container registries the TES service has access.
        /// </summary>
        /// <param name="containerRegistryOptions"></param>
        /// <param name="cacheAndRetryHandler"></param>
        /// <param name="logger"></param>
        /// <param name="azureManagementClientsFactory"></param>
        public ContainerRegistryProvider(IOptions<ContainerRegistryOptions> containerRegistryOptions, CacheAndRetryHandler cacheAndRetryHandler, AzureManagementClientsFactory azureManagementClientsFactory, ILogger<ContainerRegistryProvider> logger)
            : base(cacheAndRetryHandler, azureManagementClientsFactory, logger)
        {
            ArgumentNullException.ThrowIfNull(containerRegistryOptions);
            ArgumentNullException.ThrowIfNull(containerRegistryOptions.Value);

            this.options = containerRegistryOptions.Value;
        }

        /// <summary>
        /// Protected constructor
        /// </summary>
        protected ContainerRegistryProvider() { }

        /// <summary>
        /// Looks for the container registry information from the image name.
        /// </summary>
        /// <param name="imageName">Container image name</param>
        /// <returns>Container registry information, or null if auto-discovery is disabled or the repository was not found</returns>
        public virtual async Task<ContainerRegistryInfo> GetContainerRegistryInfoAsync(string imageName)
        {
            if (!options.AutoDiscoveryEnabled)
            {
                return null;
            }

            var containerRegistryInfo = CacheAndRetryHandler.AppCache.Get<ContainerRegistryInfo>(imageName);

            if (containerRegistryInfo is not null)
            {
                return containerRegistryInfo;
            }

            return await LookUpAndAddToCacheContainerRegistryInfoAsync(imageName);

        }

        private async Task<ContainerRegistryInfo> LookUpAndAddToCacheContainerRegistryInfoAsync(string imageName)
        {
            var repositories = await CacheAndRetryHandler.ExecuteWithRetryAsync(GetAccessibleContainerRegistriesAsync);

            var requestedRepo = repositories.FirstOrDefault(reg =>
                reg.RegistryServer.Equals(imageName.Split('/').FirstOrDefault(), StringComparison.OrdinalIgnoreCase));

            if (requestedRepo is not null)
            {
                Logger.LogInformation($"Requested repository: {imageName} was found.");

                CacheAndRetryHandler.AppCache.Add<ContainerRegistryInfo>(imageName, requestedRepo,
                    //I find kind of odd the Add method of the cache does not exposes the expiration directly as param as the GetOrAdd method does.
                    new MemoryCacheEntryOptions()
                    {
                        AbsoluteExpiration = DateTimeOffset.UtcNow.AddHours(options.RegistryInfoCacheExpirationInHours)
                    });

                return requestedRepo;
            }

            Logger.LogWarning($"The TES service did not find the requested repository: {imageName}");

            return null;
        }

        /// <summary>
        /// Gets the list of container registries the TES service has access to
        /// </summary>
        /// <returns>List of container registries. null if the TES service does not have access if auto-discovery is enabled </returns>
        private async Task<IEnumerable<ContainerRegistryInfo>> GetAccessibleContainerRegistriesAsync()
        {
            if (!options.AutoDiscoveryEnabled)
            {
                return null;
            }

            var azureClient = await ManagementClientsFactory.CreateAzureManagementClientAsync();

            var subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);

            var infos = new List<ContainerRegistryInfo>();

            Logger.LogInformation(@"GetAccessibleContainerRegistriesAsync() called.");

            foreach (var subId in subscriptionIds)
            {
                try
                {
                    var registries = (await azureClient.WithSubscription(subId).ContainerRegistries.ListAsync()).ToList();

                    Logger.LogInformation(@$"Searching {subId} for container registries.");

                    foreach (var r in registries)
                    {
                        Logger.LogInformation(@$"Found {r.Name}. AdminUserEnabled: {r.AdminUserEnabled}");

                        try
                        {
                            var server = await r.GetCredentialsAsync();

                            var info = new ContainerRegistryInfo { RegistryServer = r.LoginServerUrl, Username = server.Username, Password = server.AccessKeys[AccessKeyType.Primary] };

                            infos.Add(info);
                        }
                        catch (Exception ex)
                        {
                            Logger.LogWarning($"TES service doesn't have permission to get credentials for registry {r.LoginServerUrl}.  Please verify that 'Admin user' is enabled in the 'Access Keys' area in the Azure Portal for this container registry.  Exception: {ex}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogWarning($"TES service doesn't have permission to list container registries in subscription {subId}.  Exception: {ex}");
                }
            }

            Logger.LogInformation(@"GetAccessibleContainerRegistriesAsync() returning {Count} registries.", infos.Count);

            return infos;
        }

    }
}
