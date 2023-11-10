// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Management.ContainerRegistry.Fluent;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tes.ApiClients;
using TesApi.Web.Management.Configuration;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Provides container registry information
    /// </summary>
    public class ContainerRegistryProvider : AzureProvider
    {
        /// <summary>
        /// If users give TES access to their container registry, it will take max 5 minutes until its available
        /// </summary>
        private readonly ContainerRegistryOptions options;
        private readonly string[] knownContainerRegistries = new string[] { "mcr.microsoft.com" };

        /// <summary>
        /// If true the auto-discovery is enabled.
        /// </summary>
        public bool IsAutoDiscoveryEnabled => options.AutoDiscoveryEnabled;

        /// <summary>
        /// Provides resource information about the container registries the TES service has access.
        /// </summary>
        /// <param name="containerRegistryOptions"></param>
        /// <param name="cachingRetryHandler"></param>
        /// <param name="logger"></param>
        /// <param name="azureManagementClientsFactory"></param>
        public ContainerRegistryProvider(IOptions<ContainerRegistryOptions> containerRegistryOptions, CachingRetryHandler cachingRetryHandler, AzureManagementClientsFactory azureManagementClientsFactory, ILogger<ContainerRegistryProvider> logger)
            : base(cachingRetryHandler, azureManagementClientsFactory, logger)
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
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>Container registry information, or null if auto-discovery is disabled or the repository was not found</returns>
        public virtual async Task<ContainerRegistryInfo> GetContainerRegistryInfoAsync(string imageName, CancellationToken cancellationToken)
        {
            if (!options.AutoDiscoveryEnabled || IsKnownOrDefaultContainerRegistry(imageName))
            {
                return null;
            }

            var containerRegistryInfo = CachingRetryHandler.AppCache.Get<ContainerRegistryInfo>($"{nameof(ContainerRegistryProvider)}:{imageName}");

            if (containerRegistryInfo is not null)
            {
                return containerRegistryInfo;
            }

            return await LookUpAndAddToCacheContainerRegistryInfoAsync(imageName, cancellationToken);
        }

        /// <summary>
        /// Checks if the image is a public image
        /// </summary>
        /// <param name="imageName">the name of the image, i.e. mcr.microsoft.com/ga4gh/tes or ubuntu</param>
        /// <returns>True if the image is expected to be publically available, otherwise false</returns>
        public bool IsImagePublic(string imageName)
        {
            var lastColon = imageName.LastIndexOf(':');
            var probableImageNameWithoutTag = lastColon == -1 ? imageName : imageName[0..lastColon];

            // mcr.microsoft.com = public
            // no domain specified = public
            var host = probableImageNameWithoutTag.Split('/', StringSplitOptions.RemoveEmptyEntries).First();

            if (host.Equals("mcr.microsoft.com", StringComparison.OrdinalIgnoreCase) || !host.Contains('.'))
            {
                return true;
            }

            return false;
        }

        private async Task<ContainerRegistryInfo> LookUpAndAddToCacheContainerRegistryInfoAsync(string imageName, CancellationToken cancellationToken)
        {
            var ctx = new Polly.Context();
            ctx.SetOnRetryHandler((exception, timespan, retryCount, correlationId) =>
                Logger.LogError(exception, @"Retrying in {Method} due to '{Message}': RetryCount: {RetryCount} RetryCount: {TimeSpan} CorrelationId: {CorrelationId}",
                    nameof(LookUpAndAddToCacheContainerRegistryInfoAsync), exception.Message, retryCount, timespan.ToString("c"), correlationId.ToString("D")));
            var repositories = await CachingRetryHandler.ExecuteWithRetryAsync(GetAccessibleContainerRegistriesAsync, cancellationToken: cancellationToken, context: ctx);

            var requestedRepo = repositories?.FirstOrDefault(reg =>
                reg.RegistryServer.Equals(imageName.Split('/').FirstOrDefault(), StringComparison.OrdinalIgnoreCase));

            if (requestedRepo is not null)
            {
                Logger.LogInformation(@"Requested repository: {DockerImage} was found.", imageName);
                CachingRetryHandler.AppCache.Set($"{nameof(ContainerRegistryProvider)}:{imageName}", requestedRepo, DateTimeOffset.UtcNow.AddHours(options.RegistryInfoCacheExpirationInHours));
            }
            else
            {
                Logger.LogWarning(@"The TES service did not find the requested repository: {DockerImage}", imageName);
            }

            return requestedRepo;
        }

        private bool IsKnownOrDefaultContainerRegistry(string imageName)
        {
            var parts = imageName.Split('/');

            if (parts.Length > 1)
            {
                return knownContainerRegistries.Any(r => r.Equals(parts[0], StringComparison.OrdinalIgnoreCase));
            }

            //one part means, it is using the default registry
            return parts.Length == 1;
        }

        /// <summary>
        /// Gets the list of container registries the TES service has access to
        /// </summary>
        /// <returns>List of container registries. null if the TES service does not have access if auto-discovery is enabled </returns>
        private async Task<IEnumerable<ContainerRegistryInfo>> GetAccessibleContainerRegistriesAsync(CancellationToken cancellationToken)
        {
            if (!options.AutoDiscoveryEnabled)
            {
                return null;
            }

            var azureClient = await ManagementClientsFactory.CreateAzureManagementClientAsync(cancellationToken);

            var subscriptionIds = (await azureClient.Subscriptions.ListAsync(cancellationToken: cancellationToken)).Select(s => s.SubscriptionId);

            var infos = new List<ContainerRegistryInfo>();

            Logger.LogInformation(@"GetAccessibleContainerRegistriesAsync() called.");

            foreach (var subId in subscriptionIds)
            {
                try
                {
                    var registries = (await azureClient.WithSubscription(subId).ContainerRegistries.ListAsync(cancellationToken: cancellationToken)).ToList();

                    Logger.LogInformation(@"Searching {SubscriptionId} for container registries.", subId);

                    foreach (var r in registries)
                    {
                        Logger.LogInformation(@"Found {RegistryName}. AdminUserEnabled: {RegistryAdminUserEnabled}", r.Name, r.AdminUserEnabled);

                        try
                        {
                            var server = await r.GetCredentialsAsync(cancellationToken);

                            var info = new ContainerRegistryInfo { RegistryServer = r.LoginServerUrl, Username = server.Username, Password = server.AccessKeys[AccessKeyType.Primary] };

                            infos.Add(info);
                        }
                        catch (Exception ex)
                        {
                            Logger.LogWarning(ex, @"TES service doesn't have permission to get credentials for registry {RegistryLoginServerUrl}.  Please verify that 'Admin user' is enabled in the 'Access Keys' area in the Azure Portal for this container registry.  Exception: ({ExceptionType}): {ExceptionMessage}", r.LoginServerUrl, ex.GetType().FullName, ex.Message);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogWarning(@"TES service doesn't have permission to list container registries in subscription {SubscriptionId}.  Exception: ({ExceptionType}): {ExceptionMessage}", subId, ex.GetType().FullName, ex.Message);
                }
            }

            Logger.LogInformation(@"GetAccessibleContainerRegistriesAsync() returning {Count} registries.", infos.Count);

            return infos;
        }
    }
}
