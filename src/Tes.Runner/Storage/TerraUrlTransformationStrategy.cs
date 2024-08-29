// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.RegularExpressions;
using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using CommonUtilities;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Tes.ApiClients;
using Tes.ApiClients.Models.Terra;
using Tes.Runner.Models;
using Tes.Runner.Transfer;
using TesApi.Web.Management.Models.Terra;

namespace Tes.Runner.Storage
{
    public partial class TerraUrlTransformationStrategy : IUrlTransformationStrategy
    {
        private const int MaxNumberOfContainerResources = 10000;
        private const string LzStorageAccountNamePattern = "lz[0-9a-f]*";

        private readonly TerraWsmApiClient terraWsmApiClient;
        private readonly TerraRuntimeOptions terraRuntimeOptions;
        private readonly ILogger<TerraUrlTransformationStrategy> logger = PipelineLoggerFactory.Create<TerraUrlTransformationStrategy>();
        private static IMemoryCache memoryCache = new MemoryCache(new MemoryCacheOptions());
        private readonly int cacheExpirationInSeconds;

        public TerraUrlTransformationStrategy(TerraRuntimeOptions terraRuntimeOptions, TokenCredential tokenCredential, AzureEnvironmentConfig azureCloudIdentityConfig, int cacheExpirationInSeconds = TerraConfigConstants.CacheExpirationInSeconds)
            : this(terraRuntimeOptions, TerraWsmApiClient.CreateTerraWsmApiClient(terraRuntimeOptions.WsmApiHost, tokenCredential, azureCloudIdentityConfig), cacheExpirationInSeconds)
        { }

        public TerraUrlTransformationStrategy(TerraRuntimeOptions terraRuntimeOptions, TerraWsmApiClient terraWsmApiClient, int cacheExpirationInSeconds = TerraConfigConstants.CacheExpirationInSeconds)
        {
            ArgumentNullException.ThrowIfNull(terraRuntimeOptions);
            ArgumentNullException.ThrowIfNull(terraWsmApiClient);

            this.terraWsmApiClient = terraWsmApiClient;
            this.terraRuntimeOptions = terraRuntimeOptions;
            this.cacheExpirationInSeconds = cacheExpirationInSeconds;
        }

        public async Task<Uri> TransformUrlWithStrategyAsync(string sourceUrl, BlobSasPermissions blobSasPermissions)
        {
            var blobUriBuilder = ToBlobUriBuilder(sourceUrl);

            if (!IsTerraWorkspaceStorageAccount(blobUriBuilder.AccountName))
            {
                logger.LogWarning("The URL provide is not a valid storage account for Terra. The resolution strategy won't be applied. Host: {Host}", blobUriBuilder.Host);
                return blobUriBuilder.ToUri();
            }

            if (BlobApiHttpUtils.UrlContainsSasToken(sourceUrl))
            {
                var uri = new Uri(sourceUrl);
                logger.LogWarning("The URL provided has SAS token. The resolution strategy won't be applied. Host: {Host}", uri.Host);

                return uri;
            }

            var blobInfo = await GetTerraBlobInfoFromContainerNameAsync(sourceUrl);

            return await GetMappedSasUrlFromWsmAsync(blobInfo, blobSasPermissions);
        }

        public static void ClearCache()
        {
            memoryCache.Dispose();
            memoryCache = new MemoryCache(new MemoryCacheOptions());
        }

        /// <summary>
        /// Returns a Url with a SAS token for the given input
        /// </summary>
        /// <param name="blobInfo"></param>
        /// <param name="blobSasPermissions"></param>
        /// <returns>URL with a SAS token</returns>
        private async Task<Uri> GetMappedSasUrlFromWsmAsync(TerraBlobInfo blobInfo, BlobSasPermissions blobSasPermissions)
        {
            var tokenInfo = await GetWorkspaceSasTokenFromWsmAsync(blobInfo, blobSasPermissions);

            logger.LogInformation("Successfully obtained the SAS URL from Terra. WSM resource ID:{ContainerResourceId}", blobInfo.WsmContainerResourceId);

            var uriBuilder = new UriBuilder(tokenInfo.Url);

            if (!string.IsNullOrWhiteSpace(blobInfo.BlobName))
            {
                if (!uriBuilder.Path.Contains(blobInfo.BlobName, StringComparison.OrdinalIgnoreCase))
                {
                    uriBuilder.Path += $"/{blobInfo.BlobName.TrimStart('/')}";
                }
            }

            return uriBuilder.Uri;
        }

        private async Task<WsmSasTokenApiResponse> GetWorkspaceSasTokenFromWsmAsync(TerraBlobInfo blobInfo, BlobSasPermissions sasBlobPermissions)
        {
            var tokenParams = CreateTokenParamsFromOptions(sasBlobPermissions);

            logger.LogInformation(
                "Getting SAS URL from Terra. WSM workspace ID:{WorkspaceId}", blobInfo.WorkspaceId);

            var cacheKey = $"{blobInfo.WorkspaceId}-{blobInfo.WsmContainerResourceId}-{tokenParams.SasPermission}";

            if (memoryCache.TryGetValue(cacheKey, out WsmSasTokenApiResponse? tokenInfo))
            {
                if (tokenInfo is null)
                {
                    throw new InvalidOperationException("The value retrieved from the cache is null");
                }

                logger.LogInformation("SAS URL found in cache. WSM resource ID:{ContainerResourceId}", blobInfo.WsmContainerResourceId);
                return tokenInfo;
            }

            var token = await terraWsmApiClient.GetSasTokenAsync(
                blobInfo.WorkspaceId,
                blobInfo.WsmContainerResourceId,
                tokenParams, CancellationToken.None);

            memoryCache.Set(cacheKey, token, TimeSpan.FromSeconds(cacheExpirationInSeconds));

            return token;
        }

        private SasTokenApiParameters CreateTokenParamsFromOptions(BlobSasPermissions sasPermissions)
        {
            return new(
                terraRuntimeOptions.SasAllowedIpRange ?? string.Empty, TerraConfigConstants.TokenExpirationInSeconds,
                //setting blob name to empty string to get a SAS token for the container
                ToWsmBlobSasPermissions(sasPermissions), SasBlobName: String.Empty);
        }

        private static string ToWsmBlobSasPermissions(BlobSasPermissions blobSasPermissions)
        {
            var permissions = string.Empty;
            if (blobSasPermissions.HasFlag(BlobSasPermissions.Read))
            {
                permissions += "r";
            }

            if (blobSasPermissions.HasFlag(BlobSasPermissions.Write) || blobSasPermissions.HasFlag(BlobSasPermissions.Add))
            {
                permissions += "w";
            }

            if (blobSasPermissions.HasFlag(BlobSasPermissions.Delete))
            {
                permissions += "d";
            }

            if (blobSasPermissions.HasFlag(BlobSasPermissions.Tag))
            {
                permissions += "t";
            }

            if (blobSasPermissions.HasFlag(BlobSasPermissions.List))
            {
                permissions += "l";
            }

            return permissions;
        }

        private static void CheckIfAccountIsTerraStorageAccount(string accountName)
        {
            if (!IsTerraWorkspaceStorageAccount(accountName))
            {
                throw new InvalidOperationException($"The account name does not match the expected naming convention for Terra.");
            }
        }

        [GeneratedRegex(LzStorageAccountNamePattern)]
        private static partial Regex LzStorageAccountNamePatternRegex();

        private static bool IsTerraWorkspaceStorageAccount(string value)
        {
            var match = LzStorageAccountNamePatternRegex().Match(value);

            return match.Success;
        }

        /// <summary>
        /// Creates a Terra Blob Info from the container name in the url. The url must be a Terra managed storage URL.
        /// This method assumes that the container name contains the workspace ID and validates that the storage container is a Terra workspace resource.
        /// The BlobName property contains the blob name segment without a leading slash.
        /// </summary>
        /// <param name="url"></param>
        /// <returns>Returns a Terra Blob Info</returns>
        /// <exception cref="InvalidOperationException">This method will throw if the url is not a valid Terra blob storage url.</exception>
        private async Task<TerraBlobInfo> GetTerraBlobInfoFromContainerNameAsync(string url)
        {
            var blobUriBuilder = ToBlobUriBuilder(url);

            CheckIfAccountIsTerraStorageAccount(blobUriBuilder.AccountName);

            logger.LogInformation("Getting Workspace ID from the Container Name: {BlobContainerName}", blobUriBuilder.BlobContainerName);

            var workspaceId = ToWorkspaceId(blobUriBuilder.BlobContainerName);

            logger.LogInformation("Workspace ID to use: {WorkspaceId}", workspaceId);

            var wsmContainerResourceId = await GetWsmContainerResourceIdFromCacheOrWsmAsync(workspaceId, blobUriBuilder.BlobContainerName);

            return new TerraBlobInfo(workspaceId, wsmContainerResourceId, blobUriBuilder.BlobContainerName, blobUriBuilder.BlobName.TrimStart('/'));
        }

        private BlobUriBuilder ToBlobUriBuilder(string url)
        {
            BlobUriBuilder blobUriBuilder;
            try
            {
                blobUriBuilder = new BlobUriBuilder(new Uri(url));
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to parse the URL. The URL provided is not a valid Blob URL: {Url}", url);
                throw;
            }

            return blobUriBuilder;
        }

        private Guid ToWorkspaceId(string segmentsContainerName)
        {
            try
            {
                ArgumentException.ThrowIfNullOrEmpty(segmentsContainerName);

                var guidString = segmentsContainerName[3..]; // remove the sc- prefix

                return Guid.Parse(guidString); // throws if not a guid
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to get the workspace ID from the container name. The name provided is not a valid GUID. Container Name: {BlobContainerName}", segmentsContainerName);
                throw;
            }
        }

        private async Task<Guid> GetWsmContainerResourceIdFromCacheOrWsmAsync(Guid workspaceId, string containerName)
        {
            logger.LogInformation("Getting container resource information from WSM. Workspace ID: {WorkspaceId} Container Name: {BlobContainerName}", workspaceId, containerName);

            try
            {
                var cacheKey = $"{workspaceId}-{containerName}";

                if (memoryCache.TryGetValue(cacheKey, out Guid wsmContainerResourceId))
                {
                    logger.LogInformation("Found the container resource ID in cache. Resource ID: {ContainerResourceId} Container Name: {BlobContainerName}", wsmContainerResourceId, containerName);
                    return wsmContainerResourceId;
                }

                //the goal is to get all containers, therefore the limit is set to MaxNumberOfContainerResources (10000) which is a reasonable unreachable number of storage containers in a workspace.

                var response =
                    await terraWsmApiClient.GetContainerResourcesAsync(workspaceId, offset: 0, limit: MaxNumberOfContainerResources, CancellationToken.None);

                var metadata = response.Resources.Single(r =>
                    r.ResourceAttributes.AzureStorageContainer.StorageContainerName.Equals(containerName,
                        StringComparison.OrdinalIgnoreCase)).Metadata;

                logger.LogInformation("Found the resource ID for storage container resource. Resource ID: {ContainerResourceId} Container Name: {BlobContainerName}", metadata.ResourceId, containerName);

                var resourceId = Guid.Parse(metadata.ResourceId);

                memoryCache.Set(cacheKey, resourceId, TimeSpan.FromSeconds(TerraConfigConstants.CacheExpirationInSeconds));

                return resourceId;
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to call WSM to obtain the storage container resource ID");
                throw;
            }
        }
    }

    /// <summary>
    /// Contains the Terra and Azure Storage container properties where the blob is contained. 
    /// </summary>
    /// <param name="WorkspaceId"></param>
    /// <param name="WsmContainerResourceId"></param>
    /// <param name="WsmContainerName"></param>
    /// <param name="BlobName"></param>
    public record TerraBlobInfo(Guid WorkspaceId, Guid WsmContainerResourceId, string WsmContainerName, string BlobName);
}
