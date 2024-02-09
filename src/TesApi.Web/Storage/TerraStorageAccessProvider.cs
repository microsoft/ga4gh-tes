// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tes.ApiClients;
using Tes.ApiClients.Models.Terra;
using Tes.Extensions;
using Tes.Models;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Management.Models.Terra;
using TesApi.Web.Options;

namespace TesApi.Web.Storage
{
    /// <summary>
    /// Provides methods for blob storage access for Terra
    /// </summary>
    public class TerraStorageAccessProvider : StorageAccessProvider
    {
        private readonly TerraOptions terraOptions;
        private readonly BatchSchedulingOptions batchSchedulingOptions;
        private readonly TerraWsmApiClient terraWsmApiClient;
        private const string SasBlobPermissions = "racw";
        private const string SasContainerPermissions = "racwl";
        private const string LzStorageAccountNamePattern = "lz[0-9a-f]*";

        /// <summary>
        /// Provides methods for blob storage access for Terra
        /// for Terra
        /// </summary>
        /// <param name="terraWsmApiClient"><see cref="TerraWsmApiClient"/></param>
        /// <param name="azureProxy">Azure proxy <see cref="IAzureProxy"/></param>
        /// <param name="terraOptions"><see cref="TerraOptions"/></param>
        /// <param name="batchSchedulingOptions"><see cref="BatchSchedulingOptions"/>></param>
        /// <param name="logger">Logger <see cref="ILogger"/></param>
        public TerraStorageAccessProvider(TerraWsmApiClient terraWsmApiClient, IAzureProxy azureProxy,
            IOptions<TerraOptions> terraOptions, IOptions<BatchSchedulingOptions> batchSchedulingOptions,
            ILogger<TerraStorageAccessProvider> logger) : base(
            logger, azureProxy)
        {
            ArgumentNullException.ThrowIfNull(terraOptions);
            ArgumentNullException.ThrowIfNull(batchSchedulingOptions);
            ArgumentNullException.ThrowIfNull(batchSchedulingOptions.Value.Prefix, nameof(batchSchedulingOptions.Value.Prefix));

            this.terraWsmApiClient = terraWsmApiClient;
            this.batchSchedulingOptions = batchSchedulingOptions.Value;
            this.terraOptions = terraOptions.Value;
        }

        /// <inheritdoc />
        public override Task<bool> IsPublicHttpUrlAsync(string uriString, CancellationToken _1)
        {
            var isHttpUrl = TryParseHttpUrlFromInput(uriString, out var uri);

            if (!isHttpUrl)
            {
                return Task.FromResult(false);
            }

            if (HttpUtility.ParseQueryString(uri.Query).Get("sig") is not null)
            {
                return Task.FromResult(true);
            }

            if (StorageAccountUrlSegments.TryCreate(uriString, out var parts))
            {
                if (IsTerraWorkspaceStorageAccount(parts.AccountName))
                {
                    return Task.FromResult(false);
                }
            }

            return Task.FromResult(true);
        }

        /// <inheritdoc />
        public override async Task<Uri> MapLocalPathToSasUrlAsync(string path, CancellationToken cancellationToken, TimeSpan? sasTokenDuration = default, bool getContainerSas = false)
        {
            ArgumentException.ThrowIfNullOrEmpty(path);
            if (sasTokenDuration is not null)
            {
                throw new ArgumentException("Terra does not support extended length SAS tokens.");
            }

            if (!TryParseHttpUrlFromInput(path, out _))
            {
                throw new InvalidOperationException("The path must be a valid HTTP URL");
            }

            var terraBlobInfo = await GetTerraBlobInfoFromContainerNameAsync(path, cancellationToken);

            if (getContainerSas)
            {
                return await GetMappedSasContainerUrlFromWsmAsync(terraBlobInfo, cancellationToken);
            }

            return await GetMappedSasUrlFromWsmAsync(terraBlobInfo, cancellationToken);
        }

        /// <inheritdoc />
        /// <remarks>
        /// The resulting URL contains the TES internal segments as a prefix to the blobPath.
        /// If the blobPath is not provided(empty), a container SAS token is generated.
        /// If the blobPath is provided, a SAS token to the blobPath prefixed with the TES internal segments is generated.
        /// </remarks>
        public override async Task<Uri> GetInternalTesBlobUrlAsync(string blobPath, CancellationToken cancellationToken)
        {
            var blobInfo = GetTerraBlobInfoForInternalTes(blobPath);

            if (string.IsNullOrEmpty(blobPath))
            {
                return await GetMappedSasContainerUrlFromWsmAsync(blobInfo, cancellationToken);
            }

            return await GetMappedSasUrlFromWsmAsync(blobInfo, cancellationToken);
        }

        /// <inheritdoc />
        /// <remarks>
        /// The resulting URL contains the TES task internal segments as a prefix to the blobPath.
        /// If the blobPath is not provided(empty), a container SAS token is generated.
        /// If the blobPath is provided, a SAS token to the blobPath prefixed with the TES task internal segments is generated.
        /// </remarks>
        public override async Task<Uri> GetInternalTesTaskBlobUrlAsync(TesTask task, string blobPath, CancellationToken cancellationToken)
        {
            var blobInfo = GetTerraBlobInfoForInternalTesTask(task, blobPath);

            if (string.IsNullOrEmpty(blobPath))
            {
                return await GetMappedSasContainerUrlFromWsmAsync(blobInfo, cancellationToken);
            }

            return await GetMappedSasUrlFromWsmAsync(blobInfo, cancellationToken);
        }

        /// <inheritdoc />
        public override Uri GetInternalTesTaskBlobUrlWithoutSasToken(TesTask task, string blobPath)
        {
            var blobInfo = GetTerraBlobInfoForInternalTesTask(task, blobPath);

            var blobName = blobInfo.BlobName;

            if (!string.IsNullOrWhiteSpace(blobName))
            {
                blobName = $"/{blobName.TrimStart('/')}";
            }

            //passing the resulting string through the builder to ensure that the path is properly encoded and valid
            var builder = new BlobUriBuilder(new($"https://{terraOptions.WorkspaceStorageAccountName}.blob.core.windows.net/{blobInfo.WsmContainerName.TrimStart('/')}/{blobInfo.BlobName.TrimStart('/')}"));

            return builder.ToUri();
        }

        /// <inheritdoc />
        public override Uri GetInternalTesBlobUrlWithoutSasToken(string blobPath)
        {
            var blobInfo = GetTerraBlobInfoForInternalTes(blobPath);

            var blobName = blobInfo.BlobName;

            if (!string.IsNullOrWhiteSpace(blobName))
            {
                blobName = $"/{blobName.TrimStart('/')}";
            }

            //passing the resulting string through the builder to ensure that the path is properly encoded and valid
            var builder = new BlobUriBuilder(new($"https://{terraOptions.WorkspaceStorageAccountName}.blob.core.windows.net/{blobInfo.WsmContainerName.TrimStart('/')}{blobName}"));

            return builder.ToUri();
        }

        private TerraBlobInfo GetTerraBlobInfoForInternalTes(string blobPath)
        {
            var internalPath = GetInternalTesPath();

            if (!string.IsNullOrEmpty(blobPath))
            {
                internalPath += $"/{blobPath.TrimStart('/')}";
            }
            return new TerraBlobInfo(Guid.Parse(terraOptions.WorkspaceId), Guid.Parse(terraOptions.WorkspaceStorageContainerResourceId), terraOptions.WorkspaceStorageContainerName, internalPath);
        }

        private string GetInternalTesPath()
        {
            return $"{batchSchedulingOptions.Prefix.Trim('/')}{TesExecutionsPathPrefix}";
        }

        private TerraBlobInfo GetTerraBlobInfoForInternalTesTask(TesTask task, string blobPath)
        {
            var internalPath = $"{GetInternalTesPath()}/{task.Id}";

            if (task.Resources != null && task.Resources.ContainsBackendParameterValue(TesResources.SupportedBackendParameters.internal_path_prefix))
            {
                internalPath = $"{task.Resources.GetBackendParameterValue(TesResources.SupportedBackendParameters.internal_path_prefix).Trim('/')}";
            }

            if (!string.IsNullOrEmpty(blobPath))
            {
                internalPath += $"/{blobPath.TrimStart('/')}";
            }
            return new TerraBlobInfo(Guid.Parse(terraOptions.WorkspaceId), Guid.Parse(terraOptions.WorkspaceStorageContainerResourceId), terraOptions.WorkspaceStorageContainerName, internalPath);
        }

        /// <summary>
        /// Creates a Terra Blob Info from the container name in the path. The path must be a Terra managed storage URL.
        /// This method assumes that the container name contains the workspace ID and validates that the storage container is a Terra workspace resource.
        /// The BlobName property contains the blob name segment without a leading slash.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>Returns a Terra Blob Info</returns>
        /// <exception cref="InvalidOperationException">This method will throw if the path is not a valid Terra blob storage url.</exception>
        private async Task<TerraBlobInfo> GetTerraBlobInfoFromContainerNameAsync(string path, CancellationToken cancellationToken)
        {
            if (!StorageAccountUrlSegments.TryCreate(path, out var segments))
            {
                throw new InvalidOperationException(
                    "Invalid path provided. The path must be a valid blob storage URL or a path with the following format: /accountName/container");
            }

            CheckIfAccountIsTerraStorageAccount(segments.AccountName);

            Logger.LogInformation($"Getting Workspace ID from the Container Name: {segments.ContainerName}");

            var workspaceId = ToWorkspaceId(segments.ContainerName);

            Logger.LogInformation($"Workspace ID to use: {segments.ContainerName}");

            var wsmContainerResourceId = await GetWsmContainerResourceIdAsync(workspaceId, segments.ContainerName, cancellationToken);

            return new TerraBlobInfo(workspaceId, wsmContainerResourceId, segments.ContainerName, segments.BlobName.TrimStart('/'));
        }

        private async Task<Guid> GetWsmContainerResourceIdAsync(Guid workspaceId, string containerName, CancellationToken cancellationToken)
        {
            Logger.LogInformation($"Getting container resource information from WSM. Workspace ID: {workspaceId} Container Name: {containerName}");

            try
            {
                //the goal is to get all containers, therefore the limit is set to 10000 which is a reasonable unreachable number of storage containers in a workspace.
                var response =
                    await terraWsmApiClient.GetContainerResourcesAsync(workspaceId, offset: 0, limit: 10000, cancellationToken);

                var metadata = response.Resources.Single(r =>
                    r.ResourceAttributes.AzureStorageContainer.StorageContainerName.Equals(containerName,
                        StringComparison.OrdinalIgnoreCase)).Metadata;

                Logger.LogInformation($"Found the resource id for storage container resource. Resource ID: {metadata.ResourceId} Container Name: {containerName}");

                return Guid.Parse(metadata.ResourceId);
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Failed to call WSM to obtain the storage container resource ID");
                throw;
            }
        }

        /// <summary>
        /// Returns the workspace ID from the container name.
        /// It assumes the container name is in the format sc-{workspaceId}
        /// </summary>
        /// <param name="segmentsContainerName"></param>
        /// <returns></returns>
        private Guid ToWorkspaceId(string segmentsContainerName)
        {
            try
            {
                ArgumentException.ThrowIfNullOrEmpty(segmentsContainerName);

                var guidString = segmentsContainerName.Substring(3); // remove the sc- prefix

                return Guid.Parse(guidString); // throws if not a guid
            }
            catch (Exception e)
            {
                Logger.LogError(e, $"Failed to get the workspace ID from the container name. The name provided is not a valid GUID. Container Name: {segmentsContainerName}");
                throw;
            }
        }

        private async Task<Uri> GetMappedSasContainerUrlFromWsmAsync(TerraBlobInfo blobInfo, CancellationToken cancellationToken)
        {
            var tokenInfo = await GetWorkspaceContainerSasTokenFromWsmAsync(blobInfo, cancellationToken);

            var urlBuilder = new UriBuilder(tokenInfo.Url);

            if (!string.IsNullOrEmpty(blobInfo.BlobName))
            {
                urlBuilder.Path += $"/{blobInfo.BlobName.TrimStart('/')}";
            }

            return urlBuilder.Uri;
        }

        /// <summary>
        /// Returns a Url with a SAS token for the given input
        /// </summary>
        /// <param name="blobInfo"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>URL with a SAS token</returns>
        public async Task<Uri> GetMappedSasUrlFromWsmAsync(TerraBlobInfo blobInfo, CancellationToken cancellationToken)
        {
            var tokenInfo = await GetWorkspaceBlobSasTokenFromWsmAsync(blobInfo, cancellationToken);

            Logger.LogInformation($"Successfully obtained the Sas Url from Terra. Wsm resource id:{terraOptions.WorkspaceStorageContainerResourceId}");

            var uriBuilder = new UriBuilder(tokenInfo.Url);

            if (blobInfo.BlobName != string.Empty)
            {
                if (!uriBuilder.Path.Contains(blobInfo.BlobName, StringComparison.OrdinalIgnoreCase))
                {
                    uriBuilder.Path += $"/{blobInfo.BlobName.TrimStart('/')}";
                }
            }

            return uriBuilder.Uri;
        }

        private SasTokenApiParameters CreateTokenParamsFromOptions(string blobName, string sasPermissions)
            => new(
                terraOptions.SasAllowedIpRange,
                terraOptions.SasTokenExpirationInSeconds,
                sasPermissions, blobName);


        private async Task<WsmSasTokenApiResponse> GetWorkspaceBlobSasTokenFromWsmAsync(TerraBlobInfo blobInfo, CancellationToken cancellationToken)
        {
            var tokenParams = CreateTokenParamsFromOptions(blobInfo.BlobName, SasBlobPermissions);

            Logger.LogInformation(
                $"Getting Sas Url from Terra. Wsm workspace id:{blobInfo.WorkspaceId}");

            return await terraWsmApiClient.GetSasTokenAsync(
                blobInfo.WorkspaceId,
                blobInfo.WsmContainerResourceId,
                tokenParams, cancellationToken);
        }

        private async Task<WsmSasTokenApiResponse> GetWorkspaceContainerSasTokenFromWsmAsync(TerraBlobInfo blobInfo, CancellationToken cancellationToken)
        {
            // an empty blob name gets a container Sas token
            var tokenParams = CreateTokenParamsFromOptions(blobName: "", SasContainerPermissions);

            Logger.LogInformation(
                $"Getting Sas container Url from Terra. Wsm workspace id:{blobInfo.WorkspaceId}");

            return await terraWsmApiClient.GetSasTokenAsync(
                blobInfo.WorkspaceId,
                blobInfo.WsmContainerResourceId,
                tokenParams, cancellationToken);
        }


        private void CheckIfAccountIsTerraStorageAccount(string accountName)
        {
            if (!IsTerraWorkspaceStorageAccount(accountName))
            {
                throw new InvalidOperationException($"The account name does not match the configuration for Terra.");
            }
        }

        private bool IsTerraWorkspaceStorageAccount(string value)
        {
            var match = Regex.Match(value, LzStorageAccountNamePattern);

            return match.Success;
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
