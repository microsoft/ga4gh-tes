// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TesApi.Web.Management.Clients;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Management.Models.Terra;

namespace TesApi.Web.Storage
{
    /// <summary>
    /// Provides methods for blob storage access by using local path references in form of /storageaccount/container/blobpath
    /// </summary>
    public class TerraStorageAccessProvider : StorageAccessProvider
    {
        private readonly TerraOptions terraOptions;
        private readonly TerraWsmApiClient terraWsmApiClient;
        private const string SasBlobPermissions = "racw";
        private const string SasContainerPermissions = "racwl";

        /// <summary>
        /// Provides methods for blob storage access by using local path references in form of /storageaccount/container/blobpath
        /// for Terra
        /// </summary>
        /// <param name="logger">Logger <see cref="ILogger"/></param>
        /// <param name="terraOptions"><see cref="TerraOptions"/></param>
        /// <param name="azureProxy">Azure proxy <see cref="IAzureProxy"/></param>
        /// <param name="terraWsmApiClient"><see cref="terraWsmApiClient"/></param>
        public TerraStorageAccessProvider(ILogger<TerraStorageAccessProvider> logger,
            IOptions<TerraOptions> terraOptions, IAzureProxy azureProxy, TerraWsmApiClient terraWsmApiClient) : base(
            logger, azureProxy)
        {
            ArgumentNullException.ThrowIfNull(terraOptions);

            this.terraWsmApiClient = terraWsmApiClient;
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
        public override async Task<string> MapLocalPathToSasUrlAsync(string path, CancellationToken cancellationToken, bool getContainerSas = false)
        {
            ArgumentException.ThrowIfNullOrEmpty(path);

            var normalizedPath = path;

            if (!TryParseHttpUrlFromInput(path, out _))
            {   // if it is a local path, add the leading slash if missing.
                normalizedPath = $"/{path.TrimStart('/')}";
            }

            if (getContainerSas)
            {
                return await MapAndGetSasContainerUrlFromWsmAsync(normalizedPath, cancellationToken);
            }

            if (IsKnownExecutionFilePath(normalizedPath))
            {
                return await GetMappedSasUrlFromWsmAsync(normalizedPath, cancellationToken);
            }

            if (!StorageAccountUrlSegments.TryCreate(normalizedPath, out var segments))
            {
                throw new Exception(
                    "Invalid path provided. The path must be a valid blob storage url or a path with the following format: /accountName/container");
            }

            CheckIfAccountIsTerraStorageAccount(segments.AccountName);

            return await GetMappedSasUrlFromWsmAsync(segments.BlobName, cancellationToken);
        }

        public TerraBlobInfo GetTerraBlobInfo(string normalizedPath)
        {
            if (!StorageAccountUrlSegments.TryCreate(normalizedPath, out var segments))
            {
                throw new Exception(
                    "Invalid path provided. The path must be a valid blob storage url or a path with the following format: /accountName/container");
            }

            CheckIfAccountIsTerraStorageAccount(segments.AccountName);

            var workspaceId = ToWorkspaceId(segments.ContainerName);

            var wsmContainerResourceId = await GetWsmContainerResourceIdAsync(workspaceId, segments.ContainerName);

            return new TerraBlobInfo(workspaceId,wsmContainerResourceId, segments.BlobName);
        }

        private async Task<string> GetWsmContainerResourceIdAsync(string workspaceId, string segmentsContainerName)
        {
            //terraWsmApiClient.
        }

        private string ToWorkspaceId(string segmentsContainerName)
        {
            ArgumentException.ThrowIfNullOrEmpty(segmentsContainerName);

            var guidString = segmentsContainerName.Substring(3); // remove the sc- prefix

            var guid = Guid.Parse(guidString); // throws if not a guid

            return guid.ToString();
        }

        private async Task<string> MapAndGetSasContainerUrlFromWsmAsync(string inputPath, CancellationToken cancellationToken)
        {
            if (IsKnownExecutionFilePath(inputPath))
            {
                return await GetMappedSasContainerUrlFromWsmAsync(inputPath, cancellationToken);
            }

            if (!StorageAccountUrlSegments.TryCreate(inputPath, out var withContainerSegments))
            {
                throw new Exception(
                    "Invalid path provided. The path must be a valid blob storage url or a path with the following format: /accountName/container");
            }

            return await GetMappedSasContainerUrlFromWsmAsync(withContainerSegments.BlobName, cancellationToken);
        }

        private async Task<string> GetMappedSasContainerUrlFromWsmAsync(string pathToAppend, CancellationToken cancellationToken)
        {
            //an empty blob name gets a container Sas token
            var tokenInfo = await GetSasTokenFromWsmAsync(CreateTokenParamsFromOptions(blobName: string.Empty, SasContainerPermissions), cancellationToken);

            var urlBuilder = new UriBuilder(tokenInfo.Url);

            if (!string.IsNullOrEmpty(pathToAppend.TrimStart('/')))
            {
                urlBuilder.Path += $"/{pathToAppend.TrimStart('/')}";
            }

            return urlBuilder.Uri.ToString();
        }

        /// <summary>
        /// Returns a Url with a SAS token for the given input
        /// </summary>
        /// <param name="blobName"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>SAS Token URL</returns>
        public async Task<string> GetMappedSasUrlFromWsmAsync(string blobName, CancellationToken cancellationToken)
        {
            var normalizedBlobName = blobName.TrimStart('/');

            var tokenParams = CreateTokenParamsFromOptions(normalizedBlobName, SasBlobPermissions);

            var tokenInfo = await GetSasTokenFromWsmAsync(tokenParams, cancellationToken);

            Logger.LogInformation($"Successfully obtained the Sas Url from Terra. Wsm resource id:{terraOptions.WorkspaceStorageContainerResourceId}");

            var uriBuilder = new UriBuilder(tokenInfo.Url);

            if (normalizedBlobName != string.Empty)
            {
                if (!uriBuilder.Path.Contains(normalizedBlobName, StringComparison.OrdinalIgnoreCase))
                {
                    uriBuilder.Path += $"/{normalizedBlobName}";
                }
            }

            return uriBuilder.Uri.ToString();
        }

        private SasTokenApiParameters CreateTokenParamsFromOptions(string blobName, string sasPermissions)
            => new(
                terraOptions.SasAllowedIpRange,
                terraOptions.SasTokenExpirationInSeconds,
                sasPermissions, blobName);

        private async Task<WsmSasTokenApiResponse> GetSasTokenFromWsmAsync(SasTokenApiParameters tokenParams, CancellationToken cancellationToken)
        {
            Logger.LogInformation(
                $"Getting Sas Url from Terra. Wsm resource id:{terraOptions.WorkspaceStorageContainerResourceId}");
            return await terraWsmApiClient.GetSasTokenAsync(
                Guid.Parse(terraOptions.WorkspaceId),
                Guid.Parse(terraOptions.WorkspaceStorageContainerResourceId),
                tokenParams, cancellationToken);
        }

        private async Task<WsmSasTokenApiResponse> GetSasTokenFromWsmAsync(TerraBlobInfo blobInfo, CancellationToken cancellationToken)
        {
            var tokenParams = CreateTokenParamsFromOptions(blobInfo.BlobName, SasBlobPermissions);
            
            Logger.LogInformation(
                $"Getting Sas Url from Terra. Wsm workspace id:{terraOptions.WorkspaceStorageContainerResourceId}");

            return await terraWsmApiClient.GetSasTokenAsync(
                Guid.Parse(terraOptions.WorkspaceId),
                Guid.Parse(terraOptions.WorkspaceStorageContainerResourceId),
                tokenParams, cancellationToken);
        }


        private void CheckIfAccountIsTerraStorageAccount(string accountName)
        {
            if (!IsTerraWorkspaceStorageAccount(accountName))
            {
                throw new Exception($"The account name does not match the configuration for Terra.");
            }
        }

        private bool IsTerraWorkspaceContainer(string value)
            => terraOptions.WorkspaceStorageContainerName.Equals(value, StringComparison.OrdinalIgnoreCase);

        private bool IsTerraWorkspaceStorageAccount(string value)
            => terraOptions.WorkspaceStorageAccountName.Equals(value, StringComparison.OrdinalIgnoreCase);
    }

    public record TerraBlobInfo(string WorkspaceId, string WsmContainerResourceId, string BlobName);

}
