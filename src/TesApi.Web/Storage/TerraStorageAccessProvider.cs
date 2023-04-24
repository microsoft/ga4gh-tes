// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.WindowsAzure.Storage.Blob;
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
        public override Task<bool> IsPublicHttpUrlAsync(string uriString)
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
                if (IsTerraWorkspaceContainer(parts.ContainerName) && IsTerraWorkspaceStorageAccount(parts.AccountName))
                {
                    return Task.FromResult(false);
                }
            }

            return Task.FromResult(true);
        }

        /// <inheritdoc />
        public override async Task<string> MapLocalPathToSasUrlAsync(string path, bool getContainerSas = false, SharedAccessBlobPermissions permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Create | SharedAccessBlobPermissions.Write | SharedAccessBlobPermissions.List)
        {
            ArgumentException.ThrowIfNullOrEmpty(path);

            var normalizedPath = path;

            if (!TryParseHttpUrlFromInput(path, out _))
            {   // if it is a local path, add the leading slash if missing.
                normalizedPath = $"/{path.TrimStart('/')}";
            }

            if (getContainerSas)
            {
                return await MapAndGetSasContainerUrlFromWsmAsync(normalizedPath, permissions);
            }

            if (IsKnownExecutionFilePath(normalizedPath))
            {
                return await GetMappedSasUrlFromWsmAsync(normalizedPath, permissions);
            }

            if (!StorageAccountUrlSegments.TryCreate(normalizedPath, out var segments))
            {
                throw new Exception(
                    "Invalid path provided. The path must be a valid blob storage url or a path with the following format: /accountName/container");
            }

            CheckIfAccountAndContainerAreWorkspaceStorage(segments.AccountName, segments.ContainerName);

            return await GetMappedSasUrlFromWsmAsync(segments.BlobName, permissions);
        }

        private async Task<string> MapAndGetSasContainerUrlFromWsmAsync(string inputPath, SharedAccessBlobPermissions permissions)
        {
            if (IsKnownExecutionFilePath(inputPath))
            {
                return await GetMappedSasContainerUrlFromWsmAsync(inputPath, permissions);
            }

            if (!StorageAccountUrlSegments.TryCreate(inputPath, out var withContainerSegments))
            {
                throw new Exception(
                    "Invalid path provided. The path must be a valid blob storage url or a path with the following format: /accountName/container");
            }

            return await GetMappedSasContainerUrlFromWsmAsync(withContainerSegments.BlobName, permissions);
        }

        private async Task<string> GetMappedSasContainerUrlFromWsmAsync(string pathToAppend, SharedAccessBlobPermissions permissions)
        {
            //an empty blob name gets a container Sas token
            var tokenInfo = await GetSasTokenFromWsmAsync(CreateTokenParamsFromOptions(blobName: string.Empty, ConvertSharedAccessBlobPermissionsToString(permissions)));

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
        /// <param name="blobName">The name of the blob</param>
        /// <param name="permissions">The SAS permissions</param>
        /// <returns>SAS Token URL</returns>
        public async Task<string> GetMappedSasUrlFromWsmAsync(string blobName, SharedAccessBlobPermissions permissions)
        {
            var normalizedBlobName = blobName.TrimStart('/');

            var tokenParams = CreateTokenParamsFromOptions(normalizedBlobName, ConvertSharedAccessBlobPermissionsToString(permissions));

            var tokenInfo = await GetSasTokenFromWsmAsync(tokenParams);

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

        private async Task<WsmSasTokenApiResponse> GetSasTokenFromWsmAsync(SasTokenApiParameters tokenParams)
        {
            Logger.LogInformation(
                $"Getting Sas Url from Terra. Wsm resource id:{terraOptions.WorkspaceStorageContainerResourceId}");
            return await terraWsmApiClient.GetSasTokenAsync(
                Guid.Parse(terraOptions.WorkspaceId),
                Guid.Parse(terraOptions.WorkspaceStorageContainerResourceId),
                tokenParams);
        }

        private void CheckIfAccountAndContainerAreWorkspaceStorage(string accountName, string containerName)
        {
            if (!IsTerraWorkspaceStorageAccount(accountName))
            {
                throw new Exception($"The account name does not match the configuration for Terra.");
            }

            if (!IsTerraWorkspaceContainer(containerName))
            {
                throw new Exception($"The container name does not match the configuration for Terra");
            }
        }

        private bool IsTerraWorkspaceContainer(string value)
            => terraOptions.WorkspaceStorageContainerName.Equals(value, StringComparison.OrdinalIgnoreCase);

        private bool IsTerraWorkspaceStorageAccount(string value)
            => terraOptions.WorkspaceStorageAccountName.Equals(value, StringComparison.OrdinalIgnoreCase);

        private string ConvertSharedAccessBlobPermissionsToString(SharedAccessBlobPermissions permissions)
        {
            var result = new StringBuilder();

            if ((permissions & SharedAccessBlobPermissions.Read) == SharedAccessBlobPermissions.Read)
            {
                result.Append("r");
            }

            if ((permissions & SharedAccessBlobPermissions.Add) == SharedAccessBlobPermissions.Add)
            {
                result.Append("a");
            }

            if ((permissions & SharedAccessBlobPermissions.Create) == SharedAccessBlobPermissions.Create)
            {
                result.Append("c");
            }

            if ((permissions & SharedAccessBlobPermissions.Write) == SharedAccessBlobPermissions.Write)
            {
                result.Append("w");
            }

            if ((permissions & SharedAccessBlobPermissions.List) == SharedAccessBlobPermissions.List)
            {
                result.Append("l");
            }

            if ((permissions & SharedAccessBlobPermissions.Delete) == SharedAccessBlobPermissions.Delete)
            {
                result.Append("d");
            }

            return result.ToString();
        }
    }
}
