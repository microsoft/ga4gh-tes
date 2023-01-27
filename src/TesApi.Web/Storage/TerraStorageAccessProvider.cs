// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading.Tasks;
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
        private const int FirstSegment = 0;
        private const int SecondSegment = 1;
        private const string SasTokenPermissions = "racw";

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
            this.terraWsmApiClient = terraWsmApiClient;
            ArgumentNullException.ThrowIfNull(terraOptions);

            this.terraOptions = terraOptions.Value;

        }

        /// <inheritdoc />
        public override Task<bool> IsPublicHttpUrlAsync(string uriString)
        {
            //TODO: check if this assumption is correct
            //For Terra, if it is a Http/Https Url then it is public.
            return Task.FromResult(TryParseHttpUrlFromInput(uriString, out _));

        }

        /// <inheritdoc />
        public override async Task<string> MapLocalPathToSasUrlAsync(string path, bool getContainerSas = false)
        {
            ArgumentException.ThrowIfNullOrEmpty(path);

            var normalizedPath = path.TrimStart('/');

            if (IsItKnownFilePath(normalizedPath))
            {
                return await GetMappedSasUrlFromWsmAsync(normalizedPath);
            }

            CheckIfPathMatchesExpectedTerraLocation(normalizedPath);

            return await GetMappedSasUrlFromWsmAsync(RemoveStorageAndContainerSegments(normalizedPath));

        }

        private async Task<string> GetMappedSasUrlFromWsmAsync(string blobName)
        {
            var tokenParams = new SasTokenApiParameters(
                terraOptions.SasAllowedIpRange,
                terraOptions.SasTokenExpirationInSeconds,
                SasTokenPermissions, blobName);

            logger.LogInformation($"Getting Sas Url from Terra. Requested blobName:{blobName}. Wsm resource id:{terraOptions.WorkspaceStorageContainerResourceId}");

            var tokenInfo = await terraWsmApiClient.GetSasTokenAsync(
                 Guid.Parse(terraOptions.WorkspaceId),
                 Guid.Parse(terraOptions.WorkspaceStorageContainerResourceId),
                 tokenParams);


            logger.LogInformation($"Successfully obtained the Sas Url from Terra. Requested blobName:{blobName}. Wsm resource id:{terraOptions.WorkspaceStorageContainerResourceId}");

            return tokenInfo.Url;
        }

        private string RemoveStorageAndContainerSegments(string path)
        {
            var segments = path.Split('/');

            //removes the storage and container segments form the path, but keeps the additional segments
            return string.Join("/", segments.Skip(2).ToArray());
        }

        private void CheckIfPathMatchesExpectedTerraLocation(string path)
        {
            var segments = path.Split('/');

            var errorMsg =
                $"The path must contain the expected Terra storage location:{terraOptions.WorkspaceStorageAccountName}/{terraOptions.WorkspaceStorageContainerName}. " +
                $"Path provided:{path}.";

            if (segments.Length < 2)
            {
                throw new Exception("Invalid path provided. " + errorMsg);
            }

            if (!segments[FirstSegment].Equals(terraOptions.WorkspaceStorageAccountName, StringComparison.OrdinalIgnoreCase))
            {
                throw new Exception("The account name does not match. " + errorMsg);
            }

            if (!segments[SecondSegment].Equals(terraOptions.WorkspaceStorageContainerName, StringComparison.OrdinalIgnoreCase))
            {
                throw new Exception("The container name does not match. " + errorMsg);
            }
        }
    }
}
