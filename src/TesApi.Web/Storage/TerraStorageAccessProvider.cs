﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
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
            this.terraWsmApiClient = terraWsmApiClient;
            ArgumentNullException.ThrowIfNull(terraOptions);

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
        public override async Task<string> MapLocalPathToSasUrlAsync(string path, bool getContainerSas = false)
        {
            ArgumentException.ThrowIfNullOrEmpty(path);

            var normalizedPath = path;

            if (!TryParseHttpUrlFromInput(path, out _))
            {   // if it is a local path, add the leading slash if missing.
                normalizedPath = $"/{path.TrimStart('/')}";
            }

            if (getContainerSas)
            {
                return await MapAndGetSasContainerUrlFromWsmAsync(normalizedPath);
            }

            if (IsKnownExecutionFilePath(normalizedPath))
            {
                return await GetMappedSasUrlFromWsmAsync(normalizedPath);
            }

            if (!StorageAccountUrlSegments.TryCreate(normalizedPath, out var segments))
            {
                throw new Exception(
                    "Invalid path provided. The path must be a valid blob storage url or a path with the following format: /accountName/container");
            }

            CheckIfAccountAndContainerAreWorkspaceStorage(segments.AccountName, segments.ContainerName);

            return await GetMappedSasUrlFromWsmAsync(segments.BlobName);
        }

        private async Task<string> MapAndGetSasContainerUrlFromWsmAsync(string inputPath)
        {
            if (IsKnownExecutionFilePath(inputPath))
            {
                return await GetMappedSasContainerUrlFromWsmAsync(inputPath);
            }

            if (!StorageAccountUrlSegments.TryCreate(inputPath, out var withContainerSegments))
            {
                throw new Exception(
                    "Invalid path provided. The path must be a valid blob storage url or a path with the following format: /accountName/container");
            }

            return await GetMappedSasContainerUrlFromWsmAsync(withContainerSegments.BlobName);
        }

        private async Task<string> GetMappedSasContainerUrlFromWsmAsync(string pathToAppend)
        {
            //an empty blob name gets a container Sas token
            var tokenInfo = await GetSasTokenFromWsmAsync(CreateTokenParamsFromOptions(blobName: string.Empty, SasContainerPermissions));

            var urlBuilder = new UriBuilder(tokenInfo.Url);

            if (!string.IsNullOrEmpty(pathToAppend.TrimStart('/')))
            {
                urlBuilder.Path += $"/{pathToAppend.TrimStart('/')}";
            }

            return urlBuilder.Uri.ToString();
        }

        private async Task<string> GetMappedSasUrlFromWsmAsync(string blobName)
        {
            var normalizedBlobName = blobName.TrimStart('/');

            var tokenParams = CreateTokenParamsFromOptions(normalizedBlobName, SasBlobPermissions);

            var tokenInfo = await GetSasTokenFromWsmAsync(tokenParams);

            Logger.LogInformation($"Successfully obtained the Sas Url from Terra. Wsm resource id:{terraOptions.WorkspaceStorageContainerResourceId}");

            var uriBuilder = new UriBuilder(tokenInfo.Url);

            if (normalizedBlobName != string.Empty)
            {
                uriBuilder.Path += $"/{normalizedBlobName.TrimStart('/')}";
            }

            return uriBuilder.Uri.ToString();
        }

        private SasTokenApiParameters CreateTokenParamsFromOptions(string blobName, string sasPermissions)
        {
            return new SasTokenApiParameters(
                terraOptions.SasAllowedIpRange,
                terraOptions.SasTokenExpirationInSeconds,
                sasPermissions, blobName);
        }

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
        {
            return terraOptions.WorkspaceStorageContainerName.Equals(value, StringComparison.OrdinalIgnoreCase);
        }
        private bool IsTerraWorkspaceStorageAccount(string value)
        {
            return terraOptions.WorkspaceStorageAccountName.Equals(value, StringComparison.OrdinalIgnoreCase);
        }
    }
}
