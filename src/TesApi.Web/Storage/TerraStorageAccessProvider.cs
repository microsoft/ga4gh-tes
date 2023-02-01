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
        public override async Task<string> MapLocalPathToSasUrlAsync(
        string path, bool
        getContainerSas = false,
        [System.Runtime.CompilerServices.CallerMemberName] string memberName = "",
        [System.Runtime.CompilerServices.CallerFilePath] string sourceFilePath = "",
        [System.Runtime.CompilerServices.CallerLineNumber] int sourceLineNumber = 0)
        {
            ArgumentException.ThrowIfNullOrEmpty(path);

            var normalizedPath = path.TrimStart('/');

            if (getContainerSas)
            {
                return await GetMappedSasContainerUrlFromWsmAsync(normalizedPath);
            }

            if (IsItKnownExecutionFilePath(normalizedPath))
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

        private async Task<string> GetMappedSasContainerUrlFromWsmAsync(string pathToAppend)
        {
            //an empty blob name gets a container Sas token
            var tokenInfo = await GetSasTokenFromWsmAsync(CreateTokenParamsFromOptions(blobName: string.Empty, SasContainerPermissions));

            var urlBuilder = new UriBuilder(tokenInfo.Url);

            urlBuilder.Path += pathToAppend;

            return urlBuilder.ToString();
        }

        private async Task<string> GetMappedSasUrlFromWsmAsync(string blobName)
        {
            var tokenParams = CreateTokenParamsFromOptions(blobName, SasBlobPermissions);

            var tokenInfo = await GetSasTokenFromWsmAsync(tokenParams);

            logger.LogInformation($"Successfully obtained the Sas Url from Terra. Requested blobName:{blobName}. Wsm resource id:{terraOptions.WorkspaceStorageContainerResourceId}");

            return tokenInfo.Url;
        }

        private SasTokenApiParameters CreateTokenParamsFromOptions(string blobName, string sasPermissions)
            => new(
                terraOptions.SasAllowedIpRange,
                terraOptions.SasTokenExpirationInSeconds,
                sasPermissions, blobName);

        private async Task<WsmSasTokenApiResponse> GetSasTokenFromWsmAsync(SasTokenApiParameters tokenParams)
        {
            logger.LogInformation(
                $"Getting Sas Url from Terra. Requested blobName:{tokenParams.SasBlobName}. Wsm resource id:{terraOptions.WorkspaceStorageContainerResourceId}");

            return await terraWsmApiClient.GetSasTokenAsync(
                Guid.Parse(terraOptions.WorkspaceId),
                Guid.Parse(terraOptions.WorkspaceStorageContainerResourceId),
                tokenParams);
        }

        private void CheckIfAccountAndContainerAreWorkspaceStorage(string accountName, string containerName)
        {
            if (!IsTerraWorkspaceStorageAccount(accountName))
            {
                throw new Exception($"The account name does not match. Expected:{accountName} and Provided{terraOptions.WorkspaceStorageAccountName}");
            }

            if (!IsTerraWorkspaceContainer(containerName))
            {
                throw new Exception($"The container name does not match. Expected:{containerName} and Provided{terraOptions.WorkspaceStorageContainerName}");
            }
        }

        private bool IsTerraWorkspaceContainer(string value)
            => terraOptions.WorkspaceStorageContainerName.Equals(value, StringComparison.OrdinalIgnoreCase);

        private bool IsTerraWorkspaceStorageAccount(string value)
            => terraOptions.WorkspaceStorageAccountName.Equals(value, StringComparison.OrdinalIgnoreCase);
    }
}
