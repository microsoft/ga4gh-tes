﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Tes.Extensions;
using Tes.Models;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Options;

namespace TesApi.Web.Storage
{
    /// <summary>
    /// Provides methods for blob storage access by using local path references in form of /storageaccount/container/blobpath
    /// </summary>
    public class DefaultStorageAccessProvider : StorageAccessProvider
    {

        private static readonly TimeSpan SasTokenDuration = TimeSpan.FromDays(7); //TODO: refactor this to drive it from configuration.
        private readonly StorageOptions storageOptions;
        private readonly List<ExternalStorageContainerInfo> externalStorageContainers;

        /// <summary>
        /// Provides methods for blob storage access by using local path references in form of /storageaccount/container/blobpath
        /// </summary>
        /// <param name="logger">Logger <see cref="ILogger"/></param>
        /// <param name="storageOptions">Configuration of <see cref="StorageOptions"/></param>
        /// <param name="azureProxy">Azure proxy <see cref="IAzureProxy"/></param>
        public DefaultStorageAccessProvider(ILogger<DefaultStorageAccessProvider> logger, IOptions<StorageOptions> storageOptions, IAzureProxy azureProxy) : base(logger, azureProxy)
        {
            ArgumentNullException.ThrowIfNull(storageOptions);

            this.storageOptions = storageOptions.Value;

            externalStorageContainers = storageOptions.Value.ExternalStorageContainers?.Split(new[] { ',', ';', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(uri =>
                {
                    if (StorageAccountUrlSegments.TryCreate(uri, out var s))
                    {
                        return new ExternalStorageContainerInfo { BlobEndpoint = s.BlobEndpoint, AccountName = s.AccountName, ContainerName = s.ContainerName, SasToken = s.SasToken };
                    }
                    else
                    {
                        logger.LogError($"Invalid value '{uri}' found in 'ExternalStorageContainers' configuration. Value must be a valid azure storage account or container URL.");
                        return null;
                    }
                })
                .Where(storageAccountInfo => storageAccountInfo is not null)
                .ToList();
        }

        /// <inheritdoc />
        public override async Task<bool> IsPublicHttpUrlAsync(string uriString, CancellationToken cancellationToken)
        {
            var isHttpUrl = TryParseHttpUrlFromInput(uriString, out var uri);

            if (!isHttpUrl)
            {
                return false;
            }

            if (HttpUtility.ParseQueryString(uri.Query).Get("sig") is not null)
            {
                return true;
            }

            if (StorageAccountUrlSegments.TryCreate(uriString, out var parts))
            {
                if (await TryGetStorageAccountInfoAsync(parts.AccountName, cancellationToken))
                {
                    return false;
                }

                if (TryGetExternalStorageAccountInfo(parts.AccountName, parts.ContainerName, out _))
                {
                    return false;
                }
            }

            return true;
        }

        /// <inheritdoc />
        public override async Task<string> MapLocalPathToSasUrlAsync(string path, CancellationToken cancellationToken, TimeSpan? sasTokenDuration = default, bool getContainerSas = false)
        {
            // TODO: Optional: If path is /container/... where container matches the name of the container in the default storage account, prepend the account name to the path.
            // This would allow the user to omit the account name for files stored in the default storage account

            // /cromwell-executions/... URLs become /defaultStorageAccountName/cromwell-executions/... to unify how URLs starting with /acct/container/... pattern are handled.
            if (IsKnownExecutionFilePath(path))
            {
                path = $"/{storageOptions.DefaultAccountName}{path}";
            }

            //TODO: refactor this to throw an exception instead of logging and error and returning null.
            if (!StorageAccountUrlSegments.TryCreate(path, out var pathSegments))
            {
                Logger.LogError($"Could not parse path '{path}'.");
                return null;
            }

            if (TryGetExternalStorageAccountInfo(pathSegments.AccountName, pathSegments.ContainerName, out var externalStorageAccountInfo))
            {
                return new StorageAccountUrlSegments(externalStorageAccountInfo.BlobEndpoint, pathSegments.ContainerName, pathSegments.BlobName, externalStorageAccountInfo.SasToken).ToUriString();
            }
            else
            {
                return (await AddSasTokenAsync(pathSegments, cancellationToken, sasTokenDuration, getContainerSas, path))?.ToUriString();
            }
        }

        /// <summary>
        /// Generates SAS token for both blobs and containers.
        /// </summary>
        /// <param name="pathSegments"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="sasTokenDuration">Duration SAS should be valid.</param>
        /// <param name="getContainerSas">Get the container SAS even if path is longer than two parts</param>
        /// <param name="path">The file path to convert. Two-part path is treated as container path. Paths with three or more parts are treated as blobs.</param>
        /// <returns></returns>
        private async Task<StorageAccountUrlSegments> AddSasTokenAsync(StorageAccountUrlSegments pathSegments, CancellationToken cancellationToken, TimeSpan? sasTokenDuration = default, bool getContainerSas = false, string path = default)
        {
            StorageAccountInfo storageAccountInfo = null;

            if (!await TryGetStorageAccountInfoAsync(pathSegments.AccountName, cancellationToken, info => storageAccountInfo = info))
            {
                Logger.LogError($"Could not find storage account '{pathSegments.AccountName}' corresponding to path '{path}'. Either the account does not exist or the TES app service does not have permission to it.");
                return null;
            }

            try
            {
                var accountKey = await AzureProxy.GetStorageAccountKeyAsync(storageAccountInfo, cancellationToken);
                var resultPathSegments = new StorageAccountUrlSegments(storageAccountInfo.BlobEndpoint, pathSegments.ContainerName, pathSegments.BlobName);

                if (pathSegments.IsContainer || getContainerSas)
                {
                    var policy = new SharedAccessBlobPolicy()
                    {
                        Permissions = SharedAccessBlobPermissions.Add | SharedAccessBlobPermissions.Create | SharedAccessBlobPermissions.List | SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write,
                        SharedAccessExpiryTime = DateTime.Now.Add((sasTokenDuration ?? TimeSpan.Zero) + SasTokenDuration)
                    };

                    var containerUri = new StorageAccountUrlSegments(storageAccountInfo.BlobEndpoint, pathSegments.ContainerName).ToUri();
                    resultPathSegments.SasToken = new CloudBlobContainer(containerUri, new StorageCredentials(storageAccountInfo.Name, accountKey)).GetSharedAccessSignature(policy, null, SharedAccessProtocol.HttpsOnly, null);
                }
                else
                {
                    var policy = new SharedAccessBlobPolicy() { Permissions = SharedAccessBlobPermissions.Read, SharedAccessExpiryTime = DateTime.Now.Add((sasTokenDuration ?? TimeSpan.Zero) + SasTokenDuration) };
                    resultPathSegments.SasToken = new CloudBlob(resultPathSegments.ToUri(), new StorageCredentials(storageAccountInfo.Name, accountKey)).GetSharedAccessSignature(policy, null, null, SharedAccessProtocol.HttpsOnly, null);
                }

                return resultPathSegments;
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, $"Could not get the key of storage account '{pathSegments.AccountName}'. Make sure that the TES app service has Contributor access to it.");
                return null;
            }
        }

        /// <inheritdoc />
        public override async Task<string> GetInternalTesBlobUrlAsync(string blobPath, CancellationToken cancellationToken)
        {
            var pathSegments = StorageAccountUrlSegments.Create(GetInternalTesBlobUrlWithoutSasToken(blobPath));

            var resultPathSegments = await AddSasTokenAsync(pathSegments, cancellationToken, getContainerSas: true);

            return resultPathSegments.ToUriString();
        }


        private static string NormalizedBlobPath(string blobPath)
        {
            return string.IsNullOrEmpty(blobPath) ? string.Empty : $"/{blobPath.TrimStart('/')}";
        }

        /// <inheritdoc />
        public override string GetInternalTesTaskBlobUrlWithoutSasToken(TesTask task, string blobPath)
        {
            var normalizedBlobPath = NormalizedBlobPath(blobPath);
            var blobPathWithPrefix = $"{TesExecutionsPathPrefix}/{task.Id}{normalizedBlobPath}";

            if (task.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters
                    .internal_path_prefix) == true)
            {
                blobPathWithPrefix =
                    $"{task.Resources.GetBackendParameterValue(TesResources.SupportedBackendParameters.internal_path_prefix).Trim('/')}{normalizedBlobPath}";

                if (storageOptions.ExecutionsContainerName is not null &&
                    !blobPathWithPrefix.StartsWith(storageOptions.ExecutionsContainerName, StringComparison.OrdinalIgnoreCase))
                {
                    blobPathWithPrefix = $"{storageOptions.ExecutionsContainerName}/{blobPathWithPrefix}";
                }
            }

            //passing the resulting string through the builder to ensure that the path is properly encoded and valid
            var builder = new BlobUriBuilder(new Uri($"https://{storageOptions.DefaultAccountName}.blob.core.windows.net/{blobPathWithPrefix.TrimStart('/')}"));

            return builder.ToUri().ToString();
        }

        /// <inheritdoc />
        public override string GetInternalTesBlobUrlWithoutSasToken(string blobPath)
        {
            var normalizedBlobPath = NormalizedBlobPath(blobPath);

            //passing the resulting string through the builder to ensure that the path is properly encoded and valid
            var builder = new BlobUriBuilder(new Uri($"https://{storageOptions.DefaultAccountName}.blob.core.windows.net{TesExecutionsPathPrefix}{normalizedBlobPath}"));

            return builder.ToUri().ToString();
        }

        /// <inheritdoc />
        public override async Task<string> GetInternalTesTaskBlobUrlAsync(TesTask task, string blobPath, CancellationToken cancellationToken)
        {
            var pathSegments = StorageAccountUrlSegments.Create(GetInternalTesTaskBlobUrlWithoutSasToken(task, blobPath));

            var resultPathSegments = await AddSasTokenAsync(pathSegments, cancellationToken, getContainerSas: true);

            return resultPathSegments.ToUriString();
        }

        private async Task<bool> TryGetStorageAccountInfoAsync(string accountName, CancellationToken cancellationToken, Action<StorageAccountInfo> onSuccess = null)
        {
            try
            {
                var storageAccountInfo = await AzureProxy.GetStorageAccountInfoAsync(accountName, cancellationToken);

                if (storageAccountInfo is not null)
                {
                    onSuccess?.Invoke(storageAccountInfo);
                    return true;
                }
                else
                {
                    Logger.LogError($"Could not find storage account '{accountName}'. Either the account does not exist or the TES app service does not have permission to it.");
                }
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, $"Exception while getting storage account '{accountName}'");
            }

            return false;
        }

        private bool TryGetExternalStorageAccountInfo(string accountName, string containerName, out ExternalStorageContainerInfo result)
        {
            result = externalStorageContainers?.FirstOrDefault(c =>
                c.AccountName.Equals(accountName, StringComparison.OrdinalIgnoreCase)
                && (string.IsNullOrEmpty(c.ContainerName) || c.ContainerName.Equals(containerName, StringComparison.OrdinalIgnoreCase)));

            return result is not null;
        }
    }
}
