// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
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
        public override async Task<string> MapLocalPathToSasUrlAsync(string path, BlobSasPermissions sasPermissions, CancellationToken cancellationToken, TimeSpan? sasTokenDuration)
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
                try
                {
                    var result = pathSegments.IsContainer
                        ? await AddSasTokenAsync(pathSegments, sasTokenDuration, ConvertSasPermissions(sasPermissions, nameof(sasPermissions)), path: path, cancellationToken: cancellationToken)
                        : await AddSasTokenAsync(pathSegments, sasTokenDuration, sasPermissions, path: path, cancellationToken: cancellationToken);
                    return result.ToUriString();
                }
                catch
                {
                    return null;
                }
            }
        }

        private Task<StorageAccountUrlSegments> AddSasTokenAsync(StorageAccountUrlSegments pathSegments, TimeSpan? sasTokenDuration, BlobSasPermissions blobPermissions, CancellationToken cancellationToken, string path = default)
        {
            if (pathSegments.IsContainer)
            {
                throw new ArgumentException(nameof(blobPermissions), "BlobContainerSasPermissions must be used with containers.");
            }

            return AddSasTokenAsyncImpl(pathSegments, sasTokenDuration, expiresOn => new BlobSasBuilder(blobPermissions, expiresOn), path, cancellationToken);
        }

        private Task<StorageAccountUrlSegments> AddSasTokenAsync(StorageAccountUrlSegments pathSegments, TimeSpan? sasTokenDuration, BlobContainerSasPermissions containerPermissions, CancellationToken cancellationToken, string path = default)
        {
            if (!pathSegments.IsContainer)
            {
                throw new ArgumentException(nameof(containerPermissions), "BlobSasPermissions must be used with blobs.");
            }

            return AddSasTokenAsyncImpl(pathSegments, sasTokenDuration, expiresOn => new BlobSasBuilder(containerPermissions, expiresOn), path, cancellationToken);
        }

        private async Task<StorageAccountUrlSegments> AddSasTokenAsyncImpl(StorageAccountUrlSegments pathSegments, TimeSpan? sasTokenDuration, Func<DateTimeOffset, BlobSasBuilder> createBuilder, string path, CancellationToken cancellationToken)
        {
            StorageAccountInfo storageAccountInfo = null;

            if (!await TryGetStorageAccountInfoAsync(pathSegments.AccountName, cancellationToken, info => storageAccountInfo = info))
            {
                Logger.LogError($"Could not find storage account '{pathSegments.AccountName}' corresponding to path '{path}'. Either the account does not exist or the TES app service does not have permission to it.");
                throw new InvalidOperationException($"Could not find storage account '{pathSegments.AccountName}' corresponding to path '{path}'.");
            }

            try
            {
                var accountKey = await AzureProxy.GetStorageAccountKeyAsync(storageAccountInfo, cancellationToken);
                var resultPathSegments = new StorageAccountUrlSegments(storageAccountInfo.BlobEndpoint, pathSegments.ContainerName, pathSegments.BlobName);

                var expiresOn = DateTimeOffset.UtcNow.Add((sasTokenDuration ?? TimeSpan.Zero) + SasTokenDuration);
                var builder = createBuilder(expiresOn);

                builder.BlobContainerName = resultPathSegments.ContainerName;
                builder.BlobName = resultPathSegments.BlobName;
                builder.Protocol = SasProtocol.Https;

                resultPathSegments.SasToken = builder.ToSasQueryParameters(new StorageSharedKeyCredential(storageAccountInfo.Name, accountKey)).ToString();
                return resultPathSegments;
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, $"Could not get the key of storage account '{pathSegments.AccountName}'. Make sure that the TES app service has Contributor access to it.");
                throw;
            }
        }

        /// <inheritdoc />
        public override async Task<string> GetInternalTesBlobUrlAsync(string blobPath, BlobSasPermissions sasPermissions, CancellationToken cancellationToken)
        {
            var pathSegments = StorageAccountUrlSegments.Create(GetInternalTesBlobUrlWithoutSasToken(blobPath));

            var resultPathSegments = pathSegments.IsContainer
                ? await AddSasTokenAsync(pathSegments, SasTokenDuration, ConvertSasPermissions(sasPermissions, nameof(sasPermissions)), cancellationToken)
                : await AddSasTokenAsync(pathSegments, SasTokenDuration, sasPermissions, cancellationToken);
            return resultPathSegments.ToUriString();
        }

        private static BlobContainerSasPermissions ConvertSasPermissions(BlobSasPermissions sasPermissions, string paramName)
        {
            BlobContainerSasPermissions result = 0;

            if (sasPermissions.HasFlag(BlobSasPermissions.Read)) { result |= BlobContainerSasPermissions.Read; }
            if (sasPermissions.HasFlag(BlobSasPermissions.Add)) { result |= BlobContainerSasPermissions.Add; }
            if (sasPermissions.HasFlag(BlobSasPermissions.Create)) { result |= BlobContainerSasPermissions.Create; }
            if (sasPermissions.HasFlag(BlobSasPermissions.Write)) { result |= BlobContainerSasPermissions.Write; }
            if (sasPermissions.HasFlag(BlobSasPermissions.Delete)) { result |= BlobContainerSasPermissions.Delete; }
            if (sasPermissions.HasFlag(BlobSasPermissions.Tag)) { result |= BlobContainerSasPermissions.Tag; }
            if (sasPermissions.HasFlag(BlobSasPermissions.DeleteBlobVersion)) { result |= BlobContainerSasPermissions.DeleteBlobVersion; }
            if (sasPermissions.HasFlag(BlobSasPermissions.List)) { result |= BlobContainerSasPermissions.List; }
            if (sasPermissions.HasFlag(BlobSasPermissions.Move)) { result |= BlobContainerSasPermissions.Move; }
            if (sasPermissions.HasFlag(BlobSasPermissions.Execute)) { result |= BlobContainerSasPermissions.Execute; }
            if (sasPermissions.HasFlag(BlobSasPermissions.SetImmutabilityPolicy)) { result |= BlobContainerSasPermissions.SetImmutabilityPolicy; }
            if (sasPermissions.HasFlag(BlobSasPermissions.PermanentDelete)) { throw new ArgumentOutOfRangeException(paramName, nameof(BlobSasPermissions.PermanentDelete), "Permission that cannot be applied to container was provided."); }

            return result;
        }

        private static string NormalizedBlobPath(string blobPath)
        {
            return string.IsNullOrEmpty(blobPath) ? string.Empty : $"/{blobPath.TrimStart('/')}";
        }

        /// <inheritdoc />
        public override string GetInternalTesTaskBlobUrlWithoutSasToken(TesTask task, string blobPath)
        {
            var normalizedBlobPath = NormalizedBlobPath(blobPath);
            var blobPathWithPrefix = $"{task.Id}{normalizedBlobPath}";
            if (task.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters
                    .internal_path_prefix) == true)
            {
                blobPathWithPrefix =
                    $"{task.Resources.GetBackendParameterValue(TesResources.SupportedBackendParameters.internal_path_prefix).Trim('/')}{normalizedBlobPath}";
            }

            //passing the resulting string through the builder to ensure that the path is properly encoded and valid
            var builder = new BlobUriBuilder(new Uri($"https://{storageOptions.DefaultAccountName}.blob.core.windows.net{TesExecutionsPathPrefix}/{blobPathWithPrefix.TrimStart('/')}"));

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
        public override async Task<string> GetInternalTesTaskBlobUrlAsync(TesTask task, string blobPath, BlobSasPermissions sasPermissions, CancellationToken cancellationToken)
        {
            var normalizedBlobPath = NormalizedBlobPath(blobPath);

            if (task.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters
                    .internal_path_prefix) == true)
            {
                var internalPath = $"{task.Resources.GetBackendParameterValue(TesResources.SupportedBackendParameters.internal_path_prefix).Trim('/')}{normalizedBlobPath}";

                if (storageOptions.ExecutionsContainerName is not null &&
                    !internalPath.StartsWith(storageOptions.ExecutionsContainerName, StringComparison.OrdinalIgnoreCase))
                {
                    internalPath = $"{storageOptions.ExecutionsContainerName}/{internalPath}";
                }

                var blobPathWithPathPrefix =
                    $"/{storageOptions.DefaultAccountName}/{internalPath}";
                return await MapLocalPathToSasUrlAsync(blobPathWithPathPrefix, sasPermissions, cancellationToken, sasTokenDuration: default);
            }

            return await GetInternalTesBlobUrlAsync($"/{task.Id}{normalizedBlobPath}", sasPermissions, cancellationToken);
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
