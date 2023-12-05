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
            if (!TryParseHttpUrlFromInput(uriString, out var uri))
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
        public override async Task<Uri> MapLocalPathToSasUrlAsync(string path, BlobSasPermissions sasPermissions, CancellationToken cancellationToken, TimeSpan? sasTokenDuration)
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
                Logger.LogError("Could not parse path '{UnparsablePath}'.", path);
                return null;
            }

            if (TryGetExternalStorageAccountInfo(pathSegments.AccountName, pathSegments.ContainerName, out var externalStorageAccountInfo))
            {
                return new StorageAccountUrlSegments(externalStorageAccountInfo.BlobEndpoint, pathSegments.ContainerName, pathSegments.BlobName, externalStorageAccountInfo.SasToken).ToUri();
            }
            else
            {
                try
                {
                    var result = pathSegments.IsContainer
                        ? await AddSasTokenAsync(pathSegments, sasTokenDuration, ConvertSasPermissions(sasPermissions, nameof(sasPermissions)), path: path, cancellationToken: cancellationToken)
                        : await AddSasTokenAsync(pathSegments, sasTokenDuration, sasPermissions, path: path, cancellationToken: cancellationToken);
                    return result.ToUri();
                }
                catch
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// Generates SAS token for storage blobs.
        /// </summary>
        /// <param name="pathSegments">Represents segments of Azure Blob Storage URL.</param>
        /// <param name="sasTokenDuration">Duration SAS should be valid.</param>
        /// <param name="blobPermissions">Requested permissions to be included in the returned token.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="path">Logging metadata for failures locating storage account.</param>
        /// <returns>A <see cref="StorageAccountUrlSegments"/> targeting <paramref name="pathSegments"/> with the SAS token.</returns>
        /// <exception cref="ArgumentException"></exception>
        private Task<StorageAccountUrlSegments> AddSasTokenAsync(StorageAccountUrlSegments pathSegments, TimeSpan? sasTokenDuration, BlobSasPermissions blobPermissions, CancellationToken cancellationToken, string path = default)
        {
            if (pathSegments.IsContainer)
            {
                throw new ArgumentException("BlobContainerSasPermissions must be used with containers.", nameof(blobPermissions));
            }

            return AddSasTokenAsyncImpl(pathSegments, sasTokenDuration, (expiresOn, blobName) => new BlobSasBuilder(blobPermissions, expiresOn) { BlobName = blobPermissions.HasFlag(BlobSasPermissions.List) ? string.Empty : blobName }, path, cancellationToken);
        }

        /// <summary>
        /// Generates SAS token for storage blob containers.
        /// </summary>
        /// <param name="pathSegments">Represents segments of Azure Blob Storage URL.</param>
        /// <param name="sasTokenDuration">Duration SAS should be valid.</param>
        /// <param name="containerPermissions">Requested permissions to be included in the returned token.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="path">Logging metadata for failures locating storage account.</param>
        /// <returns>A <see cref="StorageAccountUrlSegments"/> targeting <paramref name="pathSegments"/> with the SAS token.</returns>
        /// <exception cref="ArgumentException"></exception>
        private Task<StorageAccountUrlSegments> AddSasTokenAsync(StorageAccountUrlSegments pathSegments, TimeSpan? sasTokenDuration, BlobContainerSasPermissions containerPermissions, CancellationToken cancellationToken, string path = default)
        {
            if (!pathSegments.IsContainer)
            {
                throw new ArgumentException("BlobSasPermissions must be used with blobs.", nameof(containerPermissions));
            }

            return AddSasTokenAsyncImpl(pathSegments, sasTokenDuration, (expiresOn, _1) => new BlobSasBuilder(containerPermissions, expiresOn) { BlobName = string.Empty }, path, cancellationToken);
        }

        /// <summary>
        /// Generates SAS token for both blobs and containers. Intended to be called from methods like <seealso cref="AddSasTokenAsync(StorageAccountUrlSegments, TimeSpan?, BlobSasPermissions, CancellationToken, string)"/> and <seealso cref="AddSasTokenAsync(StorageAccountUrlSegments, TimeSpan?, BlobContainerSasPermissions, CancellationToken, string)"/>.
        /// </summary>
        /// <param name="pathSegments">Represents segments of Azure Blob Storage URL.</param>
        /// <param name="sasTokenDuration">Duration SAS should be valid.</param>
        /// <param name="createBuilder">A factory that generates a <see cref="BlobSasBuilder"/>. Receives the expiration time and the blobName, which should be set on the sas builder as appropriate.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="path">Logging metadata for failures locating storage account.</param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        private async Task<StorageAccountUrlSegments> AddSasTokenAsyncImpl(StorageAccountUrlSegments pathSegments, TimeSpan? sasTokenDuration, Func<DateTimeOffset, string, BlobSasBuilder> createBuilder, string path, CancellationToken cancellationToken)
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

                var expiresOn = DateTimeOffset.UtcNow.Add(sasTokenDuration ?? SasTokenDuration);
                var builder = createBuilder(expiresOn, resultPathSegments.BlobName);

                builder.BlobContainerName = resultPathSegments.ContainerName;
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
        public override async Task<Uri> GetInternalTesBlobUrlAsync(string blobPath, BlobSasPermissions sasPermissions, CancellationToken cancellationToken)
        {
            var pathSegments = StorageAccountUrlSegments.Create(GetInternalTesBlobUrlWithoutSasToken(blobPath).AbsoluteUri);

            var resultPathSegments = pathSegments.IsContainer
                ? await AddSasTokenAsync(pathSegments, SasTokenDuration, ConvertSasPermissions(sasPermissions, nameof(sasPermissions)), cancellationToken)
                : await AddSasTokenAsync(pathSegments, SasTokenDuration, sasPermissions, cancellationToken);
            return resultPathSegments.ToUri();
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
            if (sasPermissions.HasFlag(BlobSasPermissions.PermanentDelete)) { throw new ArgumentOutOfRangeException(paramName, nameof(BlobSasPermissions.PermanentDelete), "A permission that cannot be applied to a container was provided when a container SAS was required."); }

            return result;
        }

        private static string NormalizedBlobPath(string blobPath)
        {
            return string.IsNullOrEmpty(blobPath) ? string.Empty : $"/{blobPath.TrimStart('/')}";
        }

        /// <inheritdoc />
        public override Uri GetInternalTesTaskBlobUrlWithoutSasToken(TesTask task, string blobPath)
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
            var builder = new BlobUriBuilder(new($"https://{storageOptions.DefaultAccountName}.blob.core.windows.net/{blobPathWithPrefix.TrimStart('/')}"));

            return builder.ToUri();
        }

        /// <inheritdoc />
        public override Uri GetInternalTesBlobUrlWithoutSasToken(string blobPath)
        {
            var normalizedBlobPath = NormalizedBlobPath(blobPath);

            //passing the resulting string through the builder to ensure that the path is properly encoded and valid
            var builder = new BlobUriBuilder(new($"https://{storageOptions.DefaultAccountName}.blob.core.windows.net{TesExecutionsPathPrefix}{normalizedBlobPath}"));

            return builder.ToUri();
        }

        /// <inheritdoc />
        public override async Task<Uri> GetInternalTesTaskBlobUrlAsync(TesTask task, string blobPath, BlobSasPermissions sasPermissions, CancellationToken cancellationToken)
        {
            var pathSegments = StorageAccountUrlSegments.Create(GetInternalTesTaskBlobUrlWithoutSasToken(task, blobPath).AbsoluteUri);

            var resultPathSegments = pathSegments.IsContainer
                ? await AddSasTokenAsync(pathSegments, SasTokenDuration, ConvertSasPermissions(sasPermissions, nameof(sasPermissions)), cancellationToken)
                : await AddSasTokenAsync(pathSegments, SasTokenDuration, sasPermissions, cancellationToken);
            return resultPathSegments.ToUri();
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
