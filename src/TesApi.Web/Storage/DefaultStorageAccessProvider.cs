// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;

namespace TesApi.Web.Storage
{
    /// <summary>
    /// Provides methods for blob storage access by using local path references in form of /storageaccount/container/blobpath
    /// </summary>
    public class DefaultStorageAccessProvider : StorageAccessProvider
    {

        private static readonly TimeSpan SasTokenDuration = TimeSpan.FromDays(7); //TODO: refactor this to drive it from configuration.
        private readonly string defaultStorageAccountName;
        private readonly List<ExternalStorageContainerInfo> externalStorageContainers;

        /// <summary>
        /// Provides methods for blob storage access by using local path references in form of /storageaccount/container/blobpath
        /// </summary>
        /// <param name="logger">Logger <see cref="ILogger"/></param>
        /// <param name="storageOptions">Configuration of <see cref="Options.StorageOptions"/></param>
        /// <param name="azureProxy">Azure proxy <see cref="IAzureProxy"/></param>
        public DefaultStorageAccessProvider(ILogger<DefaultStorageAccessProvider> logger, IOptions<Options.StorageOptions> storageOptions, IAzureProxy azureProxy) : base(logger, azureProxy)
        {
            //TODO: refactor to use the options pattern.
            defaultStorageAccountName = storageOptions.Value.DefaultAccountName;    // This account contains the tes-internal container
            Logger.LogInformation($"DefaultStorageAccountName: {defaultStorageAccountName}");

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
        public override async Task<bool> IsPublicHttpUrlAsync(string uriString)
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
                if (await TryGetStorageAccountInfoAsync(parts.AccountName))
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
        public override async Task<string> MapLocalPathToSasUrlAsync(string path, bool getContainerSas = false)
        {
            // TODO: Optional: If path is /container/... where container matches the name of the container in the default storage account, prepend the account name to the path.
            // This would allow the user to omit the account name for files stored in the default storage account

            // /tes-internal/... URLs become /defaultStorageAccountName/tes-internal/... to unify how URLs starting with /acct/container/... pattern are handled.
            if (IsKnownExecutionFilePath(path))
            {
                path = $"/{defaultStorageAccountName}{path}";
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
                StorageAccountInfo storageAccountInfo = null;

                if (!await TryGetStorageAccountInfoAsync(pathSegments.AccountName, info => storageAccountInfo = info))
                {
                    Logger.LogError($"Could not find storage account '{pathSegments.AccountName}' corresponding to path '{path}'. Either the account does not exist or the TES app service does not have permission to it.");
                    return null;
                }

                try
                {
                    var accountKey = await AzureProxy.GetStorageAccountKeyAsync(storageAccountInfo);
                    var resultPathSegments = new StorageAccountUrlSegments(storageAccountInfo.BlobEndpoint, pathSegments.ContainerName, pathSegments.BlobName);

                    if (pathSegments.IsContainer || getContainerSas)
                    {
                        var policy = new SharedAccessBlobPolicy()
                        {
                            Permissions = SharedAccessBlobPermissions.Add | SharedAccessBlobPermissions.Create | SharedAccessBlobPermissions.List | SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write,
                            SharedAccessExpiryTime = DateTime.Now.Add(SasTokenDuration)
                        };

                        var containerUri = new StorageAccountUrlSegments(storageAccountInfo.BlobEndpoint, pathSegments.ContainerName).ToUri();
                        resultPathSegments.SasToken = new CloudBlobContainer(containerUri, new StorageCredentials(storageAccountInfo.Name, accountKey)).GetSharedAccessSignature(policy, null, SharedAccessProtocol.HttpsOnly, null);
                    }
                    else
                    {
                        var policy = new SharedAccessBlobPolicy() { Permissions = SharedAccessBlobPermissions.Read, SharedAccessExpiryTime = DateTime.Now.Add(SasTokenDuration) };
                        resultPathSegments.SasToken = new CloudBlob(resultPathSegments.ToUri(), new StorageCredentials(storageAccountInfo.Name, accountKey)).GetSharedAccessSignature(policy, null, null, SharedAccessProtocol.HttpsOnly, null);
                    }

                    return resultPathSegments.ToUriString();
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, $"Could not get the key of storage account '{pathSegments.AccountName}'. Make sure that the TES app service has Contributor access to it.");
                    return null;
                }
            }
        }

        private async Task<bool> TryGetStorageAccountInfoAsync(string accountName, Action<StorageAccountInfo> onSuccess = null)
        {
            try
            {
                var storageAccountInfo = await AzureProxy.GetStorageAccountInfoAsync(accountName);

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
