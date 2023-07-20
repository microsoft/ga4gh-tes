// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Runtime.InteropServices;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Microsoft.Extensions.Logging;
using Tes.Runner.Transfer;

namespace Tes.Runner.Storage
{
    public class ArmSasResolutionStrategy : ISasResolutionStrategy
    {
        const string StorageHostSuffix = ".blob.core.windows.net";
        const int BlobSasTokenExpirationInHours = 48;
        const int UserDelegationKeyExpirationInHours = 1;

        private readonly ILogger logger = PipelineLoggerFactory.Create<ArmSasResolutionStrategy>();
        private readonly Dictionary<string, UserDelegationKey> userDelegationKeyDictionary;
        private readonly SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1, 1);
        private readonly Func<Uri, BlobServiceClient> blobServiceClientFactory;

        public ArmSasResolutionStrategy(Func<Uri, BlobServiceClient> blobServiceClientFactory)
        {
            ArgumentNullException.ThrowIfNull(blobServiceClientFactory);

            this.blobServiceClientFactory = blobServiceClientFactory;
            userDelegationKeyDictionary = new Dictionary<string, UserDelegationKey>();
        }

        public Task<Uri> CreateSasTokenWithStrategyAsync(string sourceUrl, BlobSasPermissions blobSasPermissions)
        {
            ArgumentNullException.ThrowIfNull(sourceUrl);

            if (!IsValidAzureStorageAccountUri(sourceUrl))
            {
                var uri = new Uri(sourceUrl);
                logger.LogWarning($"The URL provide is not a valid storage account. The resolution strategy won't be applied. Host: {uri.Host}");

                return Task.FromResult(uri);
            }

            return Task.FromResult(GetStorageUriWithSasToken(sourceUrl, blobSasPermissions));
        }

        private Uri GetStorageUriWithSasToken(string sourceUrl, BlobSasPermissions permissions)
        {
            try
            {
                var blobUrl = new BlobUriBuilder(new Uri(sourceUrl));
                var blobServiceClient = blobServiceClientFactory(new Uri($"https://{blobUrl.Host}"));

                var userKey = GetUserDelegationKey(blobServiceClient, blobUrl.AccountName);

                var sasBuilder = new BlobSasBuilder()
                {
                    BlobContainerName = blobUrl.BlobContainerName,
                    BlobName = blobUrl.BlobName,
                    Resource = "b",
                    ExpiresOn = DateTimeOffset.UtcNow.AddHours(BlobSasTokenExpirationInHours)
                };

                sasBuilder.SetPermissions(permissions);

                var blobUriWithSas = new BlobUriBuilder(blobUrl.ToUri())
                {
                    Sas = sasBuilder.ToSasQueryParameters(userKey, blobUrl.AccountName)
                };

                return blobUriWithSas.ToUri();
            }
            catch (Exception e)
            {
                logger.LogError(e, "Error while creating SAS token for blob storage");
                throw;
            }
        }

        private UserDelegationKey GetUserDelegationKey(BlobServiceClient blobServiceClient, string storageAccountName)
        {

            try
            {
                semaphoreSlim.Wait();

                var userDelegationKey = userDelegationKeyDictionary.GetValueOrDefault(storageAccountName);

                if (userDelegationKey is null || userDelegationKey.SignedExpiresOn < DateTimeOffset.UtcNow)
                {
                    userDelegationKey = blobServiceClient.GetUserDelegationKey(startsOn: default, expiresOn: DateTimeOffset.UtcNow.AddHours(UserDelegationKeyExpirationInHours));

                    userDelegationKeyDictionary[storageAccountName] = userDelegationKey;
                }

                return userDelegationKey;
            }
            catch (Exception e)
            {
                logger.LogError(e, $"Error while getting user delegation key.");
                throw;
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }

        private static bool IsValidAzureStorageAccountUri(string uri)
        {
            return Uri.TryCreate(uri, UriKind.Absolute, out var result) &&
                   result.Scheme == "https" &&
                   result.Host.EndsWith(StorageHostSuffix);
        }
    }
}
