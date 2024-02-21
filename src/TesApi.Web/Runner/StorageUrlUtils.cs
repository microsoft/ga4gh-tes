// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using TesApi.Web.Options;
using TesApi.Web.Storage;

namespace TesApi.Web.Runner
{
    /// <summary>
    /// Storage URL utility methods
    /// </summary>
    public static class StorageUrlUtils
    {
        public const string DefaultBlobEndpointHostNameSuffix = ".blob.core.windows.net";
        /// <summary>
        /// Blob endpoint Host name suffix.  This is set at starup in Program.cs
        /// </summary>
        public static string BlobEndpointHostNameSuffix = DefaultBlobEndpointHostNameSuffix;

        /// <summary>
        /// Converts a local path with the format /storageAccount/container/blobName to an Azure storage URL.
        /// Uses the value provided cromwellStorageAccountName storage account if the path starts with <see cref="StorageAccessProvider.CromwellPathPrefix"/>.  
        /// Appends the SAS token if provided.
        /// </summary>
        /// <param name="uriValue"></param>
        /// <param name="cromwellStorageAccountName"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="InvalidOperationException"></exception>
        public static string ConvertLocalPathOrCromwellLocalPathToUrl(string uriValue, string cromwellStorageAccountName)
        {

            //check if the path is a known cromwell path, and append the default storage account name as it would be missing
            var pathToConvert = uriValue;
            if (uriValue.StartsWith(StorageAccessProvider.CromwellPathPrefix))
            {
                if (string.IsNullOrWhiteSpace(cromwellStorageAccountName))
                {
                    throw new ArgumentException(
                        "The provided path can't be converted to a URL. The default storage account is not set");
                }

                pathToConvert = $"/{cromwellStorageAccountName}/{uriValue.TrimStart('/')}";
            }

            if (StorageAccountUrlSegments.TryCreate(pathToConvert, out var result))
            {
                if (String.IsNullOrWhiteSpace(result.AccountName))
                {
                    throw new InvalidOperationException(
                        $"The value provided can't be converted to URL. The storage account name is missing. Value: {uriValue}");
                }

                if (String.IsNullOrWhiteSpace(result.ContainerName))
                {
                    throw new InvalidOperationException(
                        $"The value provided can't be converted to URL. The container name is missing. Value: {uriValue}");
                }

                return $"https://{result.AccountName}{BlobEndpointHostNameSuffix}/{result.ContainerName}/{result.BlobName}";
            }

            throw new InvalidOperationException($"The value provided can't be converted to URL. Value: {uriValue}");
        }

        /// <summary>
        /// Returns true if the URI provided is a valid Azure Blob Storage URL
        /// </summary>
        /// <param name="uri"></param>
        /// <returns></returns>
        public static bool IsValidAzureStorageAccountUri(string uri)
        {
            return Uri.TryCreate(uri, UriKind.Absolute, out var result) &&
                   result.Scheme == "https" &&
                   result.Host.EndsWith(BlobEndpointHostNameSuffix);
        }

        /// <summary>
        /// Returns a list of <see cref="ExternalStorageContainerInfo"/> from the configuration options
        /// </summary>
        /// <param name="storageOptions"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public static List<ExternalStorageContainerInfo> GetExternalStorageContainerInfos(StorageOptions storageOptions)
        {
            var returnList = storageOptions.ExternalStorageContainers?.Split(new[] { ',', ';', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(uri =>
                {
                    if (StorageAccountUrlSegments.TryCreate(uri, out var s))
                    {
                        return new ExternalStorageContainerInfo { BlobEndpoint = s.BlobEndpoint, AccountName = s.AccountName, ContainerName = s.ContainerName, SasToken = s.SasToken };
                    }

                    throw new InvalidOperationException($"Invalid value '{uri}' found in 'ExternalStorageContainers' configuration. Value must be a valid azure storage account or container URL.");
                })
                .ToList();

            return returnList ?? new List<ExternalStorageContainerInfo>();
        }

        /// <summary>
        /// Removes the query string from a URL
        /// </summary>
        /// <param name="uri"></param>
        /// <returns></returns>
        public static string RemoveQueryStringFromUrl(Uri uri)
        {
            return uri.GetLeftPart(UriPartial.Path);
        }

        /// <summary>
        /// Setts or adds a SAS token to the query string
        /// </summary>
        /// <param name="existingQueryString"></param>
        /// <param name="sasToken"></param>
        /// <returns></returns>
        public static string SetOrAddSasTokenToQueryString(string existingQueryString,
            string sasToken)
        {
            if (string.IsNullOrWhiteSpace(sasToken))
            {
                return existingQueryString;
            }

            var blobQuery = existingQueryString;

            if (string.IsNullOrWhiteSpace(blobQuery))
            {
                blobQuery = sasToken;
            }
            else
            {
                blobQuery += $"&{sasToken}";
            }

            return blobQuery;
        }

        /// <summary>
        /// Checks if the provided URI is a local absolute path
        /// </summary>
        /// <param name="uriValue"></param>
        /// <returns></returns>
        public static bool IsLocalAbsolutePath(string uriValue)
        {
            if (string.IsNullOrWhiteSpace(uriValue))
            {
                return false;
            }

            return uriValue.StartsWith("/");
        }
    }
}
