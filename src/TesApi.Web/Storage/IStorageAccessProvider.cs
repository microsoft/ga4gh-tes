// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Sas;
using Tes.Models;

namespace TesApi.Web.Storage
{
    /// <summary>
    /// Provides methods for abstracting storage access by using local path references in form of /storageaccount/container/blobpath
    /// </summary>
    public interface IStorageAccessProvider
    {
        /// <summary>
        /// SAS permissions previously given all containers and when container SAS was requested.
        /// </summary>
        public BlobSasPermissions DefaultContainerPermissions => BlobSasPermissions.Add | BlobSasPermissions.Create | BlobSasPermissions.List | BlobSasPermissions.Read | BlobSasPermissions.Write;


        /// <summary>
        /// SAS permissions previously given all blobs when container SAS was not requested.
        /// </summary>
        public BlobSasPermissions DefaultBlobPermissions => BlobSasPermissions.Read;

        /// <summary>
        /// SAS default blob permissions including Create/Write.
        /// </summary>
        public BlobSasPermissions BlobPermissionsWithWrite => DefaultBlobPermissions | BlobSasPermissions.Add | BlobSasPermissions.Create | BlobSasPermissions.List | BlobSasPermissions.Write;

        /// <summary>
        /// SAS default blob permissions including Tag.
        /// </summary>
        public BlobSasPermissions BlobPermissionsWithTag => DefaultBlobPermissions | BlobSasPermissions.Tag;

        /// <summary>
        /// SAS default blob permissions including Create/Write and Tag.
        /// </summary>
        public BlobSasPermissions BlobPermissionsWithWriteAndTag => BlobPermissionsWithWrite | BlobSasPermissions.Tag;

        /// <summary>
        /// Retrieves file content
        /// </summary>
        /// <param name="blobRelativePath">Path to the file in form of /storageaccountname/container/path</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>The content of the file</returns>
        public Task<string> DownloadBlobAsync(string blobRelativePath, CancellationToken cancellationToken);

        /// <summary>
        /// Retrieves file content
        /// </summary>
        /// <param name="blobAbsoluteUrl">Blob storage URL with a SAS token</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>The content of the file</returns>
        public Task<string> DownloadBlobAsync(Uri blobAbsoluteUrl, CancellationToken cancellationToken);

        /// <summary>
        /// Updates the content of the file, creating the file if necessary
        /// </summary>
        /// <param name="blobRelativePath">Path to the file in form of /storageaccountname/container/path</param>
        /// <param name="content">The new content</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public Task UploadBlobAsync(string blobRelativePath, string content, CancellationToken cancellationToken);

        /// <summary>
        /// Uploads the content as Blob to the provided Blob URL
        /// </summary>
        /// <param name="blobAbsoluteUrl">Absolute Blob Storage URL with a SAS token</param>
        /// <param name="content">Blob content</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public Task UploadBlobAsync(Uri blobAbsoluteUrl, string content, CancellationToken cancellationToken);

        /// <summary>
        /// Updates the content of the file, creating the file if necessary
        /// </summary>
        /// <param name="blobRelativePath">Path to the file in form of /storageaccountname/container/path</param>
        /// <param name="sourceLocalFilePath">Path to the local file to get the content from</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public Task UploadBlobFromFileAsync(string blobRelativePath, string sourceLocalFilePath, CancellationToken cancellationToken);

        /// <summary>
        /// Checks if the specified string represents a HTTP URL that is publicly accessible
        /// </summary>
        /// <param name="uriString">URI string</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>True if the URL can be used as is, without adding SAS token to it</returns>
        public Task<bool> IsPublicHttpUrlAsync(string uriString, CancellationToken cancellationToken);

        /// <summary>
        /// Returns an Azure Storage Blob or Container URL with SAS token given a path that uses one of the following formats: 
        /// - /accountName/containerName
        /// - /accountName/containerName/blobName
        /// - /cromwell-executions/blobName
        /// - https://accountName.blob.core.windows.net/containerName
        /// - https://accountName.blob.core.windows.net/containerName/blobName
        /// </summary>
        /// <param name="path">The file path to convert. Two-part path is treated as container path. Paths with three or more parts are treated as blobs.</param>
        /// <param name="sasPermissions">Requested permissions to include in the SAS token.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="sasTokenDuration">Duration SAS should be valid.</param>
        /// <returns>An Azure Block Blob or Container URL with SAS token</returns>
        public Task<string> MapLocalPathToSasUrlAsync(string path, BlobSasPermissions sasPermissions, CancellationToken cancellationToken, TimeSpan? sasTokenDuration = default);

        /// <summary>
        /// Returns an Azure Storage Blob URL with a SAS token for the specified blob path in the TES internal storage location
        /// </summary>
        /// <param name="blobPath">A relative path within the blob storage space reserved for the TES server.</param>
        /// <param name="sasPermissions">Requested permissions to include in the SAS token.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>A blob storage URL with SAS token.</returns>
        public Task<string> GetInternalTesBlobUrlAsync(string blobPath, BlobSasPermissions sasPermissions, CancellationToken cancellationToken);

        /// <summary>
        /// Returns an Azure Storage Blob URL with a SAS token for the specified blob path in the TES task internal storage location.
        /// </summary>
        /// <param name="task">A <see cref="TesTask"/></param>
        /// <param name="blobPath">A relative path within the blob storage space reserved for the <paramref name="task"/>.</param>
        /// <param name="sasPermissions">Requested permissions to include in the SAS token.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>A blob storage URL with SAS token.</returns>
        public Task<string> GetInternalTesTaskBlobUrlAsync(TesTask task, string blobPath, BlobSasPermissions sasPermissions, CancellationToken cancellationToken);

        /// <summary>
        /// Returns an Azure Storage Blob URL without a SAS token for the specified blob path in the TES task internal storage location.
        /// </summary>
        /// <param name="task">A <see cref="TesTask"/></param>
        /// <param name="blobPath">A relative path within the blob storage space reserved for the <paramref name="task"/>.</param>
        /// <returns>A blob storage URL.</returns>
        public string GetInternalTesTaskBlobUrlWithoutSasToken(TesTask task, string blobPath);

        /// <summary>
        /// Returns an Azure Storage Blob URL without a SAS token for the specified blob path in the TES internal storage location.
        /// </summary>
        /// <param name="blobPath">A relative path within the blob storage space reserved for the TES server.</param>
        /// <returns>A blob storage URL.</returns>
        public string GetInternalTesBlobUrlWithoutSasToken(string blobPath);
    }
}
