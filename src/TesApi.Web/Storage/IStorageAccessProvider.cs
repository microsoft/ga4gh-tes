// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;
using Tes.Models;

namespace TesApi.Web.Storage
{
    /// <summary>
    /// Provides methods for abstracting storage access by using local path references in form of /storageaccount/container/blobpath
    /// </summary>
    public interface IStorageAccessProvider
    {
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
        public Task<string> UploadBlobAsync(Uri blobAbsoluteUrl, string content, CancellationToken cancellationToken);

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
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="getContainerSas">Get the container SAS even if path is longer than two parts</param>
        /// <returns>An Azure Block Blob or Container URL with SAS token</returns>
        public Task<string> MapLocalPathToSasUrlAsync(string path, CancellationToken cancellationToken, bool getContainerSas = false);

        /// <summary>
        /// Returns an Azure Storage Blob URL with a SAS token for the specified blob path in the TES internal storage location
        /// </summary>
        /// <param name="blobPath"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<string> GetInternalTesBlobUrlAsync(string blobPath, CancellationToken cancellationToken);

        /// <summary>
        /// Returns an Azure Storage Blob URL with a SAS token for the specified blob path in the internal storage location.
        /// </summary>
        /// <param name="task"></param>
        /// <param name="blobPath"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<string> GetInternalTesTaskBlobUrlAsync(TesTask task, string blobPath, CancellationToken cancellationToken);
    }
}
