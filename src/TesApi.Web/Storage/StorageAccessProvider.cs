// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tes.Models;

namespace TesApi.Web.Storage;

/// <summary>
/// Provides base class for abstracting storage access by using local path references in form of /storageaccount/container/blobpath
/// </summary>
public abstract class StorageAccessProvider : IStorageAccessProvider
{
    /// <summary>
    /// Cromwell path prefix
    /// </summary>
    public const string CromwellPathPrefix = "/cromwell-executions/";

    /// <summary>
    /// TES path for internal execution files
    /// </summary>
    public const string TesExecutionsPathPrefix = "/tes-internal";

    /// <summary>
    /// Logger instance. 
    /// </summary>
    protected readonly ILogger Logger;
    /// <summary>
    /// Azure proxy instance.
    /// </summary>
    protected readonly IAzureProxy AzureProxy;

    /// <summary>
    /// Provides base methods for blob storage access and local input mapping.
    /// </summary>
    /// <param name="logger">Logger <see cref="ILogger"/></param>
    /// <param name="azureProxy">Azure proxy <see cref="IAzureProxy"/></param>
    public StorageAccessProvider(ILogger logger, IAzureProxy azureProxy)
    {
        this.Logger = logger;
        this.AzureProxy = azureProxy;
    }

    /// <inheritdoc />
    public async Task<string> DownloadBlobAsync(string blobRelativePath, CancellationToken cancellationToken)
    {
        var blobUrl = await MapLocalPathToSasUrlAsync(blobRelativePath, cancellationToken);

        if (blobUrl is null)
        {
            Logger.LogWarning(@"The relative path provided could not be mapped to a valid blob URL. Download will be skipped. Blob relative path: {BlobRelativePath}", blobRelativePath);
            return default;
        }

        if (!await AzureProxy.BlobExistsAsync(blobUrl, cancellationToken))
        {
            Logger.LogWarning(@"The relative path provided was mapped to a blob URL. However, the blob does not exist in the storage account. Download will be skipped. Blob relative path: {BlobRelativePath} Storage account: {BlobHost} Blob path: {BlobAbsolutePath}", blobRelativePath, blobUrl.Host, blobUrl.AbsolutePath);
            return default;
        }

        return await this.AzureProxy.DownloadBlobAsync(blobUrl, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<string> DownloadBlobAsync(Uri blobAbsoluteUrl, CancellationToken cancellationToken)
    {
        if (!await AzureProxy.BlobExistsAsync(blobAbsoluteUrl, cancellationToken))
        {
            Logger.LogWarning(@"The blob does not exist in the storage account. Download will be skipped. Storage account: {BlobHost} Blob path: {BlobAbsolutePath}", blobAbsoluteUrl.Host, blobAbsoluteUrl.AbsolutePath);
            return default;
        }

        return await AzureProxy.DownloadBlobAsync(blobAbsoluteUrl, cancellationToken);
    }

    /// <inheritdoc />
    public async Task UploadBlobAsync(string blobRelativePath, string content, CancellationToken cancellationToken)
        => await this.AzureProxy.UploadBlobAsync(await MapLocalPathToSasUrlAsync(blobRelativePath, cancellationToken, getContainerSas: true), content, cancellationToken);

    /// <inheritdoc />
    public async Task UploadBlobAsync(Uri blobAbsoluteUrl, string content,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(blobAbsoluteUrl);

        await AzureProxy.UploadBlobAsync(blobAbsoluteUrl, content, cancellationToken);
    }

    /// <inheritdoc />
    public async Task UploadBlobFromFileAsync(string blobRelativePath, string sourceLocalFilePath, CancellationToken cancellationToken)
        => await this.AzureProxy.UploadBlobFromFileAsync(await MapLocalPathToSasUrlAsync(blobRelativePath, cancellationToken, getContainerSas: true), sourceLocalFilePath, cancellationToken);

    /// <inheritdoc />
    public abstract Task<bool> IsPublicHttpUrlAsync(string uriString, CancellationToken cancellationToken);

    /// <inheritdoc />
    public abstract Task<Uri> MapLocalPathToSasUrlAsync(string path, CancellationToken cancellationToken, TimeSpan? sasTokenDuration = default, bool getContainerSas = false);

    /// <inheritdoc />
    public abstract Task<Uri> GetInternalTesBlobUrlAsync(string blobPath, CancellationToken cancellationToken);

    /// <inheritdoc />
    public abstract Task<Uri> GetInternalTesTaskBlobUrlAsync(TesTask task, string blobPath, CancellationToken cancellationToken);

    /// <inheritdoc />
    public abstract Uri GetInternalTesTaskBlobUrlWithoutSasToken(TesTask task, string blobPath);

    /// <inheritdoc />
    public abstract Uri GetInternalTesBlobUrlWithoutSasToken(string blobPath);

    /// <summary>
    /// Tries to parse the input into a Http Url. 
    /// </summary>
    /// <param name="input">string to parse</param>
    /// <param name="uri">resulting Url if successful</param>
    /// <returns>true if the input is a Url, false otherwise</returns>
    protected static bool TryParseHttpUrlFromInput(string input, out Uri uri)
        => Uri.TryCreate(input, UriKind.Absolute, out uri) && (uri.Scheme.Equals(Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) || uri.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// True if the path is the cromwell executions folder
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    protected bool IsKnownExecutionFilePath(string path)
        => path.StartsWith(CromwellPathPrefix, StringComparison.OrdinalIgnoreCase);
}
