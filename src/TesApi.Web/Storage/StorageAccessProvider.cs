// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;

namespace TesApi.Web.Storage;

/// <summary>
/// Provides base class for abstracting storage access by using local path references in form of /storageaccount/container/blobpath
/// </summary>
public abstract class StorageAccessProvider : IStorageAccessProvider
{
    /// <summary>
    /// Cromwell path prefix
    /// </summary>
    protected const string CromwellPathPrefix = "/cromwell-executions/";
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
        try
        {
            return await this.AzureProxy.DownloadBlobAsync(new Uri(await MapLocalPathToSasUrlAsync(blobRelativePath, cancellationToken, getContainerSas: false, SharedAccessBlobPermissions.Read)), cancellationToken);
        }
        catch
        {
            return null;
        }
    }

    /// <inheritdoc />
    public async Task UploadBlobAsync(string blobRelativePath, string content, CancellationToken cancellationToken)
        => await this.AzureProxy.UploadBlobAsync(new Uri(await MapLocalPathToSasUrlAsync(blobRelativePath, cancellationToken, getContainerSas: true, SharedAccessBlobPermissions.Create | SharedAccessBlobPermissions.Write)), content, cancellationToken);

    /// <inheritdoc />
    public async Task UploadBlobFromFileAsync(string blobRelativePath, string sourceLocalFilePath, CancellationToken cancellationToken)
        => await this.AzureProxy.UploadBlobFromFileAsync(new Uri(await MapLocalPathToSasUrlAsync(blobRelativePath, cancellationToken, getContainerSas: true, SharedAccessBlobPermissions.Create | SharedAccessBlobPermissions.Write)), sourceLocalFilePath, cancellationToken);

    /// <inheritdoc />
    public abstract Task<bool> IsPublicHttpUrlAsync(string uriString, CancellationToken cancellationToken);

    /// <inheritdoc />
    public abstract Task<string> MapLocalPathToSasUrlAsync(string path, CancellationToken cancellationToken, bool getContainerSas = false, SharedAccessBlobPermissions permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Create | SharedAccessBlobPermissions.Write | SharedAccessBlobPermissions.List);

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
