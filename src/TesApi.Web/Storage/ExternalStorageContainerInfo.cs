// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Storage;

/// <summary>
/// External storage container information
/// </summary>
public class ExternalStorageContainerInfo
{
    private string sasToken;

    /// <summary>
    /// Account name
    /// </summary>
    public string AccountName { get; set; }
    /// <summary>
    /// Container name
    /// </summary>
    public string ContainerName { get; set; }
    /// <summary>
    /// Blob endpoint
    /// </summary>
    public string BlobEndpoint { get; set; }
    /// <summary>
    /// Sas Token
    /// </summary>
    public string SasToken
    {
        get => sasToken;
        set => sasToken = value?.TrimStart('?');
    }
}
