// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Management.Configuration;

/// <summary>
/// Terra integration options
/// </summary>
public class TerraOptions
{
    /// <summary>
    /// Terra configuration section
    /// </summary>
    public const string SectionName = "Terra";
    private const int DefaultSasTokenExpirationInSeconds = 60 * 24 * 3; // 3 days

    /// <summary>
    /// Landing zone id containing the Tes back-end resources
    /// </summary>
    public string LandingZoneId { get; set; }

    /// <summary>
    /// Landing zone api host. 
    /// </summary>
    public string LandingZoneApiHost { get; set; }

    /// <summary>
    /// Wsm api host.
    /// </summary>
    public string WsmApiHost { get; set; }

    /// <summary>
    /// Workspace storage container resource id
    /// </summary>
    public string WorkspaceStorageContainerResourceId { get; set; }

    /// <summary>
    /// Workspace storage container name
    /// </summary>
    public string WorkspaceStorageContainerName { get; set; }

    /// <summary>
    /// Workspace storage account name
    /// </summary>
    public string WorkspaceStorageAccountName { get; set; }

    /// <summary>
    /// Workspace Id
    /// </summary>
    public string WorkspaceId { get; set; }

    /// <summary>
    /// Sas token expiration in seconds
    /// </summary>
    public int SasTokenExpirationInSeconds { get; set; } = DefaultSasTokenExpirationInSeconds;

    /// <summary>
    /// Sas token allowed Ip ranges
    /// </summary>
    public string SasAllowedIpRange { get; set; }

    /// <summary>
    /// TES executions path prefix
    /// </summary>
    public string TesExecutionsPathPrefix { get; set; }
}
