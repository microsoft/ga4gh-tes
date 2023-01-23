namespace TesApi.Web.Management.Configuration;

/// <summary>
/// Configuration options for container registries
/// </summary>
public class ContainerRegistryOptions
{
    /// <summary>
    /// Configuration section.
    /// </summary>
    public const string ContainerRegistrySection = "ContainerRegistry";
    /// <summary>
    /// Enables/disables auto-discovery features 
    /// </summary>
    public bool AutoDiscoveryEnabled { get; set; } = true;
    /// <summary>
    /// Controls for how long registry information is cached
    /// </summary>
    public int RegistryInfoCacheExpirationInHours { get; set; } = 1;
}
