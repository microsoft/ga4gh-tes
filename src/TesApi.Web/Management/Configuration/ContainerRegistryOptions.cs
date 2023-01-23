namespace TesApi.Web.Management.Configuration;

public class ContainerRegistryOptions
{
    public const string ContainerRegistrySection = "ContainerRegistry";
    public bool AutoDiscoveryEnabled { get; set; } = true;
    public int RegistryInfoCacheExpirationInHours { get; set; } = 1;
}
