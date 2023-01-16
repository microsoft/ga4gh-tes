namespace TesApi.Web.Management.Configuration;

/// <summary>
/// Terra integration options
/// </summary>
public class TerraOptions
{
    /// <summary>
    /// Terra configuration section
    /// </summary>
    public const string Terra = "Terra";
    /// <summary>
    /// Landing zone id containing the Tes back-end resources
    /// </summary>
    public string LandingZoneId { get; set; }
    /// <summary>
    /// Landing zone api host. 
    /// </summary>
    public string LandingZoneApiHost { get; set; }
}
