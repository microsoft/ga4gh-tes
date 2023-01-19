using System.Text.Json.Serialization;

namespace TesApi.Web.Management.Models.Terra;

/// <summary>
/// Deployed resource api response
/// </summary>
public class DeployedResourceApiResponse
{
    /// <summary>
    /// Azure resource id
    /// </summary>
    [JsonPropertyName("resourceId")]
    public string ResourceId { get; set; }

    /// <summary>
    /// Resource type
    /// </summary>
    [JsonPropertyName("resourceType")]
    public string ResourceType { get; set; }

    /// <summary>
    /// Resource name
    /// </summary>
    [JsonPropertyName("resourceName")]
    public string ResourceName { get; set; }

    /// <summary>
    /// Resource parent id
    /// </summary>
    [JsonPropertyName("resourceParentId")]
    public string ResourceParentId { get; set; }

    /// <summary>
    /// Resource region
    /// </summary>
    [JsonPropertyName("region")]
    public string Region { get; set; }
}
