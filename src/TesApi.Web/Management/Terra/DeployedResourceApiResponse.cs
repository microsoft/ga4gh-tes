using System.Text.Json.Serialization;

namespace TesApi.Web.Management.Terra;

/// <summary>
/// Deployed resource api response
/// </summary>
public class DeployedResourceApiResponse
{
    [JsonPropertyName("resourceId")]
    public string ResourceId { get; set; }

    [JsonPropertyName("resourceType")]
    public string ResourceType { get; set; }

    [JsonPropertyName("resourceName")]
    public string ResourceName { get; set; }

    [JsonPropertyName("resourceParentId")]
    public string ResourceParentId { get; set; }

    [JsonPropertyName("region")]
    public string Region { get; set; }
}
