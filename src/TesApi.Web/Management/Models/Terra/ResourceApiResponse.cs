using System.Text.Json.Serialization;

namespace TesApi.Web.Management.Models.Terra;

public class ResourceApiResponse
{
    [JsonPropertyName("purpose")]
    public string Purpose { get; set; }

    [JsonPropertyName("deployedResources")]
    public DeployedResourceApiResponse[] DeployedResources { get; set; }
}
