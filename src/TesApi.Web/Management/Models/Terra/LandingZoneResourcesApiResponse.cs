using System;
using System.Text.Json.Serialization;

namespace TesApi.Web.Management.Models.Terra;

public class LandingZoneResourcesApiResponse
{
    [JsonPropertyName("id")]
    public Guid Id { get; set; }

    [JsonPropertyName("resources")]
    public ResourceApiResponse[] Resources { get; set; }
}
