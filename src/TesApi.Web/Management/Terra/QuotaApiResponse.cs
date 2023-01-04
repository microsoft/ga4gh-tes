using System;
using System.Text.Json.Serialization;

namespace TesApi.Web.Management.Terra;

/// <summary>
/// Terra quota api response
/// </summary>
public class QuotaApiResponse
{
    [JsonPropertyName("landingZoneId")]
    public Guid LandingZoneId { get; set; }

    [JsonPropertyName("azureResourceId")]
    public string AzureResourceId { get; set; }

    [JsonPropertyName("resourceType")]
    public string ResourceType { get; set; }

    [JsonPropertyName("quotaValues")]
    public QuotaValuesApiResponse QuotaValues { get; set; }

}
