using System;
using System.Text.Json.Serialization;

namespace TesApi.Web.Management.Terra;

/// <summary>
/// Terra quota api response
/// </summary>
public class QuotaApiResponse
{
    /// <summary>
    /// Landing zone id.
    /// </summary>
    [JsonPropertyName("landingZoneId")]
    public Guid LandingZoneId { get; set; }

    /// <summary>
    /// Azure resource id.
    /// </summary>
    [JsonPropertyName("azureResourceId")]
    public string AzureResourceId { get; set; }

    /// <summary>
    /// Resource type.
    /// </summary>
    [JsonPropertyName("resourceType")]
    public string ResourceType { get; set; }

    /// <summary>
    /// Quota values.
    /// </summary>
    [JsonPropertyName("quotaValues")]
    public QuotaValuesApiResponse QuotaValues { get; set; }

}
