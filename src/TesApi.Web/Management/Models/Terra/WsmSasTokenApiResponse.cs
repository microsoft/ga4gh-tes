using System.Text.Json.Serialization;

namespace TesApi.Web.Management.Models.Terra;

/// <summary>
/// Wsm Get SasToken Api response
/// </summary>
public class WsmSasTokenApiResponse
{
    /// <summary>
    /// Access token.
    /// </summary>
    [JsonPropertyName("token")]
    public string Token { get; set; }

    /// <summary>
    /// Storage resource Url.
    /// </summary>
    [JsonPropertyName("url")]
    public string Url { get; set; }
}
