// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json.Serialization;

namespace TesApi.Web.Management.Models.Terra;

/// <summary>
/// Landing zone resources api response
/// </summary>
public class LandingZoneResourcesApiResponse
{
    /// <summary>
    /// Landing zone id
    /// </summary>
    [JsonPropertyName("id")]
    public Guid Id { get; set; }

    /// <summary>
    /// List of resources in the landing zone
    /// </summary>
    [JsonPropertyName("resources")]
    public ResourceApiResponse[] Resources { get; set; }
}

[JsonSerializable(typeof(LandingZoneResourcesApiResponse))]
public partial class LandingZoneResourcesApiResponseContext : JsonSerializerContext
{ }
