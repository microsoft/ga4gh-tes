// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json.Serialization;

namespace TesApi.Web.Management.Models.Terra;

/// <summary>
/// Landing zone resources by purpose api response
/// </summary>
public class ResourceApiResponse
{
    /// <summary>
    /// Resource purpose
    /// </summary>
    [JsonPropertyName("purpose")]
    public string Purpose { get; set; }

    /// <summary>
    /// Resources with the purpose
    /// </summary>
    [JsonPropertyName("deployedResources")]
    public DeployedResourceApiResponse[] DeployedResources { get; set; }
}
