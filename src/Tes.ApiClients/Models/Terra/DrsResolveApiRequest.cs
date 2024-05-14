// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json.Serialization;

namespace Tes.ApiClients.Models.Terra
{

    public class DrsResolveRequestContent
    {
        [JsonPropertyName("url")]
        public string Url { get; set; }

        [JsonPropertyName("cloudPlatform")]
        [JsonConverter(typeof(JsonStringEnumConverter<CloudPlatform>))]
        public CloudPlatform CloudPlatform { get; set; }

        [JsonPropertyName("fields")]
        public List<string> Fields { get; set; }
    }

    public enum CloudPlatform
    {
        // Lowercase to support required JSON serialization
        azure,
        gs // google storage
    }

    [JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonSerializable(typeof(DrsResolveRequestContent))]
    public partial class DrsResolveRequestContentContext : JsonSerializerContext
    { }
}



