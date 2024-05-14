// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Runtime.Serialization;
using System.Text.Json.Serialization;

namespace Tes.ApiClients.Models.Terra
{

    public class DrsResolveRequestContent
    {
        [JsonPropertyName("url")]
        public string Url { get; set; }

        [JsonPropertyName("cloudPlatform")]
        public CloudPlatform CloudPlatform { get; set; }

        [JsonPropertyName("fields")]
        public List<string> Fields { get; set; }
    }

    [JsonConverter(typeof(JsonStringEnumMemberConverter))]
    public enum CloudPlatform
    {
        [EnumMember(Value = "azure")]
        Azure,
        [EnumMember(Value = "gs")]
        Google
    }

    [JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonSerializable(typeof(DrsResolveRequestContent))]
    public partial class DrsResolveRequestContentContext : JsonSerializerContext
    { }
}



