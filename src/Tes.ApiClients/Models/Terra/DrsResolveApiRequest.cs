// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Tes.ApiClients.Models.Terra
{
  
    public class DrsResolveRequestContent
    {
        [JsonPropertyName("url")]
        public string Url { get; set; }

        [JsonPropertyName("cloudPlatform")]
        [JsonConverter(typeof(JsonStringEnumConverter))]
        public CloudPlatform CloudPlatform { get; set; }

        [JsonPropertyName("fields")]
        public List<string> Fields { get; set; }
    }
    public enum CloudPlatform
    {
        [JsonPropertyName("azure")]
        Azure,
        [JsonPropertyName("google")]
        Google
    }

}



