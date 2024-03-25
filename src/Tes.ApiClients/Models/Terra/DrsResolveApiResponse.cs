// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Tes.ApiClients.Models.Terra
{

    public class DrsResolveApiResponse
    {
        [JsonPropertyName("contentType")]
        public string ContentType { get; set; }

        [JsonPropertyName("size")]
        public long Size { get; set; }

        [JsonPropertyName("timeCreated")]
        public DateTime TimeCreated { get; set; }

        [JsonPropertyName("timeUpdated")]
        public DateTime TimeUpdated { get; set; }

        [JsonPropertyName("bucket")]
        public string Bucket { get; set; }

        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("gsUri")]
        public string GsUri { get; set; }

        [JsonPropertyName("googleServiceAccount")]
        public SaKeyObject GoogleServiceAccount { get; set; }

        [JsonPropertyName("fileName")]
        public string FileName { get; set; }

        [JsonPropertyName("accessUrl")]
        public AccessUrl AccessUrl { get; set; }

        [JsonPropertyName("hashes")]
        public Dictionary<string, string> Hashes { get; set; }

        [JsonPropertyName("localizationPath")]
        public string LocalizationPath { get; set; }

        [JsonPropertyName("bondProvider")]
        public string BondProvider { get; set; }
    }

    public class SaKeyObject
    {
        [JsonPropertyName("data")]
        public Dictionary<string, object> Data { get; set; }
    }

    public class AccessUrl
    {
        [JsonPropertyName("url")]
        public string Url { get; set; }

        [JsonPropertyName("headers")]
        public Dictionary<string, string> Headers { get; set; }
    }

}
