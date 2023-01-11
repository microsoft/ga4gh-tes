// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Management.Configuration
{
    /// <summary>
    /// Cosmos DB configuration options
    /// </summary>
    public class CosmosDbOptions
    {
        /// <summary>
        /// Cosmos db configuration section.
        /// </summary>
        public const string CosmosDbAccount = "CosmosDb";
        /// <summary>
        /// Cosmos db shared key. If not provided, MI authentication will be used.
        /// </summary>
        public string CosmosDbKey { get; set; }
        /// <summary>
        /// Cosmos db endpoint. Required if a shared key is provided.
        /// </summary>
        public string CosmosDbEndpoint { get; set; }
        /// <summary>
        /// Cosmos db account name.
        /// </summary>
        public string AccountName { get; set; }
    }
}
