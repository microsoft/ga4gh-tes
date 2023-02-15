// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using Microsoft.Extensions.Logging;

namespace TesApi.Web.Management.Clients
{
    /// <summary>
    /// Base client for Terra API clients
    /// </summary>
    public abstract class TerraApiClient : HttpApiClient
    {
        private const string TokenScope = @"https://management.azure.com/.default";

        /// <summary>
        /// Protected parameter-less constructor
        /// </summary>
        protected TerraApiClient() { }

        /// <summary>
        /// Protected constructor of TerraApiClient
        /// </summary>
        /// <param name="tokenCredential"><see cref="TokenCredential"/></param>
        /// <param name="cacheAndRetryHandler"><see cref="CacheAndRetryHandler"/></param>
        /// <param name="logger"><see cref="ILogger{TCategoryName}"/></param>
        protected TerraApiClient(TokenCredential tokenCredential, CacheAndRetryHandler cacheAndRetryHandler, ILogger logger) : base(tokenCredential, TokenScope, cacheAndRetryHandler, logger)
        {
        }
    }
}
