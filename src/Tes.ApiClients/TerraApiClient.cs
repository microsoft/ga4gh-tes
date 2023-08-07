﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using Microsoft.Extensions.Logging;

namespace Tes.ApiClients
{
    /// <summary>
    /// Base client for Terra API clients
    /// </summary>
    public abstract class TerraApiClient : HttpApiClient
    {
        private const string TokenScope = @"https://management.azure.com/.default";
        protected readonly string ApiUrl;

        /// <summary>
        /// Protected parameter-less constructor
        /// </summary>
        protected TerraApiClient() { }

        /// <summary>
        /// Protected constructor of TerraApiClient
        /// </summary>
        /// <param name="apiUrl">API Host</param>
        /// <param name="tokenCredential"><see cref="TokenCredential"/></param>
        /// <param name="cacheAndRetryHandler"><see cref="CacheAndRetryHandler"/></param>
        /// <param name="logger"><see cref="ILogger{TCategoryName}"/></param>
        protected TerraApiClient(string apiUrl, TokenCredential tokenCredential, CacheAndRetryHandler cacheAndRetryHandler, ILogger logger) : base(tokenCredential, TokenScope, cacheAndRetryHandler, logger)
        {
            ArgumentException.ThrowIfNullOrEmpty(apiUrl);
            ArgumentNullException.ThrowIfNull(tokenCredential);
            ArgumentNullException.ThrowIfNull(cacheAndRetryHandler);
            ArgumentNullException.ThrowIfNull(logger);

            ApiUrl = apiUrl;
        }
    }
}
