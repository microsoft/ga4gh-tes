// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Microsoft.Extensions.Logging;

namespace TesApi.Web.Management.Clients
{
    /// <summary>
    /// A wrapper of the HttpClient that provides fined tuned caching and retry capabilities.
    /// </summary>
    public abstract class HttpApiClient
    {
        private static readonly HttpClient HttpClient = new();
        private readonly TokenCredential tokenCredential;
        private readonly CacheAndRetryHandler cacheAndRetryHandler;
        private readonly SHA256 sha256 = SHA256.Create();
        /// <summary>
        /// Logger instance
        /// </summary>
        protected readonly ILogger Logger;
        private readonly string tokenScope;
        private readonly SemaphoreSlim semaphore = new(1, 1);
        private AccessToken accessToken;

        /// <summary>
        /// Inner http client.
        /// </summary>
        public HttpClient InnerHttpClient => HttpClient;

        /// <summary>
        /// Constructor of base HttpApiClient
        /// </summary>
        /// <param name="cacheAndRetryHandler"></param>
        /// <param name="logger"></param>
        protected HttpApiClient(CacheAndRetryHandler cacheAndRetryHandler, ILogger logger)
        {
            ArgumentNullException.ThrowIfNull(cacheAndRetryHandler);
            ArgumentNullException.ThrowIfNull(logger);

            this.cacheAndRetryHandler = cacheAndRetryHandler;
            this.Logger = logger;
        }

        /// <summary>
        /// Constructor of base HttpApiClient
        /// </summary>
        /// <param name="tokenCredential"></param>
        /// <param name="cacheAndRetryHandler"></param>
        /// <param name="tokenScope"></param>
        /// <param name="logger"></param>
        protected HttpApiClient(TokenCredential tokenCredential, string tokenScope,
            CacheAndRetryHandler cacheAndRetryHandler, ILogger logger) : this(cacheAndRetryHandler, logger)
        {
            ArgumentNullException.ThrowIfNull(tokenCredential);
            ArgumentException.ThrowIfNullOrEmpty(tokenScope);

            this.tokenCredential = tokenCredential;
            this.tokenScope = tokenScope;
        }

        /// <summary>
        /// Protected parameter-less constructor of HttpApiClient
        /// </summary>
        protected HttpApiClient() { }

        /// <summary>
        /// Sends request with a retry policy
        /// </summary>
        /// <param name="httpRequestFactory">Factory that creates new http requests, in the event of retry the factory is called again
        /// and must be idempotent</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="setAuthorizationHeader">If true, the authentication header is set with an authentication token </param>
        /// <returns></returns>
        protected async Task<HttpResponseMessage> HttpSendRequestWithRetryPolicyAsync(
            Func<HttpRequestMessage> httpRequestFactory, CancellationToken cancellationToken, bool setAuthorizationHeader = false)
            => await cacheAndRetryHandler.ExecuteWithRetryAsync(async ct =>
            {
                var request = httpRequestFactory();
                if (setAuthorizationHeader)
                {
                    await AddAuthorizationHeaderToRequestAsync(request, ct);
                }

                return await HttpClient.SendAsync(request, ct);
            }, cancellationToken);

        /// <summary>
        /// Sends a Http Get request to the URL and returns body response as string 
        /// </summary>
        /// <param name="requestUrl"></param>
        /// <param name="setAuthorizationHeader"></param>
        /// <param name="cacheResults"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        protected async Task<string> HttpGetRequestAsync(Uri requestUrl, bool setAuthorizationHeader, bool cacheResults, CancellationToken cancellationToken)
        {
            if (cacheResults)
            {
                return await HttpGetRequestWithCachingAndRetryPolicyAsync(requestUrl, cancellationToken, setAuthorizationHeader);
            }

            return await HttpGetRequestWithRetryPolicyAsync(requestUrl, cancellationToken, setAuthorizationHeader);
        }

        /// <summary>
        /// Sends a Http Get request to the URL and deserializes the body response to the specified type 
        /// </summary>
        /// <param name="requestUrl"></param>
        /// <param name="setAuthorizationHeader"></param>
        /// <param name="cacheResults"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <typeparam name="TResponse"></typeparam>
        /// <returns></returns>
        protected async Task<TResponse> HttpGetRequestAsync<TResponse>(Uri requestUrl, bool setAuthorizationHeader,
            bool cacheResults, CancellationToken cancellationToken)
        {
            var content = await HttpGetRequestAsync(requestUrl, setAuthorizationHeader, cacheResults, cancellationToken);

            return JsonSerializer.Deserialize<TResponse>(content);
        }

        /// <summary>
        /// Checks the cache and if the request was not found, sends the GET request with a retry policy
        /// </summary>
        /// <param name="requestUrl"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="setAuthorizationHeader"></param>
        /// <returns></returns>
        protected async Task<string> HttpGetRequestWithCachingAndRetryPolicyAsync(Uri requestUrl,
            CancellationToken cancellationToken, bool setAuthorizationHeader = false)
        {
            var cacheKey = await ToCacheKeyAsync(requestUrl, setAuthorizationHeader, cancellationToken);

            return await cacheAndRetryHandler.ExecuteWithRetryAndCachingAsync(cacheKey, async ct =>
            {
                var httpRequest = await CreateGetHttpRequest(requestUrl, setAuthorizationHeader, ct);

                return await ExecuteRequestAndReadResponseBodyAsync(httpRequest, ct);
            }, cancellationToken);
        }

        /// <summary>
        /// Get request with retry policy
        /// </summary>
        /// <param name="requestUrl"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="setAuthorizationHeader"></param>
        /// <returns></returns>
        protected async Task<string> HttpGetRequestWithRetryPolicyAsync(Uri requestUrl,
            CancellationToken cancellationToken, bool setAuthorizationHeader = false)
            => await cacheAndRetryHandler.ExecuteWithRetryAsync(async ct =>
            {
                //request must be recreated in every retry.
                var httpRequest = await CreateGetHttpRequest(requestUrl, setAuthorizationHeader, ct);

                return await ExecuteRequestAndReadResponseBodyAsync(httpRequest, ct);
            }, cancellationToken);

        /// <summary>
        /// Returns an query string key-value, with the value escaped. If the value is null or empty returns an empty string
        /// </summary>
        /// <param name="name">parameter name</param>
        /// <param name="value">parameter value</param>
        /// <returns></returns>
        protected string ParseQueryStringParameter(string name, string value)
        {
            ArgumentException.ThrowIfNullOrEmpty(name);

            if (string.IsNullOrEmpty(value))
            {
                return string.Empty;
            }

            return $"{name}={Uri.EscapeDataString(value)}";
        }

        /// <summary>
        /// Creates a query string with from an array of arguments.
        /// </summary>
        /// <param name="arguments"></param>
        /// <returns></returns>
        protected string AppendQueryStringParams(params string[] arguments)
        {
            if (arguments is null || arguments.Length == 0)
            {
                return string.Empty;
            }

            var queryString = "";
            var prefix = "";

            foreach (var argument in arguments)
            {
                if (!string.IsNullOrEmpty(argument))
                {
                    queryString += prefix + argument;
                    prefix = "&";
                }
            }

            return queryString;
        }

        private async Task<HttpRequestMessage> CreateGetHttpRequest(Uri requestUrl, bool setAuthorizationHeader, CancellationToken cancellationToken)
        {
            var httpRequest = new HttpRequestMessage(HttpMethod.Get, requestUrl);

            if (setAuthorizationHeader)
            {
                await AddAuthorizationHeaderToRequestAsync(httpRequest, cancellationToken);
            }

            return httpRequest;
        }

        private static async Task<string> ExecuteRequestAndReadResponseBodyAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var response = await HttpClient.SendAsync(request, cancellationToken);

            response.EnsureSuccessStatusCode();

            return await response.Content.ReadAsStringAsync(cancellationToken);
        }

        private async Task AddAuthorizationHeaderToRequestAsync(HttpRequestMessage requestMessage, CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(tokenScope))
            {
                throw new ArgumentException("Can't set the authentication token as the token scope is missing",
                    nameof(tokenScope));
            }

            Logger.LogTrace("Getting token for scope:{}", tokenScope);

            try
            {
                var token = await GetOrRefreshAccessTokenAsync(cancellationToken);

                requestMessage.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            }
            catch (Exception e)
            {
                Logger.LogError(@"Failed to set authentication header with the access token for scope:{tokenScope}",
                    e);
                throw;
            }
        }

        private async Task<string> GetOrRefreshAccessTokenAsync(CancellationToken cancellationToken)
        {
            try
            {
                await semaphore.WaitAsync(cancellationToken);

                if (DateTimeOffset.UtcNow < accessToken.ExpiresOn)
                {
                    Logger.LogTrace(
                        $"Using existing token. Token has not expired. Token expiration date: {accessToken.ExpiresOn}");
                    return accessToken.Token;
                }

                var newAccessToken = await tokenCredential.GetTokenAsync(
                    new TokenRequestContext(new[] { tokenScope }),
                    cancellationToken);

                Logger.LogTrace($"Returning a new token with an expiration date of: {newAccessToken.ExpiresOn}");
                accessToken = newAccessToken;
                return accessToken.Token;
            }
            finally
            {
                semaphore.Release();
            }
        }

        /// <summary>
        /// Creates a string hash value from the URL that can be used as cached key.
        /// </summary>
        /// <param name="requestUrl">Request url</param>
        /// <param name="perUser">if true, caching data will be per user</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public async Task<string> ToCacheKeyAsync(Uri requestUrl, bool perUser, CancellationToken cancellationToken)
        {
            var cacheKey = requestUrl.ToString();

            if (perUser)
            {
                //append the token to create a string that is unique to the user and the URL
                var token = await GetOrRefreshAccessTokenAsync(cancellationToken);
                cacheKey += token;
            }

            return ToHash(cacheKey);
        }

        private string ToHash(string input)
        {
            var hash = sha256.ComputeHash(Encoding.ASCII.GetBytes(input));

            return hash.Aggregate("", (current, t) => current + t.ToString("X2"));
        }

        /// <summary>
        /// Returns the response content, the response is successful 
        /// </summary>
        /// <param name="response">Response</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <typeparam name="T">Response's content deserialization type</typeparam>
        /// <returns></returns>
        protected static async Task<T> GetApiResponseContentAsync<T>(HttpResponseMessage response, CancellationToken cancellationToken)
        {
            response.EnsureSuccessStatusCode();

            return JsonSerializer.Deserialize<T>(await response.Content.ReadAsStringAsync(cancellationToken));
        }
    }
}
