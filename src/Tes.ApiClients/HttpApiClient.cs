// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Azure.Core;
using CommonUtilities;
using Microsoft.Extensions.Logging;

namespace Tes.ApiClients
{
    /// <summary>
    /// A wrapper of the HttpClient that provides fined tuned caching and retry capabilities.
    /// </summary>
    public abstract class HttpApiClient
    {
        private static readonly HttpClient HttpClient = new();
        private readonly TokenCredential tokenCredential = null!;
        private readonly SHA256 sha256 = SHA256.Create();
        /// <summary>
        /// Logger instance
        /// </summary>
        protected readonly ILogger Logger = null!;
        private readonly string tokenScope = null!;
        private readonly SemaphoreSlim semaphore = new(1, 1);
        private AccessToken accessToken;

        protected readonly CachingRetryHandler.CachingAsyncRetryHandlerPolicy<HttpResponseMessage> cachingRetryHandler;

        /// <summary>
        /// Inner http client.
        /// </summary>
        public HttpClient InnerHttpClient => HttpClient;

        /// <summary>
        /// Constructor of base HttpApiClient
        /// </summary>
        /// <param name="cachingRetryPolicyBuilder"></param>
        /// <param name="logger"></param>
        protected HttpApiClient(CachingRetryPolicyBuilder cachingRetryPolicyBuilder, ILogger logger)
        {
            ArgumentNullException.ThrowIfNull(cachingRetryPolicyBuilder);
            ArgumentNullException.ThrowIfNull(logger);

            this.Logger = logger;

            cachingRetryHandler = cachingRetryPolicyBuilder
                .DefaultRetryHttpResponseMessagePolicyBuilder()
                .SetOnRetryBehavior(onRetry: LogRetryErrorOnRetryHttpResponseMessageHandler())
                .AddCaching()
                .AsyncBuild();
        }

        /// <summary>
        /// Constructor of base HttpApiClient
        /// </summary>
        /// <param name="tokenCredential"></param>
        /// <param name="cachingRetryPolicyBuilder"></param>
        /// <param name="tokenScope"></param>
        /// <param name="logger"></param>
        protected HttpApiClient(TokenCredential tokenCredential, string tokenScope,
            CachingRetryPolicyBuilder cachingRetryPolicyBuilder, ILogger logger) : this(cachingRetryPolicyBuilder, logger)
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
        /// A logging Polly retry handler.
        /// </summary>
        /// <returns><see cref="RetryHandler.OnRetryHandler{HttpResponseMessage}"/></returns>
        private RetryHandler.OnRetryHandler<HttpResponseMessage> LogRetryErrorOnRetryHttpResponseMessageHandler()
            => (result, timeSpan, retryCount, correlationId, caller) =>
            {
                if (result.Exception is null)
                {
                    Logger?.LogError(@"Retrying in {Method} due to HTTP status {HttpStatus}: RetryCount: {RetryCount} TimeSpan: {TimeSpan} CorrelationId: {CorrelationId}",
                        caller, result.Result.StatusCode.ToString("G"), retryCount, timeSpan.ToString("c"), correlationId.ToString("D"));
                }
                else
                {
                    Logger?.LogError(result.Exception, @"Retrying in {Method} due to '{Message}': RetryCount: {RetryCount} TimeSpan: {TimeSpan} CorrelationId: {CorrelationId}",
                        caller, result.Exception.Message, retryCount, timeSpan.ToString("c"), correlationId.ToString("D"));
                }
            };

        /// <summary>
        /// Sends request with a retry policy
        /// </summary>
        /// <param name="httpRequestFactory">Factory that creates new http requests, in the event of retry the factory is called again
        /// and must be idempotent.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="setAuthorizationHeader">If true, the authentication header is set with an authentication token.</param>
        /// <returns></returns>
        protected async Task<HttpResponseMessage> HttpSendRequestWithRetryPolicyAsync(
            Func<HttpRequestMessage> httpRequestFactory, CancellationToken cancellationToken, bool setAuthorizationHeader = false)
            => await cachingRetryHandler.ExecuteWithRetryAsync(async ct =>
            {
                var request = httpRequestFactory();

                if (setAuthorizationHeader)
                {
                    await AddAuthorizationHeaderToRequestAsync(request, ct);
                }

                return await HttpClient.SendAsync(request, ct);
            }, cancellationToken);

        /// <summary>
        /// Sends a Http Get request to the URL and deserializes the body response to the specified type
        /// </summary>
        /// <param name="requestUrl"></param>
        /// <param name="setAuthorizationHeader">If true, the authentication header is set with an authentication token.</param>
        /// <param name="cacheResults"></param>
        /// <param name="typeInfo">JSON serialization-related metadata.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <typeparam name="TResponse">Response's content deserialization type.</typeparam>
        /// <returns></returns>
        protected async Task<TResponse> HttpGetRequestAsync<TResponse>(Uri requestUrl, bool setAuthorizationHeader,
            bool cacheResults, JsonTypeInfo<TResponse> typeInfo, CancellationToken cancellationToken)
        {
            if (cacheResults)
            {
                return await HttpGetRequestWithCachingAndRetryPolicyAsync(requestUrl, typeInfo, cancellationToken, setAuthorizationHeader);
            }

            return await HttpGetRequestWithRetryPolicyAsync(requestUrl, typeInfo, cancellationToken, setAuthorizationHeader);
        }

        /// <summary>
        /// Checks the cache and if the request was not found, sends the GET request with a retry policy
        /// </summary>
        /// <param name="requestUrl"></param>
        /// <param name="typeInfo">JSON serialization-related metadata.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="setAuthorizationHeader">If true, the authentication header is set with an authentication token.</param>
        /// <typeparam name="TResponse">Response's content deserialization type.</typeparam>
        /// <returns></returns>
        protected async Task<TResponse> HttpGetRequestWithCachingAndRetryPolicyAsync<TResponse>(Uri requestUrl,
            JsonTypeInfo<TResponse> typeInfo, CancellationToken cancellationToken, bool setAuthorizationHeader = false)
        {
            var cacheKey = await ToCacheKeyAsync(requestUrl, setAuthorizationHeader, cancellationToken);

            return (await cachingRetryHandler.ExecuteWithRetryConversionAndCachingAsync(cacheKey, async ct =>
            {
                //request must be recreated in every retry.
                var httpRequest = await CreateGetHttpRequest(requestUrl, setAuthorizationHeader, ct);

                return await HttpClient.SendAsync(httpRequest, ct);
            },
            (m, ct) => GetApiResponseContentAsync(m, typeInfo, ct), cancellationToken))!;
        }

        /// <summary>
        /// Checks the cache and if the request was not found, sends the GET request with a retry policy.
        /// If the GET request is successful, adds it to the cache with the specified TTL.
        /// </summary>
        /// <param name="requestUrl"></param>
        /// <param name="typeInfo">JSON serialization-related metadata.</param>
        /// <param name="cacheTTL">Time after which a newly-added entry will expire from the cache.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="setAuthorizationHeader">If true, the authentication header is set with an authentication token.</param>
        /// <typeparam name="TResponse">Response's content deserialization type.</typeparam>
        /// <returns></returns>
        protected async Task<TResponse> HttpGetRequestWithExpirableCachingAndRetryPolicyAsync<TResponse>(Uri requestUrl,
            JsonTypeInfo<TResponse> typeInfo, TimeSpan cacheTTL, CancellationToken cancellationToken, bool setAuthorizationHeader = false)
        {
            var cacheKey = await ToCacheKeyAsync(requestUrl, setAuthorizationHeader, cancellationToken);

            return (await cachingRetryHandler.ExecuteWithRetryConversionAndCachingAsync(cacheKey, async ct =>
            {
                //request must be recreated in every retry.
                var httpRequest = await CreateGetHttpRequest(requestUrl, setAuthorizationHeader, ct);

                return await HttpClient.SendAsync(httpRequest, ct);
            },
            (m, ct) => GetApiResponseContentAsync(m, typeInfo, ct), DateTimeOffset.Now + cacheTTL, cancellationToken))!;
        }

        /// <summary>
        /// Get request with retry policy
        /// </summary>
        /// <param name="requestUrl"></param>
        /// <param name="typeInfo">JSON serialization-related metadata.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="setAuthorizationHeader">If true, the authentication header is set with an authentication token.</param>
        /// <typeparam name="TResponse">Response's content deserialization type.</typeparam>
        /// <returns></returns>
        protected async Task<TResponse> HttpGetRequestWithRetryPolicyAsync<TResponse>(Uri requestUrl,
            JsonTypeInfo<TResponse> typeInfo, CancellationToken cancellationToken, bool setAuthorizationHeader = false)
            => await cachingRetryHandler.ExecuteWithRetryAndConversionAsync(async ct =>
            {
                //request must be recreated in every retry.
                var httpRequest = await CreateGetHttpRequest(requestUrl, setAuthorizationHeader, ct);

                return await HttpClient.SendAsync(httpRequest, ct);
            },
            (m, ct) => GetApiResponseContentAsync(m, typeInfo, ct), cancellationToken);

        /// <summary>
        /// Returns an query string key-value, with the value escaped. If the value is null or empty returns an empty string
        /// </summary>
        /// <param name="name">parameter name.</param>
        /// <param name="value">parameter value.</param>
        /// <returns></returns>
        protected static string ParseQueryStringParameter(string name, string value)
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
        protected static string AppendQueryStringParams(params string[] arguments)
        {
            return string.Join("&", arguments.Where(argument => !string.IsNullOrEmpty(argument)));
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

        /// <summary>
        /// Sends an Http request to the URL and deserializes the body response to the specified type
        /// </summary>
        /// <param name="httpRequestFactory">Factory that creates new http requests, in the event of retry the factory is called again
        /// and must be idempotent.</param>
        /// <param name="typeInfo">JSON serialization-related metadata.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="setAuthorizationHeader">If true, the authentication header is set with an authentication token.</param>
        /// <typeparam name="TResponse">Response's content deserialization type.</typeparam>
        /// <returns></returns>
        protected async Task<TResponse> HttpGetRequestWithRetryPolicyAsync<TResponse>(
            Func<HttpRequestMessage> httpRequestFactory, JsonTypeInfo<TResponse> typeInfo, CancellationToken cancellationToken, bool setAuthorizationHeader = false)
        {
            return await cachingRetryHandler.ExecuteWithRetryAndConversionAsync(async ct =>
            {
                var request = httpRequestFactory();

                if (setAuthorizationHeader)
                {
                    await AddAuthorizationHeaderToRequestAsync(request, ct);
                }

                return await HttpClient.SendAsync(request, ct);
            },
            (m, ct) => GetApiResponseContentAsync(m, typeInfo, ct), cancellationToken);
        }

        private async Task AddAuthorizationHeaderToRequestAsync(HttpRequestMessage requestMessage, CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(tokenScope))
            {
                throw new ArgumentException("Can't set the authentication token as the token scope is missing",
                    nameof(tokenScope));
            }

            Logger.LogTrace("Getting token for scope:{TokenScope}", tokenScope);

            try
            {
                var token = await GetOrRefreshAccessTokenAsync(cancellationToken);

                requestMessage.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            }
            catch (Exception e)
            {
                Logger.LogError(e, @"Failed to set authentication header with the access token for scope:{TokenScope}",
                    tokenScope);
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
                        @"Using existing token. Token has not expired. Token expiration date: {TokenExpiresOn}", accessToken.ExpiresOn);
                    return accessToken.Token;
                }

                var newAccessToken = await tokenCredential.GetTokenAsync(new TokenRequestContext([tokenScope]), cancellationToken);

                Logger.LogTrace(@"Returning a new token with an expiration date of: {TokenExpiresOn}", newAccessToken.ExpiresOn);
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
        /// <param name="requestUrl">Request url.</param>
        /// <param name="perUser">if true, caching data will be per user.</param>
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

            return hash.Aggregate(string.Empty, (current, t) => current + t.ToString("X2"));
        }

        /// <summary>
        /// Returns the response content, the response is successful
        /// </summary>
        /// <param name="response">Response.</param>
        /// <param name="typeInfo">JSON serialization-related metadata.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <typeparam name="TResponse">Response's content deserialization type.</typeparam>
        /// <returns></returns>
        protected static async Task<TResponse> GetApiResponseContentAsync<TResponse>(HttpResponseMessage response, JsonTypeInfo<TResponse> typeInfo, CancellationToken cancellationToken)
        {
            response.EnsureSuccessStatusCode();

            return JsonSerializer.Deserialize(await ReadResponseBodyAsync(response, cancellationToken), typeInfo)!;
        }

        /// <summary>
        /// Returns the response content, the response is successful
        /// </summary>
        /// <param name="response">Response.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        protected static async Task<string> ReadResponseBodyAsync(HttpResponseMessage response, CancellationToken cancellationToken)
        {
            return await response.Content.ReadAsStringAsync(cancellationToken);
        }


        /// <summary>
        /// Creates a string content from an object
        /// </summary>
        protected static HttpContent CreateJsonStringContent<T>(T contentObject, JsonTypeInfo<T> typeInfo)
        {
            var stringContent = new StringContent(JsonSerializer.Serialize(contentObject,
                typeInfo), Encoding.UTF8, System.Net.Mime.MediaTypeNames.Application.Json);

            return stringContent;
        }

        protected async Task LogResponseContentAsync(HttpResponseMessage response, string errMessage, Exception ex, CancellationToken cancellationToken)
        {
            var responseContent = string.Empty;

            if (response is not null)
            {
                responseContent = await ReadResponseBodyAsync(response, cancellationToken);
            }

            Logger.LogError(ex, @"{ErrorMessage}. Response content:{ResponseContent}", errMessage, responseContent);
        }
    }
}
