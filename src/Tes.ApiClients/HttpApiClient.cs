// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Azure.Core;
using Microsoft.Extensions.Caching.Memory;
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
        private readonly CachingRetryHandler cachingRetryHandler = null!;
        private readonly SHA256 sha256 = SHA256.Create();
        /// <summary>
        /// Logger instance
        /// </summary>
        protected readonly ILogger Logger = null!;
        private readonly string tokenScope = null!;
        private readonly SemaphoreSlim semaphore = new(1, 1);
        private AccessToken accessToken;

        /// <summary>
        /// Inner http client.
        /// </summary>
        public HttpClient InnerHttpClient => HttpClient;

        /// <summary>
        /// Constructor of base HttpApiClient
        /// </summary>
        /// <param name="cachingRetryHandler"></param>
        /// <param name="logger"></param>
        protected HttpApiClient(CachingRetryHandler cachingRetryHandler, ILogger logger)
        {
            ArgumentNullException.ThrowIfNull(cachingRetryHandler);
            ArgumentNullException.ThrowIfNull(logger);

            this.cachingRetryHandler = cachingRetryHandler;
            this.Logger = logger;
        }

        /// <summary>
        /// Constructor of base HttpApiClient
        /// </summary>
        /// <param name="tokenCredential"></param>
        /// <param name="cachingRetryHandler"></param>
        /// <param name="tokenScope"></param>
        /// <param name="logger"></param>
        protected HttpApiClient(TokenCredential tokenCredential, string tokenScope,
            CachingRetryHandler cachingRetryHandler, ILogger logger) : this(cachingRetryHandler, logger)
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
        /// <param name="caller">Calling method name.</param>
        /// <returns><see cref="RetryHandler.OnRetryHandler{System.Net.Http.HttpResponseMessage}"/></returns>
        private RetryHandler.OnRetryHandler<HttpResponseMessage> LogRetryErrorOnRetryHttpResponseMessageHandler([System.Runtime.CompilerServices.CallerMemberName] string caller = default)
            => new((result, timeSpan, retryCount, correlationId) =>
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
            });

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
        {
            return await cachingRetryHandler.ExecuteWithRetryAsync(async ct =>
            {
                var request = httpRequestFactory();
                if (setAuthorizationHeader)
                {
                    await AddAuthorizationHeaderToRequestAsync(request, ct);
                }

                return await HttpClient.SendAsync(request, ct);
            }, cancellationToken, LogRetryErrorOnRetryHttpResponseMessageHandler());
        }

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

            return JsonSerializer.Deserialize<TResponse>(content)!;
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

            return (await cachingRetryHandler.AppCache.GetOrCreateAsync(cacheKey,
                async _ => await HttpGetRequestWithRetryPolicyAsync(requestUrl, cancellationToken, setAuthorizationHeader)))!;
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
        {
            return await cachingRetryHandler.ExecuteWithRetryAsync(async token =>
            {
                var response = await cachingRetryHandler.ExecuteWithRetryAsync(async ct =>
                {
                    //request must be recreated in every retry.
                    var httpRequest = await CreateGetHttpRequest(requestUrl, setAuthorizationHeader, ct);

                    var httpResponse = await HttpClient.SendAsync(httpRequest, ct);
                    return httpResponse.EnsureSuccessStatusCode();
                }, token, LogRetryErrorOnRetryHttpResponseMessageHandler());

                return await ReadResponseBodyAsync(response, token);
            }, cancellationToken, RetryHandler.LogRetryErrorOnRetryHandler(Logger));
        }

        /// <summary>
        /// Returns an query string key-value, with the value escaped. If the value is null or empty returns an empty string
        /// </summary>
        /// <param name="name">parameter name</param>
        /// <param name="value">parameter value</param>
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
            if (arguments.Length == 0)
            {
                return string.Empty;
            }

            var queryString = string.Empty;
            var prefix = string.Empty;

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

        protected async Task<string> ReadResponseBodyAsync(HttpResponseMessage response, CancellationToken cancellationToken)
        {
            return await cachingRetryHandler.ExecuteWithRetryAsync(response.Content.ReadAsStringAsync, cancellationToken, RetryHandler.LogRetryErrorOnRetryHandler(Logger));
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

                var newAccessToken = await tokenCredential.GetTokenAsync(
                    new TokenRequestContext(new[] { tokenScope }),
                    cancellationToken);

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

            return hash.Aggregate(string.Empty, (current, t) => current + t.ToString("X2"));
        }

        /// <summary>
        /// Returns the response content, the response is successful 
        /// </summary>
        /// <param name="response">Response</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <typeparam name="T">Response's content deserialization type</typeparam>
        /// <returns></returns>
        protected async Task<T> GetApiResponseContentAsync<T>(HttpResponseMessage response, CancellationToken cancellationToken)
        {
            response.EnsureSuccessStatusCode();

            return JsonSerializer.Deserialize<T>(await ReadResponseBodyAsync(response, cancellationToken))!;
        }
    }
}
