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
        private static readonly HttpClient HttpClient = new HttpClient();
        private readonly TokenCredential tokenCredential;
        private readonly CacheAndRetryHandler cacheAndRetryHandler;
        private readonly SHA256 sha256 = SHA256.Create();
        private readonly ILogger<HttpApiClient> logger;
        private readonly string tokenScope;

        /// <summary>
        /// Inner http client.
        /// </summary>
        public HttpClient InnerHttpClient => HttpClient;

        /// <summary>
        /// Constructor of base HttpApiClient
        /// </summary>
        /// <param name="cacheAndRetryHandler"></param>
        /// <param name="logger"></param>
        protected HttpApiClient(CacheAndRetryHandler cacheAndRetryHandler, ILogger<HttpApiClient> logger)
        {
            ArgumentNullException.ThrowIfNull(cacheAndRetryHandler);
            ArgumentNullException.ThrowIfNull(logger);

            this.cacheAndRetryHandler = cacheAndRetryHandler;
            this.logger = logger;
        }

        /// <summary>
        /// Constructor of base HttpApiClient
        /// </summary>
        /// <param name="tokenCredential"></param>
        /// <param name="cacheAndRetryHandler"></param>
        /// <param name="tokenScope"></param>
        /// <param name="logger"></param>
        protected HttpApiClient(TokenCredential tokenCredential, string tokenScope, CacheAndRetryHandler cacheAndRetryHandler, ILogger<HttpApiClient> logger) : this(cacheAndRetryHandler, logger)
        {
            ArgumentNullException.ThrowIfNull(tokenCredential);
            ArgumentException.ThrowIfNullOrEmpty(tokenScope);

            this.tokenCredential = tokenCredential;
            this.tokenScope = tokenScope;
        }

        /// <summary>
        /// Sends request with a retry policy.
        /// </summary>
        /// <param name="httpRequest"></param>
        /// <param name="setAuthorizationHeader"></param>
        /// <returns></returns>
        protected async Task<HttpResponseMessage> HttpSendRequestWithRetryPolicyAsync(HttpRequestMessage httpRequest, bool setAuthorizationHeader = false)
        {
            if (setAuthorizationHeader)
            {
                await AddAuthorizationHeaderToRequestAsync(httpRequest);
            }

            return await cacheAndRetryHandler.ExecuteWithRetryAsync(() => HttpClient.SendAsync(httpRequest));
        }

        /// <summary>
        /// Sends a Http Get request to the URL and returns body response as string 
        /// </summary>
        /// <param name="requestUrl"></param>
        /// <param name="setAuthorizationHeader"></param>
        /// <param name="cacheResults"></param>
        /// <returns></returns>
        protected async Task<string> HttpGetRequestAsync(Uri requestUrl, bool setAuthorizationHeader, bool cacheResults)
        {
            if (cacheResults)
            {
                return await HttpGetRequestWithCachingAndRetryPolicyAsync(requestUrl, setAuthorizationHeader);
            }

            return await HttpGetRequestWithRetryPolicyAsync(requestUrl);
        }
        /// <summary>
        /// Sends a Http Get request to the URL and deserializes the body response to the specified type 
        /// </summary>
        /// <param name="requestUrl"></param>
        /// <param name="setAuthorizationHeader"></param>
        /// <param name="cacheResults"></param>
        /// <typeparam name="TResponse"></typeparam>
        /// <returns></returns>
        protected async Task<TResponse> HttpGetRequestAsync<TResponse>(Uri requestUrl, bool setAuthorizationHeader, bool cacheResults)
        {
            var content = await HttpGetRequestAsync(requestUrl, setAuthorizationHeader, cacheResults);

            return JsonSerializer.Deserialize<TResponse>(content);
        }

        /// <summary>
        /// Checks the cache and if the request was not found, sends the GET request with a retry policy
        /// </summary>
        /// <param name="requestUrl"></param>
        /// <param name="setAuthorizationHeader"></param>
        /// <returns></returns>
        protected async Task<string> HttpGetRequestWithCachingAndRetryPolicyAsync(Uri requestUrl,
            bool setAuthorizationHeader = false)
        {
            var httpRequest = await CreateGetHttpRequest(requestUrl, setAuthorizationHeader);

            var cacheKey = ToCacheKey(httpRequest);

            return await cacheAndRetryHandler.ExecuteWithRetryAndCachingAsync(cacheKey, () => ExecuteRequestAndReadResponseBodyAsync(httpRequest));
        }

        /// <summary>
        /// Get request with retry policy
        /// </summary>
        /// <param name="requestUrl"></param>
        /// <param name="setAuthorizationHeader"></param>
        /// <returns></returns>
        protected async Task<string> HttpGetRequestWithRetryPolicyAsync(Uri requestUrl,
            bool setAuthorizationHeader = false)
        {
            var httpRequest = await CreateGetHttpRequest(requestUrl, setAuthorizationHeader);

            return await cacheAndRetryHandler.ExecuteWithRetryAsync(() => ExecuteRequestAndReadResponseBodyAsync(httpRequest));
        }

        private async Task<HttpRequestMessage> CreateGetHttpRequest(Uri requestUrl, bool setAuthorizationHeader)
        {
            var httpRequest = new HttpRequestMessage(HttpMethod.Get, requestUrl);

            if (setAuthorizationHeader)
            {
                await AddAuthorizationHeaderToRequestAsync(httpRequest);
            }

            return httpRequest;
        }

        private async Task<string> ExecuteRequestAndReadResponseBodyAsync(HttpRequestMessage request)
        {
            var response = await HttpClient.SendAsync(request);

            response.EnsureSuccessStatusCode();

            return await response.Content.ReadAsStringAsync();
        }

        private async Task AddAuthorizationHeaderToRequestAsync(HttpRequestMessage requestMessage)
        {
            if (string.IsNullOrEmpty(tokenScope))
            {
                throw new ArgumentException("Can't set the authentication token as the token scope is missing", nameof(tokenScope));
            }

            logger.LogInformation("Getting token for scope:{}", tokenScope);
            try
            {
                var accessToken = await tokenCredential.GetTokenAsync(new TokenRequestContext(new[]
                    {
                        tokenScope
                    }),
                    CancellationToken.None);
                requestMessage.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken.Token);

            }
            catch (Exception e)
            {
                logger.LogError(@"Failed to set authentication header with the access token for scope:{tokenScope}", e);
                throw;
            }
        }

        /// <summary>
        /// Creates a unique cache key that from the request data.
        /// </summary>
        /// <param name="httpRequest">request</param>
        /// <returns></returns>
        public string ToCacheKey(HttpRequestMessage httpRequest)
        {
            // ToString() returns the URI, headers and method from the request. 
            var cacheKey = httpRequest.ToString();

            var hash = sha256.ComputeHash(Encoding.ASCII.GetBytes(cacheKey));

            return hash.Aggregate("", (current, t) => current + t.ToString("X2"));
        }
    }
}
