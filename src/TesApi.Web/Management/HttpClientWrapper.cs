using System;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Microsoft.Extensions.Logging;

namespace TesApi.Web.Management
{
    /// <summary>
    /// A wrapper of the HttpClient that provides fined tuned caching and retry capabilities.
    /// </summary>
    public class HttpClientWrapper : IHttpClientWrapper
    {
        private readonly HttpClient httpClient;
        private readonly TokenCredential tokenCredential;
        private readonly CacheAndRetryHandler cacheAndRetryPolicies;
        private readonly SHA256 sha256 = SHA256.Create();
        private readonly ILogger<HttpClientWrapper> logger;
        private readonly string tokenScope;

        /// <summary>
        /// Inner http client.
        /// </summary>
        public HttpClient InnerHttpClient => httpClient;

        /// <summary>
        /// Constructor of HttpClientWithCacheAndRetryPolicies
        /// </summary>
        /// <param name="httpClient"></param>
        /// <param name="tokenCredential"></param>
        /// <param name="cacheAndRetryPolicies"></param>
        /// <param name="logger"></param>
        /// <param name="tokenScope"></param>
        public HttpClientWrapper(HttpClient httpClient, TokenCredential tokenCredential, CacheAndRetryHandler cacheAndRetryPolicies, string tokenScope, ILogger<HttpClientWrapper> logger)
        {
            ArgumentNullException.ThrowIfNull(httpClient);
            ArgumentNullException.ThrowIfNull(tokenCredential);
            ArgumentNullException.ThrowIfNull(cacheAndRetryPolicies);
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentException.ThrowIfNullOrEmpty(tokenScope);

            this.httpClient = httpClient;
            this.tokenCredential = tokenCredential;
            this.cacheAndRetryPolicies = cacheAndRetryPolicies;
            this.logger = logger;
            this.tokenScope = tokenScope;
        }

        /// <summary>
        /// Sends request with a retry policy.
        /// </summary>
        /// <param name="httpRequest"></param>
        /// <param name="setAuthorizationHeader"></param>
        /// <returns></returns>
        public async Task<HttpResponseMessage> SendRequestWithRetryPolicyAsync(HttpRequestMessage httpRequest, bool setAuthorizationHeader = false)
        {
            if (setAuthorizationHeader)
            {
                await AddAuthorizationHeaderToRequestAsync(httpRequest);
            }

            return await cacheAndRetryPolicies.ExecuteWithRetryAsync(() => httpClient.SendAsync(httpRequest));
        }

        /// <summary>
        /// Checks the cache and if the request was not found, sends the request with a retry policy
        /// </summary>
        /// <param name="httpRequest"></param>
        /// <param name="setAuthorizationHeader"></param>
        /// <returns></returns>
        public async Task<HttpResponseMessage> SendRequestWithCachingAndRetryPolicyAsync(HttpRequestMessage httpRequest, bool setAuthorizationHeader = false)
        {
            if (setAuthorizationHeader)
            {
                await AddAuthorizationHeaderToRequestAsync(httpRequest);
            }

            var cacheKey = ToCacheKey(httpRequest);

            return await cacheAndRetryPolicies.ExecuteWithRetryAndCachingAsync(cacheKey, () => httpClient.SendAsync(httpRequest));
        }

        private async Task AddAuthorizationHeaderToRequestAsync(HttpRequestMessage requestMessage)
        {
            logger.LogInformation("Getting token for scope:{}", tokenScope);
            try
            {
                var accessToken = await tokenCredential.GetTokenAsync(new TokenRequestContext(new string[]
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

        private string ToCacheKey(HttpRequestMessage httpRequest)
        {
            // ToString() returns the URI, headers and method from the request. 
            var cacheKey = httpRequest.ToString();

            var hash = sha256.ComputeHash(Encoding.ASCII.GetBytes(cacheKey));

            return hash.Aggregate("", (current, t) => current + t.ToString("X2"));
        }
    }
}
