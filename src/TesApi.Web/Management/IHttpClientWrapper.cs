using System.Net.Http;
using System.Threading.Tasks;

namespace TesApi.Web.Management;

/// <summary>
/// Wraps the HTTP client with retry and caching capabilities
/// </summary>
public interface IHttpClientWrapper
{
    /// <summary>
    /// Inner http client.
    /// </summary>
    HttpClient InnerHttpClient { get; }

    /// <summary>
    /// Sends request with a retry policy.
    /// </summary>
    /// <param name="httpRequest"></param>
    /// <param name="setAuthorizationHeader"></param>
    /// <returns></returns>
    Task<HttpResponseMessage> SendRequestWithRetryPolicyAsync(HttpRequestMessage httpRequest, bool setAuthorizationHeader = false);

    /// <summary>
    /// Checks the cache and if the request was not found, sends the request with a retry policy
    /// </summary>
    /// <param name="httpRequest"></param>
    /// <param name="setAuthorizationHeader"></param>
    /// <returns></returns>
    Task<HttpResponseMessage> SendRequestWithCachingAndRetryPolicyAsync(HttpRequestMessage httpRequest, bool setAuthorizationHeader = false);
}
