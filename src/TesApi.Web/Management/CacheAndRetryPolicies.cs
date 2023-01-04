using LazyCache;
using Polly.Retry;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Contains an App Cache instances and retry policies. 
    /// </summary>
    /// <param name="AppCache">App cache</param>
    /// <param name="RetryPolicy">Retry policy</param>
    /// <param name="AsyncRetryPolicy">Async retry policy</param>
    public record CacheAndRetryPolicies(
        IAppCache AppCache,
        RetryPolicy RetryPolicy,
        AsyncRetryPolicy AsyncRetryPolicy)
    {
    }
}
