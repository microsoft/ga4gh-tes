// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Extensions.Http;
using Polly.Retry;
using Tes.ApiClients.Options;

namespace Tes.ApiClients;

/// <summary>
/// Utility class that facilitates the retry policy implementations for HTTP clients. 
/// </summary>
public class RetryHandler
{
    private readonly RetryPolicy retryPolicy = null!;
    private readonly AsyncRetryPolicy asyncRetryPolicy = null!;
    private readonly AsyncRetryPolicy<HttpResponseMessage> asyncHttpRetryPolicy = null!;

    /// <summary>
    /// Synchronous retry policy instance.
    /// </summary>
    public virtual RetryPolicy RetryPolicy => retryPolicy;

    public RetryHandler(IOptions<RetryPolicyOptions> retryPolicyOptions)
    {
        ArgumentNullException.ThrowIfNull(retryPolicyOptions);

        this.retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetry(retryPolicyOptions.Value.MaxRetryCount,
                (attempt) => TimeSpan.FromSeconds(Math.Pow(retryPolicyOptions.Value.ExponentialBackOffExponent,
                    attempt)));
        this.asyncRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(retryPolicyOptions.Value.MaxRetryCount,
                (attempt) => TimeSpan.FromSeconds(Math.Pow(retryPolicyOptions.Value.ExponentialBackOffExponent,
                    attempt)));
        this.asyncHttpRetryPolicy = HttpPolicyExtensions.HandleTransientHttpError()
            .OrResult(r => r.StatusCode == HttpStatusCode.TooManyRequests)
            .WaitAndRetryAsync(retryPolicyOptions.Value.MaxRetryCount,
                (attempt) => TimeSpan.FromSeconds(Math.Pow(retryPolicyOptions.Value.ExponentialBackOffExponent,
                    attempt)));
    }

    /// <summary>
    /// Parameter-less constructor for mocking
    /// </summary>
    protected RetryHandler() { }

    /// <summary>
    /// Asynchronous retry policy instance.
    /// </summary>
    public virtual AsyncRetryPolicy AsyncRetryPolicy => asyncRetryPolicy;

    /// <summary>
    /// Executes a delegate with the specified policy.
    /// </summary>
    /// <param name="action">Action to execute</param>
    /// <returns>Result instance</returns>
    public void ExecuteWithRetry(Action action)
    {
        ArgumentNullException.ThrowIfNull(action);

        retryPolicy.Execute(action);
    }

    /// <summary>
    /// Executes a delegate with the specified policy.
    /// </summary>
    /// <param name="action">Action to execute</param>
    /// <returns>Result instance</returns>
    public TResult ExecuteWithRetry<TResult>(Func<TResult> action)
    {
        ArgumentNullException.ThrowIfNull(action);

        return retryPolicy.Execute(action);
    }

    /// <summary>
    /// Executes a delegate with the specified async policy. 
    /// </summary>
    /// <param name="action">Action to execute</param>
    /// <typeparam name="TResult">Result type</typeparam>
    /// <returns>Result instance</returns>
    public virtual Task<TResult> ExecuteWithRetryAsync<TResult>(Func<Task<TResult>> action)
    {
        ArgumentNullException.ThrowIfNull(action);

        return asyncRetryPolicy.ExecuteAsync(action);
    }

    /// <summary>
    /// Executes a delegate with the specified async policy.
    /// </summary>
    /// <param name="action">Action to execute</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
    /// <typeparam name="TResult">Result type</typeparam>
    /// <returns>Result instance</returns>
    public virtual Task<TResult> ExecuteWithRetryAsync<TResult>(Func<CancellationToken, Task<TResult>> action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);

        return asyncRetryPolicy.ExecuteAsync(action, cancellationToken);
    }

    /// <summary>
    /// Executes a delegate with the specified async policy.
    /// </summary>
    /// <param name="action">Action to execute</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
    /// <returns>Result instance</returns>
    public async Task ExecuteWithRetryAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);

        await asyncRetryPolicy.ExecuteAsync(action, cancellationToken);
    }

    /// <summary>
    /// Executes a delegate with the specified async policy. 
    /// </summary>
    /// <param name="action">Action to execute</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
    /// <returns>Result HttpResponse</returns>
    public virtual async Task<HttpResponseMessage> ExecuteHttpRequestWithRetryAsync(Func<CancellationToken, Task<HttpResponseMessage>> action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);

        return await asyncHttpRetryPolicy.ExecuteAsync(action, cancellationToken);
    }
}
