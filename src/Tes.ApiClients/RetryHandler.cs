﻿// Copyright (c) Microsoft Corporation.
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
    /// The key in <see cref="Context"/> where <see cref="OnRetry(Exception, TimeSpan, int, Context)"/> or <see cref="OnRetry{T}(DelegateResult{T}, TimeSpan, int, Context)"/> is stored.
    /// </summary>
    public const string OnRetryHandlerKey = "OnRetryHandler";

    /// <summary>
    /// The action to call on each retry.
    /// </summary>
    /// <param name="outcome">The handled exception.</param>
    /// <param name="timespan">The current sleep duration.</param>
    /// <param name="retryCount">The current retry count. It starts at 1 between the first handled condition and the first wait, then 2, etc.</param>
    /// <param name="correlationId">A Guid guaranteed to be unique to each execution. Acts as a correlation id so that events specific to a single execution can be identified in logging and telemetry.</param>
    /// <remarks>This is called right before the wait.</remarks>
    public delegate void OnRetryHandler(Exception outcome, TimeSpan timespan, int retryCount, Guid correlationId);

    /// <summary>
    /// The action to call on each retry.
    /// </summary>
    /// <typeparam name="TResult">See <see cref="PolicyBuilder{TResult}"/>.</typeparam>
    /// <param name="result">The handled exception or result.</param>
    /// <param name="timespan">The current sleep duration.</param>
    /// <param name="retryCount">The current retry count. It starts at 1 between the first handled condition and the first wait, then 2, etc.</param>
    /// <param name="correlationId">A Guid guaranteed to be unique to each execution. Acts as a correlation id so that events specific to a single execution can be identified in logging and telemetry.</param>
    /// <remarks>This is called right before the wait.</remarks>
    public delegate void OnRetryHandler<TResult>(DelegateResult<TResult> result, TimeSpan timespan, int retryCount, Guid correlationId);

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
                    attempt)), OnRetry);
        this.asyncRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(retryPolicyOptions.Value.MaxRetryCount,
                (attempt) => TimeSpan.FromSeconds(Math.Pow(retryPolicyOptions.Value.ExponentialBackOffExponent,
                    attempt)), OnRetry);
        this.asyncHttpRetryPolicy = HttpPolicyExtensions.HandleTransientHttpError()
            .OrResult(r => r.StatusCode == HttpStatusCode.TooManyRequests)
            .WaitAndRetryAsync(retryPolicyOptions.Value.MaxRetryCount,
                (attempt) => TimeSpan.FromSeconds(Math.Pow(retryPolicyOptions.Value.ExponentialBackOffExponent,
                    attempt)), OnRetry<HttpResponseMessage>);
    }

    public static void OnRetry<T>(DelegateResult<T> result, TimeSpan span, int retryCount, Context ctx)
    {
        ctx.GetOnRetryHandler<T>()?.Invoke(result, span, retryCount, ctx.CorrelationId);
    }

    public static void OnRetry(Exception outcome, TimeSpan timespan, int retryCount, Context ctx)
    {
        ctx.GetOnRetryHandler()?.Invoke(outcome, timespan, retryCount, ctx.CorrelationId);
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
    /// <param name="context"></param>
    /// <typeparam name="TResult">Result type</typeparam>
    /// <returns>Result instance</returns>
    public TResult ExecuteWithRetry<TResult>(Func<TResult> action, Context? context = default)
    {
        ArgumentNullException.ThrowIfNull(action);

        return retryPolicy.Execute(_ => action(), context ?? new());
    }

    /// <summary>
    /// Executes a delegate with the specified async policy.
    /// </summary>
    /// <param name="action">Action to execute</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
    /// <param name="context"></param>
    /// <typeparam name="TResult">Result type</typeparam>
    /// <returns>Result instance</returns>
    public virtual Task<TResult> ExecuteWithRetryAsync<TResult>(Func<CancellationToken, Task<TResult>> action, CancellationToken cancellationToken, Context? context = default)
    {
        ArgumentNullException.ThrowIfNull(action);

        return asyncRetryPolicy.ExecuteAsync((_, ct) => action(ct), context ?? new(), cancellationToken);
    }

    /// <summary>
    /// Executes a delegate with the specified async policy.
    /// </summary>
    /// <param name="action">Action to execute</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
    /// <param name="context"></param>
    /// <returns>Result instance</returns>
    public async Task ExecuteWithRetryAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken, Context? context = default)
    {
        ArgumentNullException.ThrowIfNull(action);

        await asyncRetryPolicy.ExecuteAsync((_, ct) => action(ct), context ?? new(), cancellationToken);
    }

    /// <summary>
    /// Executes a delegate with the specified async policy. 
    /// </summary>
    /// <param name="action">Action to execute</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
    /// <param name="context"></param>
    /// <returns>Result HttpResponse</returns>
    public virtual async Task<HttpResponseMessage> ExecuteWithRetryAsync(Func<CancellationToken, Task<HttpResponseMessage>> action, CancellationToken cancellationToken, Context? context = default)
    {
        ArgumentNullException.ThrowIfNull(action);

        return await asyncHttpRetryPolicy.ExecuteAsync((_, ct) => action(ct), context ?? new(), cancellationToken);
    }
}

public static class RetryHandlerExtensions
{
    public static void SetOnRetryHandler<T>(this Context context, RetryHandler.OnRetryHandler<T> onRetry)
    {
        context[RetryHandler.OnRetryHandlerKey] = onRetry;
    }

    public static RetryHandler.OnRetryHandler<T>? GetOnRetryHandler<T>(this Context context)
    {
        return context.TryGetValue(RetryHandler.OnRetryHandlerKey, out var handler) ? (RetryHandler.OnRetryHandler<T>)handler : default;
    }

    public static void SetOnRetryHandler(this Context context, RetryHandler.OnRetryHandler onRetry)
    {
        context[RetryHandler.OnRetryHandlerKey] = onRetry;
    }

    public static RetryHandler.OnRetryHandler? GetOnRetryHandler(this Context context)
    {
        return context.TryGetValue(RetryHandler.OnRetryHandlerKey, out var handler) ? (RetryHandler.OnRetryHandler)handler : default;
    }
}
