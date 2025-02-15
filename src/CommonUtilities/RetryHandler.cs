// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Polly;

namespace CommonUtilities;

/// <summary>
/// Utility class that facilitates the retry policy implementations for HTTP clients.
/// </summary>
public static class RetryHandler
{
    /// <summary>
    /// Polly Context key for caller method name
    /// </summary>
    public const string CallerMemberNameKey = $"Tes.ApiClients.{nameof(RetryHandler)}.CallerMemberName";

    /// <summary>
    /// Polly Context key for backup skip increment setting
    /// </summary>
    public const string BackupSkipProvidedIncrementKey = $"Tes.ApiClients.{nameof(RetryHandler)}.BackupSkipProvidedIncrementCount";

    /// Polly Context key combined sleep method and enumerable duration policies
    /// </summary>
    public const string CombineSleepDurationsKey = $"Tes.ApiClients.{nameof(RetryHandler)}.CombineSleepDurations";

    #region RetryHandlerPolicies
    /// <summary>
    /// Non-generic synchronous retry policy
    /// </summary>
    public class RetryHandlerPolicy
    {
        private readonly ISyncPolicy retryPolicy = null!;

        /// <remarks>For extensions</remarks>
        public ISyncPolicy RetryPolicy => retryPolicy;

        /// <summary>
        /// Public constructor
        /// </summary>
        /// <param name="retryPolicy">Synchronous retry policy.</param>
        public RetryHandlerPolicy(ISyncPolicy retryPolicy)
        {
            ArgumentNullException.ThrowIfNull(retryPolicy);
            this.retryPolicy = retryPolicy;
        }

        /// <remarks>For mocking</remarks>
        public RetryHandlerPolicy() { }


        /// <summary>
        /// Executes a delegate with the configured policy.
        /// </summary>
        /// <param name="action">Action to execute.</param>
        /// <param name="caller">Name of method originating the retriable operation.</param>
        public virtual void ExecuteWithRetry(Action action, [System.Runtime.CompilerServices.CallerMemberName] string? caller = default)
        {
            ArgumentNullException.ThrowIfNull(action);

            retryPolicy.Execute(_ => action(), PrepareContext(caller));
        }

        /// <summary>
        /// Executes a delegate with the configured policy.
        /// </summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="action">Action to execute.</param>
        /// <param name="caller">Name of method originating the retriable operation.</param>
        /// <returns><typeparamref name="TResult"/> instance.</returns>
        public virtual TResult ExecuteWithRetry<TResult>(Func<TResult> action, [System.Runtime.CompilerServices.CallerMemberName] string? caller = default)
        {
            ArgumentNullException.ThrowIfNull(action);

            return retryPolicy.Execute(_ => action(), PrepareContext(caller));
        }
    }

    // TODO: if/when needed
    ///// <summary>
    ///// Generic synchronous retry policy
    ///// </summary>
    //public class RetryHandlerPolicy<TResult>

    /// <summary>
    /// Non-generic asynchronous retry policy
    /// </summary>
    public class AsyncRetryHandlerPolicy
    {
        private readonly IAsyncPolicy retryPolicy = null!;

        /// <remarks>For extensions</remarks>
        public IAsyncPolicy RetryPolicy => retryPolicy;

        /// <summary>
        /// Public constructor
        /// </summary>
        /// <param name="retryPolicy">Asynchronous retry policy.</param>
        public AsyncRetryHandlerPolicy(IAsyncPolicy retryPolicy)
        {
            ArgumentNullException.ThrowIfNull(retryPolicy);
            this.retryPolicy = retryPolicy;
        }

        /// <remarks>For mocking</remarks>
        public AsyncRetryHandlerPolicy() { }

        /// <summary>
        /// Executes a delegate with the configured async policy.
        /// </summary>
        /// <param name="action">Action to execute.</param>
        /// <param name="caller">Name of method originating the retriable operation.</param>
        /// <returns></returns>
        public virtual Task ExecuteWithRetryAsync(Func<Task> action, [System.Runtime.CompilerServices.CallerMemberName] string? caller = default)
        {
            ArgumentNullException.ThrowIfNull(action);

            return retryPolicy.ExecuteAsync((_, _) => action(), PrepareContext(caller), CancellationToken.None);
        }

        /// <summary>
        /// Executes a delegate with the configured async policy.
        /// </summary>
        /// <param name="action">Action to execute.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="caller">Name of method originating the retriable operation.</param>
        /// <returns></returns>
        public virtual Task ExecuteWithRetryAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string? caller = default)
        {
            ArgumentNullException.ThrowIfNull(action);

            return retryPolicy.ExecuteAsync((_, ct) => action(ct), PrepareContext(caller), cancellationToken);
        }

        /// <summary>
        /// Executes a delegate with the configured async policy.
        /// </summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="action">Action to execute.</param>
        /// <param name="caller">Name of method originating the retriable operation.</param>
        /// <returns><typeparamref name="TResult"/> instance.</returns>
        public virtual Task<TResult> ExecuteWithRetryAsync<TResult>(Func<Task<TResult>> action, [System.Runtime.CompilerServices.CallerMemberName] string? caller = default)
        {
            ArgumentNullException.ThrowIfNull(action);

            return retryPolicy.ExecuteAsync((_, _) => action(), PrepareContext(caller), CancellationToken.None);
        }

        /// <summary>
        /// Executes a delegate with the configured async policy.
        /// </summary>
        /// <typeparam name="TResult">Result type.</typeparam>
        /// <param name="action">Action to execute.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="caller">Name of method originating the retriable operation.</param>
        /// <returns><typeparamref name="TResult"/> instance.</returns>
        public virtual Task<TResult> ExecuteWithRetryAsync<TResult>(Func<CancellationToken, Task<TResult>> action, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string? caller = default)
        {
            ArgumentNullException.ThrowIfNull(action);

            return retryPolicy.ExecuteAsync((_, ct) => action(ct), PrepareContext(caller), cancellationToken);
        }

        /// <summary>
        /// Executes the specified asynchronous action within the policy and returns the captured result.
        /// </summary>
        /// <param name="action">The action to perform.</param>
        /// <param name="cancellationToken">A cancellation token which can be used to cancel the action.  When a retry policy in use, also cancels any further retries.</param>
        /// <param name="caller">Name of method originating the retriable operation.</param>
        /// <returns>The captured result.</returns>
        public virtual Task<PolicyResult> ExecuteAndCaptureAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string? caller = default)
        {
            ArgumentNullException.ThrowIfNull(action);

            return retryPolicy.ExecuteAndCaptureAsync((_, token) => action(token), PrepareContext(caller), cancellationToken);
        }
    }

    /// <summary>
    /// Generic asynchronous retry policy
    /// </summary>
    public class AsyncRetryHandlerPolicy<TResult>
    {
        private readonly IAsyncPolicy<TResult> retryPolicy = null!;

        /// <remarks>For extensions</remarks>
        public IAsyncPolicy<TResult> RetryPolicy => retryPolicy;

        /// <summary>
        /// Public constructor
        /// </summary>
        /// <param name="retryPolicy">Asynchronous retry policy.</param>
        public AsyncRetryHandlerPolicy(IAsyncPolicy<TResult> retryPolicy)
        {
            ArgumentNullException.ThrowIfNull(retryPolicy);
            this.retryPolicy = retryPolicy;
        }

        /// <remarks>For mocking</remarks>
        public AsyncRetryHandlerPolicy() { }


        /// <summary>
        /// Executes a delegate with the configured async policy.
        /// </summary>
        /// <param name="action">Action to execute</param>
        /// <param name="caller">Name of method originating the retriable operation.</param>
        /// <returns></returns>
        public virtual Task<TResult> ExecuteWithRetryAsync(Func<Task<TResult>> action, [System.Runtime.CompilerServices.CallerMemberName] string? caller = default)
        {
            ArgumentNullException.ThrowIfNull(action);

            return retryPolicy.ExecuteAsync((_, _) => action(), PrepareContext(caller), CancellationToken.None);
        }

        /// <summary>
        /// Executes a delegate with the configured async policy.
        /// </summary>
        /// <param name="action">Action to execute</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="caller">Name of method originating the retriable operation.</param>
        /// <returns></returns>
        public virtual Task<TResult> ExecuteWithRetryAsync(Func<CancellationToken, Task<TResult>> action, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string? caller = default)
        {
            ArgumentNullException.ThrowIfNull(action);

            return retryPolicy.ExecuteAsync((_, ct) => action(ct), PrepareContext(caller), cancellationToken);
        }

        /// <summary>
        /// Executes a delegate with the configured async policy.
        /// </summary>
        /// <typeparam name="T">Return type</typeparam>
        /// <param name="action">Action to execute</param>
        /// <param name="convert">Method to convert</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="caller">Name of method originating the retriable operation.</param>
        /// <returns></returns>
        public virtual async Task<T> ExecuteWithRetryAndConversionAsync<T>(Func<CancellationToken, Task<TResult>> action, Func<TResult, CancellationToken, Task<T>> convert, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string? caller = default)
        {
            ArgumentNullException.ThrowIfNull(action);

            return await convert(await retryPolicy.ExecuteAsync((_, ct) => action(ct), PrepareContext(caller), cancellationToken), cancellationToken);
        }
    }
    #endregion

    #region Delegates for custom on-retry handlers
    /// <summary>
    /// The action to call on each retry.
    /// </summary>
    /// <param name="outcome">The handled exception.</param>
    /// <param name="timespan">The current sleep duration.</param>
    /// <param name="retryCount">The current retry count. It starts at 1 between the first handled condition and the first wait, then 2, etc.</param>
    /// <param name="correlationId">A Guid guaranteed to be unique to each execution. Acts as a correlation id so that events specific to a single execution can be identified in logging and telemetry.</param>
    /// <param name="caller">Name of method originating the retriable operation.</param>
    /// <remarks>This is called right before the wait.</remarks>
    public delegate void OnRetryHandler(Exception outcome, TimeSpan timespan, int retryCount, Guid correlationId, string? caller);

    /// <summary>
    /// The action to call on each retry.
    /// </summary>
    /// <param name="outcome">The handled exception.</param>
    /// <param name="timespan">The current sleep duration.</param>
    /// <param name="retryCount">The current retry count. It starts at 1 between the first handled condition and the first wait, then 2, etc.</param>
    /// <param name="correlationId">A Guid guaranteed to be unique to each execution. Acts as a correlation id so that events specific to a single execution can be identified in logging and telemetry.</param>
    /// <param name="caller">Name of method originating the retriable operation.</param>
    /// <remarks>This is called right before the wait.</remarks>
    public delegate Task OnRetryHandlerAsync(Exception outcome, TimeSpan timespan, int retryCount, Guid correlationId, string? caller);

    /// <summary>
    /// The action to call on each retry.
    /// </summary>
    /// <typeparam name="TResult">See <see cref="PolicyBuilder{TResult}"/>.</typeparam>
    /// <param name="result">The handled exception or result.</param>
    /// <param name="timespan">The current sleep duration.</param>
    /// <param name="retryCount">The current retry count. It starts at 1 between the first handled condition and the first wait, then 2, etc.</param>
    /// <param name="correlationId">A Guid guaranteed to be unique to each execution. Acts as a correlation id so that events specific to a single execution can be identified in logging and telemetry.</param>
    /// <param name="caller">Name of method originating the retriable operation.</param>
    /// <remarks>This is called right before the wait.</remarks>
    public delegate void OnRetryHandler<TResult>(DelegateResult<TResult> result, TimeSpan timespan, int retryCount, Guid correlationId, string? caller);

    /// <summary>
    /// The action to call on each retry.
    /// </summary>
    /// <typeparam name="TResult">See <see cref="PolicyBuilder{TResult}"/>.</typeparam>
    /// <param name="result">The handled exception or result.</param>
    /// <param name="timespan">The current sleep duration.</param>
    /// <param name="retryCount">The current retry count. It starts at 1 between the first handled condition and the first wait, then 2, etc.</param>
    /// <param name="correlationId">A Guid guaranteed to be unique to each execution. Acts as a correlation id so that events specific to a single execution can be identified in logging and telemetry.</param>
    /// <param name="caller">Name of method originating the retriable operation.</param>
    /// <remarks>This is called right before the wait.</remarks>
    public delegate Task OnRetryHandlerAsync<TResult>(DelegateResult<TResult> result, TimeSpan timespan, int retryCount, Guid correlationId, string? caller);
    #endregion

    public static Context PrepareContext(string? caller) => new()
    {
        [CallerMemberNameKey] = caller ?? throw new ArgumentNullException(nameof(caller))
    };
}
