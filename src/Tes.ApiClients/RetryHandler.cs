// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using Tes.ApiClients.Options;

namespace Tes.ApiClients;

/// <summary>
/// Extension methods for <see cref="RetryHandler"/>
/// </summary>
public static class RetryHandlerExtensions
{
    /// <summary>
    /// Executes a delegate with the specified policy.
    /// </summary>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <param name="retryPolicy">Synchronous retry policy.</param>
    /// <param name="action">Action to execute.</param>
    /// <param name="caller">Name of method originating the retriable operation.</param>
    /// <returns><typeparamref name="TResult"/> instance.</returns>
    public static TResult ExecuteWithRetry<TResult>(this ISyncPolicy retryPolicy, Func<TResult> action, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
    {
        ArgumentNullException.ThrowIfNull(retryPolicy);
        ArgumentNullException.ThrowIfNull(action);

        return retryPolicy.Execute(_ => action(), PrepareContext(caller));
    }

    /// <summary>
    /// Executes a delegate with the specified async policy.
    /// </summary>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <param name="retryPolicy">Asynchronous retry policy.</param>
    /// <param name="action">Action to execute.</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
    /// <param name="caller">Name of method originating the retriable operation.</param>
    /// <returns><typeparamref name="TResult"/> instance.</returns>
    public static Task<TResult> ExecuteWithRetryAsync<TResult>(this IAsyncPolicy retryPolicy, Func<CancellationToken, Task<TResult>> action, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
    {
        ArgumentNullException.ThrowIfNull(retryPolicy);
        ArgumentNullException.ThrowIfNull(action);

        return retryPolicy.ExecuteAsync((_, ct) => action(ct), PrepareContext(caller), cancellationToken);
    }


    /// <summary>
    /// Executes a delegate with the specified async policy.
    /// </summary>
    /// <param name="retryPolicy">Asynchronous retry policy.</param>
    /// <param name="action">Action to execute.</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
    /// <param name="caller">Name of method originating the retriable operation.</param>
    /// <returns></returns>
    public static Task ExecuteWithRetryAsync(this IAsyncPolicy retryPolicy, Func<CancellationToken, Task> action, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
    {
        ArgumentNullException.ThrowIfNull(retryPolicy);
        ArgumentNullException.ThrowIfNull(action);

        return retryPolicy.ExecuteAsync((_, ct) => action(ct), PrepareContext(caller), cancellationToken);
    }

    /// <summary>
    /// Executes a delegate with the specified async policy.
    /// </summary>
    /// <param name="retryPolicy">Asynchronous retry policy</param>
    /// <param name="action">Action to execute</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
    /// <param name="caller">Name of method originating the retriable operation.</param>
    /// <returns>Result HttpResponse</returns>
    public static Task<HttpResponseMessage> ExecuteWithRetryAsync(this IAsyncPolicy<HttpResponseMessage> retryPolicy, Func<CancellationToken, Task<HttpResponseMessage>> action, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
    {
        ArgumentNullException.ThrowIfNull(retryPolicy);
        ArgumentNullException.ThrowIfNull(action);

        return retryPolicy.ExecuteAsync((_, ct) => action(ct), PrepareContext(caller), cancellationToken);
    }

    private static Context PrepareContext(string caller) => new()
    {
        [RetryHandler.CallerMemberNameKey] = caller
    };
}

/// <summary>
/// Utility class that facilitates the retry policy implementations for HTTP clients. 
/// </summary>
public class RetryHandler
{
    // Keys used by this implementation in Polly Contexts
    public const string CallerMemberNameKey = $"Tes.ApiClients.{nameof(RetryHandler)}.CallerMemberName";
    public const string BackupSkipProvidedIncrementKey = $"Tes.ApiClients.{nameof(RetryHandler)}.BackupSkipProvidedIncrementCount";

    /// <summary>
    /// The main starting point for building retry policies
    /// </summary>
    public IPolicyBuilderPolicy PolicyBuilder => new PolicyBuilderPolicy(defaultOptions);

    /// <summary>
    /// The default HttpResponseMessage retry policy builder.
    /// </summary>
    public static PolicyBuilder<HttpResponseMessage> DefaultHttpResponseMessagePolicyBuilder =>
        Polly.Extensions.Http.HttpPolicyExtensions.HandleTransientHttpError()
                .OrResult(r => r.StatusCode == HttpStatusCode.TooManyRequests);

    /// <remarks>Shortcut starting point for testing.</remarks>
    public virtual IPolicyBuilderWait RetryDefaultPolicyBuilder()
        => PolicyBuilder
            .OpinionatedRetryPolicy()
            .WithRetryPolicyOptionsWait();

    /// <remarks>Shortcut starting point for testing.</remarks>
    public virtual IPolicyBuilderWait<HttpResponseMessage> RetryDefaultHttpResponseMessagePolicyBuilder()
        => PolicyBuilder
            .OpinionatedRetryPolicy(DefaultHttpResponseMessagePolicyBuilder)
            .WithRetryPolicyOptionsWait();

    public RetryHandler(IOptions<RetryPolicyOptions> retryPolicyOptions)
    {
        ArgumentNullException.ThrowIfNull(retryPolicyOptions);
        defaultOptions = new(this, retryPolicyOptions.Value);
    }

    /// <summary>
    /// Parameter-less constructor for mocking
    /// </summary>
    protected RetryHandler() { }

    #region Builder interfaces
    public interface IPolicyBuilderPolicy
    {
        /// <summary>
        /// Default retry policy.
        /// </summary>
        /// <returns><see cref="Exception"/> retry policy builder.</returns>
        IPolicyBuilderBase OpinionatedRetryPolicy();

        /// <summary>
        /// Custom retry policy.
        /// </summary>
        /// <param name="policyBuilder">Builder class that holds the list of current exception predicates.</param>
        /// <returns>Custom retry policy builder.</returns>
        IPolicyBuilderBase OpinionatedRetryPolicy(PolicyBuilder policyBuilder);

        /// <summary>
        /// Generic retry policy.
        /// </summary>
        /// <typeparam name="TResult">Result values.</typeparam>
        /// <param name="policyBuilder">Builder class that holds the list of current execution predicates filtering TResult result values.</param>
        /// <returns>Generic retry policy builder.</returns>
        IPolicyBuilderBase<TResult> OpinionatedRetryPolicy<TResult>(PolicyBuilder<TResult> policyBuilder);
    }

    public interface IPolicyBuilderBase
    {
        /// <summary>
        /// Default wait policy.
        /// </summary>
        /// <returns>Wait policy.</returns>
        IPolicyBuilderWait WithRetryPolicyOptionsWait();

        /// <summary>
        /// Custom exponential wait policy.
        /// </summary>
        /// <param name="maxRetryCount">Maximum number of retries.</param>
        /// <param name="exponentialBackOffExponent">Value in seconds which is raised by the power of the retry attempt.</param>
        /// <returns>Wait policy.</returns>
        IPolicyBuilderWait WithExponentialBackoffWait(int maxRetryCount, double exponentialBackOffExponent);

        /// <summary>
        /// Custom exception-based wait policy.
        /// </summary>
        /// <param name="maxRetryCount">Maximum number of retries.</param>
        /// <param name="waitDurationProvider">Wait policy.</param>
        /// <returns>Wait policy.</returns>
        IPolicyBuilderWait WithCustomizedRetryPolicyOptionsWait(int maxRetryCount, Func<int, Exception, TimeSpan> waitDurationProvider);

        /// <summary>
        /// Custom optional exception-based wait policy backed up by an exponential wait policy.
        /// </summary>
        /// <param name="waitDurationProvider">Wait policy that can return <see cref="Nullable{TimeSpan}"/> to use the backup wait policy.</param>
        /// <param name="maxRetryCount">Maximum number of retries.</param>
        /// <param name="exponentialBackOffExponent">Value in seconds which is raised by the power of the backup retry attempt.</param>
        /// <param name="backupSkipProvidedIncrements">True to pass backup wait provider its own attempt values, False to provide overall attemp values.</param>
        /// <returns>Wait policy.</returns>
        IPolicyBuilderWait WithExceptionBasedWaitWithExponentialBackoffBackup(Func<int, Exception, TimeSpan?> waitDurationProvider, int maxRetryCount, int exponentialBackOffExponent, bool backupSkipProvidedIncrements);

        /// <summary>
        /// Custom optional exception-based wait policy backed up by the default wait policy.
        /// </summary>
        /// <param name="waitDurationProvider">Wait policy that can return <see cref="Nullable{TimeSpan}"/> to use the backup wait policy.</param>
        /// <param name="backupSkipProvidedIncrements">True to pass backup wait provider its own attempt values, False to provide overall attemp values.</param>
        /// <returns>Wait policy.</returns>
        IPolicyBuilderWait WithExceptionBasedWaitWithRetryPolicyOptionsBackup(Func<int, Exception, TimeSpan?> waitDurationProvider, bool backupSkipProvidedIncrements);
    }

    public interface IPolicyBuilderBase<TResult>
    {
        /// <summary>
        /// Default wait policy.
        /// </summary>
        /// <returns>Wait policy.</returns>
        IPolicyBuilderWait<TResult> WithRetryPolicyOptionsWait();

        /// <summary>
        /// Custom exponential wait policy.
        /// </summary>
        /// <param name="maxRetryCount">Maximum number of retries.</param>
        /// <param name="exponentialBackOffExponent">Value in seconds which is raised by the power of the retry attempt.</param>
        /// <returns>Wait policy.</returns>
        IPolicyBuilderWait<TResult> WithExponentialBackoffWait(int retryCount, double exponentialBackOffExponent);

        /// <summary>
        /// Custom result-based policy.
        /// </summary>
        /// <param name="maxRetryCount">Maximum number of retries.</param>
        /// <param name="waitDurationProvider">Wait policy.</param>
        /// <returns>Wait policy.</returns>
        /// <returns></returns>
        IPolicyBuilderWait<TResult> WithCustomizedRetryPolicyOptionsWait(int maxRetryCount, Func<int, DelegateResult<TResult>, TimeSpan> waitDurationProvider);

        /// <summary>
        /// Custom exception-based wait policy.
        /// </summary>
        /// <param name="maxRetryCount">Maximum number of retries.</param>
        /// <param name="waitDurationProvider">Wait policy.</param>
        /// <returns>Wait policy.</returns>
        IPolicyBuilderWait<TResult> WithCustomizedRetryPolicyOptionsWait(int maxRetryCount, Func<int, Exception, TimeSpan> waitDurationProvider);

        /// <summary>
        /// Custom optional exception-based wait policy backed up by an exponential wait policy.
        /// </summary>
        /// <param name="waitDurationProvider">Wait policy that can return <see cref="Nullable{TimeSpan}"/> to use the backup wait policy.</param>
        /// <param name="maxRetryCount">Maximum number of retries.</param>
        /// <param name="exponentialBackOffExponent">Value in seconds which is raised by the power of the backup retry attempt.</param>
        /// <param name="backupSkipProvidedIncrements">True to pass backup wait provider its own attempt values, False to provide overall attemp values.</param>
        /// <returns>Wait policy.</returns>
        IPolicyBuilderWait<TResult> WithExceptionBasedWaitWithExponentialBackoffBackup(Func<int, Exception, TimeSpan?> waitDurationProvider, int retryCount, int exponentialBackOffExponent, bool backupSkipProvidedIncrements);

        /// <summary>
        /// Custom optional exception-based wait policy backed up by the default wait policy.
        /// </summary>
        /// <param name="waitDurationProvider">Wait policy that can return <see cref="Nullable{TimeSpan}"/> to use the backup wait policy.</param>
        /// <param name="backupSkipProvidedIncrements">True to pass backup wait provider its own attempt values, False to provide overall attemp values.</param>
        /// <returns>Wait policy.</returns>
        IPolicyBuilderWait<TResult> WithExceptionBasedWaitWithRetryPolicyOptionsBackup(Func<int, Exception, TimeSpan?> waitDurationProvider, bool backupSkipProvidedIncrements);
    }

    public interface IPolicyBuilderWait
    {
        /// <summary>
        /// OnRetry behaviors
        /// </summary>
        /// <param name="logger">Logger to enable retry logging.</param>
        /// <param name="onRetry">Custom onretry handler.</param>
        /// <param name="onRetryAsync">Custom async onretry handler. Only applies to <see cref="AsyncRetryPolicy"/>.</param>
        /// <returns>OnRetry hander</returns>
        IPolicyBuilderBuild SetOnRetryBehavior(ILogger logger = default, OnRetryHandler onRetry = default, OnRetryHandlerAsync onRetryAsync = default);
    }

    public interface IPolicyBuilderWait<TResult>
    {
        /// <summary>
        /// OnRetry behaviors
        /// </summary>
        /// <param name="logger">Logger to enable retry logging.</param>
        /// <param name="onRetry">Custom onretry handler.</param>
        /// <param name="onRetryAsync">Custom async onretry handler. Only applies to <see cref="AsyncRetryPolicy{TResult}"/>.</param>
        /// <returns>OnRetry hander</returns>
        IPolicyBuilderBuild<TResult> SetOnRetryBehavior(ILogger logger = default, OnRetryHandler<TResult> onRetry = default, OnRetryHandlerAsync<TResult> onRetryAsync = default);
    }

    public interface IPolicyBuilderBuild
    {
        /// <summary>
        /// Builds <see cref="RetryPolicy"/>.
        /// </summary>
        /// <returns>Retry policy.</returns>
        ISyncPolicy Build();

        /// <summary>
        /// Builds <see cref="AsyncRetryPolicy"/>.
        /// </summary>
        /// <returns>Async retry policy.</returns>
        IAsyncPolicy BuildAsync();

        /// <summary>
        /// Retrives the instance of the retryhandler to accomodate extensions to the builder
        /// </summary>
        RetryHandler PolicyBuilderBase { get; }
    }

    public interface IPolicyBuilderBuild<TResult>
    {
        /// <summary>
        /// Builds <see cref="RetryPolicy"/>.
        /// </summary>
        /// <returns>Retry policy.</returns>
        ISyncPolicy<TResult> Build();

        /// <summary>
        /// Builds <see cref="AsyncRetryPolicy"/>.
        /// </summary>
        /// <returns>Async retry policy.</returns>
        IAsyncPolicy<TResult> BuildAsync();

        /// <summary>
        /// Retrives the instance of the retryhandler to accomodate extensions to the builder
        /// </summary>
        RetryHandler PolicyBuilderBase { get; }
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
    public delegate void OnRetryHandler(Exception outcome, TimeSpan timespan, int retryCount, Guid correlationId, string caller);

    /// <summary>
    /// The action to call on each retry.
    /// </summary>
    /// <param name="outcome">The handled exception.</param>
    /// <param name="timespan">The current sleep duration.</param>
    /// <param name="retryCount">The current retry count. It starts at 1 between the first handled condition and the first wait, then 2, etc.</param>
    /// <param name="correlationId">A Guid guaranteed to be unique to each execution. Acts as a correlation id so that events specific to a single execution can be identified in logging and telemetry.</param>
    /// <param name="caller">Name of method originating the retriable operation.</param>
    /// <remarks>This is called right before the wait.</remarks>
    public delegate Task OnRetryHandlerAsync(Exception outcome, TimeSpan timespan, int retryCount, Guid correlationId, string caller);

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
    public delegate void OnRetryHandler<TResult>(DelegateResult<TResult> result, TimeSpan timespan, int retryCount, Guid correlationId, string caller);

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
    public delegate Task OnRetryHandlerAsync<TResult>(DelegateResult<TResult> result, TimeSpan timespan, int retryCount, Guid correlationId, string caller);
    #endregion

    #region Builder interface implementations
    private readonly Defaults defaultOptions;

    private readonly struct Defaults
    {
        public readonly RetryPolicyOptions PolicyOptions;
        public readonly RetryHandler PolicyBuilderBase;

        internal Defaults(RetryHandler retryHandler, RetryPolicyOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);
            PolicyOptions = options;
            PolicyBuilderBase = retryHandler;
        }
    }

    private readonly struct PolicyBuilderPolicy : IPolicyBuilderPolicy
    {
        private readonly Defaults Defaults;

        internal PolicyBuilderPolicy(Defaults options)
        {
            ArgumentNullException.ThrowIfNull(options);
            Defaults = options;
        }

        /// <inheritdoc/>
        IPolicyBuilderBase IPolicyBuilderPolicy.OpinionatedRetryPolicy()
            => new PolicyBuilderBase<IPolicyBuilderPolicy>(Policy.Handle<Exception>(), Defaults);

        /// <inheritdoc/>
        IPolicyBuilderBase IPolicyBuilderPolicy.OpinionatedRetryPolicy(PolicyBuilder policy)
            => new PolicyBuilderBase<IPolicyBuilderPolicy>(policy, Defaults);

        /// <inheritdoc/>
        IPolicyBuilderBase<TResult> IPolicyBuilderPolicy.OpinionatedRetryPolicy<TResult>(PolicyBuilder<TResult> policy)
            => new PolicyBuilderBase<TResult>(policy, Defaults);

        private readonly struct PolicyBuilderBase<TResult> : IPolicyBuilderBase, IPolicyBuilderBase<TResult>
        {
            private readonly PolicyBuilder policyBuilder;
            private readonly PolicyBuilder<TResult> genericPolicyBuilder;
            private readonly Defaults Defaults;

            private Func<int, Exception, Context, TimeSpan> DefaultSleepDurationProvider()
                => ExponentialSleepDurationProvider(Defaults.PolicyOptions.ExponentialBackOffExponent);

            private static Func<int, Exception, Context, TimeSpan> ExponentialSleepDurationProvider(double exponentialBackOffExponent)
                => (attempt, _1, _2) => TimeSpan.FromSeconds(Math.Pow(exponentialBackOffExponent, attempt));

            private static Func<int, Exception, Context, TimeSpan> ExceptionBasedSleepDurationProviderWithExponentialBackoffBackup(Func<int, Exception, TimeSpan?> sleepDurationProvider, int exponentialBackOffExponent, bool backupSkipProvidedIncrements)
                => (attempt, exception, ctx) =>
                {
                    return backupSkipProvidedIncrements
                        ? AdjustAttemptIfNeeded()
                        : sleepDurationProvider(attempt, exception) ?? ExponentialSleepDurationProvider(exponentialBackOffExponent)(attempt, exception, ctx);

                    TimeSpan AdjustAttemptIfNeeded()
                    {
                        if (!ctx.TryGetValue(BackupSkipProvidedIncrementKey, out var value) || value is not int)
                        {
                            ctx[BackupSkipProvidedIncrementKey] = value = 0;
                        }

                        var result = sleepDurationProvider(attempt, exception);

                        if (result is null)
                        {
                            var skipIncrement = (int)value;
                            attempt -= skipIncrement;
                            ctx[BackupSkipProvidedIncrementKey] = ++skipIncrement;
                            result = ExponentialSleepDurationProvider(exponentialBackOffExponent)(attempt, exception, ctx);
                        }

                        return result.Value;
                    }
                };

            internal PolicyBuilderBase(PolicyBuilder policyBuilder, Defaults defaults)
            {
                ArgumentNullException.ThrowIfNull(policyBuilder);
                ArgumentNullException.ThrowIfNull(defaults);
                this.policyBuilder = policyBuilder;
                Defaults = defaults;
            }

            internal PolicyBuilderBase(PolicyBuilder<TResult> policyBuilder, Defaults defaults)
            {
                ArgumentNullException.ThrowIfNull(policyBuilder);
                ArgumentNullException.ThrowIfNull(defaults);
                this.genericPolicyBuilder = policyBuilder;
                Defaults = defaults;
            }

            /// <inheritdoc/>
            IPolicyBuilderWait IPolicyBuilderBase.WithRetryPolicyOptionsWait()
                => new PolicyBuilderWait<IPolicyBuilderPolicy>(policyBuilder, Defaults.PolicyBuilderBase, Defaults.PolicyOptions.MaxRetryCount, DefaultSleepDurationProvider());

            /// <inheritdoc/>
            IPolicyBuilderWait IPolicyBuilderBase.WithCustomizedRetryPolicyOptionsWait(int maxRetryCount, Func<int, Exception, TimeSpan> sleepDurationProvider)
                => new PolicyBuilderWait<IPolicyBuilderPolicy>(policyBuilder, Defaults.PolicyBuilderBase, maxRetryCount, (attempt, outcome, _1) => sleepDurationProvider(attempt, outcome));

            /// <inheritdoc/>
            IPolicyBuilderWait IPolicyBuilderBase.WithExponentialBackoffWait(int retryCount, double exponentialBackOffExponent)
                => new PolicyBuilderWait<IPolicyBuilderPolicy>(policyBuilder, Defaults.PolicyBuilderBase, retryCount, ExponentialSleepDurationProvider(exponentialBackOffExponent));

            /// <inheritdoc/>
            IPolicyBuilderWait IPolicyBuilderBase.WithExceptionBasedWaitWithRetryPolicyOptionsBackup(Func<int, Exception, TimeSpan?> sleepDurationProvider, bool backupSkipProvidedIncrements)
                => new PolicyBuilderWait<IPolicyBuilderPolicy>(policyBuilder, Defaults.PolicyBuilderBase, Defaults.PolicyOptions.MaxRetryCount, ExceptionBasedSleepDurationProviderWithExponentialBackoffBackup(sleepDurationProvider, Defaults.PolicyOptions.ExponentialBackOffExponent, backupSkipProvidedIncrements));

            /// <inheritdoc/>
            IPolicyBuilderWait IPolicyBuilderBase.WithExceptionBasedWaitWithExponentialBackoffBackup(Func<int, Exception, TimeSpan?> sleepDurationProvider, int retryCount, int exponentialBackOffExponent, bool backupSkipProvidedIncrements)
                => new PolicyBuilderWait<IPolicyBuilderPolicy>(policyBuilder, Defaults.PolicyBuilderBase, retryCount, ExceptionBasedSleepDurationProviderWithExponentialBackoffBackup(sleepDurationProvider, exponentialBackOffExponent, backupSkipProvidedIncrements));


            /// <inheritdoc/>
            IPolicyBuilderWait<TResult> IPolicyBuilderBase<TResult>.WithRetryPolicyOptionsWait()
                => new PolicyBuilderWait<TResult>(genericPolicyBuilder, Defaults.PolicyBuilderBase, Defaults.PolicyOptions.MaxRetryCount, default, DefaultSleepDurationProvider());

            /// <inheritdoc/>
            IPolicyBuilderWait<TResult> IPolicyBuilderBase<TResult>.WithCustomizedRetryPolicyOptionsWait(int maxRetryCount, Func<int, Exception, TimeSpan> waitDurationProvider)
                => new PolicyBuilderWait<TResult>(genericPolicyBuilder, Defaults.PolicyBuilderBase, maxRetryCount, default, (attempt, outcome, _1) => waitDurationProvider(attempt, outcome));

            /// <inheritdoc/>
            IPolicyBuilderWait<TResult> IPolicyBuilderBase<TResult>.WithCustomizedRetryPolicyOptionsWait(int maxRetryCount, Func<int, DelegateResult<TResult>, TimeSpan> sleepDurationProvider)
                => new PolicyBuilderWait<TResult>(genericPolicyBuilder, Defaults.PolicyBuilderBase, maxRetryCount, (attempt, outcome, _1) => sleepDurationProvider(attempt, outcome), default);

            /// <inheritdoc/>
            IPolicyBuilderWait<TResult> IPolicyBuilderBase<TResult>.WithExponentialBackoffWait(int retryCount, double exponentialBackOffExponent)
                => new PolicyBuilderWait<TResult>(genericPolicyBuilder, Defaults.PolicyBuilderBase, retryCount, default, ExponentialSleepDurationProvider(exponentialBackOffExponent));

            /// <inheritdoc/>
            IPolicyBuilderWait<TResult> IPolicyBuilderBase<TResult>.WithExceptionBasedWaitWithRetryPolicyOptionsBackup(Func<int, Exception, TimeSpan?> sleepDurationProvider, bool backupSkipProvidedIncrements)
                => new PolicyBuilderWait<TResult>(genericPolicyBuilder, Defaults.PolicyBuilderBase, Defaults.PolicyOptions.MaxRetryCount, default, ExceptionBasedSleepDurationProviderWithExponentialBackoffBackup(sleepDurationProvider, Defaults.PolicyOptions.ExponentialBackOffExponent, backupSkipProvidedIncrements));

            /// <inheritdoc/>
            IPolicyBuilderWait<TResult> IPolicyBuilderBase<TResult>.WithExceptionBasedWaitWithExponentialBackoffBackup(Func<int, Exception, TimeSpan?> sleepDurationProvider, int retryCount, int exponentialBackOffExponent, bool backupSkipProvidedIncrements)
                => new PolicyBuilderWait<TResult>(genericPolicyBuilder, Defaults.PolicyBuilderBase, retryCount, default, ExceptionBasedSleepDurationProviderWithExponentialBackoffBackup(sleepDurationProvider, exponentialBackOffExponent, backupSkipProvidedIncrements));
        }

        private readonly struct PolicyBuilderWait<TResult> : IPolicyBuilderWait, IPolicyBuilderWait<TResult>
        {
            private readonly PolicyBuilder policyBuilder;
            private readonly PolicyBuilder<TResult> genericPolicyBuilder;
            private readonly Func<int, Exception, Context, TimeSpan> sleepDurationProvider;
            private readonly Func<int, DelegateResult<TResult>, Context, TimeSpan> genericSleepDurationProvider;
            private readonly int retryCount;
            private readonly RetryHandler builderBase;

            private static Func<int, DelegateResult<TResult>, Context, TimeSpan> PickSetProvider(Func<int, DelegateResult<TResult>, Context, TimeSpan> tResultProvider, Func<int, Exception, Context, TimeSpan> exceptionProvider)
                => (attempt, outcome, ctx) => tResultProvider is null ? exceptionProvider(attempt, outcome.Exception, ctx) : tResultProvider(attempt, outcome, ctx);

            internal PolicyBuilderWait(PolicyBuilder policyBuilder, RetryHandler builderBase, int retryCount, Func<int, Exception, Context, TimeSpan> sleepDurationProvider)
            {
                ArgumentNullException.ThrowIfNull(sleepDurationProvider);
                this.policyBuilder = policyBuilder;
                this.retryCount = retryCount;
                this.sleepDurationProvider = sleepDurationProvider;
                this.builderBase = builderBase;
            }

            internal PolicyBuilderWait(PolicyBuilder<TResult> policyBuilder, RetryHandler builderBase, int retryCount, Func<int, DelegateResult<TResult>, Context, TimeSpan> sleepDurationProviderResult, Func<int, Exception, Context, TimeSpan> sleepDurationProviderException)
            {
                if (sleepDurationProviderException is null && sleepDurationProviderResult is null)
                {
                    throw new ArgumentNullException();
                }

                this.genericPolicyBuilder = policyBuilder;
                this.retryCount = retryCount;
                this.sleepDurationProvider = sleepDurationProviderException;
                this.genericSleepDurationProvider = sleepDurationProviderResult;
                this.builderBase = builderBase;
            }

            /// <inheritdoc/>
            IPolicyBuilderBuild IPolicyBuilderWait.SetOnRetryBehavior(ILogger logger, OnRetryHandler onRetry, OnRetryHandlerAsync onRetryAsync)
                => new PolicyBuilderBuild<IPolicyBuilderPolicy>(policyBuilder, builderBase, retryCount, sleepDurationProvider, logger, onRetry, onRetryAsync);

            /// <inheritdoc/>
            IPolicyBuilderBuild<TResult> IPolicyBuilderWait<TResult>.SetOnRetryBehavior(ILogger logger, OnRetryHandler<TResult> onRetry, OnRetryHandlerAsync<TResult> onRetryAsync)
                => new PolicyBuilderBuild<TResult>(genericPolicyBuilder, builderBase, retryCount, PickSetProvider(genericSleepDurationProvider, sleepDurationProvider), logger, onRetry, onRetryAsync);
        }

        private readonly struct PolicyBuilderBuild<TResult> : IPolicyBuilderBuild, IPolicyBuilderBuild<TResult>
        {
            private readonly PolicyBuilder policyBuilder;
            private readonly PolicyBuilder<TResult> genericPolicyBuilder;
            private readonly Func<int, Exception, Context, TimeSpan> sleepDurationProvider;
            private readonly Func<int, DelegateResult<TResult>, Context, TimeSpan> genericSleepDurationProvider;
            private readonly int retryCount;
            private readonly ILogger logger;
            private readonly OnRetryHandler onRetryHandler;
            private readonly OnRetryHandlerAsync onRetryHandlerAsync;
            private readonly OnRetryHandler<TResult> genericOnRetryHandler;
            private readonly OnRetryHandlerAsync<TResult> genericOnRetryHandlerAsync;

            /// <inheritdoc/>
            public RetryHandler PolicyBuilderBase { get; }

            internal PolicyBuilderBuild(PolicyBuilder policyBuilder, RetryHandler builderBase, int retryCount, Func<int, Exception, Context, TimeSpan> sleepDurationProvider, ILogger logger, OnRetryHandler onRetry, OnRetryHandlerAsync onRetryAsync)
            {
                ArgumentNullException.ThrowIfNull(sleepDurationProvider);
                this.policyBuilder = policyBuilder;
                this.retryCount = retryCount;
                this.sleepDurationProvider = sleepDurationProvider;
                this.logger = logger;
                this.onRetryHandler = onRetry;
                this.onRetryHandlerAsync = onRetryAsync;
                this.PolicyBuilderBase = builderBase;
            }

            internal PolicyBuilderBuild(PolicyBuilder<TResult> policyBuilder, RetryHandler builderBase, int retryCount, Func<int, DelegateResult<TResult>, Context, TimeSpan> sleepDurationProvider, ILogger logger, OnRetryHandler<TResult> onRetry, OnRetryHandlerAsync<TResult> onRetryAsync)
            {
                ArgumentNullException.ThrowIfNull(sleepDurationProvider);
                this.genericPolicyBuilder = policyBuilder;
                this.retryCount = retryCount;
                this.genericSleepDurationProvider = sleepDurationProvider;
                this.logger = logger;
                this.genericOnRetryHandler = onRetry;
                this.genericOnRetryHandlerAsync = onRetryAsync;
                this.PolicyBuilderBase = builderBase;
            }

            private Action<Exception, TimeSpan, int, Context> Logger()
            {
                var logger = this.logger;

                return (exception, timeSpan, retryCount, ctx) =>
                    logger?.LogError(exception, @"Retrying in {Method}: RetryCount: {RetryCount} TimeSpan: {TimeSpan:c} CorrelationId: {CorrelationId:D} ErrorMessage: {ExceptionMessage}", ctx[CallerMemberNameKey], retryCount, timeSpan, ctx.CorrelationId, exception.Message);
            }

            private Action<DelegateResult<TResult>, TimeSpan, int, Context> GenericLogger()
            {
                var logger = this.logger;

                return (outcome, timeSpan, retryCount, ctx) =>
                {
                    if (outcome.Exception is null)
                    {
                        logger?.LogError(@"Retrying in {Method}: RetryCount: {RetryCount} TimeSpan: {TimeSpan:c} CorrelationId: {CorrelationId:D}", ctx[CallerMemberNameKey], retryCount, timeSpan, ctx.CorrelationId);
                    }
                    else
                    {
                        logger?.LogError(outcome.Exception, @"Retrying in {Method}: RetryCount: {RetryCount} TimeSpan: {TimeSpan:c} CorrelationId: {CorrelationId:D} ErrorMessage: {ExceptionMessage}", ctx[CallerMemberNameKey], retryCount, timeSpan, ctx.CorrelationId, outcome.Exception.Message);
                    }
                };
            }

            private Action<Exception, TimeSpan, int, Context> OnRetryHandler()
            {
                var handler = onRetryHandler ?? new((exception, timeSpan, retryCount, correlationId, caller) => { });
                var logger = Logger();

                return (exception, timeSpan, retryCount, ctx) =>
                {
                    handler(exception, timeSpan, retryCount, ctx.CorrelationId, ctx[CallerMemberNameKey] as string);
                    logger(exception, timeSpan, retryCount, ctx);
                };
            }

            private Action<DelegateResult<TResult>, TimeSpan, int, Context> OnGenericRetryHandler()
            {
                var handler = onRetryHandler ?? new((exception, timeSpan, retryCount, correlationId, caller) => { });
                var genericHandler = genericOnRetryHandler ?? new((outcome, timeSpan, retryCount, correlationId, caller) => handler(outcome.Exception, timeSpan, retryCount, correlationId, caller));
                var logger = GenericLogger();

                return (outcome, timeSpan, retryCount, ctx) =>
                {
                    genericHandler(outcome, timeSpan, retryCount, ctx.CorrelationId, ctx[CallerMemberNameKey] as string);
                    logger(outcome, timeSpan, retryCount, ctx);
                };
            }

            private Func<Exception, TimeSpan, int, Context, Task> OnRetryHandlerAsync()
            {
                var handler = onRetryHandler ?? new((exception, timeSpan, retryCount, correlationId, caller) => { });
                var handlerAsync = onRetryHandlerAsync ?? new((exception, timeSpan, retryCount, correlationId, caller) =>
                {
                    handler(exception, timeSpan, retryCount, correlationId, caller);
                    return Task.CompletedTask;
                });
                var logger = Logger();

                return async (exception, timeSpan, retryCount, ctx) =>
                {
                    await handlerAsync(exception, timeSpan, retryCount, ctx.CorrelationId, ctx[CallerMemberNameKey] as string);
                    logger(exception, timeSpan, retryCount, ctx);
                };
            }

            private Func<DelegateResult<TResult>, TimeSpan, int, Context, Task> OnGenericRetryHandlerAsync()
            {
                var handler = onRetryHandler ?? new((exception, timeSpan, retryCount, correlationId, caller) => { });
                var genericHandler = genericOnRetryHandler ?? new((outcome, timeSpan, retryCount, correlationId, caller) => handler(outcome.Exception, timeSpan, retryCount, correlationId, caller));
                var handlerAsync = onRetryHandlerAsync;
                var genericHandlerAsync = genericOnRetryHandlerAsync ?? new((outcome, timespan, retryCount, correlationId, caller) =>
                {
                    if (handlerAsync is null)
                    {
                        genericHandler(outcome, timespan, retryCount, correlationId, caller);
                        return Task.CompletedTask;
                    }

                    return handlerAsync(outcome.Exception, timespan, retryCount, correlationId, caller);
                });
                var logger = GenericLogger();

                return async (outcome, timespan, retryCount, ctx) =>
                {
                    await genericHandlerAsync(outcome, timespan, retryCount, ctx.CorrelationId, ctx[CallerMemberNameKey] as string);
                    logger(outcome, timespan, retryCount, ctx);
                };
            }

            /// <inheritdoc/>
            ISyncPolicy IPolicyBuilderBuild.Build()
            {
                var waitProvider = sleepDurationProvider;
                var onRetryProvider = OnRetryHandler();

                return policyBuilder.WaitAndRetry(retryCount, (attempt, ctx) => waitProvider(attempt, default, ctx), onRetryProvider);
            }

            /// <inheritdoc/>
            IAsyncPolicy IPolicyBuilderBuild.BuildAsync()
            {
                var waitProvider = sleepDurationProvider;
                var onRetryProvider = OnRetryHandlerAsync();

                return policyBuilder.WaitAndRetryAsync(retryCount, waitProvider, onRetryProvider);
            }

            /// <inheritdoc/>
            ISyncPolicy<TResult> IPolicyBuilderBuild<TResult>.Build()
            {
                var waitProvider = genericSleepDurationProvider;
                var onRetryProvider = OnGenericRetryHandler();

                return genericPolicyBuilder.WaitAndRetry(retryCount, waitProvider, onRetryProvider);
            }

            /// <inheritdoc/>
            IAsyncPolicy<TResult> IPolicyBuilderBuild<TResult>.BuildAsync()
            {
                var waitProvider = genericSleepDurationProvider;
                var onRetryProvider = OnGenericRetryHandlerAsync();

                return genericPolicyBuilder.WaitAndRetryAsync(retryCount, waitProvider, onRetryProvider);
            }
        }
        #endregion
    }
}
