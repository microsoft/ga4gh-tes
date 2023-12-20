// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net;
using CommonUtilities.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

namespace CommonUtilities;

/// <summary>
/// Utility class that facilitates the retry policy implementations for HTTP clients.
/// </summary>
public class RetryPolicyBuilder
{
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

    /// <remarks>Shortcut starting point for testing. Can be used in production as well.</remarks>
    public virtual IPolicyBuilderWait DefaultRetryPolicyBuilder()
        => PolicyBuilder
            .OpinionatedRetryPolicy()
            .WithRetryPolicyOptionsWait();

    /// <remarks>Shortcut starting point for testing. Can be used in production as well.</remarks>
    public virtual IPolicyBuilderWait<HttpResponseMessage> DefaultRetryHttpResponseMessagePolicyBuilder()
        => PolicyBuilder
            .OpinionatedRetryPolicy(DefaultHttpResponseMessagePolicyBuilder)
            .WithRetryPolicyOptionsWait();

    /// <summary>
    /// Public constructor
    /// </summary>
    /// <param name="retryPolicyOptions">Retry policy options</param>
    public RetryPolicyBuilder(IOptions<RetryPolicyOptions> retryPolicyOptions)
    {
        ArgumentNullException.ThrowIfNull(retryPolicyOptions);
        defaultOptions = new(this, retryPolicyOptions.Value);
    }

    /// <summary>
    /// Parameter-less constructor for mocking
    /// </summary>
    protected RetryPolicyBuilder() { }

    #region Builder interfaces
    /// <summary>
    /// Selects retry policy type.
    /// </summary>
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

    /// <summary>
    /// Selects retry policy wait algorithm.
    /// </summary>
    public interface IPolicyBuilderBase
    {
        /// <summary>
        /// Default wait policy.
        /// </summary>
        /// <returns>OnRetry hander</returns>
        IPolicyBuilderWait WithRetryPolicyOptionsWait();

        /// <summary>
        /// Custom exponential wait policy.
        /// </summary>
        /// <param name="maxRetryCount">Maximum number of retries.</param>
        /// <param name="exponentialBackOffExponent">Value in seconds which is raised by the power of the retry attempt.</param>
        /// <returns>OnRetry hander</returns>
        IPolicyBuilderWait WithExponentialBackoffWait(int maxRetryCount, double exponentialBackOffExponent);

        /// <summary>
        /// Custom exception-based wait policy.
        /// </summary>
        /// <param name="maxRetryCount">Maximum number of retries.</param>
        /// <param name="waitDurationProvider">Wait policy.</param>
        /// <returns>OnRetry hander</returns>
        IPolicyBuilderWait WithCustomizedRetryPolicyOptionsWait(int maxRetryCount, Func<int, Exception?, TimeSpan> waitDurationProvider);

        /// <summary>
        /// Custom optional exception-based wait policy backed up by an exponential wait policy.
        /// </summary>
        /// <param name="waitDurationProvider">Wait policy that can return <see cref="Nullable{TimeSpan}"/> to use the backup wait policy.</param>
        /// <param name="maxRetryCount">Maximum number of retries.</param>
        /// <param name="exponentialBackOffExponent">Value in seconds which is raised by the power of the backup retry attempt.</param>
        /// <param name="backupSkipProvidedIncrements">True to pass backup wait provider its own attempt values, False to provide overall attemp values.</param>
        /// <returns>OnRetry hander</returns>
        IPolicyBuilderWait WithExceptionBasedWaitWithExponentialBackoffBackup(Func<int, Exception?, TimeSpan?> waitDurationProvider, int maxRetryCount, double exponentialBackOffExponent, bool backupSkipProvidedIncrements);

        /// <summary>
        /// Custom optional exception-based wait policy backed up by the default wait policy.
        /// </summary>
        /// <param name="waitDurationProvider">Wait policy that can return <see cref="Nullable{TimeSpan}"/> to use the backup wait policy.</param>
        /// <param name="backupSkipProvidedIncrements">True to pass backup wait provider its own attempt values, False to provide overall attemp values.</param>
        /// <returns>OnRetry hander</returns>
        IPolicyBuilderWait WithExceptionBasedWaitWithRetryPolicyOptionsBackup(Func<int, Exception?, TimeSpan?> waitDurationProvider, bool backupSkipProvidedIncrements);
    }

    /// <summary>
    /// Selects retry policy wait algorithm.
    /// </summary>
    public interface IPolicyBuilderBase<TResult>
    {
        /// <summary>
        /// Default wait policy.
        /// </summary>
        /// <returns>OnRetry hander</returns>
        IPolicyBuilderWait<TResult> WithRetryPolicyOptionsWait();

        /// <summary>
        /// Custom exponential wait policy.
        /// </summary>
        /// <param name="maxRetryCount">Maximum number of retries.</param>
        /// <param name="exponentialBackOffExponent">Value in seconds which is raised by the power of the retry attempt.</param>
        /// <returns>OnRetry hander</returns>
        IPolicyBuilderWait<TResult> WithExponentialBackoffWait(int maxRetryCount, double exponentialBackOffExponent);

        /// <summary>
        /// Custom result-based policy.
        /// </summary>
        /// <param name="maxRetryCount">Maximum number of retries.</param>
        /// <param name="waitDurationProvider">Wait policy.</param>
        /// <returns>Wait policy.</returns>
        /// <returns>OnRetry hander</returns>
        IPolicyBuilderWait<TResult> WithCustomizedRetryPolicyOptionsWait(int maxRetryCount, Func<int, DelegateResult<TResult>, TimeSpan> waitDurationProvider);

        /// <summary>
        /// Custom exception-based wait policy.
        /// </summary>
        /// <param name="maxRetryCount">Maximum number of retries.</param>
        /// <param name="waitDurationProvider">Wait policy.</param>
        /// <returns>OnRetry hander</returns>
        IPolicyBuilderWait<TResult> WithCustomizedRetryPolicyOptionsWait(int maxRetryCount, Func<int, Exception?, TimeSpan> waitDurationProvider);

        /// <summary>
        /// Custom optional exception-based wait policy backed up by an exponential wait policy.
        /// </summary>
        /// <param name="waitDurationProvider">Wait policy that can return <see cref="Nullable{TimeSpan}"/> to use the backup wait policy.</param>
        /// <param name="maxRetryCount">Maximum number of retries.</param>
        /// <param name="exponentialBackOffExponent">Value in seconds which is raised by the power of the backup retry attempt.</param>
        /// <param name="backupSkipProvidedIncrements">True to pass backup wait provider its own attempt values, False to provide overall attemp values.</param>
        /// <returns>OnRetry hander</returns>
        IPolicyBuilderWait<TResult> WithExceptionBasedWaitWithExponentialBackoffBackup(Func<int, Exception?, TimeSpan?> waitDurationProvider, int retryCount, double exponentialBackOffExponent, bool backupSkipProvidedIncrements);

        /// <summary>
        /// Custom optional exception-based wait policy backed up by the default wait policy.
        /// </summary>
        /// <param name="waitDurationProvider">Wait policy that can return <see cref="Nullable{TimeSpan}"/> to use the backup wait policy.</param>
        /// <param name="backupSkipProvidedIncrements">True to pass backup wait provider its own attempt values, False to provide overall attemp values.</param>
        /// <returns>OnRetry hander</returns>
        IPolicyBuilderWait<TResult> WithExceptionBasedWaitWithRetryPolicyOptionsBackup(Func<int, Exception?, TimeSpan?> waitDurationProvider, bool backupSkipProvidedIncrements);
    }

    /// <summary>
    /// Sets on-retry handlers.
    /// </summary>
    public interface IPolicyBuilderWait
    {
        /// <summary>
        /// OnRetry behaviors
        /// </summary>
        /// <param name="logger">Logger to enable retry logging.</param>
        /// <param name="onRetry">Custom onretry handler.</param>
        /// <param name="onRetryAsync">Custom async onretry handler. Only applies to <see cref="AsyncRetryPolicy"/>.</param>
        /// <returns>OnRetry builder</returns>
        IPolicyBuilderBuild SetOnRetryBehavior(ILogger? logger = default, RetryHandler.OnRetryHandler? onRetry = default, RetryHandler.OnRetryHandlerAsync? onRetryAsync = default);
    }

    /// <summary>
    /// Sets on-retry handlers.
    /// </summary>
    public interface IPolicyBuilderWait<TResult>
    {
        /// <summary>
        /// OnRetry behaviors
        /// </summary>
        /// <param name="logger">Logger to enable retry logging.</param>
        /// <param name="onRetry">Custom onretry handler.</param>
        /// <param name="onRetryAsync">Custom async onretry handler. Only applies to <see cref="AsyncRetryPolicy{TResult}"/>.</param>
        /// <returns>OnRetry builder</returns>
        IPolicyBuilderBuild<TResult> SetOnRetryBehavior(ILogger? logger = default, RetryHandler.OnRetryHandler<TResult>? onRetry = default, RetryHandler.OnRetryHandlerAsync<TResult>? onRetryAsync = default);
    }

    public interface IPolicyBuilderBuild
    {
        /// <summary>
        /// Builds <see cref="RetryPolicy"/>.
        /// </summary>
        /// <returns>Retry policy.</returns>
        RetryHandler.RetryHandlerPolicy SyncBuild();

        /// <summary>
        /// Builds <see cref="RetryPolicy"/> for extensions to the builder.
        /// </summary>
        /// <returns>Retry policy.</returns>
        ISyncPolicy SyncBuildPolicy();

        /// <summary>
        /// Builds <see cref="AsyncRetryPolicy"/>.
        /// </summary>
        /// <returns>Async retry policy.</returns>
        RetryHandler.AsyncRetryHandlerPolicy AsyncBuild();

        /// <summary>
        /// Builds <see cref="AsyncRetryPolicy"/> for extensions to the builder.
        /// </summary>
        /// <returns>Async retry policy.</returns>
        IAsyncPolicy AsyncBuildPolicy();

        /// <summary>
        /// Retrives the instance of the retryhandler to accomodate extensions to the builder
        /// </summary>
        RetryPolicyBuilder PolicyBuilderBase { get; }
    }

    public interface IPolicyBuilderBuild<TResult>
    {
        ///// <summary>
        ///// Builds <see cref="RetryPolicy{TResult}"/>.
        ///// </summary>
        ///// <returns>Retry policy.</returns>
        //RetryHandlerPolicy<TResult> SyncBuild();

        ///// <summary>
        ///// Builds <see cref="RetryPolicy{TResult}"/> for extensions to the builder.
        ///// </summary>
        ///// <returns>Retry policy.</returns>
        //ISyncPolicy<TResult> SyncBuildPolicy();

        /// <summary>
        /// Builds <see cref="AsyncRetryPolicy{TResult}"/>.
        /// </summary>
        /// <returns>Async retry policy.</returns>
        RetryHandler.AsyncRetryHandlerPolicy<TResult> AsyncBuild();

        /// <summary>
        /// Builds <see cref="AsyncRetryPolicy{TResult}"/> for extensions to the builder.
        /// </summary>
        /// <returns>Async retry policy.</returns>
        IAsyncPolicy<TResult> AsyncBuildPolicy();

        /// <summary>
        /// Retrives the instance of the retryhandler to accomodate extensions to the builder
        /// </summary>
        RetryPolicyBuilder PolicyBuilderBase { get; }
    }
    #endregion

    #region Builder interface implementations
    private readonly Defaults defaultOptions;

    private readonly struct Defaults
    {
        public readonly RetryPolicyOptions PolicyOptions;
        public readonly RetryPolicyBuilder PolicyBuilderBase;

        public Defaults(RetryPolicyBuilder retryHandler, RetryPolicyOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);
            PolicyOptions = options;
            PolicyBuilderBase = retryHandler;
        }
    }

    private readonly struct PolicyBuilderPolicy : IPolicyBuilderPolicy
    {
        private readonly Defaults Defaults;

        public PolicyBuilderPolicy(Defaults options)
        {
            ArgumentNullException.ThrowIfNull(options);
            Defaults = options;
        }

        /// <inheritdoc/>
        IPolicyBuilderBase IPolicyBuilderPolicy.OpinionatedRetryPolicy()
            => new PolicyBuilderBase(Policy.Handle<Exception>(), Defaults);

        /// <inheritdoc/>
        IPolicyBuilderBase IPolicyBuilderPolicy.OpinionatedRetryPolicy(PolicyBuilder policy)
            => new PolicyBuilderBase(policy, Defaults);

        /// <inheritdoc/>
        IPolicyBuilderBase<TResult> IPolicyBuilderPolicy.OpinionatedRetryPolicy<TResult>(PolicyBuilder<TResult> policy)
            => new PolicyBuilderBase<TResult>(policy, Defaults);

        private readonly struct PolicyBuilderBase : IPolicyBuilderBase
        {
            public readonly PolicyBuilder policyBuilder;
            public readonly Defaults Defaults;

            public PolicyBuilderBase(PolicyBuilder policyBuilder, Defaults defaults)
            {
                ArgumentNullException.ThrowIfNull(policyBuilder);
                ArgumentNullException.ThrowIfNull(defaults);
                this.policyBuilder = policyBuilder;
                Defaults = defaults;
            }

            public static Func<int, Exception?, Context, TimeSpan> DefaultSleepDurationProvider(Defaults defaults)
                => ExponentialSleepDurationProvider(defaults.PolicyOptions.ExponentialBackOffExponent);

            public static Func<int, Exception?, Context, TimeSpan> ExponentialSleepDurationProvider(double exponentialBackOffExponent)
                => (attempt, _1, _2) => TimeSpan.FromSeconds(Math.Pow(exponentialBackOffExponent, attempt));

            public static Func<int, Exception?, Context, TimeSpan> ExceptionBasedSleepDurationProviderWithExponentialBackoffBackup(Func<int, Exception?, TimeSpan?> sleepDurationProvider, double exponentialBackOffExponent, bool backupSkipProvidedIncrements)
                => (attempt, exception, ctx) =>
                {
                    return backupSkipProvidedIncrements
                        ? AdjustAttemptIfNeeded()
                        : sleepDurationProvider(attempt, exception) ?? ExponentialSleepDurationProvider(exponentialBackOffExponent)(attempt, exception, ctx);

                    TimeSpan AdjustAttemptIfNeeded()
                    {
                        if (!ctx.TryGetValue(RetryHandler.BackupSkipProvidedIncrementKey, out var value) || value is not int || attempt < 2)
                        {
                            ctx[RetryHandler.BackupSkipProvidedIncrementKey] = value = 0;
                        }

                        var result = sleepDurationProvider(attempt, exception);

                        if (result is null)
                        {
                            var skipIncrement = (int)value;
                            attempt -= skipIncrement;
                            ctx[RetryHandler.BackupSkipProvidedIncrementKey] = ++skipIncrement;
                            result = ExponentialSleepDurationProvider(exponentialBackOffExponent)(attempt, exception, ctx);
                        }

                        return result.Value;
                    }
                };


            /// <inheritdoc/>
            IPolicyBuilderWait IPolicyBuilderBase.WithRetryPolicyOptionsWait()
                => new PolicyBuilderWait(this, Defaults.PolicyOptions.MaxRetryCount, DefaultSleepDurationProvider(Defaults));

            /// <inheritdoc/>
            IPolicyBuilderWait IPolicyBuilderBase.WithCustomizedRetryPolicyOptionsWait(int maxRetryCount, Func<int, Exception?, TimeSpan> sleepDurationProvider)
                => new PolicyBuilderWait(this, maxRetryCount, (attempt, outcome, _1) => sleepDurationProvider(attempt, outcome));

            /// <inheritdoc/>
            IPolicyBuilderWait IPolicyBuilderBase.WithExponentialBackoffWait(int retryCount, double exponentialBackOffExponent)
                => new PolicyBuilderWait(this, retryCount, ExponentialSleepDurationProvider(exponentialBackOffExponent));

            /// <inheritdoc/>
            IPolicyBuilderWait IPolicyBuilderBase.WithExceptionBasedWaitWithRetryPolicyOptionsBackup(Func<int, Exception?, TimeSpan?> sleepDurationProvider, bool backupSkipProvidedIncrements)
                => new PolicyBuilderWait(this, Defaults.PolicyOptions.MaxRetryCount, ExceptionBasedSleepDurationProviderWithExponentialBackoffBackup(sleepDurationProvider, Defaults.PolicyOptions.ExponentialBackOffExponent, backupSkipProvidedIncrements));

            /// <inheritdoc/>
            IPolicyBuilderWait IPolicyBuilderBase.WithExceptionBasedWaitWithExponentialBackoffBackup(Func<int, Exception?, TimeSpan?> sleepDurationProvider, int retryCount, double exponentialBackOffExponent, bool backupSkipProvidedIncrements)
                => new PolicyBuilderWait(this, retryCount, ExceptionBasedSleepDurationProviderWithExponentialBackoffBackup(sleepDurationProvider, exponentialBackOffExponent, backupSkipProvidedIncrements));
        }

        private readonly struct PolicyBuilderBase<TResult> : IPolicyBuilderBase<TResult>
        {
            public readonly PolicyBuilder<TResult> policyBuilder;
            public readonly Defaults Defaults;

            public PolicyBuilderBase(PolicyBuilder<TResult> policyBuilder, Defaults defaults)
            {
                ArgumentNullException.ThrowIfNull(policyBuilder);
                ArgumentNullException.ThrowIfNull(defaults);
                this.policyBuilder = policyBuilder;
                Defaults = defaults;
            }

            /// <inheritdoc/>
            IPolicyBuilderWait<TResult> IPolicyBuilderBase<TResult>.WithRetryPolicyOptionsWait()
                => new PolicyBuilderWait<TResult>(this, Defaults.PolicyOptions.MaxRetryCount, default, PolicyBuilderBase.DefaultSleepDurationProvider(Defaults));

            /// <inheritdoc/>
            IPolicyBuilderWait<TResult> IPolicyBuilderBase<TResult>.WithCustomizedRetryPolicyOptionsWait(int maxRetryCount, Func<int, Exception, TimeSpan> waitDurationProvider)
                => new PolicyBuilderWait<TResult>(this, maxRetryCount, default, (attempt, outcome, _1) => waitDurationProvider(attempt, outcome));

            /// <inheritdoc/>
            IPolicyBuilderWait<TResult> IPolicyBuilderBase<TResult>.WithCustomizedRetryPolicyOptionsWait(int maxRetryCount, Func<int, DelegateResult<TResult>, TimeSpan> sleepDurationProvider)
                => new PolicyBuilderWait<TResult>(this, maxRetryCount, (attempt, outcome, _1) => sleepDurationProvider(attempt, outcome), default);

            /// <inheritdoc/>
            IPolicyBuilderWait<TResult> IPolicyBuilderBase<TResult>.WithExponentialBackoffWait(int maxRetryCount, double exponentialBackOffExponent)
                => new PolicyBuilderWait<TResult>(this, maxRetryCount, default, PolicyBuilderBase.ExponentialSleepDurationProvider(exponentialBackOffExponent));

            /// <inheritdoc/>
            IPolicyBuilderWait<TResult> IPolicyBuilderBase<TResult>.WithExceptionBasedWaitWithRetryPolicyOptionsBackup(Func<int, Exception?, TimeSpan?> sleepDurationProvider, bool backupSkipProvidedIncrements)
                => new PolicyBuilderWait<TResult>(this, Defaults.PolicyOptions.MaxRetryCount, default, PolicyBuilderBase.ExceptionBasedSleepDurationProviderWithExponentialBackoffBackup(sleepDurationProvider, Defaults.PolicyOptions.ExponentialBackOffExponent, backupSkipProvidedIncrements));

            /// <inheritdoc/>
            IPolicyBuilderWait<TResult> IPolicyBuilderBase<TResult>.WithExceptionBasedWaitWithExponentialBackoffBackup(Func<int, Exception?, TimeSpan?> sleepDurationProvider, int retryCount, double exponentialBackOffExponent, bool backupSkipProvidedIncrements)
                => new PolicyBuilderWait<TResult>(this, retryCount, default, PolicyBuilderBase.ExceptionBasedSleepDurationProviderWithExponentialBackoffBackup(sleepDurationProvider, exponentialBackOffExponent, backupSkipProvidedIncrements));
        }

        private readonly struct PolicyBuilderWait : IPolicyBuilderWait
        {
            public readonly PolicyBuilderBase builderBase;
            public readonly Func<int, Exception?, Context, TimeSpan> sleepDurationProvider;
            public readonly int maxRetryCount;

            public PolicyBuilderWait(PolicyBuilderBase builderBase, int maxRetryCount, Func<int, Exception?, Context, TimeSpan> sleepDurationProvider)
            {
                ArgumentNullException.ThrowIfNull(sleepDurationProvider);
                this.builderBase = builderBase;
                this.maxRetryCount = maxRetryCount;
                this.sleepDurationProvider = sleepDurationProvider;
            }

            /// <inheritdoc/>
            IPolicyBuilderBuild IPolicyBuilderWait.SetOnRetryBehavior(ILogger? logger, RetryHandler.OnRetryHandler? onRetry, RetryHandler.OnRetryHandlerAsync? onRetryAsync)
                => new PolicyBuilderBuild(this, sleepDurationProvider, logger, onRetry, onRetryAsync);
        }

        private readonly struct PolicyBuilderWait<TResult> : IPolicyBuilderWait<TResult>
        {
            public readonly PolicyBuilderBase<TResult> builderBase;
            public readonly Func<int, Exception, Context, TimeSpan>? sleepDurationProvider;
            public readonly Func<int, DelegateResult<TResult>, Context, TimeSpan>? genericSleepDurationProvider;
            public readonly int maxRetryCount;

            private static Func<int, DelegateResult<TResult>, Context, TimeSpan> PickSleepDurationProvider(Func<int, DelegateResult<TResult>, Context, TimeSpan>? tResultProvider, Func<int, Exception, Context, TimeSpan>? exceptionProvider)
                => tResultProvider is null ? (attempt, outcome, ctx) => exceptionProvider!(attempt, outcome.Exception, ctx) : tResultProvider;

            public PolicyBuilderWait(PolicyBuilderBase<TResult> builderBase, int maxRetryCount, Func<int, DelegateResult<TResult>, Context, TimeSpan>? sleepDurationProviderResult, Func<int, Exception, Context, TimeSpan>? sleepDurationProviderException)
            {
                if (sleepDurationProviderException is null && sleepDurationProviderResult is null)
                {
                    throw new ArgumentNullException(null, $"At least one of {nameof(sleepDurationProviderResult)} or {nameof(sleepDurationProviderException)} must be provided.");
                }

                this.builderBase = builderBase;
                this.maxRetryCount = maxRetryCount;
                this.sleepDurationProvider = sleepDurationProviderException;
                this.genericSleepDurationProvider = sleepDurationProviderResult;
            }

            /// <inheritdoc/>
            IPolicyBuilderBuild<TResult> IPolicyBuilderWait<TResult>.SetOnRetryBehavior(ILogger? logger, RetryHandler.OnRetryHandler<TResult>? onRetry, RetryHandler.OnRetryHandlerAsync<TResult>? onRetryAsync)
                => new PolicyBuilderBuild<TResult>(this, PickSleepDurationProvider(genericSleepDurationProvider, sleepDurationProvider), logger, onRetry, onRetryAsync);
        }

        private readonly struct PolicyBuilderBuild : IPolicyBuilderBuild
        {
            private readonly PolicyBuilderWait builderWait;
            private readonly Func<int, Exception?, Context, TimeSpan> sleepDurationProvider;
            private readonly ILogger? logger;
            private readonly RetryHandler.OnRetryHandler? onRetryHandler;
            private readonly RetryHandler.OnRetryHandlerAsync? onRetryHandlerAsync;

            /// <inheritdoc/>
            public RetryPolicyBuilder PolicyBuilderBase { get; }

            public PolicyBuilderBuild(PolicyBuilderWait builderWait, Func<int, Exception?, Context, TimeSpan> sleepDurationProvider, ILogger? logger, RetryHandler.OnRetryHandler? onRetry, RetryHandler.OnRetryHandlerAsync? onRetryAsync)
            {
                ArgumentNullException.ThrowIfNull(sleepDurationProvider);
                this.builderWait = builderWait;
                this.sleepDurationProvider = sleepDurationProvider;
                this.logger = logger;
                this.onRetryHandler = onRetry;
                this.onRetryHandlerAsync = onRetryAsync;
                this.PolicyBuilderBase = builderWait.builderBase.Defaults.PolicyBuilderBase;
            }

            public static Action<Exception, TimeSpan, int, Context> Logger(ILogger? logger)
            {
                return (exception, timeSpan, retryCount, ctx) =>
                    logger?.LogError(exception, @"Retrying in {Method}: RetryCount: {RetryCount} TimeSpan: {TimeSpan:c} CorrelationId: {CorrelationId:D} ErrorMessage: {ExceptionMessage}", ctx[RetryHandler.CallerMemberNameKey], retryCount, timeSpan, ctx.CorrelationId, exception.Message);
            }

            public static Action<Exception, TimeSpan, int, Context> OnRetryHandler(ILogger? logger, RetryHandler.OnRetryHandler? onRetryHandler)
            {
                var handler = onRetryHandler ?? new((exception, timeSpan, retryCount, correlationId, caller) => { });

                return (exception, timeSpan, retryCount, ctx) =>
                {
                    handler(exception, timeSpan, retryCount, ctx.CorrelationId, ctx[RetryHandler.CallerMemberNameKey] as string);
                    Logger(logger)(exception, timeSpan, retryCount, ctx);
                };
            }

            public static Func<Exception, TimeSpan, int, Context, Task> OnRetryHandlerAsync(ILogger? logger, RetryHandler.OnRetryHandler? onRetryHandler, RetryHandler.OnRetryHandlerAsync? onRetryHandlerAsync)
            {
                var handler = onRetryHandler ?? new((exception, timeSpan, retryCount, correlationId, caller) => { });
                var handlerAsync = onRetryHandlerAsync ?? new((exception, timeSpan, retryCount, correlationId, caller) =>
                {
                    handler(exception, timeSpan, retryCount, correlationId, caller);
                    return Task.CompletedTask;
                });

                return async (exception, timeSpan, retryCount, ctx) =>
                {
                    await handlerAsync(exception, timeSpan, retryCount, ctx.CorrelationId, ctx[RetryHandler.CallerMemberNameKey] as string);
                    Logger(logger)(exception, timeSpan, retryCount, ctx);
                };
            }

            /// <inheritdoc/>
            ISyncPolicy IPolicyBuilderBuild.SyncBuildPolicy()
            {
                var waitProvider = sleepDurationProvider;
                var onRetryProvider = OnRetryHandler(logger, onRetryHandler);

                return builderWait.builderBase.policyBuilder.WaitAndRetry(builderWait.maxRetryCount, (attempt, ctx) => waitProvider(attempt, default, ctx), onRetryProvider);
            }

            /// <inheritdoc/>
            IAsyncPolicy IPolicyBuilderBuild.AsyncBuildPolicy()
            {
                var waitProvider = sleepDurationProvider;
                var onRetryProvider = OnRetryHandlerAsync(logger, onRetryHandler, onRetryHandlerAsync);

                return builderWait.builderBase.policyBuilder.WaitAndRetryAsync(builderWait.maxRetryCount, waitProvider, onRetryProvider);
            }

            /// <inheritdoc/>
            RetryHandler.RetryHandlerPolicy IPolicyBuilderBuild.SyncBuild()
                => new(((IPolicyBuilderBuild)this).SyncBuildPolicy());

            /// <inheritdoc/>
            RetryHandler.AsyncRetryHandlerPolicy IPolicyBuilderBuild.AsyncBuild()
                => new(((IPolicyBuilderBuild)this).AsyncBuildPolicy());
        }

        private readonly struct PolicyBuilderBuild<TResult> : IPolicyBuilderBuild<TResult>
        {
            private readonly PolicyBuilderWait<TResult> builderWait;
            private readonly Func<int, DelegateResult<TResult>, Context, TimeSpan> sleepDurationProvider;
            private readonly ILogger? logger;
            private readonly RetryHandler.OnRetryHandler<TResult>? onRetryHandler;
            private readonly RetryHandler.OnRetryHandlerAsync<TResult>? onRetryHandlerAsync;

            /// <inheritdoc/>
            public RetryPolicyBuilder PolicyBuilderBase { get; }

            public PolicyBuilderBuild(PolicyBuilderWait<TResult> builderWait, Func<int, DelegateResult<TResult>, Context, TimeSpan> sleepDurationProvider, ILogger? logger, RetryHandler.OnRetryHandler<TResult>? onRetry, RetryHandler.OnRetryHandlerAsync<TResult>? onRetryAsync)
            {
                ArgumentNullException.ThrowIfNull(sleepDurationProvider);
                this.builderWait = builderWait;
                this.sleepDurationProvider = sleepDurationProvider;
                this.logger = logger;
                this.onRetryHandler = onRetry;
                this.onRetryHandlerAsync = onRetryAsync;
                this.PolicyBuilderBase = builderWait.builderBase.Defaults.PolicyBuilderBase;
            }

            private static Action<DelegateResult<TResult>, TimeSpan, int, Context> Logger(ILogger? logger)
            {
                return (outcome, timeSpan, retryCount, ctx) =>
                {
                    if (outcome.Exception is null)
                    {
                        logger?.LogError(@"Retrying in {Method}: RetryCount: {RetryCount} TimeSpan: {TimeSpan:c} CorrelationId: {CorrelationId:D}", ctx[RetryHandler.CallerMemberNameKey], retryCount, timeSpan, ctx.CorrelationId);
                    }
                    else
                    {
                        logger?.LogError(outcome.Exception, @"Retrying in {Method}: RetryCount: {RetryCount} TimeSpan: {TimeSpan:c} CorrelationId: {CorrelationId:D} ErrorMessage: {ExceptionMessage}", ctx[RetryHandler.CallerMemberNameKey], retryCount, timeSpan, ctx.CorrelationId, outcome.Exception.Message);
                    }
                };
            }

            //private static Action<DelegateResult<TResult>, TimeSpan, int, Context> OnRetryHandler(ILogger? logger, RetryHandler.OnRetryHandler<TResult>>? onRetryHandler)
            //{
            //    var genericHandler = onRetryHandler ?? new((outcome, timeSpan, retryCount, correlationId, caller) => { });

            //    return (outcome, timeSpan, retryCount, ctx) =>
            //    {
            //        genericHandler(outcome, timeSpan, retryCount, ctx.CorrelationId, ctx[RetryHandler.CallerMemberNameKey] as string);
            //        Logger(logger)(outcome, timeSpan, retryCount, ctx);
            //    };
            //}

            private static Func<DelegateResult<TResult>, TimeSpan, int, Context, Task> OnRetryHandlerAsync(ILogger? logger, RetryHandler.OnRetryHandler<TResult>? onRetryHandler, RetryHandler.OnRetryHandlerAsync<TResult>? onRetryHandlerAsync)
            {
                var handler = onRetryHandler ?? new((outcome, timeSpan, retryCount, correlationId, caller) => { });
                var asyncHandler = onRetryHandlerAsync ?? new((outcome, timespan, retryCount, correlationId, caller) =>
                {
                    handler(outcome, timespan, retryCount, correlationId, caller);
                    return Task.CompletedTask;
                });

                return async (outcome, timespan, retryCount, ctx) =>
                {
                    await asyncHandler(outcome, timespan, retryCount, ctx.CorrelationId, ctx[RetryHandler.CallerMemberNameKey] as string);
                    Logger(logger)(outcome, timespan, retryCount, ctx);
                };
            }

            ///// <inheritdoc/>
            //ISyncPolicy<TResult> IPolicyBuilderBuild<TResult>.BuildPolicy()
            //{
            //    var waitProvider = sleepDurationProvider;
            //    var onRetryProvider = OnRetryHandler(logger, onRetryHandler);

            //    return builderWait.builderBase.policyBuilder.WaitAndRetry(builderWait.retryCount, waitProvider, onRetryProvider);
            //}

            /// <inheritdoc/>
            IAsyncPolicy<TResult> IPolicyBuilderBuild<TResult>.AsyncBuildPolicy()
            {
                var waitProvider = sleepDurationProvider;
                var onRetryProvider = OnRetryHandlerAsync(logger, onRetryHandler, onRetryHandlerAsync);

                return builderWait.builderBase.policyBuilder.WaitAndRetryAsync(builderWait.maxRetryCount, waitProvider, onRetryProvider);
            }

            ///// <inheritdoc/>
            //RetryHandlerPolicy<TResult> IPolicyBuilderBuild<TResult>.Build()
            //    => new(((IPolicyBuilderBuild<TResult>)this).BuildPolicy());

            /// <inheritdoc/>
            RetryHandler.AsyncRetryHandlerPolicy<TResult> IPolicyBuilderBuild<TResult>.AsyncBuild()
                => new(((IPolicyBuilderBuild<TResult>)this).AsyncBuildPolicy());
        }
    }
    #endregion
}
