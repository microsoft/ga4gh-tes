// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using static CommonUtilities.RetryHandler;

namespace CommonUtilities
{
    /// <summary>
    /// Extension methods and implementations for enumerating paged enumeration/collection types from Azure
    /// </summary>
    public static class PagedInterfaceExtensions
    {
        /// <summary>
        /// Adapts calls returning <see cref="IAsyncEnumerable{T}"/> to <see cref="AsyncRetryPolicy"/>.
        /// </summary>
        /// <typeparam name="T">Type of results returned in <see cref="IAsyncEnumerable{T}"/> by <paramref name="func"/>.</typeparam>
        /// <param name="asyncRetryPolicy">Policy retrying calls made while enumerating results returned by <paramref name="func"/>.</param>
        /// <param name="func">Method returning <see cref="IAsyncEnumerable{T}"/>.</param>
        /// <param name="retryPolicy">Policy retrying call to <paramref name="func"/>.</param>
        /// <param name="caller">Name of method originating the retriable operation.</param>
        /// <returns></returns>
        public static IAsyncEnumerable<T> ExecuteWithRetryAsync<T>(this AsyncRetryHandlerPolicy asyncRetryPolicy, Func<IAsyncEnumerable<T>> func, RetryHandlerPolicy retryPolicy, [System.Runtime.CompilerServices.CallerMemberName] string? caller = default)
        {
            ArgumentNullException.ThrowIfNull(asyncRetryPolicy);
            ArgumentNullException.ThrowIfNull(func);
            ArgumentNullException.ThrowIfNull(retryPolicy);

            var ctx = PrepareContext(caller);
            return new PollyAsyncEnumerable<T>(retryPolicy.RetryPolicy.Execute(_ => func(), ctx), asyncRetryPolicy, ctx);
        }

        ///// <summary>
        ///// Adapts calls returning <see cref="IAsyncEnumerable{T}"/> to <see cref="AsyncRetryPolicy"/>.
        ///// </summary>
        ///// <typeparam name="T">Type of results returned in <see cref="IAsyncEnumerable{T}"/> by <paramref name="func"/>.</typeparam>
        ///// <param name="asyncRetryPolicy">Policy retrying call to <paramref name="func"/> and calls made while enumerating results returned by <paramref name="func"/>.</param>
        ///// <param name="func">Method returning <see cref="ValueTask{IAsyncEnumerable{T}}"/>.</param>
        ///// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        ///// <param name="caller">Name of method originating the retriable operation.</param>
        ///// <returns></returns>
        //public static async ValueTask<IAsyncEnumerable<T>> ExecuteWithRetryAsync<T>(this AsyncRetryHandlerPolicy asyncRetryPolicy, Func<CancellationToken, ValueTask<IAsyncEnumerable<T>>> func, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string? caller = default)
        //{
        //    ArgumentNullException.ThrowIfNull(asyncRetryPolicy);
        //    ArgumentNullException.ThrowIfNull(func);

        //    var ctx = PrepareContext(caller);
        //    return new PollyAsyncEnumerable<T>(await asyncRetryPolicy.RetryPolicy.ExecuteAsync((_, ct) => func(ct).AsTask(), ctx, cancellationToken), asyncRetryPolicy, ctx);
        //}

        ///// <summary>
        ///// Adapts calls returning <see cref="IAsyncEnumerable{T}"/> to <see cref="AsyncRetryPolicy"/>.
        ///// </summary>
        ///// <typeparam name="T">Type of results returned in <see cref="IAsyncEnumerable{T}"/> by <paramref name="func"/>.</typeparam>
        ///// <param name="asyncRetryPolicy">Policy retrying call to <paramref name="func"/> and calls made while enumerating results returned by <paramref name="func"/>.</param>
        ///// <param name="func">Method returning <see cref="Task{IAsyncEnumerable{T}}"/>.</param>
        ///// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        ///// <param name="caller">Name of method originating the retriable operation.</param>
        ///// <returns></returns>
        //public static async Task<IAsyncEnumerable<T>> ExecuteWithRetryAsync<T>(this AsyncRetryHandlerPolicy asyncRetryPolicy, Func<CancellationToken, Task<IAsyncEnumerable<T>>> func, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string? caller = default)
        //{
        //    ArgumentNullException.ThrowIfNull(asyncRetryPolicy);
        //    ArgumentNullException.ThrowIfNull(func);

        //    var ctx = PrepareContext(caller);
        //    return new PollyAsyncEnumerable<T>(await asyncRetryPolicy.RetryPolicy.ExecuteAsync((_, ct) => func(ct), ctx, cancellationToken), asyncRetryPolicy, ctx);
        //}

        #region Implementation classes
        private sealed class PollyAsyncEnumerable<T> : IAsyncEnumerable<T>
        {
            private readonly IAsyncEnumerable<T> _source;
            private readonly RetryHandler.AsyncRetryHandlerPolicy _retryPolicy;
            private readonly Polly.Context _ctx;

            public PollyAsyncEnumerable(IAsyncEnumerable<T> source, RetryHandler.AsyncRetryHandlerPolicy retryPolicy, Polly.Context ctx)
            {
                ArgumentNullException.ThrowIfNull(source);
                ArgumentNullException.ThrowIfNull(retryPolicy);
                ArgumentNullException.ThrowIfNull(ctx);

                _source = source;
                _retryPolicy = retryPolicy;
                _ctx = ctx;
            }

            /// <inheritdoc/>
            IAsyncEnumerator<T> IAsyncEnumerable<T>.GetAsyncEnumerator(CancellationToken cancellationToken)
                => new PollyAsyncEnumerator<T>(_source.GetAsyncEnumerator(cancellationToken), _retryPolicy, _ctx, cancellationToken);
        }

        private sealed class PollyAsyncEnumerator<T> : IAsyncEnumerator<T>
        {
            private readonly IAsyncEnumerator<T> _source;
            private readonly RetryHandler.AsyncRetryHandlerPolicy _retryPolicy;
            private readonly CancellationToken _cancellationToken;
            private readonly Polly.Context _ctx;

            public PollyAsyncEnumerator(IAsyncEnumerator<T> source, RetryHandler.AsyncRetryHandlerPolicy retryPolicy, Polly.Context ctx, CancellationToken cancellationToken)
            {
                ArgumentNullException.ThrowIfNull(source);
                ArgumentNullException.ThrowIfNull(retryPolicy);
                ArgumentNullException.ThrowIfNull(ctx);

                _source = source;
                _retryPolicy = retryPolicy;
                _ctx = ctx;
                _cancellationToken = cancellationToken;
            }

            /// <inheritdoc/>
            T IAsyncEnumerator<T>.Current
                => _source.Current;

            /// <inheritdoc/>
            ValueTask IAsyncDisposable.DisposeAsync()
                => _source.DisposeAsync();

            /// <inheritdoc/>
            ValueTask<bool> IAsyncEnumerator<T>.MoveNextAsync()
                => new(_retryPolicy.RetryPolicy.ExecuteAsync((_, ct) => _source.MoveNextAsync(ct).AsTask(), new(_ctx.OperationKey, _ctx), _cancellationToken));
        }
        #endregion
    }
}
