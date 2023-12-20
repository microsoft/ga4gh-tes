// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Rest.Azure;
using static CommonUtilities.RetryHandler;

namespace CommonUtilities
{
    /// <summary>
    /// Extension methods and implementations for enumerating paged enumeration/collection types from Azure
    /// </summary>
    public static class PagedInterfaceExtensions
    {
        /// <summary>
        /// Creates an <see cref="IAsyncEnumerable{T}"/> from an <see cref="IPagedCollection{T}"/>
        /// </summary>
        /// <typeparam name="T">The type of objects to enumerate.</typeparam>
        /// <param name="source">The <see cref="IPagedCollection{T}"/> to enumerate.</param>
        /// <returns>An <see cref="IAsyncEnumerable{T}"/></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IPagedCollection<T> source)
            => new AsyncEnumerable<T>(source);


        /// <summary>
        /// Creates an <see cref="IAsyncEnumerable{T}"/> from an <see cref="IPage{T}"/>
        /// </summary>
        /// <typeparam name="T">The type of objects to enumerate.</typeparam>
        /// <param name="source">The <see cref="IPage{T}"/> to enumerate.</param>
        /// <param name="nextPageFunc">The function taking the nextPageLink and returning a new <see cref="IPage{T}"/>.</param>
        /// <returns>An <see cref="IAsyncEnumerable{T}"/></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IPage<T> source, Func<string, CancellationToken, Task<IPage<T>?>> nextPageFunc)
            => new AsyncEnumerable<T>(source, nextPageFunc);

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

        /// <summary>
        /// Adapts calls returning <see cref="IAsyncEnumerable{T}"/> to <see cref="AsyncRetryPolicy"/>.
        /// </summary>
        /// <typeparam name="T">Type of results returned in <see cref="IAsyncEnumerable{T}"/> by <paramref name="func"/>.</typeparam>
        /// <param name="asyncRetryPolicy">Policy retrying call to <paramref name="func"/> and calls made while enumerating results returned by <paramref name="func"/>.</param>
        /// <param name="func">Method returning <see cref="ValueTask{IAsyncEnumerable{T}}"/>.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="caller">Name of method originating the retriable operation.</param>
        /// <returns></returns>
        public static async ValueTask<IAsyncEnumerable<T>> ExecuteWithRetryAsync<T>(this AsyncRetryHandlerPolicy asyncRetryPolicy, Func<CancellationToken, ValueTask<IAsyncEnumerable<T>>> func, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string? caller = default)
        {
            ArgumentNullException.ThrowIfNull(asyncRetryPolicy);
            ArgumentNullException.ThrowIfNull(func);

            var ctx = PrepareContext(caller);
            return new PollyAsyncEnumerable<T>(await asyncRetryPolicy.RetryPolicy.ExecuteAsync((_, ct) => func(ct).AsTask(), ctx, cancellationToken), asyncRetryPolicy, ctx);
        }

        /// <summary>
        /// Adapts calls returning <see cref="IAsyncEnumerable{T}"/> to <see cref="AsyncRetryPolicy"/>.
        /// </summary>
        /// <typeparam name="T">Type of results returned in <see cref="IAsyncEnumerable{T}"/> by <paramref name="func"/>.</typeparam>
        /// <param name="asyncRetryPolicy">Policy retrying call to <paramref name="func"/> and calls made while enumerating results returned by <paramref name="func"/>.</param>
        /// <param name="func">Method returning <see cref="Task{IAsyncEnumerable{T}}"/>.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="caller">Name of method originating the retriable operation.</param>
        /// <returns></returns>
        public static async Task<IAsyncEnumerable<T>> ExecuteWithRetryAsync<T>(this AsyncRetryHandlerPolicy asyncRetryPolicy, Func<CancellationToken, Task<IAsyncEnumerable<T>>> func, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string? caller = default)
        {
            ArgumentNullException.ThrowIfNull(asyncRetryPolicy);
            ArgumentNullException.ThrowIfNull(func);

            var ctx = PrepareContext(caller);
            return new PollyAsyncEnumerable<T>(await asyncRetryPolicy.RetryPolicy.ExecuteAsync((_, ct) => func(ct), ctx, cancellationToken), asyncRetryPolicy, ctx);
        }

        #region Implementation classes
        private readonly struct AsyncEnumerable<T> : IAsyncEnumerable<T>
        {
            private readonly Func<CancellationToken, IAsyncEnumerator<T>> _getEnumerator;

            public AsyncEnumerable(IPagedCollection<T> source)
            {
                ArgumentNullException.ThrowIfNull(source);

                _getEnumerator = c => new PagedCollectionEnumerator<T>(source, c);
            }

            public AsyncEnumerable(IPage<T> source, Func<string, CancellationToken, Task<IPage<T>?>> nextPageFunc)
            {
                ArgumentNullException.ThrowIfNull(source);

                _getEnumerator = c => new PageEnumerator<T>(source, nextPageFunc, c);
            }

            /// <inheritdoc/>
            IAsyncEnumerator<T> IAsyncEnumerable<T>.GetAsyncEnumerator(CancellationToken cancellationToken)
                => _getEnumerator(cancellationToken);
        }

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

        private sealed class PageEnumerator<T> : PagingEnumerator<T, IPage<T>>
        {
            public PageEnumerator(IPage<T> source, Func<string, CancellationToken, Task<IPage<T>?>> nextPageFunc, CancellationToken cancellationToken)
                : base(source, s => s.GetEnumerator(), (s, ct) => s.NextPageLink is null ? Task.FromResult<IPage<T>?>(null) : nextPageFunc(s.NextPageLink, ct), cancellationToken)
            { }
        }

        private sealed class PagedCollectionEnumerator<T> : PagingEnumerator<T, IPagedCollection<T>>
        {
            public PagedCollectionEnumerator(IPagedCollection<T> source, CancellationToken cancellationToken)
                : base(source, s => s.GetEnumerator(), (s, ct) => s.GetNextPageAsync(ct), cancellationToken)
            { }
        }

        private abstract class PagingEnumerator<TItem, TSource> : AbstractEnumerator<TItem, IEnumerator<TItem>>
        {
            protected TSource? _source;

            private readonly Func<TSource, IEnumerator<TItem>> _getEnumerator;
            private readonly Func<TSource, CancellationToken, Task<TSource?>> _getNext;

            protected PagingEnumerator(TSource source, Func<TSource, IEnumerator<TItem>> getEnumerator, Func<TSource, CancellationToken, Task<TSource?>> getNext, CancellationToken cancellationToken)
                : base(getEnumerator(source), e => e.Current, cancellationToken)
            {
                ArgumentNullException.ThrowIfNull(source);
                ArgumentNullException.ThrowIfNull(getEnumerator);
                ArgumentNullException.ThrowIfNull(getNext);

                _source = source;
                _getEnumerator = getEnumerator;
                _getNext = getNext;
            }

            /// <inheritdoc/>
            public override ValueTask<bool> MoveNextAsync()
            {
                CancellationToken.ThrowIfCancellationRequested();
                return Enumerator?.MoveNext() switch
                {
                    null => ValueTask.FromResult(false),
                    true => ValueTask.FromResult(true),
                    false => new(MoveToNextSource())
                };

                async Task<bool> MoveToNextSource()
                {
                    do
                    {
                        Enumerator?.Dispose();
                        Enumerator = null;
                        _source = await _getNext(_source!, CancellationToken);

                        if (_source is null)
                        {
                            return false;
                        }

                        Enumerator = _getEnumerator(_source);
                    }
                    while (!(Enumerator?.MoveNext() ?? false));

                    return true;
                }
            }
        }

        public abstract class AbstractEnumerator<TItem, TEnumerator> : IAsyncEnumerator<TItem> where TEnumerator : IDisposable
        {
            protected readonly CancellationToken CancellationToken;
            protected TEnumerator? Enumerator;

            private readonly Func<TEnumerator, TItem> GetCurrent;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="enumerator">Initial enumerator.</param>
            /// <param name="getCurrent">Method that returns the equivalent of <see cref="IEnumerator{T}.Current"/>'s value.</param>
            /// <param name="cancellationToken"></param>
            protected AbstractEnumerator(TEnumerator enumerator, Func<TEnumerator, TItem> getCurrent, CancellationToken cancellationToken)
            {
                ArgumentNullException.ThrowIfNull(enumerator);
                ArgumentNullException.ThrowIfNull(getCurrent);

                GetCurrent = getCurrent;
                Enumerator = enumerator;
                CancellationToken = cancellationToken;
            }

            /// <inheritdoc/>
            TItem IAsyncEnumerator<TItem>.Current => GetCurrent(Enumerator!);

            /// <inheritdoc/>
            public abstract ValueTask<bool> MoveNextAsync();

            /// <summary>
            /// <c>DisposeAsync</c> pattern method.
            /// </summary>
            /// <remarks>https://learn.microsoft.com/dotnet/standard/garbage-collection/implementing-disposeasync</remarks>
            protected virtual ValueTask DisposeAsyncCore()
            {
                Enumerator?.Dispose();
                return ValueTask.CompletedTask;
            }

            /// <inheritdoc/>
            async ValueTask IAsyncDisposable.DisposeAsync()
            {
                await DisposeAsyncCore();
                GC.SuppressFinalize(this);
            }
        }
        #endregion
    }
}
