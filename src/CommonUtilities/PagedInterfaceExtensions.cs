// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Rest.Azure;
using Polly.Retry;

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
        /// Creates an <see cref="IAsyncEnumerable{T}"/> from an <see cref="Microsoft.Rest.Azure.IPage{T}"/>
        /// </summary>
        /// <typeparam name="T">The type of objects to enumerate.</typeparam>
        /// <param name="source">The <see cref="IPage{T}"/> to enumerate.</param>
        /// <param name="nextPageFunc">The function taking the nextPageLink and returning a new <see cref="IPage{T}"/>.</param>
        /// <returns>An <see cref="IAsyncEnumerable{T}"/></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IPage<T> source, Func<string, CancellationToken, Task<IPage<T>>> nextPageFunc)
            => new AsyncEnumerable<T>(source, nextPageFunc);

        /// <summary>
        /// Adapts calls returning <see cref="IAsyncEnumerable{T}"/> to <see cref="AsyncRetryPolicy"/>.
        /// </summary>
        /// <typeparam name="T">Type of results returned in <see cref="IAsyncEnumerable{T}"/> by <paramref name="func"/>.</typeparam>
        /// <param name="asyncRetryPolicy">Policy retrying calls made while enumerating results returned by <paramref name="func"/>.</param>
        /// <param name="func">Method returning <see cref="IAsyncEnumerable{T}"/>.</param>
        /// <param name="retryPolicy">Policy retrying call to <paramref name="func"/>.</param>
        /// <returns></returns>
        public static IAsyncEnumerable<T> ExecuteAsync<T>(this AsyncRetryPolicy asyncRetryPolicy, Func<IAsyncEnumerable<T>> func, RetryPolicy retryPolicy)
        {
            ArgumentNullException.ThrowIfNull(asyncRetryPolicy);
            ArgumentNullException.ThrowIfNull(func);
            ArgumentNullException.ThrowIfNull(retryPolicy);

            return new PollyAsyncEnumerable<T>((retryPolicy).Execute(() => func()), asyncRetryPolicy);
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

            public AsyncEnumerable(IPage<T> source, Func<string, CancellationToken, Task<IPage<T>>> nextPageFunc)
            {
                ArgumentNullException.ThrowIfNull(source);

                _getEnumerator = c => new PageEnumerator<T>(source, nextPageFunc, c);
            }

            IAsyncEnumerator<T> IAsyncEnumerable<T>.GetAsyncEnumerator(CancellationToken cancellationToken)
                => _getEnumerator(cancellationToken);
        }

        private sealed class PollyAsyncEnumerable<T> : IAsyncEnumerable<T>
        {
            private readonly IAsyncEnumerable<T> _source;
            private readonly AsyncRetryPolicy _retryPolicy;

            public PollyAsyncEnumerable(IAsyncEnumerable<T> source, AsyncRetryPolicy retryPolicy)
            {
                ArgumentNullException.ThrowIfNull(source);
                ArgumentNullException.ThrowIfNull(retryPolicy);

                _source = source;
                _retryPolicy = retryPolicy;
            }

            IAsyncEnumerator<T> IAsyncEnumerable<T>.GetAsyncEnumerator(CancellationToken cancellationToken)
                => new PollyAsyncEnumerator<T>(_source.GetAsyncEnumerator(cancellationToken), _retryPolicy, cancellationToken);
        }

        private sealed class PollyAsyncEnumerator<T> : IAsyncEnumerator<T>
        {
            private readonly IAsyncEnumerator<T> _source;
            private readonly AsyncRetryPolicy _retryPolicy;
            private readonly CancellationToken _cancellationToken;

            public PollyAsyncEnumerator(IAsyncEnumerator<T> source, AsyncRetryPolicy retryPolicy, CancellationToken cancellationToken)
            {
                ArgumentNullException.ThrowIfNull(source);
                ArgumentNullException.ThrowIfNull(retryPolicy);

                _source = source;
                _retryPolicy = retryPolicy;
                _cancellationToken = cancellationToken;
            }

            T IAsyncEnumerator<T>.Current
                => _source.Current;

            ValueTask IAsyncDisposable.DisposeAsync()
                => _source.DisposeAsync();

            ValueTask<bool> IAsyncEnumerator<T>.MoveNextAsync()
                => new(_retryPolicy.ExecuteAsync(ct => _source.MoveNextAsync(ct).AsTask(), _cancellationToken));
        }

        private sealed class PageEnumerator<T> : EnumeratorEnumerator<T, IPage<T>>
        {
            public PageEnumerator(IPage<T> source, Func<string, CancellationToken, Task<IPage<T>>> nextPageFunc, CancellationToken cancellationToken)
                : base(source, s => s.GetEnumerator(), (s, ct) => s.NextPageLink is null ? Task.FromResult<IPage<T>>(null!) : nextPageFunc(s.NextPageLink, ct), cancellationToken)
            { }
        }

        private sealed class PagedCollectionEnumerator<T> : EnumeratorEnumerator<T, IPagedCollection<T>>
        {
            public PagedCollectionEnumerator(IPagedCollection<T> source, CancellationToken cancellationToken)
                : base(source, s => s.GetEnumerator(), (s, ct) => s.GetNextPageAsync(ct), cancellationToken)
            { }
        }

        private abstract class EnumeratorEnumerator<TItem, TSource> : Enumerator<TItem, IEnumerator<TItem>>
        {
            protected TSource _source;

            private readonly Func<TSource, IEnumerator<TItem>> _getEnumerator;
            private readonly Func<TSource, CancellationToken, Task<TSource>> _getNext;

            protected EnumeratorEnumerator(TSource source, Func<TSource, IEnumerator<TItem>> getEnumerator, Func<TSource, CancellationToken, Task<TSource>> getNext, CancellationToken cancellationToken)
                : base(e => e.Current, cancellationToken)
            {
                ArgumentNullException.ThrowIfNull(source);
                ArgumentNullException.ThrowIfNull(getEnumerator);
                ArgumentNullException.ThrowIfNull(getNext);

                _source = source;
                _enumerator = getEnumerator(source);
                _getEnumerator = getEnumerator;
                _getNext = getNext;
            }

            public override ValueTask<bool> MoveNextAsync()
            {
                _cancellationToken.ThrowIfCancellationRequested();
                return _enumerator!.MoveNext()
                    ? ValueTask.FromResult(true)
                    : new(MoveToNextSource());

                async Task<bool> MoveToNextSource()
                {
                    do
                    {
                        _enumerator?.Dispose();
                        _enumerator = null;
                        _source = await _getNext(_source, _cancellationToken);

                        if (_source is null)
                        {
                            return false;
                        }

                        _enumerator = _getEnumerator(_source);
                    }
                    while (!(_enumerator?.MoveNext() ?? false));

                    return true;
                }
            }
        }

        public abstract class Enumerator<TItem, TEnumerator> : IAsyncEnumerator<TItem> where TEnumerator : IDisposable
        {
            protected readonly CancellationToken _cancellationToken;
            protected TEnumerator? _enumerator;

            private readonly Func<TEnumerator, TItem> _getCurrent;

            protected Enumerator(Func<TEnumerator, TItem> getCurrent, CancellationToken cancellationToken)
            {
                ArgumentNullException.ThrowIfNull(getCurrent);

                _getCurrent = getCurrent;
                _cancellationToken = cancellationToken;
            }

            protected Enumerator(TEnumerator enumerator, Func<TEnumerator, TItem> getCurrent, CancellationToken cancellationToken)
            {
                ArgumentNullException.ThrowIfNull(enumerator);
                ArgumentNullException.ThrowIfNull(getCurrent);

                _getCurrent = getCurrent;
                _enumerator = enumerator;
                _cancellationToken = cancellationToken;
            }

            TItem IAsyncEnumerator<TItem>.Current => _getCurrent(_enumerator!);

            public abstract ValueTask<bool> MoveNextAsync();

            protected virtual ValueTask DisposeAsyncCore()
            {
                _enumerator?.Dispose();
                return ValueTask.CompletedTask;
            }

            async ValueTask IAsyncDisposable.DisposeAsync()
            {
                await DisposeAsyncCore().ConfigureAwait(false);
                GC.SuppressFinalize(this);
            }
        }
        #endregion
    }
}
