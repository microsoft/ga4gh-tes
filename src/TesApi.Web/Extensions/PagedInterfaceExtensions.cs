﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;

namespace TesApi.Web.Extensions
{
    /// <summary>
    /// Extension methods and implementations for enumerating paged enumeration/collection types from Azure
    /// </summary>
    public static class PagedInterfaceExtensions
    {
        /// <summary>
        /// Creates an <see cref="IAsyncEnumerable{T}"/> from an <see cref="IPagedEnumerable{T}"/>.
        /// </summary>
        /// <typeparam name="T">The type of objects to enumerate.</typeparam>
        /// <param name="source">The <see cref="IPagedEnumerable{T}"/> to enumerate.</param>
        /// <returns>An <see cref="IAsyncEnumerable{T}"/></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IPagedEnumerable<T> source)
            => new AsyncEnumerable<T>(source);

        #region Implementation classes
        private readonly struct AsyncEnumerable<T> : IAsyncEnumerable<T>
        {
            private readonly Func<CancellationToken, IAsyncEnumerator<T>> GetEnumerator;

            public AsyncEnumerable(IPagedEnumerable<T> source)
            {
                ArgumentNullException.ThrowIfNull(source);

                GetEnumerator = c => new PagedEnumerableEnumerator<T>(source, c);
            }

            /// <inheritdoc/>
            IAsyncEnumerator<T> IAsyncEnumerable<T>.GetAsyncEnumerator(CancellationToken cancellationToken)
                => GetEnumerator(cancellationToken);
        }

        private sealed class PagedEnumerableEnumerator<T> : AbstractEnumerator<T, IPagedEnumerator<T>>
        {
            public PagedEnumerableEnumerator(IPagedEnumerable<T> source, CancellationToken cancellationToken)
                : base(source.GetPagedEnumerator(), e => e.Current, cancellationToken)
            { }

            /// <inheritdoc/>
            public override async ValueTask<bool> MoveNextAsync()
            {
                CancellationToken.ThrowIfCancellationRequested();
                return await Enumerator.MoveNextAsync(CancellationToken);
            }
        }

        private abstract class AbstractEnumerator<TItem, TEnumerator> : IAsyncEnumerator<TItem> where TEnumerator : IDisposable
        {
            protected readonly CancellationToken CancellationToken;
            protected TEnumerator Enumerator;

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
