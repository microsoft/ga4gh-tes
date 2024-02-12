// Copyright (c) Microsoft Corporation.
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
            private readonly Func<CancellationToken, IAsyncEnumerator<T>> _getEnumerator;

            public AsyncEnumerable(IPagedEnumerable<T> source)
            {
                ArgumentNullException.ThrowIfNull(source);

                _getEnumerator = c => new PagedEnumerableEnumerator<T>(source, c);
            }

            /// <inheritdoc/>
            IAsyncEnumerator<T> IAsyncEnumerable<T>.GetAsyncEnumerator(CancellationToken cancellationToken)
                => _getEnumerator(cancellationToken);
        }

        private sealed class PagedEnumerableEnumerator<T> : CommonUtilities.PagedInterfaceExtensions.AbstractEnumerator<T, IPagedEnumerator<T>>
        {
            public PagedEnumerableEnumerator(IPagedEnumerable<T> source, CancellationToken cancellationToken)
                : base(source?.GetPagedEnumerator(), e => e.Current, cancellationToken)
            { }

            /// <inheritdoc/>
            public override async ValueTask<bool> MoveNextAsync()
            {
                CancellationToken.ThrowIfCancellationRequested();
                return await Enumerator.MoveNextAsync(CancellationToken);
            }
        }
        #endregion
    }
}
