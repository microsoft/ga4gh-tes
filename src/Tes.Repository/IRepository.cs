// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Tes.Repository
{
    /// <summary>
    /// A general repository interface for persistence of T instances
    /// </summary>
    /// <typeparam name="T">The type of the instance</typeparam>
    public interface IRepository<T> : IDisposable where T : RepositoryItem<T>
    {
        /// <summary>
        /// Create a new item
        /// </summary>
        /// <param name="item">The item to create</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>The created item</returns>
        Task<T> CreateItemAsync(T item, CancellationToken cancellationToken);

        /// <summary>
        /// Delete an existing item
        /// </summary>
        /// <param name="id">The ID of the item to delete</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        Task DeleteItemAsync(string id, CancellationToken cancellationToken);

        /// <summary>
        /// Get an item by ID
        /// </summary>
        /// <param name="id">The ID of the item to retrieve</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="onSuccess">The action to run when the item with the ID is found</param>
        /// <returns>The item instance</returns>
        Task<bool> TryGetItemAsync(string id, CancellationToken cancellationToken, Action<T> onSuccess = null);

        /// <summary>
        /// Reads a collection of items from the repository. Intended for task servicing
        /// </summary>
        /// <param name="predicate">The 'where' clause</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>The collection of retrieved items</returns>
        Task<IEnumerable<T>> GetItemsAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken);

        /// <summary>
        /// Reads a collection of items from the repository. Intended for the TES API
        /// </summary>
        /// <param name="continuationToken">A token to continue retrieving tasks if the max is returned.</param>
        /// <param name="pageSize">The max number of tasks to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="rawPredicate">Raw 'where' clause for cases where EF does not have translations.</param>
        /// <param name="efPredicates">The 'where' clause parts. It is appended if both <paramref name="rawPredicate"/> and this are provided.</param>
        /// <returns>A continuation token string and the retrieved items</returns>
        Task<GetItemsResult> GetItemsAsync(string continuationToken, int pageSize, CancellationToken cancellationToken, FormattableString rawPredicate = default, IEnumerable<Expression<Func<T, bool>>> efPredicates = default);

        /// <summary>
        /// Update the item in the repository
        /// </summary>
        /// <param name="id">The ID of the item to update</param>
        /// <param name="item">The item to persist</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>The created document</returns>
        Task<T> UpdateItemAsync(T item, CancellationToken cancellationToken);

        /// <summary>
        /// Removes an item from the cache if it exists.  This method exists for cache optimizations
        /// </summary>
        /// <param name="Item">The item to remove</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        ValueTask<bool> TryRemoveItemFromCacheAsync(T item, CancellationToken cancellationToken);

        /// <summary>
        /// Prepends the name of the <typeparamref name="T"/> property being accessed via a raw sql string. Intended to be used to complete a <c>WHERE</c> clause in a POSTGRESQL JSON query.
        /// </summary>
        /// <returns>A string containing <c>"json"->'<paramref name="property"/>'</c> prepended to <paramref name="sql"/>.</returns>
        FormattableString JsonFormattableRawString(string property, FormattableString sql);

        /// <summary>
        /// The results of a continuable query.
        /// </summary>
        /// <param name="ContinuationToken">Continuation token</param>
        /// <param name="Items">Query results.</param>
        record struct GetItemsResult(string ContinuationToken, IEnumerable<T> Items);
    }
}
