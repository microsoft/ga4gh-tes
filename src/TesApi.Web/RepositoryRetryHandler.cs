// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Tes.Repository;
using TesApi.Web.Management;

namespace TesApi.Web
{
    /// <summary>
    /// Implements retries for <see cref="IRepository{T}"/>.
    /// </summary>
    /// <typeparam name="T">Type of the item</typeparam>
    public sealed class RepositoryRetryHandler<T> : IRepository<T> where T : RepositoryItem<T>
    {
        private readonly IRepository<T> _repository;
        private readonly CacheAndRetryHandler _cacheAndRetryHandler;

        /// <summary>
        /// Constructor for <see cref="RepositoryRetryHandler{T}"/>.
        /// </summary>
        /// <param name="repository">The <see cref="IRepository{T}"/> to wrap.</param>
        /// <param name="cacheAndRetryHandler">The <see cref="CacheAndRetryHandler"/> to use to implement retries.</param>
        public RepositoryRetryHandler(IRepository<T> repository, CacheAndRetryHandler cacheAndRetryHandler)
        {
            ArgumentNullException.ThrowIfNull(repository);
            ArgumentNullException.ThrowIfNull(cacheAndRetryHandler);

            _cacheAndRetryHandler = cacheAndRetryHandler;
            _repository = repository;
        }

        /// <inheritdoc/>
        void IDisposable.Dispose() => _repository.Dispose();

        /// <inheritdoc/>
        public Task<T> CreateItemAsync(T item, CancellationToken cancellationToken)
            => _cacheAndRetryHandler.AsyncRetryPolicy.ExecuteAsync(ct => _repository.CreateItemAsync(item, ct), cancellationToken);

        /// <inheritdoc/>
        public Task DeleteItemAsync(string id, CancellationToken cancellationToken)
            => _cacheAndRetryHandler.AsyncRetryPolicy.ExecuteAsync(ct => _repository.DeleteItemAsync(id, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<IEnumerable<T>> GetItemsAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken)
            => _cacheAndRetryHandler.AsyncRetryPolicy.ExecuteAsync(ct => _repository.GetItemsAsync(predicate, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<(string, IEnumerable<T>)> GetItemsAsync(Expression<Func<T, bool>> predicate, int pageSize, string continuationToken, CancellationToken cancellationToken)
            => _cacheAndRetryHandler.AsyncRetryPolicy.ExecuteAsync(ct => _repository.GetItemsAsync(predicate, pageSize, continuationToken, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<bool> TryGetItemAsync(string id, Action<T> onSuccess, CancellationToken cancellationToken)
            => _cacheAndRetryHandler.AsyncRetryPolicy.ExecuteAsync(ct => _repository.TryGetItemAsync(id, onSuccess, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<T> UpdateItemAsync(T item, CancellationToken cancellationToken)
            => _cacheAndRetryHandler.AsyncRetryPolicy.ExecuteAsync(ct => _repository.UpdateItemAsync(item, ct), cancellationToken);
    }
}
