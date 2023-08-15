// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using Tes.ApiClients.Options;
using Tes.Repository;

namespace TesApi.Web
{
    /// <summary>
    /// Implements retries for <see cref="IRepository{T}"/>.
    /// </summary>
    /// <typeparam name="T">Type of the item</typeparam>
    public sealed class RepositoryRetryHandler<T> : IRepository<T> where T : RepositoryItem<T>
    {
        private readonly IRepository<T> _repository;
        private readonly AsyncRetryPolicy _asyncRetryPolicy;

        /// <summary>
        /// Constructor for <see cref="RepositoryRetryHandler{T}"/>.
        /// </summary>
        /// <param name="repository">The <see cref="IRepository{T}"/> to wrap.</param>
        /// <param name="retryPolicyOptions">The <see cref="RetryPolicyOptions"/> to use. Note that we will quadruple the max retry count set in options.</param>
        public RepositoryRetryHandler(IRepository<T> repository, IOptions<RetryPolicyOptions> retryPolicyOptions)
        {
            ArgumentNullException.ThrowIfNull(repository);
            ArgumentNullException.ThrowIfNull(retryPolicyOptions);

            _asyncRetryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(retryPolicyOptions.Value.MaxRetryCount * 4,
                    (attempt) => TimeSpan.FromSeconds(Math.Pow(retryPolicyOptions.Value.ExponentialBackOffExponent,
                        attempt)));
            _repository = repository;
        }

        /// <inheritdoc/>
        void IDisposable.Dispose() => _repository.Dispose();

        /// <inheritdoc/>
        public Task<T> CreateItemAsync(T item, CancellationToken cancellationToken)
            => _asyncRetryPolicy.ExecuteAsync(() => _repository.CreateItemAsync(item, cancellationToken));

        /// <inheritdoc/>
        public Task DeleteItemAsync(string id, CancellationToken cancellationToken)
            => _asyncRetryPolicy.ExecuteAsync(() => _repository.DeleteItemAsync(id, cancellationToken));

        /// <inheritdoc/>
        public Task<IEnumerable<T>> GetItemsAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken)
            => _asyncRetryPolicy.ExecuteAsync(() => _repository.GetItemsAsync(predicate, cancellationToken));

        /// <inheritdoc/>
        public Task<(string, IEnumerable<T>)> GetItemsAsync(Expression<Func<T, bool>> predicate, int pageSize, string continuationToken, CancellationToken cancellationToken)
            => _asyncRetryPolicy.ExecuteAsync(() => _repository.GetItemsAsync(predicate, pageSize, continuationToken, cancellationToken));

        /// <inheritdoc/>
        public Task<bool> TryGetItemAsync(string id, CancellationToken cancellationToken, Action<T> onSuccess)
            => _asyncRetryPolicy.ExecuteAsync(() => _repository.TryGetItemAsync(id, cancellationToken, onSuccess));

        /// <inheritdoc/>
        public Task<T> UpdateItemAsync(T item, CancellationToken cancellationToken)
            => _asyncRetryPolicy.ExecuteAsync(() => _repository.UpdateItemAsync(item, cancellationToken));

        /// <inheritdoc/>
        public ValueTask<bool> TryRemoveItemFromCacheAsync(T item, CancellationToken cancellationToken)
            => _repository.TryRemoveItemFromCacheAsync(item, cancellationToken);
    }
}
