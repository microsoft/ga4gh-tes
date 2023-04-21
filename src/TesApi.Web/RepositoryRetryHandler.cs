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
        /// <param name="retryPolicyOptions">The <see cref="Management.Configuration.RetryPolicyOptions"/> to use. Note that we will quadruple the max retry count set in options.</param>
        public RepositoryRetryHandler(IRepository<T> repository, IOptions<Management.Configuration.RetryPolicyOptions> retryPolicyOptions)
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
        public Task<T> CreateItemAsync(T item)
            => _asyncRetryPolicy.ExecuteAsync(() => _repository.CreateItemAsync(item));

        /// <inheritdoc/>
        public Task DeleteItemAsync(string id)
            => _asyncRetryPolicy.ExecuteAsync(() => _repository.DeleteItemAsync(id));

        /// <inheritdoc/>
        public Task<IEnumerable<T>> GetItemsAsync(Expression<Func<T, bool>> predicate)
            => _asyncRetryPolicy.ExecuteAsync(() => _repository.GetItemsAsync(predicate));

        /// <inheritdoc/>
        public Task<(string, IEnumerable<T>)> GetItemsAsync(Expression<Func<T, bool>> predicate, int pageSize, string continuationToken)
            => _asyncRetryPolicy.ExecuteAsync(() => _repository.GetItemsAsync(predicate, pageSize, continuationToken));

        /// <inheritdoc/>
        public Task<bool> TryGetItemAsync(string id, Action<T> onSuccess)
            => _asyncRetryPolicy.ExecuteAsync(() => _repository.TryGetItemAsync(id, onSuccess));

        /// <inheritdoc/>
        public Task<T> UpdateItemAsync(T item)
            => _asyncRetryPolicy.ExecuteAsync(() => _repository.UpdateItemAsync(item));
    }
}
