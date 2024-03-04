// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using CommonUtilities.Options;
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
            => _asyncRetryPolicy.ExecuteAsync(ct => _repository.CreateItemAsync(item, ct), cancellationToken);

        /// <inheritdoc/>
        public Task DeleteItemAsync(string id, CancellationToken cancellationToken)
            => _asyncRetryPolicy.ExecuteAsync(ct => _repository.DeleteItemAsync(id, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<IEnumerable<T>> GetItemsAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken)
            => _asyncRetryPolicy.ExecuteAsync(ct => _repository.GetItemsAsync(predicate, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<(string, IEnumerable<T>)> GetItemsAsync(string continuationToken, int pageSize, CancellationToken cancellationToken, FormattableString rawPredicate, Expression<Func<T, bool>> predicate)
            => _asyncRetryPolicy.ExecuteAsync(ct => _repository.GetItemsAsync(continuationToken, pageSize, ct, rawPredicate, predicate), cancellationToken);

        /// <inheritdoc/>
        public Task<bool> TryGetItemAsync(string id, CancellationToken cancellationToken, Action<T> onSuccess)
            => _asyncRetryPolicy.ExecuteAsync(ct => _repository.TryGetItemAsync(id, ct, onSuccess), cancellationToken);

        /// <inheritdoc/>
        public Task<T> UpdateItemAsync(T item, CancellationToken cancellationToken)
            => _asyncRetryPolicy.ExecuteAsync(ct => _repository.UpdateItemAsync(item, ct), cancellationToken);

        /// <inheritdoc/>
        public ValueTask<bool> TryRemoveItemFromCacheAsync(T item, CancellationToken cancellationToken)
            => _repository.TryRemoveItemFromCacheAsync(item, cancellationToken);

        /// <inheritdoc/>
        public FormattableString JsonFormattableRawString(string property, FormattableString sql)
            => _repository.JsonFormattableRawString(property, sql);
    }
}
