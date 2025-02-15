// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using CommonUtilities;
using CommonUtilities.Options;
using Microsoft.Extensions.Options;
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
        private readonly RetryHandler.AsyncRetryHandlerPolicy _asyncRetryPolicy;

        /// <summary>
        /// Constructor for <see cref="RepositoryRetryHandler{T}"/>.
        /// </summary>
        /// <param name="repository">The <see cref="IRepository{T}"/> to wrap.</param>
        /// <param name="retryPolicyOptions">The <see cref="RetryPolicyOptions"/> to use. Note that we will quadruple the max retry count set in options.</param>
        /// <param name="logger">An instance used to perform logging.</param>
        public RepositoryRetryHandler(IRepository<T> repository, IOptions<RetryPolicyOptions> retryPolicyOptions, Microsoft.Extensions.Logging.ILogger<RepositoryRetryHandler<T>> logger)
        {
            ArgumentNullException.ThrowIfNull(repository);
            ArgumentNullException.ThrowIfNull(retryPolicyOptions);

            _asyncRetryPolicy = new RetryPolicyBuilder(retryPolicyOptions)
                .PolicyBuilder
                .OpinionatedRetryPolicy(Polly.Policy.Handle<Exception>(ex => ex is not RepositoryCollisionException<T>))
                .WithRetryPolicyOptionsWait()
                .SetOnRetryBehavior(logger)
                .AsyncBuild();
            _repository = repository;
        }

        /// <inheritdoc/>
        void IDisposable.Dispose() => _repository.Dispose();

        /// <inheritdoc/>
        public Task<T> CreateItemAsync(T item, CancellationToken cancellationToken)
            => _asyncRetryPolicy.ExecuteWithRetryAsync(ct => _repository.CreateItemAsync(item, ct), cancellationToken);

        /// <inheritdoc/>
        public Task DeleteItemAsync(string id, CancellationToken cancellationToken)
            => _asyncRetryPolicy.ExecuteWithRetryAsync(ct => _repository.DeleteItemAsync(id, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<IEnumerable<T>> GetItemsAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken)
            => _asyncRetryPolicy.ExecuteWithRetryAsync(ct => _repository.GetItemsAsync(predicate, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<IRepository<T>.GetItemsResult> GetItemsAsync(string continuationToken, int pageSize, CancellationToken cancellationToken, FormattableString rawPredicate, IEnumerable<Expression<Func<T, bool>>> predicates)
            => _asyncRetryPolicy.ExecuteWithRetryAsync(ct => _repository.GetItemsAsync(continuationToken, pageSize, ct, rawPredicate, predicates), cancellationToken);

        /// <inheritdoc/>
        public Task<bool> TryGetItemAsync(string id, CancellationToken cancellationToken, Action<T> onSuccess)
            => _asyncRetryPolicy.ExecuteWithRetryAsync(ct => _repository.TryGetItemAsync(id, ct, onSuccess), cancellationToken);

        /// <inheritdoc/>
        public Task<T> UpdateItemAsync(T item, CancellationToken cancellationToken)
            => _asyncRetryPolicy.ExecuteWithRetryAsync(ct => _repository.UpdateItemAsync(item, ct), cancellationToken);

        /// <inheritdoc/>
        public ValueTask<bool> TryRemoveItemFromCacheAsync(T item, CancellationToken cancellationToken)
            => _repository.TryRemoveItemFromCacheAsync(item, cancellationToken);

        /// <inheritdoc/>
        public FormattableString JsonFormattableRawString(string property, FormattableString sql)
            => _repository.JsonFormattableRawString(property, sql);
    }
}
