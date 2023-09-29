// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Polly;

namespace Tes.Repository
{
    public abstract class PostgreSqlCachingRepository<T> : IDisposable where T : class
    {
        private readonly TimeSpan _writerWaitTime = TimeSpan.FromMilliseconds(50);
        private readonly int _batchSize = 1000;
        private static readonly TimeSpan defaultCompletedTaskCacheExpiration = TimeSpan.FromDays(1);

        protected readonly AsyncPolicy _asyncPolicy = Policy
            .Handle<Npgsql.NpgsqlException>(e => e.IsTransient)
            .WaitAndRetryAsync(10, i => TimeSpan.FromSeconds(Math.Pow(2, i)));

        private readonly ConcurrentQueue<(T, WriteAction, TaskCompletionSource<T>)> _itemsToWrite = new(); // Current _writerWorkerTask work queue
        private readonly ConcurrentDictionary<T, object> _updatingItems = new(); // Collection of all pending items to be written.
        private readonly CancellationTokenSource _writerWorkerCancellationTokenSource = new();
        private readonly Task _writerWorkerTask;
        private readonly Guid tempGuid = new Guid();

        protected enum WriteAction { Add, Update, Delete }

        protected Func<TesDbContext> CreateDbContext { get; init; }
        protected readonly ICache<T> _cache;
        protected readonly ILogger _logger;

        private bool _disposedValue;

        protected PostgreSqlCachingRepository(ILogger logger = default, ICache<T> cache = default)
        {
            try
            {
                _logger = logger;
                _cache = cache;
                _logger.LogInformation($"PostgreSqlCachingRepository constructor start {tempGuid} {DateTime.UtcNow}");
                // The only "normal" exit for _writerWorkerTask is "cancelled". Anything else should force the process to exit because it means that this repository will no longer write to the database!
                _writerWorkerTask = Task.Run(() => WriterWorkerAsync(_writerWorkerCancellationTokenSource.Token))
                    .ContinueWith(async task =>
                    {
                        switch (task.Status)
                        {
                            case TaskStatus.Faulted:
                                _logger.LogCritical("Repository WriterWorkerAsync failed unexpectedly with: {ErrorMessage}.", task.Exception.Message);
                                break;
                            case TaskStatus.RanToCompletion:
                                _logger.LogCritical($"Repository WriterWorkerAsync unexpectedly completed.  {DateTime.UtcNow}");
                                break;
                        }

                        await Task.Delay(50); // Give the logger time to flush.
                        _logger.LogCritical($"Throwing UnreachableException in PostgreSqlCachingRepository  {DateTime.UtcNow}");
                        throw new System.Diagnostics.UnreachableException($"Repository WriterWorkerAsync unexpectedly ended.  {DateTime.UtcNow}"); // Force the process to exit via this being an unhandled exception.
                    },
                    TaskContinuationOptions.NotOnCanceled);

                _logger.LogInformation($"PostgreSqlCachingRepository constructor end {tempGuid}  {DateTime.UtcNow}");
            }
            catch (Exception exc)
            {
                _logger.LogCritical(exc, $"PostgreSqlCachingRepository threw an exception in the constructor  {DateTime.UtcNow}");
                throw;
            }
        }

        /// <summary>
        /// Adds item to cache if active and not already present or updates it if already present.
        /// </summary>
        /// <param name="item"><see cref="T"/> to add or update to the cache.</param>
        /// <param name="getKey">Function to provide cache key from <see cref="T"/>.</param>
        /// <param name="isActive">Predicate to determine if <see cref="T"/> is active.</param>
        /// <returns><paramref name="item"/> (for convenience in fluent/LINQ usage patterns).</returns>
        protected T EnsureActiveItemInCache(T item, Func<T, string> getKey, Predicate<T> isActive)
        {
            if (_cache is not null)
            {
                if (_cache.TryGetValue(getKey(item), out _))
                {
                    _ = _cache.TryUpdate(getKey(item), item, isActive(item) ? default : defaultCompletedTaskCacheExpiration);
                }
                else if (isActive(item))
                {
                    _ = _cache.TryAdd(getKey(item), item);
                }
            }

            return item;
        }

        /// <summary>
        /// Retrieves items from the database in a consistent fashion.
        /// </summary>
        /// <param name="dbSet">The <see cref="DbSet{TEntity}"/> of <typeparamref name="TDatabaseItem"/> to query.</param>
        /// <param name="predicate">The WHERE clause <see cref="Expression"/> for <typeparamref name="TDatabaseItem"/> selection in the query.</param>
        /// <param name="cancellationToken"></param>
        /// <param name="orderBy"></param>
        /// <param name="pagination"></param>
        /// <returns></returns>
        /// <remarks>Ensure that the <see cref="DbContext"/> from which <paramref name="dbSet"/> comes isn't disposed until the entire query completes.</remarks>
        protected async Task<IEnumerable<T>> GetItemsAsync(DbSet<T> dbSet, Expression<Func<T, bool>> predicate, CancellationToken cancellationToken, Func<IQueryable<T>, IQueryable<T>> orderBy = default, Func<IQueryable<T>, IQueryable<T>> pagination = default)
        {
            ArgumentNullException.ThrowIfNull(dbSet);
            ArgumentNullException.ThrowIfNull(predicate);
            orderBy ??= q => q;
            pagination ??= q => q;

            // Search for items in the JSON
            var query = pagination(orderBy(dbSet.Where(predicate)));
            //var sqlQuery = query.ToQueryString();
            //System.Diagnostics.Debugger.Break();

            return await _asyncPolicy.ExecuteAsync(query.ToListAsync, cancellationToken);
        }

        /// <summary>
        /// Adds entry into WriterWorker queue.
        /// </summary>
        /// <param name="item"></param>
        /// <param name="action"></param>
        /// <returns></returns>
        protected Task<T> AddUpdateOrRemoveItemInDbAsync(T item, WriteAction action, CancellationToken cancellationToken)
        {
            var source = new TaskCompletionSource<T>();
            var result = source.Task;

            if (action == WriteAction.Update)
            {
                if (_updatingItems.TryAdd(item, null))
                {
                    result = source.Task.ContinueWith(RemoveUpdatingItem).Unwrap();
                }
                else
                {
                    throw new RepositoryCollisionException();
                }
            }

            _itemsToWrite.Enqueue((item, action, source));
            return result;

            Task<T> RemoveUpdatingItem(Task<T> task)
            {
                _ = _updatingItems.Remove(item, out _);
                return task.Status switch
                {
                    TaskStatus.RanToCompletion => Task.FromResult(task.Result),
                    TaskStatus.Faulted => Task.FromException<T>(task.Exception),
                    _ => Task.FromCanceled<T>(cancellationToken)
                };
            }
        }

        /// <summary>
        /// Continuously writes items to the database
        /// </summary>
        private async ValueTask WriterWorkerAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation($"WriterWorkerAsync running... {DateTime.UtcNow}");
            var list = new List<(T, WriteAction, TaskCompletionSource<T>)>();

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (_itemsToWrite.TryDequeue(out var itemToWrite))
                {
                    list.Add(itemToWrite);

                    if (list.Count < _batchSize)
                    {
                        continue;
                    }
                }

                if (list.Count == 0)
                {
                    // Wait, because the queue is empty
                    await Task.Delay(_writerWaitTime, cancellationToken);
                    continue;
                }

                await WriteItemsAsync(list, cancellationToken);
                list.Clear();
            }
        }

        private async ValueTask WriteItemsAsync(IList<(T DbItem, WriteAction Action, TaskCompletionSource<T> TaskSource)> dbItems, CancellationToken cancellationToken)
        {
            if (dbItems.Count == 0) { return; }

            cancellationToken.ThrowIfCancellationRequested();
            using var dbContext = CreateDbContext();

            // Manually set entity state to avoid potential NPG PostgreSql bug
            dbContext.ChangeTracker.AutoDetectChangesEnabled = false;

            try
            {
                dbContext.RemoveRange(dbItems.Where(e => WriteAction.Delete.Equals(e.Action)).Select(e => e.DbItem));
                dbContext.UpdateRange(dbItems.Where(e => WriteAction.Update.Equals(e.Action)).Select(e => e.DbItem));
                dbContext.AddRange(dbItems.Where(e => WriteAction.Add.Equals(e.Action)).Select(e => e.DbItem));
                await _asyncPolicy.ExecuteAsync(dbContext.SaveChangesAsync, cancellationToken);
            }
            catch (Exception ex)
            {
                // It doesn't matter which item the failure was for, we will fail all items in this round.
                // TODO: are there exceptions Postgre will send us that will tell us which item(s) failed?
                FailAll(dbItems.Select(e => e.TaskSource), ex);
                return;
            }

            _ = Parallel.ForEach(dbItems, e => e.TaskSource.TrySetResult(e.DbItem));

            static void FailAll(IEnumerable<TaskCompletionSource<T>> sources, Exception ex)
                => _ = Parallel.ForEach(sources, s => s.TrySetException(new AggregateException(Enumerable.Empty<Exception>().Append(ex))));
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _logger?.LogInformation($"PostgreSqlCachingRepository disposing... _writerWorkerCancellationTokenSource will be cancelled.  tempGuid: {tempGuid}  {DateTime.UtcNow}");
                    _writerWorkerCancellationTokenSource.Cancel();
                    _writerWorkerTask.Wait();
                }

                _disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
