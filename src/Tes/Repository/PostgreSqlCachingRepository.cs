// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Polly;

namespace Tes.Repository
{
    public abstract class PostgreSqlCachingRepository<T> : IDisposable where T : class
    {
        private const int BatchSize = 1000;
        private static readonly TimeSpan defaultCompletedTaskCacheExpiration = TimeSpan.FromDays(1);

        protected readonly AsyncPolicy asyncPolicy = Policy
            .Handle<Npgsql.NpgsqlException>(e => e.IsTransient)
            .WaitAndRetryAsync(10, i => TimeSpan.FromSeconds(Math.Pow(2, i)));

        private readonly Channel<(T, WriteAction, TaskCompletionSource<T>)> itemsToWrite = Channel.CreateUnbounded<(T, WriteAction, TaskCompletionSource<T>)>();
        private readonly ConcurrentDictionary<T, object> updatingItems = new(); // Collection of all pending items to be written.
        private readonly CancellationTokenSource writerWorkerCancellationTokenSource = new();
        private readonly Task writerWorkerTask;

        protected enum WriteAction { Add, Update, Delete }

        protected Func<TesDbContext> CreateDbContext { get; init; }
        protected readonly ICache<T> Cache;
        protected readonly ILogger Logger;

        private bool _disposedValue;

        protected PostgreSqlCachingRepository(Microsoft.Extensions.Hosting.IHostApplicationLifetime hostApplicationLifetime, ILogger logger = default, ICache<T> cache = default)
        {
            Logger = logger;
            Cache = cache;

            // The only "normal" exit for _writerWorkerTask is "cancelled". Anything else should force the process to exit because it means that this repository will no longer write to the database!
            writerWorkerTask = Task.Run(() => WriterWorkerAsync(writerWorkerCancellationTokenSource.Token))
                .ContinueWith(async task =>
                {
                    switch (task.Status)
                    {
                        case TaskStatus.Faulted:
                            Logger.LogCritical("Repository WriterWorkerAsync failed unexpectedly with: {ErrorMessage}.", task.Exception.Message);
                            break;
                        case TaskStatus.RanToCompletion:
                            Logger.LogCritical("Repository WriterWorkerAsync unexpectedly completed.");
                            break;
                    }

                    await Task.Delay(50); // Give the logger time to flush.
                    hostApplicationLifetime?.StopApplication();
                },
                TaskContinuationOptions.NotOnCanceled);
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
            if (Cache is not null)
            {
                if (Cache.TryGetValue(getKey(item), out _))
                {
                    _ = Cache.TryUpdate(getKey(item), item, isActive(item) ? default : defaultCompletedTaskCacheExpiration);
                }
                else if (isActive(item))
                {
                    _ = Cache.TryAdd(getKey(item), item);
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

            return await asyncPolicy.ExecuteAsync(query.ToListAsync, cancellationToken);
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
                if (updatingItems.TryAdd(item, null))
                {
                    result = source.Task.ContinueWith(RemoveUpdatingItem).Unwrap();
                }
                else
                {
                    throw new RepositoryCollisionException();
                }
            }

            if (!itemsToWrite.Writer.TryWrite((item, action, source)))
            {
                throw new InvalidOperationException("Failed to TryWrite to _itemsToWrite channel.");
            }

            return result;

            Task<T> RemoveUpdatingItem(Task<T> task)
            {
                _ = updatingItems.Remove(item, out _);
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
        private async Task WriterWorkerAsync(CancellationToken cancellationToken)
        {
            var list = new List<(T, WriteAction, TaskCompletionSource<T>)>();

            await foreach (var itemToWrite in itemsToWrite.Reader.ReadAllAsync(cancellationToken))
            {
                list.Add(itemToWrite);

                // Get remaining items up to _batchSize or until no more items are immediately available.
                while (list.Count < BatchSize && itemsToWrite.Reader.TryRead(out var additionalItem))
                {
                    list.Add(additionalItem);
                }

                await WriteItemsAsync(list, cancellationToken);
                list.Clear();
            }

            // If cancellation is requested, do not write any more items
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
                await asyncPolicy.ExecuteAsync(dbContext.SaveChangesAsync, cancellationToken);
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
                    writerWorkerCancellationTokenSource.Cancel();

                    try
                    {
                        writerWorkerTask.Wait();
                    }
                    catch (OperationCanceledException ex) when (writerWorkerCancellationTokenSource.Token == ex.CancellationToken)
                    { } // Expected return from Wait().
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
