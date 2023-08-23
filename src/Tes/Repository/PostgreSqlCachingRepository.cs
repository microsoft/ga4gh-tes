﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
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
        private readonly TimeSpan _writerWaitTime = TimeSpan.FromSeconds(1);
        private readonly int _batchSize = 1000;
        private static readonly TimeSpan defaultCompletedTaskCacheExpiration = TimeSpan.FromDays(1);

        protected readonly AsyncPolicy _asyncPolicy = Policy
            .Handle<Npgsql.NpgsqlException>(e => e.IsTransient)
            .WaitAndRetryAsync(10, i => TimeSpan.FromSeconds(Math.Pow(2, i)));

        private readonly ConcurrentQueue<(T, WriteAction, TaskCompletionSource<T>)> _itemsToWrite = new();
        private readonly ConcurrentDictionary<T, object> _updatingItems = new();
        private readonly BackgroundWorker _writerWorker = new();

        protected enum WriteAction { Add, Update, Delete }

        protected Func<TesDbContext> CreateDbContext { get; init; }
        protected readonly ICache<T> _cache;
        protected readonly ILogger _logger;

        private bool _disposedValue;

        protected PostgreSqlCachingRepository(ILogger logger = default, ICache<T> cache = default)
        {
            _logger = logger;
            _cache = cache;

            _writerWorker.WorkerSupportsCancellation = true;
            _writerWorker.RunWorkerCompleted += WriterWorkerCompleted;
            _writerWorker.DoWork += WriterWorkerProc;
            _writerWorker.RunWorkerAsync();
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

            return await _asyncPolicy.ExecuteAsync(ct => query.ToListAsync(ct), cancellationToken);
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

        private void WriterWorkerCompleted(object _1, RunWorkerCompletedEventArgs e)
        {
            if (e.Error is not null)
            {
                _logger?.LogCritical(e.Error, "Repository writer worker failed. Restarting worker.");
                _writerWorker.RunWorkerAsync();
            }
        }


        private void WriterWorkerProc(object _1, DoWorkEventArgs _2)
        {
            while (!_writerWorker.CancellationPending)
            {
                var list = new List<(T, WriteAction, TaskCompletionSource<T>)>();

                while (_itemsToWrite.TryDequeue(out var itemToWrite))
                {
                    list.Add(itemToWrite);
                }

                while (list.Count > 0)
                {
                    if (_writerWorker.CancellationPending)
                    {
                        break;
                    }

                    try
                    {
                        var work = list.Take(_batchSize).ToList();
                        list = list.Except(work).ToList();
                        WriteItemsAsync(work).Wait();
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Repository writer worker: WriteItemsAsync failed: {Message}.", ex.Message);
                    }
                }

                if (_writerWorker.CancellationPending)
                {
                    continue;
                }

                Thread.Sleep(_writerWaitTime);
            }
        }

        private async Task WriteItemsAsync(IList<(T DbItem, WriteAction Action, TaskCompletionSource<T> TaskSource)> dbItems)
        {
            if (dbItems.Count == 0) { return; }

            using var dbContext = CreateDbContext();

            // Manually set entity state to avoid potential NPG PostgreSql bug
            dbContext.ChangeTracker.AutoDetectChangesEnabled = false;

            try
            {
                dbContext.AddRange(dbItems.Where(e => WriteAction.Add.Equals(e.Action)).Select(e => e.DbItem));
                dbContext.RemoveRange(dbItems.Where(e => WriteAction.Delete.Equals(e.Action)).Select(e => e.DbItem));
                dbContext.UpdateRange(dbItems.Where(e => WriteAction.Update.Equals(e.Action)).Select(e => e.DbItem));
                await _asyncPolicy.ExecuteAsync(() => dbContext.SaveChangesAsync());
            }
            catch (Exception ex)
            {
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
                    _writerWorker.CancelAsync();
                    while (_writerWorker.IsBusy) { Thread.Sleep(10); } // Wait for background thread to exit
                    _writerWorker.Dispose();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                _disposedValue = true;
            }
        }

        // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
        // ~PostgreSqlCachingRepository()
        // {
        //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        //     Dispose(disposing: false);
        // }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
