// Copyright (c) Microsoft Corporation.
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

namespace Tes.Repository
{
    public abstract class PostgreSqlCachingRepository<TDatabaseItem> : IDisposable where TDatabaseItem : class
    {
        private readonly TimeSpan writerWaitTime = TimeSpan.FromSeconds(1);

        private readonly ConcurrentQueue<(TDatabaseItem, WriteAction, TaskCompletionSource<TDatabaseItem>)> itemsToUpdate = new();
        private readonly BackgroundWorker updaterWorker = new();

        protected Func<TesDbContext> createDbContext { get; init; }
        protected readonly ICache<TDatabaseItem> cache;
        protected readonly ILogger logger;

        protected PostgreSqlCachingRepository(ICache<TDatabaseItem> cache, ILogger logger)
        {
            this.cache = cache;
            this.logger = logger;

            updaterWorker.WorkerSupportsCancellation = true;
            updaterWorker.RunWorkerCompleted += UpdaterWorkerCompleted;
            updaterWorker.DoWork += UpdaterWorkerProc;
            updaterWorker.RunWorkerAsync();
        }

        protected enum WriteAction { Add, Update, Delete };

        protected TDatabaseItem EnsureActiveTaskInCache(TDatabaseItem item, Func<TDatabaseItem, string> getKey, Predicate<TDatabaseItem> isActive)
        {
            if (cache.TryGetValue(getKey(item), out _))
            {
                if (isActive(item))
                {
                    cache.TryUpdate(getKey(item), item);
                }
                else
                {
                    // Cache optimization because we can assume that most of the time, the workflow engine will no longer "GET" after a terminal state
                    cache.TryRemove(getKey(item));
                }
            }
            else if (isActive(item))
            {
                cache.TryAdd(getKey(item), item);
            }

            return item;
        }

        /// <summary>
        /// Retrieves items from the database in a consistent fashion.
        /// </summary>
        /// <param name="dbSet"></param>
        /// <param name="predicate"></param>
        /// <param name="orderBy"></param>
        /// <param name="pagination"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>Ensure that the <see cref="DbContext"/> from which <paramref name="dbSet"/> comes isn't disposed until the entire query completes.</remarks>
        protected async Task<IEnumerable<TDatabaseItem>> GetItemsAsync(DbSet<TDatabaseItem> dbSet, Expression<Func<TDatabaseItem, bool>> predicate, Func<IQueryable<TDatabaseItem>, IQueryable<TDatabaseItem>> orderBy, Func<IQueryable<TDatabaseItem>, IQueryable<TDatabaseItem>> pagination, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(dbSet);
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(orderBy);
            ArgumentNullException.ThrowIfNull(pagination);

            // Search for items in the JSON
            var query = pagination(orderBy(dbSet.Where(predicate)));
            //var sqlQuery = query.ToQueryString();
            //System.Diagnostics.Debugger.Break();

            return await query.ToListAsync(cancellationToken);
        }

        protected Task<TDatabaseItem> AddUpdateOrRemoveTaskInDbAsync(TDatabaseItem tesTask, WriteAction action)
        {
            var source = new TaskCompletionSource<TDatabaseItem>();
            itemsToUpdate.Enqueue((tesTask, action, source));
            return source.Task;
        }

        private void UpdaterWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            if (e.Error is not null)
            {
                logger?.LogCritical(e.Error, "Updater worker failed. Restarting worker.");
                updaterWorker.RunWorkerAsync(); // Restart worker
            }
        }

        private void UpdaterWorkerProc(object sender, DoWorkEventArgs e)
        {
            while (!updaterWorker.CancellationPending)
            {
                var list = new List<(TDatabaseItem, WriteAction, TaskCompletionSource<TDatabaseItem>)>();
                while (itemsToUpdate.TryDequeue(out var updateTesTask))
                {
                    list.Add(updateTesTask);
                }

                while (list.Count != 0)
                {
                    if (updaterWorker.CancellationPending)
                    {
                        break;
                    }

                    try
                    {
                        var work = list.Take(1000).ToList();
                        list = list.Except(work).ToList();
                        WriteTesTasksAsync(work).Wait();
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "Updater worker: UpdateTesTasksAsync failed.");
                    }
                }

                if (updaterWorker.CancellationPending)
                {
                    continue;
                }

                Thread.Sleep(writerWaitTime);
            }
        }

        private async Task WriteTesTasksAsync(List<(TDatabaseItem DbItem, WriteAction Action, TaskCompletionSource<TDatabaseItem> TaskSource)> updateDbItems)
        {
            if (updateDbItems.Count == 0) { return; }

            using var dbContext = createDbContext();

            // Manually set entity state to avoid potential NPG PostgreSql bug
            dbContext.ChangeTracker.AutoDetectChangesEnabled = false;

            try
            {
                dbContext.AddRange(updateDbItems.Where(e => WriteAction.Add.Equals(e.Action)).Select(e => e.DbItem));
                dbContext.RemoveRange(updateDbItems.Where(e => WriteAction.Delete.Equals(e.Action)).Select(e => e.DbItem));
                dbContext.UpdateRange(updateDbItems.Where(e => WriteAction.Update.Equals(e.Action)).Select(e => e.DbItem));
                await dbContext.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                FailAll(updateDbItems.Select(e => e.TaskSource), ex);
                return;
            }

            // Complete each awaiting task, returning its TDatabaseItem
            _ = Parallel.ForEach(updateDbItems,
                entry => entry.TaskSource.TrySetResult(entry.DbItem));

            static void FailAll(IEnumerable<TaskCompletionSource<TDatabaseItem>> sources, Exception ex)
                => _ = Parallel.ForEach(sources,
                    source => _ = source.TrySetException(new AggregateException(Enumerable.Empty<Exception>().Append(ex))));

            //static void FailEach<T>(IEnumerable<T> sources, Func<T, TaskCompletionSource<TDatabaseItem>> getTaskSource, Func<T, Exception> createEx)
            //    => _ = Parallel.ForEach(sources,
            //        source => _ = getTaskSource(source).TrySetException(new AggregateException(Enumerable.Empty<Exception>().Append(createEx(source)))));
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                updaterWorker.CancelAsync();
                while (updaterWorker.IsBusy) { }
                updaterWorker.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        //~PostgreSqlCachingRepository()
        //    => Dispose(false);
    }
}
