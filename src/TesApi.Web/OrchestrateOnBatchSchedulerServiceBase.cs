// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Tes.Extensions;
using Tes.Models;
using Tes.Repository;

namespace TesApi.Web
{
    /// <summary>
    /// Common functionality for <see cref="IHostedService"/>s that perform operations in the batch system, including common functionality for services using <see cref="BackgroundService"/>.
    /// </summary>
    /// <param name="hostApplicationLifetime">Used for requesting termination of the current application. Pass null to allow this service to stop during initialization without taking down the application.</param>
    /// <param name="repository">The main TES task database repository implementation.</param>
    /// <param name="batchScheduler">The batch scheduler implementation.</param>
    /// <param name="logger">The logger instance.</param>
    internal abstract class OrchestrateOnBatchSchedulerServiceBase(IHostApplicationLifetime hostApplicationLifetime, IRepository<TesTask> repository, IBatchScheduler batchScheduler, ILogger logger) : BackgroundService
    {
        protected readonly IRepository<TesTask> Repository = repository;
        protected readonly IBatchScheduler BatchScheduler = batchScheduler;
        protected readonly ILogger Logger = logger;

        /// <summary>
        /// Prepends the log message with the ultimately derived class's name.
        /// </summary>
        /// <param name="message">Log message tail.</param>
        /// <returns><paramref name="message"/> prepended with the class name.</returns>
        protected string MarkLogMessage(string message)
        {
            return GetType().Name + " " + message;
        }

        /// <inheritdoc />
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2254:Template should be a static expression", Justification = "Used to provide service's name in log message.")]
        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            Logger.LogInformation(MarkLogMessage("stopping..."));
            await base.StopAsync(cancellationToken);
        }

        /// <inheritdoc />
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2254:Template should be a static expression", Justification = "Used to provide service's name in log message.")]
        protected sealed override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Logger.LogInformation(MarkLogMessage("starting..."));

            try
            {
                // The order of these two calls is critical.
                ExecuteSetup(stoppingToken);
                await ExecuteSetupAsync(stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException oce || oce.CancellationToken == CancellationToken.None)
            {
                Logger.LogCritical(ex, "Service {ServiceName} was unable to initialize due to '{Message}'.", GetType().Name, ex.Message);
                hostApplicationLifetime?.StopApplication();
            }

            Logger.LogInformation(MarkLogMessage("started."));

            await ExecuteCoreAsync(stoppingToken);

            Logger.LogInformation(MarkLogMessage("gracefully stopped."));
        }

        /// <summary>
        /// This method is called when the <see cref="IHostedService"/> starts. The implementation should return a task that represents
        /// the lifetime of the long running operation(s) being performed.
        /// </summary>
        /// <param name="cancellationToken">Triggered when <see cref="IHostedService.StopAsync(CancellationToken)"/> is called.</param>
        /// <returns>A <see cref="ValueTask"/> that represents the long running operations.</returns>
        /// <remarks>See <see href="https://docs.microsoft.com/dotnet/core/extensions/workers">Worker Services in .NET</see> for implementation guidelines.</remarks>
        protected abstract ValueTask ExecuteCoreAsync(CancellationToken cancellationToken);

        /// <summary>
        /// This method is called right before <see cref="ExecuteCoreAsync(CancellationToken)"/>. It can be used to prepare the service or the system before the service's operations begin.
        /// </summary>
        /// <param name="cancellationToken">Triggered when <see cref="IHostedService.StopAsync(CancellationToken)"/> is called.</param>
        /// <returns>A <see cref="Task"/> that represents this method's operations.</returns>
        /// <remarks>This method runs right after <see cref="ExecuteSetup(CancellationToken)"/>.</remarks>
        protected virtual ValueTask ExecuteSetupAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        /// <summary>
        /// This method is called right before <see cref="ExecuteCoreAsync(CancellationToken)"/>. It can be used to prepare the service or the system before the service's operations begin.
        /// </summary>
        /// <param name="cancellationToken">Triggered when <see cref="IHostedService.StopAsync(CancellationToken)"/> is called.</param>
        /// <remarks>This method's lifetime will delay the exit of <see cref="IHostedService.StartAsync(CancellationToken)"/> in the base class, thus delaying the start of subsequent services in the system.</remarks>
        protected virtual void ExecuteSetup(CancellationToken cancellationToken) { }

        /// <summary>
        /// Runs <paramref name="action"/> repeatedly at an interval of <paramref name="runInterval"/>.
        /// </summary>
        /// <param name="runInterval">Interval to rerun <paramref name="action"/>.</param>
        /// <param name="action">Action to repeatedly run.</param>
        /// <param name="cancellationToken">Triggered when <see cref="IHostedService.StopAsync(CancellationToken)"/> is called.</param>
        /// <returns>A <see cref="ValueTask"/> that represents this method's operations.</returns>
        protected async Task ExecuteActionOnIntervalAsync(TimeSpan runInterval, Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(action);

            using PeriodicTimer timer = new(runInterval);

            try
            {
                do
                {
                    try
                    {
                        await action(cancellationToken);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception exc)
                    {
                        Logger.LogError(exc, "{Exception}: {Message}", exc.GetType().FullName, exc.Message);
                    }
                }
                while (await timer.WaitForNextTickAsync(cancellationToken));
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            { }
        }

        /// <summary>
        /// Updates the repository with the changes to the TesTask, with exception-based failure reporting.
        /// </summary>
        /// <param name="pollName">Tag to disambiguate the state and/or action workflow performed in log messages.</param>
        /// <param name="task"><see cref="TesTask"/>.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        protected async ValueTask ProcessOrchestratedTesTaskAsync(string pollName, RelatedTask<TesTask, bool> task, CancellationToken cancellationToken)
        {
            var tesTask = task.Related;

            try
            {
                var isModified = false;

                try
                {
                    isModified = await task;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exc)
                {
                    if (++tesTask.ErrorCount > 3 || // TODO: Should we increment this for exceptions here (current behavior) or the attempted executions on the batch?
                        IsExceptionHttpConflictWhereTaskIsComplete(exc))
                    {
                        tesTask.State = TesState.SYSTEM_ERROR;
                        tesTask.EndTime = DateTimeOffset.UtcNow;
                        tesTask.SetFailureReason(AzureBatchTaskState.UnknownError, exc.Message, exc.StackTrace);
                    }

                    if (exc is Microsoft.Azure.Batch.Common.BatchException batchException)
                    {
                        var requestInfo = batchException.RequestInformation;
                        var reason = (batchException.InnerException as Microsoft.Azure.Batch.Protocol.Models.BatchErrorException)?.Response?.ReasonPhrase;
                        var logs = new List<string>();

                        if (requestInfo?.ServiceRequestId is not null)
                        {
                            logs.Add($"Azure batch ServiceRequestId: {requestInfo.ServiceRequestId}");
                        }

                        if (requestInfo?.BatchError is not null)
                        {
                            logs.Add($"BatchErrorCode: {requestInfo.BatchError.Code}");
                            logs.Add($"BatchErrorMessage ({requestInfo.BatchError.Message.Language}): {requestInfo.BatchError.Message.Value}");

                            foreach (var detail in requestInfo.BatchError.Values?.Select(d => $"BatchErrorDetail: '{d.Key}': '{d.Value}'") ?? [])
                            {
                                logs.Add(detail);
                            }
                        }

                        tesTask.AddToSystemLog(logs);
                    }

                    if (exc is Azure.RequestFailedException requestFailedException)
                    {
                        var logs = new List<string>();

                        if (!string.IsNullOrWhiteSpace(requestFailedException.ErrorCode))
                        {
                            logs.Add(requestFailedException.ErrorCode);
                        }

                        if (!string.IsNullOrWhiteSpace(requestFailedException.Message))
                        {
                            logs.Add(requestFailedException.Message);
                        }

                        if (requestFailedException.Data is not null)
                        {
                            foreach (var detail in requestFailedException.Data.Keys.Cast<string>().Zip(requestFailedException.Data.Values.Cast<string>()).Select(p => $"RequestFailureDetail '{p.First}': '{p.Second}'"))
                            {
                                logs.Add(detail);
                            }
                        }
                    }

                    Logger.LogError(exc, "TES task: {TesTask} threw an exception in OrchestrateTesTasksOnBatch({Poll}).", tesTask.Id, pollName);
                    await Repository.UpdateItemAsync(tesTask, cancellationToken);
                }

                if (isModified)
                {
                    var hasErrored = false;
                    var hasEnded = false;

                    switch (tesTask.State)
                    {
                        case TesState.CANCELED:
                        case TesState.COMPLETE:
                            hasEnded = true;
                            break;

                        case TesState.EXECUTOR_ERROR:
                        case TesState.SYSTEM_ERROR:
                            hasErrored = true;
                            hasEnded = true;
                            break;

                        default:
                            break;
                    }

                    if (hasEnded)
                    {
                        tesTask.EndTime = DateTimeOffset.UtcNow;
                    }

                    if (hasErrored)
                    {
                        Logger.LogDebug("{TesTask} failed, state: {TesTaskState}, reason: {TesTaskFailureReason}", tesTask.Id, tesTask.State, tesTask.FailureReason);
                    }

                    await Repository.UpdateItemAsync(tesTask, cancellationToken);
                }
            }
            catch (RepositoryCollisionException<TesTask> rce)
            {
                Logger.LogError(rce, "RepositoryCollisionException in OrchestrateTesTasksOnBatch({Poll})", pollName);

                try
                {
                    var currentTesTask = await rce.Task;

                    if (currentTesTask is not null && currentTesTask.IsActiveState())
                    {
                        currentTesTask.SetWarning(rce.Message);

                        if (currentTesTask.IsActiveState())
                        {
                            // TODO: merge tesTask and currentTesTask
                        }

                        await Repository.UpdateItemAsync(currentTesTask, cancellationToken);
                    }
                }
                catch (Exception exc)
                {
                    // Consider retrying repository.UpdateItemAsync() if this exception was thrown from 'await rce.Task'
                    Logger.LogError(exc, "Updating TES Task '{TesTask}' threw {ExceptionType}: '{ExceptionMessage}'. Stack trace: {ExceptionStackTrace}", tesTask.Id, exc.GetType().FullName, exc.Message, exc.StackTrace);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exc)
            {
                Logger.LogError(exc, "Updating TES Task '{TesTask}' threw {ExceptionType}: '{ExceptionMessage}'. Stack trace: {ExceptionStackTrace}", tesTask.Id, exc.GetType().FullName, exc.Message, exc.StackTrace);
            }

            static bool IsExceptionHttpConflictWhereTaskIsComplete(Exception exc)
            {
                if (exc is Microsoft.Azure.Batch.Common.BatchException batchException)
                {
                    return System.Net.HttpStatusCode.Conflict.Equals(batchException.RequestInformation?.HttpStatusCode) &&
                        Microsoft.Azure.Batch.Common.BatchErrorCodeStrings.TaskCompleted.Equals(batchException.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase);
                }

                return false;
            }
        }

        /// <summary>
        /// Retrieves provided actionable TES tasks from the database using <paramref name="tesTaskGetter"/>, performs an action in the batch system using <paramref name="tesTaskProcessor"/>, and updates the resultant state in the repository.
        /// </summary>
        /// <param name="pollName">Tag to disambiguate the state and/or action workflow performed in log messages.</param>
        /// <param name="tesTaskGetter">Provides array of <see cref="TesTask"/>s on which to perform actions through <paramref name="tesTaskProcessor"/>.</param>
        /// <param name="tesTaskProcessor">Method operating on <paramref name="tesTaskGetter"/> returning <see cref="RelatedTask{TRelated, TResult}"/> indicating if each <see cref="TesTask"/> needs updating into the repository.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="unitsLabel">Tag to indicate the underlying unit quantity of items processed in log messages.</param>
        /// <param name="needPoolFlush">True to process <see cref="IBatchScheduler.NeedPoolFlush"/> even if there are no tasks processed.</param>
        /// <returns>A <see cref="ValueTask"/> that represents this method's operations.</returns>
        protected async ValueTask OrchestrateTesTasksOnBatchAsync(string pollName, Func<CancellationToken, ValueTask<IAsyncEnumerable<TesTask>>> tesTaskGetter, Func<TesTask[], CancellationToken, IAsyncEnumerable<RelatedTask<TesTask, bool>>> tesTaskProcessor, CancellationToken cancellationToken, string unitsLabel = "tasks", bool needPoolFlush = false)
        {
            var tesTasks = await (await tesTaskGetter(cancellationToken)).ToArrayAsync(cancellationToken);
            var noTasks = tesTasks.All(task => task is null);

            if (noTasks && !needPoolFlush)
            {
                // Quick return for no tasks
                return;
            }

            var startTime = DateTime.UtcNow;

            if (!noTasks)
            {
                await Parallel.ForEachAsync(tesTaskProcessor(tesTasks, cancellationToken), cancellationToken, (task, token) => ProcessOrchestratedTesTaskAsync(pollName, task, token));
            }

            if (BatchScheduler.NeedPoolFlush)
            {
                var pools = (await Repository.GetItemsAsync(task => task.State == TesState.INITIALIZING || task.State == TesState.RUNNING, cancellationToken)).Select(task => task.PoolId).Distinct();
                await BatchScheduler.FlushPoolsAsync(pools, cancellationToken);
            }

            Logger.LogDebug("OrchestrateTesTasksOnBatch({Poll}) for {TaskCount} {UnitsLabel} completed in {TotalSeconds:c}.", pollName, tesTasks.Where(task => task is not null).Count(), unitsLabel, DateTime.UtcNow.Subtract(startTime));
        }
    }
}
