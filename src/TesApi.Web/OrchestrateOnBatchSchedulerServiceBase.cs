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
    /// A background service template that schedules TES tasks in the batch system, orchestrates their lifecycle, and updates their state.
    /// This should only be used to build system-wide singleton services.  This class does not support scale-out on multiple machines,
    /// nor does it implement a leasing mechanism.  In the future, consider using the Lease Blob operation.
    /// </summary>
    internal abstract class OrchestrateOnBatchSchedulerServiceBase : BackgroundService
    {
        private readonly IHostApplicationLifetime hostApplicationLifetime;
        protected readonly IRepository<TesTask> repository;
        protected readonly IBatchScheduler batchScheduler;
        protected readonly ILogger logger;

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="hostApplicationLifetime">Used for requesting termination of the current application. Pass null to allow this service to stop during initialization without taking down the application.</param>
        /// <param name="repository">The main TES task database repository implementation</param>
        /// <param name="batchScheduler">The batch scheduler implementation</param>
        /// <param name="logger">The logger instance</param>
        protected OrchestrateOnBatchSchedulerServiceBase(IHostApplicationLifetime hostApplicationLifetime, IRepository<TesTask> repository, IBatchScheduler batchScheduler, ILogger logger)
        {
            this.hostApplicationLifetime = hostApplicationLifetime;
            this.repository = repository;
            this.batchScheduler = batchScheduler;
            this.logger = logger;
        }

        /// <summary>
        /// Prepends the log message with the ultimately derived class's name.
        /// </summary>
        /// <param name="message"></param>
        /// <returns><paramref name="message"/> prepended with the class name.</returns>
        protected string MarkLogMessage(string message)
        {
            return GetType().Name + " " + message;
        }

        /// <inheritdoc />
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2254:Template should be a static expression", Justification = "Used to provide service's name in log message.")]
        public override Task StopAsync(CancellationToken cancellationToken)
        {
            logger.LogInformation(MarkLogMessage("stopping..."));
            return base.StopAsync(cancellationToken);
        }

        /// <inheritdoc />
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2254:Template should be a static expression", Justification = "Used to provide service's name in log message.")]
        protected sealed override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                // The order of these two calls is critical.
                ExecuteSetup(stoppingToken);
                await ExecuteSetupAsync(stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException oce || oce.CancellationToken == CancellationToken.None)
            {
                logger.LogCritical(ex, "Service {ServiceName} was unable to initialize due to '{Message}'.", GetType().Name, ex.Message);
                hostApplicationLifetime?.StopApplication();
            }

            logger.LogInformation(MarkLogMessage("started."));

            await ExecuteCoreAsync(stoppingToken);

            logger.LogInformation(MarkLogMessage("gracefully stopped."));
        }

        /// <summary>
        /// This method is called when the <see cref="IHostedService"/> starts. The implementation should return a task that represents
        /// the lifetime of the long running operation(s) being performed.
        /// </summary>
        /// <param name="stoppingToken">Triggered when <see cref="IHostedService.StopAsync(CancellationToken)"/> is called.</param>
        /// <returns>A <see cref="Task"/> that represents the long running operations.</returns>
        /// <remarks>See <see href="https://docs.microsoft.com/dotnet/core/extensions/workers">Worker Services in .NET</see> for implementation guidelines.</remarks>
        protected abstract Task ExecuteCoreAsync(CancellationToken stoppingToken);

        /// <summary>
        /// This method is called right before <see cref="ExecuteCoreAsync(CancellationToken)"/>. It can be used to prepare the service or the system before the service's operations begin.
        /// </summary>
        /// <param name="stoppingToken">Triggered when <see cref="IHostedService.StopAsync(CancellationToken)"/> is called.</param>
        /// <returns>A <see cref="Task"/> that represents this method's operations.</returns>
        protected virtual Task ExecuteSetupAsync(CancellationToken stoppingToken) => Task.CompletedTask;

        /// <summary>
        /// This method is called right before <see cref="ExecuteCoreAsync(CancellationToken)"/>. It can be used to prepare the service or the system before the service's operations begin.
        /// </summary>
        /// <param name="stoppingToken">Triggered when <see cref="IHostedService.StopAsync(CancellationToken)"/> is called.</param>
        /// <remarks>This method's lifetime will delay the exit of <see cref="IHostedService.StartAsync(CancellationToken)"/> in the base class, thus delaying the start of subsequent services in the system.</remarks>
        protected virtual void ExecuteSetup(CancellationToken stoppingToken) { }

        /// <summary>
        /// Runs <paramref name="action"/> repeatedly at an interval of <paramref name="runInterval"/>.
        /// </summary>
        /// <param name="runInterval">Interval to rerun <paramref name="action"/>.</param>
        /// <param name="action">Action to repeatedly run.</param>
        /// <param name="stoppingToken">Triggered when <see cref="IHostedService.StopAsync(CancellationToken)"/> is called.</param>
        /// <returns>A System.Threading.Tasks.Task that represents the long running operations.</returns>
        protected async Task ExecuteActionOnIntervalAsync(TimeSpan runInterval, Func<CancellationToken, ValueTask> action, CancellationToken stoppingToken)
        {
            ArgumentNullException.ThrowIfNull(action);

            using PeriodicTimer timer = new(runInterval);

            try
            {
                do
                {
                    try
                    {
                        await action(stoppingToken);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception exc)
                    {
                        logger.LogError(exc, "{Message}", exc.Message);
                    }
                }
                while (await timer.WaitForNextTickAsync(stoppingToken));
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            { }
        }

        /// <summary>
        /// Retrieves provided actionable TES tasks from the database using <paramref name="tesTaskGetter"/>, performs an action in the batch system using <paramref name="tesTaskProcessor"/>, and updates the resultant state
        /// </summary>
        /// <returns>A System.Threading.Tasks.ValueTask that represents the long running operations.</returns>
        protected async ValueTask OrchestrateTesTasksOnBatchAsync(string pollName, Func<CancellationToken, ValueTask<IAsyncEnumerable<TesTask>>> tesTaskGetter, Func<TesTask[], CancellationToken, IAsyncEnumerable<RelatedTask<TesTask, bool>>> tesTaskProcessor, CancellationToken stoppingToken, string unitsLabel = "tasks")
        {
            var tesTasks = await (await tesTaskGetter(stoppingToken)).ToArrayAsync(stoppingToken);

            if (tesTasks.All(task => task is null))
            {
                // Quick return for no tasks
                return;
            }

            var startTime = DateTime.UtcNow;

            await foreach (var tesTaskTask in tesTaskProcessor(tesTasks, stoppingToken).WithCancellation(stoppingToken))
            {
                var tesTask = tesTaskTask.Related;

                try
                {
                    var isModified = false;

                    try
                    {
                        isModified = await tesTaskTask;
                    }
                    catch (Exception exc)
                    {
                        if (++tesTask.ErrorCount > 3) // TODO: Should we increment this for exceptions here (current behaviour) or the attempted executions on the batch?
                        {
                            tesTask.State = TesState.SYSTEMERROREnum;
                            tesTask.EndTime = DateTimeOffset.UtcNow;
                            tesTask.SetFailureReason("UnknownError", exc.Message, exc.StackTrace);
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

                                foreach (var detail in requestInfo.BatchError.Values?.Select(d => $"BatchErrorDetail: '{d.Key}': '{d.Value}'") ?? Enumerable.Empty<string>())
                                {
                                    logs.Add(detail);
                                }
                            }

                            tesTask.AddToSystemLog(logs);
                        }

                        logger.LogError(exc, "TES task: {TesTask} threw an exception in OrchestrateTesTasksOnBatch({Poll}).", tesTask.Id, pollName);
                        await repository.UpdateItemAsync(tesTask, stoppingToken);
                    }

                    if (isModified)
                    {
                        var hasErrored = false;
                        var hasEnded = false;

                        switch (tesTask.State)
                        {
                            case TesState.CANCELEDEnum:
                            case TesState.COMPLETEEnum:
                                hasEnded = true;
                                break;

                            case TesState.EXECUTORERROREnum:
                            case TesState.SYSTEMERROREnum:
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
                            logger.LogDebug("{TesTask} failed, state: {TesTaskState}, reason: {TesTaskFailureReason}", tesTask.Id, tesTask.State, tesTask.FailureReason);
                        }

                        await repository.UpdateItemAsync(tesTask, stoppingToken);
                    }
                }
                catch (RepositoryCollisionException exc)
                {
                    logger.LogError(exc, "RepositoryCollisionException in OrchestrateTesTasksOnBatch({Poll})", pollName);
                    //TODO: retrieve fresh task if possible and add logs to the task in a similar way to the commanted out code block below.
                    //Also: consider doing the same in the other place(s) this exception is caught.
                }
                //catch (Microsoft.Azure.Cosmos.CosmosException exc)
                //{
                //    TesTask currentTesTask = default;
                //    _ = await repository.TryGetItemAsync(tesTask.Id, t => currentTesTask = t);

                //    if (exc.StatusCode == System.Net.HttpStatusCode.PreconditionFailed)
                //    {
                //        logger.LogError(exc, $"Updating TES Task '{tesTask.Id}' threw an exception attempting to set state: {tesTask.State}. Another actor set state: {currentTesTask?.State}");
                //        currentTesTask?.SetWarning("ConcurrencyWriteFailure", tesTask.State.ToString(), exc.Message, exc.StackTrace);
                //    }
                //    else
                //    {
                //        logger.LogError(exc, $"Updating TES Task '{tesTask.Id}' threw {exc.GetType().FullName}: '{exc.Message}'. Stack trace: {exc.StackTrace}");
                //        currentTesTask?.SetWarning("UnknownError", exc.Message, exc.StackTrace);
                //    }

                //    if (currentTesTask is not null)
                //    {
                //        await repository.UpdateItemAsync(currentTesTask);
                //    }
                //}
                catch (Exception exc)
                {
                    logger.LogError(exc, "Updating TES Task '{TesTask}' threw {ExceptionType}: '{ExceptionMessage}'. Stack trace: {ExceptionStackTrace}", tesTask.Id, exc.GetType().FullName, exc.Message, exc.StackTrace);
                }
            }

            if (batchScheduler.NeedPoolFlush)
            {
                var pools = (await repository.GetItemsAsync(task => task.State == TesState.INITIALIZINGEnum || task.State == TesState.RUNNINGEnum, stoppingToken)).Select(task => task.PoolId).Distinct();
                await batchScheduler.FlushPoolsAsync(pools, stoppingToken);
            }

            logger.LogDebug("OrchestrateTesTasksOnBatch({Poll}) for {TaskCount} {UnitsLabel} completed in {TotalSeconds} seconds.", pollName, tesTasks.Where(task => task is not null).Count(), unitsLabel, DateTime.UtcNow.Subtract(startTime).TotalSeconds);
        }
    }
}
