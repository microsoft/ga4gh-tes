// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using CommonUtilities;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace TesApi.Web
{
    /// <summary>
    /// Represents a pool in an Azure Batch Account.
    /// </summary>
    public sealed partial class BatchPool
    {
        /// <summary>
        /// Minimum property set required for <see cref="CloudPool"/> provided to constructors of this class
        /// </summary>
        public const string CloudPoolSelectClause = "id,creationTime,metadata";

        /// <summary>
        /// Autoscale evalutation interval
        /// </summary>
        public static TimeSpan AutoScaleEvaluationInterval { get; } = TimeSpan.FromMinutes(5);

        private const int MaxComputeNodesToRemoveAtOnce = 100; // https://learn.microsoft.com/en-us/rest/api/batchservice/pool/remove-nodes?tabs=HTTP#request-body nodeList description

        private readonly ILogger _logger;
        private readonly IAzureProxy _azureProxy;

        /// <summary>
        /// Constructor of <see cref="BatchPool"/>.
        /// </summary>
        /// <param name="batchScheduler"></param>
        /// <param name="batchSchedulingOptions"></param>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        /// <exception cref="ArgumentException"></exception>
        public BatchPool(IBatchScheduler batchScheduler, IOptions<Options.BatchSchedulingOptions> batchSchedulingOptions, IAzureProxy azureProxy, ILogger<BatchPool> logger)
        {
            var rotationDays = batchSchedulingOptions.Value.PoolRotationForcedDays;
            if (rotationDays == 0) { rotationDays = Options.BatchSchedulingOptions.DefaultPoolRotationForcedDays; }
            _forcePoolRotationAge = TimeSpan.FromDays(rotationDays);

            this._azureProxy = azureProxy;
            this._logger = logger;
            _batchPools = batchScheduler as BatchScheduler ?? throw new ArgumentException("batchScheduler must be of type BatchScheduler", nameof(batchScheduler));
        }

        private Queue<TaskFailureInformation> StartTaskFailures { get; } = new();
        private Queue<ResizeError> ResizeErrors { get; } = new();

        internal IAsyncEnumerable<CloudTask> GetTasksAsync(bool includeCompleted)
            => _azureProxy.ListTasksAsync(Pool.PoolId, new ODATADetailLevel { SelectClause = "id,stateTransitionTime", FilterClause = includeCompleted ? default : "state ne 'completed'" });

        private async ValueTask RemoveNodesAsync(IList<ComputeNode> nodesToRemove, CancellationToken cancellationToken)
        {
            _logger.LogDebug("Removing {Nodes} nodes from {PoolId}", nodesToRemove.Count, Pool.PoolId);
            _resizeErrorsRetrieved = false;
            await _azureProxy.DeleteBatchComputeNodesAsync(Pool.PoolId, nodesToRemove, cancellationToken);
        }
    }

    /// <content>
    /// Implements the various ServicePool* methods.
    /// </content>
    public sealed partial class BatchPool
    {
        private enum ScalingMode
        {
            Unknown,
            AutoScaleEnabled,
            SettingManualScale,
            RemovingFailedNodes,
            WaitingForAutoScale,
            SettingAutoScale
        }

        private ScalingMode _scalingMode = ScalingMode.Unknown;
        private DateTime _autoScaleWaitTime;

        private readonly TimeSpan _forcePoolRotationAge;
        private readonly BatchScheduler _batchPools;
        private bool _resizeErrorsRetrieved;
        private bool _resetAutoScalingRequired;

        private DateTime? Creation { get; set; }
        private bool IsDedicated { get; set; }

        private void EnsureScalingModeSet(bool? autoScaleEnabled)
        {
            if (ScalingMode.Unknown == _scalingMode)
            {
                _scalingMode = autoScaleEnabled switch
                {
                    true => ScalingMode.AutoScaleEnabled,
                    false => ScalingMode.RemovingFailedNodes,
                    null => _scalingMode,
                };
            }
        }

        private async ValueTask ServicePoolGetResizeErrorsAsync(CancellationToken cancellationToken)
        {
            var currentAllocationState = await _azureProxy.GetFullAllocationStateAsync(Pool.PoolId, cancellationToken);
            EnsureScalingModeSet(currentAllocationState.AutoScaleEnabled);

            if (_scalingMode == ScalingMode.AutoScaleEnabled)
            {
                if (currentAllocationState.AllocationState == AllocationState.Steady)
                {
                    if (!_resizeErrorsRetrieved)
                    {
                        ResizeErrors.Clear();
                        var pool = await _azureProxy.GetBatchPoolAsync(Pool.PoolId, cancellationToken, new ODATADetailLevel { SelectClause = "resizeErrors" });

                        foreach (var error in pool.ResizeErrors ?? Enumerable.Empty<ResizeError>())
                        {
                            switch (error.Code)
                            {
                                // Errors to ignore
                                case PoolResizeErrorCodes.RemoveNodesFailed:
                                case PoolResizeErrorCodes.AccountCoreQuotaReached:
                                case PoolResizeErrorCodes.AccountLowPriorityCoreQuotaReached:
                                case PoolResizeErrorCodes.CommunicationEnabledPoolReachedMaxVMCount:
                                case PoolResizeErrorCodes.AccountSpotCoreQuotaReached:
                                case PoolResizeErrorCodes.AllocationTimedOut:
                                    break;

                                // Errors to force autoscale to be reset
                                case PoolResizeErrorCodes.ResizeStopped:
                                    _resetAutoScalingRequired |= true;
                                    break;

                                // Errors to both force resetting autoscale and fail tasks
                                case PoolResizeErrorCodes.AllocationFailed:
                                    _resetAutoScalingRequired |= true;
                                    goto default;

                                // Errors to fail tasks should be directed here
                                default:
                                    ResizeErrors.Enqueue(error);
                                    break;
                            }
                        }

                        _resizeErrorsRetrieved = true;
                    }
                }
                else
                {
                    _resetAutoScalingRequired = false;
                    _resizeErrorsRetrieved = false;
                }
            }
        }

        /// <summary>
        /// Generates a formula for Azure batch account auto-pool usage
        /// </summary>
        /// <param name="preemptable">false if compute nodes are dedicated, true otherwise.</param>
        /// <param name="initialTarget">Number of compute nodes to allocate the first time this formula is evaluated.</param>
        /// <remarks>Implements <see cref="IAzureProxy.BatchPoolAutoScaleFormulaFactory"/>.</remarks>
        /// <returns></returns>
        public static string AutoPoolFormula(bool preemptable, int initialTarget)
        /*
          Notes on the formula:
              Reference: https://docs.microsoft.com/en-us/azure/batch/batch-automatic-scaling

          In order to avoid confusion, some of the builtin variable names in batch's autoscale formulas named in a way that may not appear intuitive:
              Running tasks are named RunningTasks, which is fine
              Queued tasks are named ActiveTasks, which matches the same value of the "state" property
              The sum of running & queued tasks (what I would have named TotalTasks) is named PendingTasks

          The type of ~Tasks is what batch calls a "doubleVec", which needs to be first turned into a "doubleVecList" before it can be turned into a scaler.
          This is accomplished by calling doubleVec's GetSample method, which returns some number of the most recent available samples of the related metric.
          Then, a function is used to extract a scaler from the list of scalers (measurements). NOTE: there does not seem to be a "last" function.

          Whenever autoscaling is first turned on, including when the pool is first created, there are no sampled metrics available. Thus, we need to prevent the
          expected errors that would result from trying to extract the samples. Later on, if recent samples aren't available, we prefer that the formula fails
          (1- so we can potentially capture that, and 2- so that we don't suddenly try to remove all nodes from the pool when there's still demand) so we use a
          timed scheme to substitue an "initial value" (aka initialTarget).

          We set NodeDeallocationOption to taskcompletion to prevent wasting time/money by stopping a running task, only to requeue it onto another node, or worse,
          fail it, just because batch's last sample was taken longer ago than a task's assignment was made to a node, because the formula evaluations are not coordinated
          with the metric sampling based on my observations. This does mean that some resizes will time out, so we mustn't simply consider timeout to be a fatal error.

          internal variables in this formula:
            * lifespan: the amount of time between the creation of the formula and the evaluation time of the formula.
            * span: the amount of time of sampled data to use.
            * startup: the amount of time we use the initialTarget value instead of using the sampled data.
            * ratio: the minimum percentage of "valid" measurements to within `span` needed to consider the data collection to be valid.
        */
        {
            var targetVariable = preemptable ? "TargetLowPriorityNodes" : "TargetDedicated";
            return string.Join(Environment.NewLine, new[]
            {
                "$NodeDeallocationOption = taskcompletion;",
                $"""lifespan = time() - time("{DateTime.UtcNow:r}");""",
                "span = TimeInterval_Second * 90;",
                "startup = TimeInterval_Minute * 2;",
                "ratio = 10;",
                $"${targetVariable} = (lifespan > startup ? min($PendingTasks.GetSample(span, ratio)) : {initialTarget});"
            });
        }

        private async ValueTask ServicePoolManagePoolScalingAsync(CancellationToken cancellationToken)
        {
            // This method implememts a state machine to disable/enable autoscaling as needed to clear certain conditions that can be observed

            var (allocationState, autoScaleEnabled, _, _, _, _) = await _azureProxy.GetFullAllocationStateAsync(Pool.PoolId, cancellationToken);
            EnsureScalingModeSet(autoScaleEnabled);

            if (allocationState == AllocationState.Steady)
            {
                switch (_scalingMode)
                {
                    case ScalingMode.AutoScaleEnabled:
                        if (_resetAutoScalingRequired || await GetNodesToRemove(false).AnyAsync(cancellationToken))
                        {
                            _logger.LogInformation(@"Switching pool {PoolId} to manual scale to clear resize errors and/or compute nodes in invalid states.", Pool.PoolId);
                            await _azureProxy.DisableBatchPoolAutoScaleAsync(Pool.PoolId, cancellationToken);
                            _scalingMode = ScalingMode.SettingManualScale;
                        }
                        break;

                    case ScalingMode.SettingManualScale:
                        {
                            var nodesToRemove = Enumerable.Empty<ComputeNode>();

                            // It's documented that a max of 100 nodes can be removed at a time. Excess eligible nodes will be removed in a future call to this method.
                            await foreach (var node in GetNodesToRemove(true).Take(MaxComputeNodesToRemoveAtOnce).WithCancellation(cancellationToken))
                            {
                                switch (node.State)
                                {
                                    case ComputeNodeState.Unusable:
                                        _logger.LogDebug("Found unusable node {NodeId}", node.Id);
                                        break;

                                    case ComputeNodeState.StartTaskFailed:
                                        _logger.LogDebug("Found starttaskfailed node {NodeId}", node.Id);
                                        StartTaskFailures.Enqueue(node.StartTaskInformation.FailureInformation);
                                        break;

                                    case ComputeNodeState.Preempted:
                                        _logger.LogDebug("Found preempted node {NodeId}", node.Id);
                                        break;

                                    default: // Should never reach here. Skip.
                                        continue;
                                }

                                nodesToRemove = nodesToRemove.Append(node);
                                _resizeErrorsRetrieved = false;
                            }

                            nodesToRemove = nodesToRemove.ToList();

                            if (nodesToRemove.Any())
                            {
                                await RemoveNodesAsync((IList<ComputeNode>)nodesToRemove, cancellationToken);
                                _scalingMode = ScalingMode.RemovingFailedNodes;
                            }
                            else
                            {
                                goto case ScalingMode.RemovingFailedNodes;
                            }
                        }
                        break;

                    case ScalingMode.RemovingFailedNodes:
                        _scalingMode = ScalingMode.RemovingFailedNodes;
                        ResizeErrors.Clear();
                        _resizeErrorsRetrieved = true;
                        _logger.LogInformation(@"Switching pool {PoolId} back to autoscale.", Pool.PoolId);
                        await _azureProxy.EnableBatchPoolAutoScaleAsync(Pool.PoolId, !IsDedicated, AutoScaleEvaluationInterval, (p, t) => AutoPoolFormula(p, GetTaskCount(t)), cancellationToken);
                        _autoScaleWaitTime = DateTime.UtcNow + (3 * AutoScaleEvaluationInterval) + BatchPoolService.RunInterval;
                        _scalingMode = _resetAutoScalingRequired ? ScalingMode.WaitingForAutoScale : ScalingMode.SettingAutoScale;
                        break;

                    case ScalingMode.WaitingForAutoScale:
                        _resetAutoScalingRequired = false;
                        if (DateTime.UtcNow > _autoScaleWaitTime)
                        {
                            _scalingMode = ScalingMode.SettingAutoScale;
                        }
                        break;

                    case ScalingMode.SettingAutoScale:
                        _scalingMode = ScalingMode.AutoScaleEnabled;
                        break;
                }

                int GetTaskCount(int @default) // Used to make reenabling auto-scale more performant by attempting to gather the current number of "pending" tasks, falling back on the current target.
                {
                    try
                    {
                        return GetTasksAsync(includeCompleted: false).CountAsync(cancellationToken).AsTask().Result;
                    }
                    catch
                    {
                        return @default;
                    }
                }
            }

            IAsyncEnumerable<ComputeNode> GetNodesToRemove(bool withState)
                => _azureProxy.ListComputeNodesAsync(Pool.PoolId, new ODATADetailLevel(filterClause: @"state eq 'starttaskfailed' or state eq 'preempted' or state eq 'unusable'", selectClause: withState ? @"id,state,startTaskInfo" : @"id"));
        }

        private bool DetermineIsAvailable(DateTime? creation)
            => creation.HasValue && creation.Value + _forcePoolRotationAge > DateTime.UtcNow;

        private ValueTask ServicePoolRotateAsync(CancellationToken _1)
        {
            if (IsAvailable)
            {
                IsAvailable = DetermineIsAvailable(Creation);
            }

            return ValueTask.CompletedTask;
        }

        private async ValueTask ServicePoolRemovePoolIfEmptyAsync(CancellationToken cancellationToken)
        {
            if (!IsAvailable)
            {
                var (_, _, _, lowPriorityNodes, _, dedicatedNodes) = await _azureProxy.GetFullAllocationStateAsync(Pool.PoolId, cancellationToken);
                if (lowPriorityNodes.GetValueOrDefault(0) == 0 && dedicatedNodes.GetValueOrDefault(0) == 0 && !await GetTasksAsync(includeCompleted: true).AnyAsync(cancellationToken))
                {
                    _ = _batchPools.RemovePoolFromList(this);
                    await _batchPools.DeletePoolAsync(this, cancellationToken);
                }
            }
        }
    }

    /// <content>
    /// Implements the <see cref="IBatchPool"/> interface.
    /// </content>
    public sealed partial class BatchPool : IBatchPool
    {
        private static readonly SemaphoreSlim lockObj = new(1, 1);

        /// <summary>
        /// Types of maintenance calls offered by the <see cref="ServicePoolAsync(ServiceKind, CancellationToken)"/> service method.
        /// </summary>
        internal enum ServiceKind
        {
            /// <summary>
            /// Queues resize errors (if available).
            /// </summary>
            GetResizeErrors,

            /// <summary>
            /// Proactively removes errored nodes from pool and manages certain autopool error conditions.
            /// </summary>
            ManagePoolScaling,

            /// <summary>
            /// Removes <see cref="CloudPool"/> if it's retired and empty.
            /// </summary>
            RemovePoolIfEmpty,

            /// <summary>
            /// Stages rotating or retiring this <see cref="CloudPool"/> if needed.
            /// </summary>
            Rotate,
        }

        /// <inheritdoc/>
        public bool IsAvailable { get; private set; } = true;

        /// <inheritdoc/>
        public PoolInformation Pool { get; private set; }

        /// <inheritdoc/>
        public async ValueTask<bool> CanBeDeleted(CancellationToken cancellationToken = default)
        {
            if (await GetTasksAsync(includeCompleted: true).AnyAsync(cancellationToken))
            {
                return false;
            }

            await foreach (var node in _azureProxy.ListComputeNodesAsync(Pool.PoolId, new ODATADetailLevel(selectClause: "state")).WithCancellation(cancellationToken))
            {
                switch (node.State)
                {
                    case ComputeNodeState.Rebooting:
                    case ComputeNodeState.Reimaging:
                    case ComputeNodeState.Running:
                    case ComputeNodeState.Creating:
                    case ComputeNodeState.Starting:
                    case ComputeNodeState.WaitingForStartTask:
                        return false;
                }
            }

            return true;
        }

        /// <inheritdoc/>
        public ResizeError PopNextResizeError()
            => ResizeErrors.TryDequeue(out var resizeError) ? resizeError : default;

        /// <inheritdoc/>
        public TaskFailureInformation PopNextStartTaskFailure()
            => StartTaskFailures.TryDequeue(out var failure) ? failure : default;

        /// <summary>
        /// Service methods dispatcher.
        /// </summary>
        /// <param name="serviceKind">The type of <see cref="ServiceKind"/> service call.</param>
        /// <param name="cancellationToken"></param>
        internal async ValueTask ServicePoolAsync(ServiceKind serviceKind, CancellationToken cancellationToken = default)
        {
            Func<CancellationToken, ValueTask> func = serviceKind switch
            {
                ServiceKind.GetResizeErrors => ServicePoolGetResizeErrorsAsync,
                ServiceKind.ManagePoolScaling => ServicePoolManagePoolScalingAsync,
                ServiceKind.RemovePoolIfEmpty => ServicePoolRemovePoolIfEmptyAsync,
                ServiceKind.Rotate => ServicePoolRotateAsync,
                _ => throw new ArgumentOutOfRangeException(nameof(serviceKind)),
            };

            await lockObj.WaitAsync(cancellationToken); // Don't release if we never acquire. Thus, don't put this inside the try/finally where the finally calls Release().

            try // Don't add any code that can throw between this line and the call above to acquire lockObj.
            {
                await func(cancellationToken);
            }
            finally
            {
                _ = lockObj.Release();
            }
        }

        /// <inheritdoc/>
        public async ValueTask ServicePoolAsync(CancellationToken cancellationToken = default)
        {
            var exceptions = new List<Exception>();

            _ = await PerformTask(ServicePoolAsync(ServiceKind.GetResizeErrors, cancellationToken), cancellationToken) &&
            await PerformTask(ServicePoolAsync(ServiceKind.ManagePoolScaling, cancellationToken), cancellationToken) &&
            await PerformTask(ServicePoolAsync(ServiceKind.Rotate, cancellationToken), cancellationToken) &&
            await PerformTask(ServicePoolAsync(ServiceKind.RemovePoolIfEmpty, cancellationToken), cancellationToken);

            switch (exceptions.Count)
            {
                case 0:
                    return;

                case 1:
                    throw exceptions.First();

                default:
                    throw new AggregateException(exceptions.SelectMany(Flatten));
            }

            static IEnumerable<Exception> Flatten(Exception ex)
                => ex switch
                {
                    AggregateException aggregateException => aggregateException.InnerExceptions,
                    _ => Enumerable.Empty<Exception>().Append(ex),
                };

            async ValueTask<bool> PerformTask(ValueTask serviceAction, CancellationToken cancellationToken)
            {
                if (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        await serviceAction;
                        return true;
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                        return await RemoveMissingPools(ex);
                    }
                }

                return false;
            }

            async ValueTask<bool> RemoveMissingPools(Exception ex)
            {
                switch (ex)
                {
                    case AggregateException aggregateException:
                        var result = true;
                        foreach (var e in aggregateException.InnerExceptions)
                        {
                            result &= await RemoveMissingPools(e);
                        }
                        return result;

                    case BatchException batchException:
                        if (batchException.RequestInformation.BatchError.Code == BatchErrorCodeStrings.PoolNotFound)
                        {
                            _logger.LogError(ex, "Batch pool {PoolId} is missing. Removing it from TES's active pool list.", Pool.PoolId);
                            _ = _batchPools.RemovePoolFromList(this);
                            // TODO: Consider moving any remaining tasks to another pool, or failing tasks explicitly
                            await _batchPools.DeletePoolAsync(this, cancellationToken); // Ensure job removal too
                            return false;
                        }
                        break;
                }
                return true;
            }
        }

        /// <inheritdoc/>
        public async ValueTask<DateTime> GetAllocationStateTransitionTime(CancellationToken cancellationToken = default)
            => (await _azureProxy.GetBatchPoolAsync(Pool.PoolId, cancellationToken, new ODATADetailLevel { SelectClause = "allocationStateTransitionTime" })).AllocationStateTransitionTime ?? DateTime.UtcNow;

        /// <inheritdoc/>
        public async ValueTask CreatePoolAndJobAsync(Microsoft.Azure.Management.Batch.Models.Pool poolModel, bool isPreemptible, CancellationToken cancellationToken)
        {
            try
            {
                CloudPool pool = default;
                await Task.WhenAll(
                    _azureProxy.CreateBatchJobAsync(new() { PoolId = poolModel.Name }, cancellationToken),
                    Task.Run(async () =>
                    {
                        var poolInfo = await _azureProxy.CreateBatchPoolAsync(poolModel, isPreemptible, cancellationToken);
                        pool = await _azureProxy.GetBatchPoolAsync(poolInfo.PoolId, cancellationToken, new ODATADetailLevel { SelectClause = CloudPoolSelectClause });
                    }, cancellationToken));

                Configure(pool);
            }
            catch (AggregateException ex)
            {
                var exception = ex.Flatten();
                // If there is only one contained exception, we don't need an AggregateException, and we have a simple path to success (following this if block)
                // In the extremely unlikely event that there are no innerexceptions, we don't want to change the existing code flow nor do we want to complicate the (less than 2) path.
                if (exception.InnerExceptions?.Count != 1)
                {
                    throw new AggregateException(exception.Message, exception.InnerExceptions?.Select(HandleException) ?? Enumerable.Empty<Exception>());
                }

                throw HandleException(exception.InnerException);
            }
            catch (Exception ex)
            {
                throw HandleException(ex);
            }

            Exception HandleException(Exception ex)
            {
                // When the batch management API creating the pool times out, it may or may not have created the pool. Add an inactive record to delete it if it did get created and try again later. That record will be removed later whether or not the pool was created.
                Pool ??= new() { PoolId = poolModel.Name };
                _ = _batchPools.AddPool(this);
                return ex switch
                {
                    OperationCanceledException => ex.InnerException is null ? ex : new AzureBatchPoolCreationException(ex.Message, true, ex),
                    var x when x is RequestFailedException rfe && rfe.Status == 0 && rfe.InnerException is System.Net.WebException webException && webException.Status == System.Net.WebExceptionStatus.Timeout => new AzureBatchPoolCreationException(ex.Message, true, ex),
                    var x when IsInnermostExceptionSocketException(x) => new AzureBatchPoolCreationException(ex.Message, ex),
                    _ => new AzureBatchPoolCreationException(ex.Message, ex),
                };

                static bool IsInnermostExceptionSocketException(Exception ex)
                {
                    for (var e = ex; e is not System.Net.Sockets.SocketException; e = e.InnerException)
                    {
                        if (e.InnerException is null) { return false; }
                    }

                    return true;
                }
            }
        }

        /// <inheritdoc/>
        public async ValueTask AssignPoolAsync(CloudPool pool, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(pool);

            if (pool.Id is null || pool.CreationTime is null || pool.Metadata is null || !pool.Metadata.Any(m => BatchScheduler.PoolHostName.Equals(m.Name, StringComparison.Ordinal)) || !pool.Metadata.Any(m => BatchScheduler.PoolIsDedicated.Equals(m.Name, StringComparison.Ordinal)))
            {
                throw new ArgumentException("CloudPool is either not configured correctly or was not retrieved with all required metadata.", nameof(pool));
            }

            // Pool is "broken" if job is missing/not active. Reject this pool via the side effect of the exception that is thrown.
            if (1 != (await _azureProxy.GetBatchJobAsync(pool.Id, cancellationToken, new ODATADetailLevel { SelectClause = "id,state"/*, FilterClause = "state eq 'active'"*/ }).ToAsyncEnumerable().Where(j => j.State == JobState.Active).ToListAsync(cancellationToken)).Count)
            {
                // TODO: investigate why FilterClause throws "Type Microsoft.Azure.Batch.Protocol.BatchRequests.JobGetBatchRequest does not support a filter clause. (Parameter 'detailLevel')"
                throw new InvalidOperationException($"Active Job not found for Pool {pool.Id}");
            }

            Configure(pool);
        }

        private void Configure(CloudPool pool)
        {
            ArgumentNullException.ThrowIfNull(pool);

            Pool = new() { PoolId = pool.Id };
            IsAvailable = DetermineIsAvailable(pool.CreationTime);

            if (IsAvailable)
            {
                Creation = pool.CreationTime.Value;
            }

            IsDedicated = bool.Parse(pool.Metadata.First(m => BatchScheduler.PoolIsDedicated.Equals(m.Name, StringComparison.Ordinal)).Value);
            _ = _batchPools.AddPool(this);
        }
    }

    /// <content>
    /// Used for unit/module testing.
    /// </content>
    public sealed partial class BatchPool
    {
        internal int TestPendingReservationsCount => GetTasksAsync(includeCompleted: false).CountAsync().AsTask().Result;

        internal int? TestTargetDedicated => _azureProxy.GetFullAllocationStateAsync(Pool.PoolId, CancellationToken.None).Result.TargetDedicated;
        internal int? TestTargetLowPriority => _azureProxy.GetFullAllocationStateAsync(Pool.PoolId, CancellationToken.None).Result.TargetLowPriority;

        internal TimeSpan TestRotatePoolTime
            => _forcePoolRotationAge;

        internal void TestSetAvailable(bool available)
            => IsAvailable = available;

        internal void TimeShift(TimeSpan shift)
            => Creation -= shift;
    }
}
