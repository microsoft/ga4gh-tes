// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

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

        private readonly ILogger _logger;
        private readonly IAzureProxy _azureProxy;

        /// <summary>
        /// Constructor of <see cref="BatchPool"/>.
        /// </summary>
        /// <param name="batchScheduler"></param>
        /// <param name="configuration"></param>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        /// <exception cref="ArgumentException"></exception>
        public BatchPool(IBatchScheduler batchScheduler, IConfiguration configuration, IAzureProxy azureProxy, ILogger<BatchPool> logger)
        {
            _forcePoolRotationAge = TimeSpan.FromDays(GetConfigurationValue(configuration, "BatchPoolRotationForcedDays", 30));

            this._azureProxy = azureProxy;
            this._logger = logger;
            _batchPools = batchScheduler as BatchScheduler ?? throw new ArgumentException("batchScheduler must be of type BatchScheduler", nameof(batchScheduler));

            // IConfiguration.GetValue<double>(string key, double defaultValue) throws an exception if the value is defined as blank
            static double GetConfigurationValue(IConfiguration configuration, string key, double defaultValue)
            {
                var value = configuration.GetValue(key, string.Empty);
                return string.IsNullOrWhiteSpace(value) ? defaultValue : double.Parse(value);
            }
        }

        private Queue<TaskFailureInformation> StartTaskFailures { get; } = new();
        private Queue<ResizeError> ResizeErrors { get; } = new();

        internal IAsyncEnumerable<CloudTask> GetTasksAsync()
            => _azureProxy.ListTasksAsync(Pool.PoolId, new ODATADetailLevel { SelectClause = "id,stateTransitionTime", FilterClause = "state ne 'completed'" });

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
        private bool _resizeStoppedReceived;

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
            var currentAllocationState = await _azureProxy.GetComputeNodeAllocationStateAsync(Pool.PoolId, cancellationToken);
            EnsureScalingModeSet(currentAllocationState.AutoScaleEnabled);

            if (_scalingMode == ScalingMode.AutoScaleEnabled)
            {
                if (currentAllocationState.AllocationState == AllocationState.Steady)
                {
                    if (!_resizeErrorsRetrieved)
                    {
                        ResizeErrors.Clear();
                        var pool = await _azureProxy.GetBatchPoolAsync(Pool.PoolId, new ODATADetailLevel { SelectClause = "resizeErrors" }, cancellationToken);

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
                                    _resizeStoppedReceived |= true;
                                    break;

                                // Errors to both force resetting autoscale and fail tasks
                                case PoolResizeErrorCodes.AllocationFailed:
                                    _resizeStoppedReceived |= true;
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
                    _resizeStoppedReceived = false;
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

              In my not at all humble opinion, some of the builtin variable names in batch's autoscale formulas are very badly named:
                  Running tasks are named RunningTasks, which is fine
                  Queued tasks are named ActiveTasks, which matches the "state", but isn't the best name around
                  The sum of running & queued tasks (what should be named TotalTasks) is named PendingTasks, an absolutely awful name

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
            */
            => string.Format(@"
$NodeDeallocationOption=taskcompletion;
lifespan         = time() - time(""{1}"");
span             = TimeInterval_Second * 90;
startup          = TimeInterval_Minute * 2;
ratio            = 10;
${0} = (lifespan > startup ? min($PendingTasks.GetSample(span, ratio)) : {2});
",
                /* {0} */ preemptable ? "TargetLowPriorityNodes" : "TargetDedicated",
                /* {1} */ DateTime.UtcNow.ToString("r"),
                /* {2} */ initialTarget);


        private async ValueTask ServicePoolManagePoolScalingAsync(CancellationToken cancellationToken)
        {
            // This method implememts a state machine to disable/enable autoscaling as needed to clear certain conditions that can be observed

            var (allocationState, autoScaleEnabled, _, _) = await _azureProxy.GetComputeNodeAllocationStateAsync(Pool.PoolId, cancellationToken);
            EnsureScalingModeSet(autoScaleEnabled);

            if (allocationState == AllocationState.Steady)
            {
                switch (_scalingMode)
                {
                    case ScalingMode.AutoScaleEnabled:
                        if (_resizeStoppedReceived || await GetNodesToRemove(false).AnyAsync(cancellationToken))
                        {
                            await _azureProxy.DisableBatchPoolAutoScaleAsync(Pool.PoolId, cancellationToken);
                            _scalingMode = ScalingMode.SettingManualScale;
                        }
                        break;

                    case ScalingMode.SettingManualScale:
                        {
                            var nodesToRemove = Enumerable.Empty<ComputeNode>();

                            // It's documented that a max of 100 nodes can be removed at a time. Excess eligible nodes will be removed in a future call to this method.
                            await foreach (var node in GetNodesToRemove(true).Take(100).WithCancellation(cancellationToken))
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

                            if (!nodesToRemove.Any())
                            {
                                _scalingMode = ScalingMode.RemovingFailedNodes;
                            }
                            else
                            {
                                await RemoveNodesAsync((IList<ComputeNode>)nodesToRemove, cancellationToken);
                            }
                        }
                        break;

                    case ScalingMode.RemovingFailedNodes:
                        ResizeErrors.Clear();
                        _resizeErrorsRetrieved = true;
                        await _azureProxy.EnableBatchPoolAutoScaleAsync(Pool.PoolId, !IsDedicated, AutoScaleEvaluationInterval, AutoPoolFormula, cancellationToken);
                        _autoScaleWaitTime = DateTime.UtcNow + AutoScaleEvaluationInterval;
                        _scalingMode = _resizeStoppedReceived ? ScalingMode.WaitingForAutoScale : ScalingMode.SettingAutoScale;
                        break;

                    case ScalingMode.WaitingForAutoScale:
                        _resizeStoppedReceived = false;
                        if (DateTime.UtcNow > _autoScaleWaitTime)
                        {
                            _scalingMode = ScalingMode.SettingAutoScale;
                        }
                        break;

                    case ScalingMode.SettingAutoScale:
                        _scalingMode = ScalingMode.AutoScaleEnabled;
                        break;
                }
            }

            IAsyncEnumerable<ComputeNode> GetNodesToRemove(bool withState)
                => _azureProxy.ListComputeNodesAsync(Pool.PoolId, new ODATADetailLevel(filterClause: @"state eq 'starttaskfailed' or state eq 'preempted' or state eq 'unusable'", selectClause: withState ? @"id,state,startTaskInfo" : @"id"));
        }

        private ValueTask ServicePoolRotateAsync(CancellationToken _1)
        {
            if (IsAvailable)
            {
                IsAvailable = Creation + _forcePoolRotationAge > DateTime.UtcNow;
            }

            return ValueTask.CompletedTask;
        }

        private async ValueTask ServicePoolRemovePoolIfEmptyAsync(CancellationToken cancellationToken)
        {
            if (!IsAvailable)
            {
                var (lowPriorityNodes, dedicatedNodes) = await _azureProxy.GetCurrentComputeNodesAsync(Pool.PoolId, cancellationToken);
                if ((lowPriorityNodes is null || lowPriorityNodes == 0) && (dedicatedNodes is null || dedicatedNodes == 0) && !await GetTasksAsync().AnyAsync(cancellationToken))
                {
                    await _batchPools.DeletePoolAsync(this, cancellationToken);
                    _ = _batchPools.RemovePoolFromList(this);
                }
            }
        }
    }

    /// <content>
    /// Implements the <see cref="IBatchPool"/> interface.
    /// </content>
    public sealed partial class BatchPool : IBatchPool
    {
        private sealed class LockObj : IDisposable
        {
            private static readonly SemaphoreSlim lockObj = new(1, 1);

            public static async ValueTask<LockObj> Lock()
            {
                await lockObj.WaitAsync();
                return new LockObj();
            }

            public static async ValueTask<LockObj> Lock(TimeSpan timeout)
            {
                if (await lockObj.WaitAsync(timeout))
                {
                    return new LockObj();
                }

                throw new TimeoutException();
            }

            public void Dispose()
                => lockObj.Release();
        }

        /// <inheritdoc/>
        public bool IsAvailable { get; private set; } = true;

        /// <inheritdoc/>
        public PoolInformation Pool { get; private set; }

        /// <inheritdoc/>
        public async ValueTask<bool> CanBeDeleted(CancellationToken cancellationToken = default)
        {
            if (await GetTasksAsync().AnyAsync(cancellationToken))
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

        /// <inheritdoc/>
        public async ValueTask ServicePoolAsync(IBatchPool.ServiceKind serviceKind, CancellationToken cancellationToken = default)
        {
            Func<CancellationToken, ValueTask> func = serviceKind switch
            {
                IBatchPool.ServiceKind.GetResizeErrors => ServicePoolGetResizeErrorsAsync,
                IBatchPool.ServiceKind.ManagePoolScaling => ServicePoolManagePoolScalingAsync,
                IBatchPool.ServiceKind.RemovePoolIfEmpty => ServicePoolRemovePoolIfEmptyAsync,
                IBatchPool.ServiceKind.Rotate => ServicePoolRotateAsync,
                _ => throw new ArgumentOutOfRangeException(nameof(serviceKind)),
            };

            using var @lock = await LockObj.Lock();
            await func(cancellationToken);
        }

        /// <inheritdoc/>
        public async ValueTask ServicePoolAsync(CancellationToken cancellationToken = default)
        {
            var exceptions = new List<Exception>();

            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.GetResizeErrors, cancellationToken), cancellationToken);
            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.ManagePoolScaling, cancellationToken), cancellationToken);
            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.Rotate, cancellationToken), cancellationToken);
            await PerformTask(ServicePoolAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty, cancellationToken), cancellationToken);

            switch (exceptions.Count)
            {
                case 0:
                    return;

                case 1:
                    throw exceptions.First();

                default:
                    throw new AggregateException(exceptions.SelectMany(Flatten).ToArray());
            }

            static IEnumerable<Exception> Flatten(Exception ex)
                => ex switch
                {
                    AggregateException aggregateException => aggregateException.InnerExceptions,
                    _ => Enumerable.Empty<Exception>().Append(ex),
                };

            async ValueTask PerformTask(ValueTask serviceAction, CancellationToken cancellationToken)
            {
                if (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        await serviceAction;
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                        RemoveMissingPools(ex);
                    }
                }
            }

            void RemoveMissingPools(Exception ex)
            {
                switch (ex)
                {
                    case AggregateException aggregateException:
                        foreach (var e in aggregateException.InnerExceptions)
                        {
                            RemoveMissingPools(e);
                        }
                        break;

                    case BatchException batchException:
                        if (batchException.RequestInformation.BatchError.Code == BatchErrorCodeStrings.PoolNotFound)
                        {
                            _logger.LogError(ex, "Batch pool {PoolId} is missing. Removing it from TES's active pool list.", Pool.PoolId);
                            _ = _batchPools.RemovePoolFromList(this);
                        }
                        break;
                }
            }
        }

        /// <inheritdoc/>
        public async ValueTask<DateTime> GetAllocationStateTransitionTime(CancellationToken cancellationToken = default) // TODO: put this at front of list by returning earliest possible time, or put at end of list by returning UtcNow?
            => (await _azureProxy.GetBatchPoolAsync(Pool.PoolId, new ODATADetailLevel { SelectClause = "allocationStateTransitionTime" }, cancellationToken)).AllocationStateTransitionTime ?? DateTime.UtcNow;

        /// <inheritdoc/>
        public async ValueTask CreatePoolAndJobAsync(Microsoft.Azure.Management.Batch.Models.Pool poolModel, bool isPreemptible, CancellationToken cancellationToken)
        {
            try
            {
                CloudPool pool = default;
                var poolInfo = await _azureProxy.CreateBatchPoolAsync(poolModel, isPreemptible);
                await Task.WhenAll(
                    Task.Run(async () => pool = await _azureProxy.GetBatchPoolAsync(poolInfo.PoolId, new ODATADetailLevel { SelectClause = CloudPoolSelectClause }, cancellationToken)),
                    _azureProxy.CreateBatchJobAsync(poolInfo, cancellationToken));
                Configure(pool);
                _ = _batchPools.AddPool(this);
            }
            catch (AggregateException ex)
            {
                if (ex.InnerExceptions.Count < 2)
                {
                    throw new AggregateException(ex.InnerExceptions.Select(HandleException).ToArray());
                }

                throw HandleException(ex.InnerException);

                Exception HandleException(Exception e)
                {
                    switch (e)
                    {
                        case OperationCanceledException:
                        case RequestFailedException rfe when rfe.Status == 0 && rfe.InnerException is System.Net.WebException we && we.Status == System.Net.WebExceptionStatus.Timeout:
                        case Exception when IsInnermostExceptionSocketException125(e):
                            return ProcessException();
                        default:
                            return ProcessException(e);
                    }
                }
            }

            static bool IsInnermostExceptionSocketException125(Exception ex)
            {
                // errno: ECANCELED 125 Operation canceled
                for (var e = ex; e is System.Net.Sockets.SocketException /*se && se.ErrorCode == 125*/; e = e.InnerException)
                {
                    if (e.InnerException is null) { return false; }
                }
                return true;
            }

            Exception ProcessException(Exception ex = default)
            {
                // When the batch management API creating the pool times out, it may or may not have created the pool. Add an inactive record to delete it if it did get created and try again later. That record will be removed later whether or not the pool was created.
                Pool ??= new() { PoolId = poolModel.Name };
                _ = _batchPools.AddPool(this);
                return ex switch
                {
                    null => new AzureBatchQuotaMaxedOutException("Pool creation timed out"),
                    OperationCanceledException => ex,
                    var x when x is RequestFailedException rfe && rfe.Status == 0 && rfe.InnerException is System.Net.WebException webException && webException.Status == System.Net.WebExceptionStatus.Timeout => new AzureBatchQuotaMaxedOutException("Pool creation timed out", ex),
                    var x when IsInnermostExceptionSocketException125(x) => new AzureBatchQuotaMaxedOutException("Pool creation timed out", ex),
                    _ => new Exception(ex.Message, ex),
                };
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

            // Pool is "broken" if job is missing/not active. Reject this pool.
            _ = await _azureProxy.GetBatchJobAsync(pool.Id, new ODATADetailLevel { SelectClause = "id", FilterClause = "state eq 'active'" }, cancellationToken);

            Configure(pool);
        }

        private void Configure(CloudPool pool)
        {
            Pool = new() { PoolId = pool.Id };

            Creation = pool.CreationTime ?? DateTime.UtcNow;
            IsAvailable = true;
            IsDedicated = bool.Parse(pool.Metadata.First(m => BatchScheduler.PoolIsDedicated.Equals(m.Name, StringComparison.Ordinal)).Value);
        }
    }

    /// <content>
    /// Used for unit/module testing.
    /// </content>
    public sealed partial class BatchPool
    {
        internal int TestPendingReservationsCount => GetTasksAsync().CountAsync().AsTask().Result;

        internal int? TestTargetDedicated => _azureProxy.GetComputeNodeAllocationStateAsync(Pool.PoolId).Result.TargetDedicated;
        internal int? TestTargetLowPriority => _azureProxy.GetComputeNodeAllocationStateAsync(Pool.PoolId).Result.TargetLowPriority;

        internal TimeSpan TestRotatePoolTime
            => _forcePoolRotationAge;

        internal void TestSetAvailable(bool available)
            => IsAvailable = available;

        internal void TimeShift(TimeSpan shift)
            => Creation -= shift;
    }
}
