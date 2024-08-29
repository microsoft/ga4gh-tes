﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.ResourceManager.Batch;
using CommonUtilities;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TesApi.Web.Storage;

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
        public const string CloudPoolSelectClause = "creationTime,id,identity,metadata";

        /// <summary>
        /// Autoscale evalutation interval
        /// </summary>
        public static TimeSpan AutoScaleEvaluationInterval { get; } = TimeSpan.FromMinutes(5);

        private const int MaxComputeNodesToRemoveAtOnce = 100; // https://learn.microsoft.com/en-us/rest/api/batchservice/pool/remove-nodes?tabs=HTTP#request-body nodeList description

        private readonly ILogger _logger;
        private readonly IAzureProxy _azureProxy;
        private readonly Management.Batch.IBatchPoolManager _batchPoolManager;
        private readonly IStorageAccessProvider _storageAccessProvider;

        /// <summary>
        /// Constructor of <see cref="BatchPool"/>.
        /// </summary>
        /// <param name="batchScheduler"></param>
        /// <param name="batchSchedulingOptions"></param>
        /// <param name="azureProxy"></param>
        /// <param name="batchPoolManager"></param>
        /// <param name="storageAccessProvider"></param>
        /// <param name="logger"></param>
        /// <exception cref="ArgumentException"></exception>
        public BatchPool(IBatchScheduler batchScheduler, IOptions<Options.BatchSchedulingOptions> batchSchedulingOptions, IAzureProxy azureProxy, Management.Batch.IBatchPoolManager batchPoolManager, IStorageAccessProvider storageAccessProvider, ILogger<BatchPool> logger)
        {
            var rotationDays = batchSchedulingOptions.Value.PoolRotationForcedDays;
            if (rotationDays == 0) { rotationDays = Options.BatchSchedulingOptions.DefaultPoolRotationForcedDays; }
            _forcePoolRotationAge = TimeSpan.FromDays(rotationDays);

            _azureProxy = azureProxy;
            _batchPoolManager = batchPoolManager;
            _storageAccessProvider = storageAccessProvider;
            _logger = logger;
            _batchPools = batchScheduler as BatchScheduler ?? throw new ArgumentException("batchScheduler must be of type BatchScheduler", nameof(batchScheduler));
        }

        private Queue<IBatchPool.StartTaskFailureInformation> StartTaskFailures { get; } = new();
        private Queue<ResizeError> ResizeErrors { get; } = new();

        internal IAsyncEnumerable<CloudTask> GetTasksAsync(bool includeCompleted)
            => _azureProxy.ListTasksAsync(PoolId, new ODATADetailLevel { SelectClause = "id,stateTransitionTime", FilterClause = includeCompleted ? default : "state ne 'completed'" });

        private async ValueTask RemoveNodesAsync(IList<ComputeNode> nodesToRemove, CancellationToken cancellationToken)
        {
            _logger.LogDebug("Removing {Nodes} nodes from {PoolId}", nodesToRemove.Count, PoolId);
            await _azureProxy.DeleteBatchComputeNodesAsync(PoolId, nodesToRemove, cancellationToken);
        }
    }

    /// <content>
    /// Implements the various ServicePool* methods.
    /// </content>
    public sealed partial class BatchPool
    {
        /// <summary>
        /// Scaling state machine states.
        /// </summary>
        /// <remarks>
        /// These states mostly describe the action initiated when the state started, not the next action to perform.
        /// The machine mostly rolls through each state (except Unknown) in order, rotating from the end back to the beginning. Depending on the circumstances, it may skip WaitingForAutoScale.
        /// </remarks>
        private enum ScalingMode
        {
            /// <summary>
            /// Has not been set. Except in a brand new pool, should never be seen during normal operation.
            /// </summary>
            Unknown,

            /// <summary>
            /// Normal long-term state. Autoscale is enabled.
            /// </summary>
            /// <remarks>
            /// If the pool's scaling settings need to be reset, or if compute nodes need to be manually ejected, disable autoscale.
            /// </remarks>
            AutoScaleEnabled,

            /// <summary>
            /// Autoscale was disabled.
            /// </summary>
            /// <remarks>
            /// Nodes that require manual ejection can be removed in this state. If there are no nodes to remove, this becomes equivalent to <see cref="RemovingFailedNodes"/>.
            /// </remarks>
            AutoScaleDisabled,

            /// <summary>
            /// Compute nodes have been ejected.
            /// </summary>
            /// <remarks>
            /// When this state is reached, autoscale will be reenabled.
            /// </remarks>
            RemovingFailedNodes,

            /// <summary>
            /// Wait for first successful autoscale evaluation using batch metrics.
            /// </summary>
            /// <remarks>
            /// This state is not needed if the disabling of autoscale was only taken to eject compute nodes.
            /// </remarks>
            WaitingForAutoScale,

            /// <summary>
            /// Reset state to <see cref="AutoScaleEnabled"/>.
            /// </summary>
            /// <remarks>
            /// This state exists to eliminate premature redisabling of autoscale mode.
            /// </remarks>
            SettingAutoScale
        }

        private ScalingMode _scalingMode = ScalingMode.Unknown;
        private DateTime _autoScaleWaitTime = DateTime.MinValue;

        private readonly TimeSpan _forcePoolRotationAge;
        private readonly BatchScheduler _batchPools;
        private bool _resetAutoScalingRequired;

        private DateTime? Creation { get; set; }
        private DateTime? AllocationStateTransitionTime { get; set; }
        private bool IsDedicated { get; set; }

        private void EnsureScalingModeSet(bool? autoScaleEnabled)
        {
            /*
             * If the the scaling mode does not correspond to the actual state of autoscale enablement, this method guides us towards the desired state.
             *
             * Barring outside intervention, at each and every time this method is called, the following should always hold true:
             * |------------------|---------------------|-------------------------|-------------------------|
             * | autoScaleEnabled |     ScalingMode     |       Last action       |       Next action       |
             * |------------------|---------------------|-------------------------|-------------------------|
             * |       true       |   AutoScaleEnabled  | Normal long-term state  |Change for select errrors|
             * |       false      |  AutoScaleDisabled  |  Recently disabled AS   |  Perform needed actions |
             * |       false      | RemovingFailedNodes | Manual resizing actions | Reenable autoscale mode |
             * |       true       | WaitingForAutoScale | Ensure autoscale works  | Delay and re-assess     |
             * |       true       |   SettingAutoScale  |  Assess pool response   | Restore normal long-term|
             * |------------------|---------------------|-------------------------|-------------------------|
             *
             * The first time this method is called, ScalingMode will be Unknown. Initialize it to an appropriate value to initialize the state machine's state.
             * If autoScaleEnabled is null, don't change anything.
             *
             * If a pool's autoscale was disabled by an outside agent, the state machine should work to reenable it. Use the state RemovingFailedNodes for that.
             *
             * If a pool was expected to switch scaling modes, but didn't, the pool's changeover has silently failed. Consider this pool for early retirement.
             */

            (var failed, _scalingMode) = autoScaleEnabled switch
            {
                true => _scalingMode switch
                {
                    ScalingMode.Unknown => (false, ScalingMode.AutoScaleEnabled),
                    ScalingMode.AutoScaleEnabled => (false, ScalingMode.AutoScaleEnabled),
                    ScalingMode.AutoScaleDisabled => (true, ScalingMode.AutoScaleEnabled),
                    ScalingMode.RemovingFailedNodes => (false, ScalingMode.WaitingForAutoScale),  // manual intervention
                    ScalingMode.WaitingForAutoScale => (false, ScalingMode.WaitingForAutoScale),
                    ScalingMode.SettingAutoScale => (false, ScalingMode.SettingAutoScale),
                    _ => (true, ScalingMode.Unknown)
                },
                false => _scalingMode switch
                {
                    ScalingMode.Unknown => (false, ScalingMode.RemovingFailedNodes),
                    ScalingMode.AutoScaleEnabled => (false, ScalingMode.RemovingFailedNodes), // manual intervention
                    ScalingMode.AutoScaleDisabled => (false, ScalingMode.AutoScaleDisabled),
                    ScalingMode.RemovingFailedNodes => (false, ScalingMode.RemovingFailedNodes),
                    ScalingMode.WaitingForAutoScale => (true, ScalingMode.RemovingFailedNodes),
                    ScalingMode.SettingAutoScale => (true, ScalingMode.RemovingFailedNodes),
                    _ => (true, ScalingMode.Unknown)
                },
                null => (true, _scalingMode),
            };

            if (failed)
            {
                IsAvailable = false;
                _resetAutoScalingRequired = true;
            }
        }

        private async ValueTask ServicePoolGetResizeErrorsAsync(CancellationToken cancellationToken)
        {
            // This method must only collect error information when allocation state is not Steady. It only obtains resize errors once after each time the allocation state returned to Steady.

            var (allocationState, allocationStateTransitionTime, autoScaleEnabled, _, _, _, _) = await _azureProxy.GetFullAllocationStateAsync(PoolId, cancellationToken);

            if (allocationState == AllocationState.Steady)
            {
                var pool = await _azureProxy.GetBatchPoolAsync(PoolId, cancellationToken, new ODATADetailLevel
                {
                    SelectClause = autoScaleEnabled ?? false
                        ? "id,allocationStateTransitionTime,autoScaleFormula,autoScaleRun,resizeErrors"
                        : "id,allocationStateTransitionTime,resizeErrors"
                });

                if ((autoScaleEnabled ?? false) && pool.AutoScaleRun?.Error is not null)
                {
                    _resetAutoScalingRequired |= true;
                    _logger.LogError(@"Pool {PoolId} Autoscale evaluation resulted in failure({ErrorCode}): '{ErrorMessage}'.", PoolId, pool.AutoScaleRun.Error.Code, pool.AutoScaleRun.Error.Message);
                }
                else if ((autoScaleEnabled ?? false) && pool.AutoScaleRun?.Timestamp < DateTime.UtcNow - (5 * AutoScaleEvaluationInterval)) // It sometimes takes some cycles to reset autoscale, so give batch some time to catch up on its own.
                {
                    _resetAutoScalingRequired |= true;
                    _logger.LogWarning(@"Pool {PoolId} Autoscale evaluation last ran at {AutoScaleRunTimestamp}.", PoolId, pool.AutoScaleRun.Timestamp);
                }

                if (AllocationStateTransitionTime != allocationStateTransitionTime)
                {
                    AllocationStateTransitionTime = allocationStateTransitionTime;

                    ResizeErrors.Clear();

                    foreach (var error in pool.ResizeErrors ?? Enumerable.Empty<ResizeError>())
                    {
                        switch (error.Code)
                        {
                            // Errors to ignore
                            case PoolResizeErrorCodes.RemoveNodesFailed:
                            case PoolResizeErrorCodes.CommunicationEnabledPoolReachedMaxVMCount:
                            case PoolResizeErrorCodes.AllocationTimedOut:
                            case PoolResizeErrorCodes.AccountCoreQuotaReached:
                            case PoolResizeErrorCodes.AccountSpotCoreQuotaReached:
                            case PoolResizeErrorCodes.AccountLowPriorityCoreQuotaReached:
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

          In order to avoid confusion, some of the builtin variable names in batch's autoscale formulas are named in a way that may not initially appear intuitive:
              Running tasks are named RunningTasks, which is fine
              Queued tasks are named ActiveTasks, which matches the same value of the "state" property
              The sum of running & queued tasks (what I would have named TotalTasks) is named PendingTasks

          The type of ~Tasks is what batch calls a "doubleVec", which needs to be first turned into a "doubleVecList" before it can be turned into a scaler.
          This is accomplished by calling doubleVec's GetSample method, which returns some number of the most recent available samples of the related metric.
          Then, a function is used to extract a scaler from the list of scalers (measurements). NOTE: there does not seem to be a "last" function.

          Whenever autoscaling is turned on, whether or not the pool was just created, there are no sampled metrics available. Thus, we need to prevent the
          expected errors that would result from trying to extract the samples. Later on, if recent samples aren't available, we prefer that the formula fails
          (firstly, so we can potentially capture that, and secondly, so that we don't suddenly try to remove all nodes from the pool when there's still demand)
          so we use a timed scheme to substitue an "initial value" (aka initialTarget).

          We set NodeDeallocationOption to taskcompletion to prevent wasting time/money by stopping a running task, only to requeue it onto another node, or worse,
          fail it, just because batch's last sample was taken longer ago than a task's assignment was made to a node, because the formula evaluations intervals are not coordinated
          with the metric sampling intervals based on my observations. This does mean that some resizes will time out, so we mustn't simply consider timeout to be a fatal error.

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
            // This method implements a state machine to disable/enable autoscaling as needed to clear certain conditions that can be observed
            // Inputs are _resetAutoScalingRequired, compute nodes in ejectable states, and the current _scalingMode, along with the pool's
            // allocation state and autoscale enablement.
            // This method must no-op when the allocation state is not Steady.

            var (allocationState, _, autoScaleEnabled, _, _, _, _) = await _azureProxy.GetFullAllocationStateAsync(PoolId, cancellationToken);

            if (allocationState == AllocationState.Steady)
            {
                EnsureScalingModeSet(autoScaleEnabled);

                switch (_scalingMode)
                {
                    case ScalingMode.AutoScaleEnabled:
                        if (_resetAutoScalingRequired || await GetNodesToRemove(false).AnyAsync(cancellationToken))
                        {
                            _logger.LogInformation(@"Switching pool {PoolId} to manual scale to clear resize errors and/or compute nodes in invalid states.", PoolId);
                            await _azureProxy.DisableBatchPoolAutoScaleAsync(PoolId, cancellationToken);
                            _scalingMode = ScalingMode.AutoScaleDisabled;
                        }
                        break;

                    case ScalingMode.AutoScaleDisabled:
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
                                        StartTaskFailures.Enqueue(new(node.Id, node.StartTaskInformation.FailureInformation));
                                        break;

                                    case ComputeNodeState.Preempted:
                                        _logger.LogDebug("Found preempted node {NodeId}", node.Id);
                                        break;

                                    default:
                                        throw new System.Diagnostics.UnreachableException($"Unexpected compute node state '{node.State}' received while looking for nodes to remove from the pool.");
                                }

                                nodesToRemove = nodesToRemove.Append(node);
                            }

                            nodesToRemove = nodesToRemove.ToList();

                            if (nodesToRemove.Any())
                            {
                                await nodesToRemove
                                    .Where(node => ComputeNodeState.StartTaskFailed.Equals(node.State))
                                    .SelectMany<ComputeNode, (ComputeNode Node, string Log)>(node => [(node, "stdout.txt"), (node, "stderr.txt")])
                                    .ForEachAsync((logInfo, token) => TransferStartTaskLogAsync(logInfo.Node, logInfo.Log, token), cancellationToken);
                                await RemoveNodesAsync((IList<ComputeNode>)nodesToRemove, cancellationToken);
                                _resetAutoScalingRequired = false;
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
                        _logger.LogInformation(@"Switching pool {PoolId} back to autoscale.", PoolId);
                        await _azureProxy.EnableBatchPoolAutoScaleAsync(PoolId, !IsDedicated, AutoScaleEvaluationInterval, (p, t) => AutoPoolFormula(p, GetTaskCount(t)), cancellationToken);
                        _autoScaleWaitTime = DateTime.UtcNow + (3 * AutoScaleEvaluationInterval) + (BatchPoolService.RunInterval / 2);
                        _scalingMode = _resetAutoScalingRequired ? ScalingMode.WaitingForAutoScale : ScalingMode.SettingAutoScale;
                        _resetAutoScalingRequired = false;
                        break;

                    case ScalingMode.WaitingForAutoScale:
                        if (DateTime.UtcNow > _autoScaleWaitTime)
                        {
                            _scalingMode = ScalingMode.SettingAutoScale;
                        }
                        break;

                    case ScalingMode.SettingAutoScale:
                        _scalingMode = ScalingMode.AutoScaleEnabled;
                        _logger.LogInformation(@"Pool {PoolId} is back to normal resize and monitoring status.", PoolId);
                        break;
                }

                async ValueTask TransferStartTaskLogAsync(ComputeNode node, string log, CancellationToken cancellationToken)
                {
                    var file = await node.GetNodeFileAsync($"startup/{log}", cancellationToken: cancellationToken);
                    var content = await file.ReadAsStringAsync(cancellationToken: cancellationToken);
                    var blobUri = await _storageAccessProvider.GetInternalTesBlobUrlAsync($"/pools/{PoolId}/nodes/{node.Id}/{log}", cancellationToken);
                    await _azureProxy.UploadBlobAsync(blobUri, content, cancellationToken);
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
                => _azureProxy.ListComputeNodesAsync(PoolId, new ODATADetailLevel(filterClause: @"state eq 'starttaskfailed' or state eq 'preempted' or state eq 'unusable'", selectClause: withState ? @"id,state,startTaskInfo" : @"id"));
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
                // Get current node counts
                var (_, _, _, _, lowPriorityNodes, _, dedicatedNodes) = await _azureProxy.GetFullAllocationStateAsync(PoolId, cancellationToken);

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
        public string PoolId { get; private set; }

        /// <inheritdoc/>
        public async ValueTask<bool> CanBeDeleted(CancellationToken cancellationToken = default)
        {
            if (await GetTasksAsync(includeCompleted: true).AnyAsync(cancellationToken))
            {
                return false;
            }

            await foreach (var node in _azureProxy.ListComputeNodesAsync(PoolId, new ODATADetailLevel(selectClause: "state")).WithCancellation(cancellationToken))
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
        public IBatchPool.StartTaskFailureInformation PopNextStartTaskFailure()
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
                    AggregateException aggregateException => aggregateException.Flatten().InnerExceptions,
                    _ => Enumerable.Empty<Exception>().Append(ex),
                };

            // Returns true to continue to the next action
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
                        return !await RemoveMissingPoolsAsync(ex, cancellationToken);
                    }
                }

                return false;
            }

            // Returns true when pool/job was removed because it was not found. Returns false otherwise.
            ValueTask<bool> RemoveMissingPoolsAsync(Exception ex, CancellationToken cancellationToken)
            {
                return ex switch
                {
                    AggregateException aggregateException => ParseAggregateException(aggregateException, cancellationToken),
                    BatchException batchException => ParseBatchException(batchException, cancellationToken),
                    _ => ParseException(ex, cancellationToken),
                };

                ValueTask<bool> ParseException(Exception exception, CancellationToken cancellationToken)
                {
                    if (exception.InnerException is not null)
                    {
                        return RemoveMissingPoolsAsync(exception.InnerException, cancellationToken);
                    }

                    return ValueTask.FromResult(false);
                }

                async ValueTask<bool> ParseAggregateException(AggregateException aggregateException, CancellationToken cancellationToken)
                {
                    var result = false;

                    foreach (var exception in aggregateException.InnerExceptions)
                    {
                        result |= await RemoveMissingPoolsAsync(exception, cancellationToken);
                    }

                    return result;
                }

                async ValueTask<bool> ParseBatchException(BatchException batchException, CancellationToken cancellationToken)
                {
                    if (batchException.RequestInformation.BatchError.Code == BatchErrorCodeStrings.PoolNotFound ||
                        batchException.RequestInformation.BatchError.Code == BatchErrorCodeStrings.JobNotFound)
                    {
                        _logger.LogError(ex, "Batch pool and/or job {PoolId} is missing. Removing them from TES's active pool list.", PoolId);
                        _ = _batchPools.RemovePoolFromList(this);
                        await _batchPools.DeletePoolAsync(this, cancellationToken); // TODO: Consider moving any remaining tasks to another pool, or failing job/tasks explicitly
                        return true;
                    }

                    return false;
                }
            }
        }

        /// <inheritdoc/>
        public async ValueTask<DateTime> GetAllocationStateTransitionTime(CancellationToken cancellationToken = default)
            => (await _azureProxy.GetBatchPoolAsync(PoolId, cancellationToken, new ODATADetailLevel { SelectClause = "allocationStateTransitionTime" })).AllocationStateTransitionTime ?? DateTime.UtcNow;

        /// <inheritdoc/>
        public async ValueTask CreatePoolAndJobAsync(BatchAccountPoolData poolModel, bool isPreemptible, CancellationToken cancellationToken)
        {
            var jobId = poolModel.Metadata.Single(i => string.IsNullOrEmpty(i.Name)).Value;

            try
            {
                CloudPool pool = default;
                await Task.WhenAll(
                    _azureProxy.CreateBatchJobAsync(jobId, jobId, cancellationToken),
                    Task.Run(async () =>
                    {
                        var poolId = await _batchPoolManager.CreateBatchPoolAsync(poolModel, isPreemptible, cancellationToken);
                        pool = await _azureProxy.GetBatchPoolAsync(poolId, cancellationToken, new ODATADetailLevel { SelectClause = CloudPoolSelectClause });
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
                // When the batch management API creating the pool times out, it may or may not have created the pool.
                // Add an inactive record to delete it if it did get created and try again later. That record will be removed later whether or not the pool was created.
                PoolId ??= jobId;
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

            var broken = pool.Id is null || pool.CreationTime is null || pool.Metadata is null || !pool.Metadata.Any();

            // Immediately remove old-style pools. Tasks will be requeued.
            if (!broken && pool.Metadata.Any(m => BatchScheduler.PoolDeprecated.Equals(m.Name, StringComparison.OrdinalIgnoreCase)))
            {
                PoolId = pool.Id;
                IsAvailable = false;
                await _azureProxy.DeleteBatchJobAsync(pool.Id, cancellationToken);
                return;
            }

            try
            {
                broken |= !pool.Metadata.Any(m => BatchScheduler.PoolMetadata.Equals(m.Name, StringComparison.Ordinal)) ||
                    !IBatchScheduler.PoolMetadata.Create(pool.Metadata.Single(m => BatchScheduler.PoolMetadata.Equals(m.Name, StringComparison.Ordinal)).Value).Validate();
            }
            catch (InvalidOperationException)
            {
                broken = true;
            }
            catch (ArgumentNullException)
            {
                broken = true;
            }
            catch (NotSupportedException)
            {
                broken = true;
            }
            catch (System.Text.Json.JsonException)
            {
                broken = true;
            }

            if (broken)
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

            PoolId = pool.Id;
            IsAvailable = DetermineIsAvailable(pool.CreationTime);
            //IReadOnlyDictionary<string, string> Identity = pool.Identity.UserAssignedIdentities.ToDictionary(identity => identity.ResourceId, identity => identity.ClientId, StringComparer.OrdinalIgnoreCase).AsReadOnly();

            if (IsAvailable)
            {
                Creation = pool.CreationTime.Value;
            }

            IsDedicated = IBatchScheduler.PoolMetadata.Create(pool.Metadata.First(m => BatchScheduler.PoolMetadata.Equals(m.Name, StringComparison.Ordinal)).Value).IsDedicated;
            _ = _batchPools.AddPool(this);
        }
    }

    /// <content>
    /// Used for unit/module testing.
    /// </content>
    public sealed partial class BatchPool
    {
        internal int TestPendingReservationsCount => GetTasksAsync(includeCompleted: false).CountAsync().AsTask().Result;

        internal int? TestTargetDedicated => _azureProxy.GetFullAllocationStateAsync(PoolId, CancellationToken.None).Result.TargetDedicated;
        internal int? TestTargetLowPriority => _azureProxy.GetFullAllocationStateAsync(PoolId, CancellationToken.None).Result.TargetLowPriority;

        internal TimeSpan TestRotatePoolTime
            => _forcePoolRotationAge;

        internal void TestSetAvailable(bool available)
            => IsAvailable = available;

        internal void TimeShift(TimeSpan shift)
            => Creation -= shift;
    }
}
