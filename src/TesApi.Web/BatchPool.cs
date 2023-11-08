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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using static TesApi.Web.IBatchPool;

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
        private readonly Storage.IStorageAccessProvider _storageAccessProvider;

        /// <summary>
        /// Constructor of <see cref="BatchPool"/>.
        /// </summary>
        /// <param name="batchScheduler"></param>
        /// <param name="batchSchedulingOptions"></param>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        /// <param name="storageAccessProvider"></param>
        /// <exception cref="ArgumentException"></exception>
        public BatchPool(IBatchScheduler batchScheduler, IOptions<Options.BatchSchedulingOptions> batchSchedulingOptions, IAzureProxy azureProxy, ILogger<BatchPool> logger, Storage.IStorageAccessProvider storageAccessProvider)
        {
            _storageAccessProvider = storageAccessProvider;
            var rotationDays = batchSchedulingOptions.Value.PoolRotationForcedDays;
            if (rotationDays == 0) { rotationDays = Options.BatchSchedulingOptions.DefaultPoolRotationForcedDays; }
            _forcePoolRotationAge = TimeSpan.FromDays(rotationDays);

            this._azureProxy = azureProxy;
            this._logger = logger;
            _batchPools = batchScheduler as BatchScheduler ?? throw new ArgumentException("batchScheduler must be of type BatchScheduler", nameof(batchScheduler));
        }

        private Queue<TaskFailureInformation> StartTaskFailures { get; } = new();
        private Queue<ResizeError> ResizeErrors { get; } = new();

        private IAsyncEnumerable<CloudTask> GetTasksAsync(string select, string filter, string expand)
            => _removedFromService ? AsyncEnumerable.Empty<CloudTask>() : _azureProxy.ListTasksAsync(Id, new ODATADetailLevel { SelectClause = select, FilterClause = filter, ExpandClause = expand });

        internal IAsyncEnumerable<CloudTask> GetTasksAsync(bool includeCompleted)
            => GetTasksAsync("id,stateTransitionTime", includeCompleted ? default : "state ne 'completed'", null);

        private async ValueTask RemoveNodesAsync(IList<ComputeNode> nodesToRemove, CancellationToken cancellationToken)
        {
            _logger.LogDebug("Removing {Nodes} nodes from {PoolId}", nodesToRemove.Count, Id);
            await _azureProxy.DeleteBatchComputeNodesAsync(Id, nodesToRemove, cancellationToken);
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
                IsAvailable = false; // TODO: Move 'active' (aka queued) tasks from this pool's job to another pool's job with the same key
                _resetAutoScalingRequired = true;
            }
        }

        private async ValueTask ServicePoolGetResizeErrorsAsync(CancellationToken cancellationToken)
        {
            // This method must only collect error information when allocation state is not Steady. It only obtains resize errors once after each time the allocation state returned to Steady.
            var (allocationState, allocationStateTransitionTime, autoScaleEnabled, _, _, _, _) = await _azureProxy.GetFullAllocationStateAsync(Id, cancellationToken);

            if (allocationState == AllocationState.Steady)
            {
                var pool = await _azureProxy.GetBatchPoolAsync(Id, cancellationToken, new ODATADetailLevel
                {
                    SelectClause = autoScaleEnabled ?? false
                        ? "id,allocationStateTransitionTime,autoScaleRun,resizeErrors"
                        : "id,allocationStateTransitionTime,resizeErrors",
                    ExpandClause = autoScaleEnabled ?? false
                        ? "autoScaleRun,resizeErrors"
                        : "resizeErrors",
                });

                if ((autoScaleEnabled ?? false) && pool.AutoScaleRun?.Error is not null)
                {
                    _resetAutoScalingRequired |= true;
                    _logger.LogError(@"Pool {PoolId} Autoscale evaluation resulted in failure({ErrorCode}): '{ErrorMessage}'.", Id, pool.AutoScaleRun.Error.Code, pool.AutoScaleRun.Error.Message);
                }
                else if ((autoScaleEnabled ?? false) && pool.AutoScaleRun?.Timestamp < DateTime.UtcNow - (5 * AutoScaleEvaluationInterval)) // It sometimes takes some cycles to reset autoscale, so give batch some time to catch up on its own.
                {
                    _resetAutoScalingRequired |= true;
                    _logger.LogWarning(@"Pool {PoolId} Autoscale evaluation last ran at {AutoScaleRunTimestamp}.", Id, pool.AutoScaleRun.Timestamp);
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

          Whenever autoscaling is turned on, whether or not the pool waw just created, there are no sampled metrics available. Thus, we need to prevent the
          expected errors that would result from trying to extract the samples. Later on, if recent samples aren't available, we prefer that the formula fails
          (1- so we can potentially capture that, and 2- so that we don't suddenly try to remove all nodes from the pool when there's still demand) so we use a
          timed scheme to substitue an "initial value" (aka initialTarget).

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
            var nodeList = await GetNodesToRemove(false).ToDictionaryAsync(node => node.Id, cancellationToken: cancellationToken);
            await foreach (var task in _azureProxy.ListTasksAsync(Id, new ODATADetailLevel { SelectClause = "id,executionInfo,nodeInfo", ExpandClause = "executionInfo,nodeInfo" }).WithCancellation(cancellationToken))
            {
                var nodeId = task.ComputeNodeInformation?.ComputeNodeId;
                if (nodeId is not null && nodeList.ContainsKey(nodeId))
                {
                    await _azureProxy.UploadBlobAsync(
                        new Uri(await _storageAccessProvider.GetInternalTesBlobUrlAsync(
                        $"nodeError/{nodeId}/{task.Id}-{new Guid():N}",
                        Azure.Storage.Sas.BlobSasPermissions.Create,
                        cancellationToken)),
                        System.Text.Json.JsonSerializer.Serialize(task.ExecutionInformation,
                        new System.Text.Json.JsonSerializerOptions()
                        {
                            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingDefault,
                            Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter(System.Text.Json.JsonNamingPolicy.CamelCase) }
                        }),
                        cancellationToken);
                }
            }

            // This method implememts a state machine to disable/enable autoscaling as needed to clear certain conditions that can be observed
            // Inputs are _resetAutoScalingRequired, compute nodes in ejectable states, and the current _scalingMode, along with the pool's
            // allocation state and autoscale enablement.
            // This method must no-op when the allocation state is not Steady.

            var (allocationState, _, autoScaleEnabled, _, _, _, _) = await _azureProxy.GetFullAllocationStateAsync(Id, cancellationToken);

            if (allocationState == AllocationState.Steady)
            {
                EnsureScalingModeSet(autoScaleEnabled);

                switch (_scalingMode)
                {
                    case ScalingMode.AutoScaleEnabled when autoScaleEnabled != true:
                        _scalingMode = ScalingMode.RemovingFailedNodes;
                        break;

                    case ScalingMode.AutoScaleEnabled when autoScaleEnabled == true:
                        if (_resetAutoScalingRequired || await GetNodesToRemove(false).AnyAsync(cancellationToken))
                        {
                            _logger.LogInformation(@"Switching pool {PoolId} to manual scale to clear resize errors and/or compute nodes in invalid states.", Id);
                            await _azureProxy.DisableBatchPoolAutoScaleAsync(Id, cancellationToken);
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
                                        await SendNodeTaskInformation(node.Id, node.RecentTasks);
                                        //node.RecentTasks[0].ExecutionInformation.FailureInformation.Code == TaskFailureInformationCodes.DiskFull
                                        // TODO: notify running tasks that task will switch nodes?
                                        break;

                                    case ComputeNodeState.StartTaskFailed:
                                        _logger.LogDebug("Found starttaskfailed node {NodeId}", node.Id);
                                        StartTaskFailures.Enqueue(node.StartTaskInformation.FailureInformation);
                                        break;

                                    case ComputeNodeState.Preempted:
                                        _logger.LogDebug("Found preempted node {NodeId}", node.Id);
                                        await SendNodeTaskInformation(node.Id, node.RecentTasks);
                                        //node.RecentTasks[0].TaskId
                                        //node.RecentTasks[0].ExecutionInformation.FailureInformation.Category == ErrorCategory.ServerError
                                        // TODO: notify running tasks that task will switch nodes? Or, in the future, terminate the task?
                                        break;

                                    default: // Should never reach here. Skip.
                                        continue;
                                }

                                nodesToRemove = nodesToRemove.Append(node);
                            }

                            nodesToRemove = nodesToRemove.ToList();

                            if (nodesToRemove.Any())
                            {
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
                        _logger.LogInformation(@"Switching pool {PoolId} back to autoscale.", Id);
                        await _azureProxy.EnableBatchPoolAutoScaleAsync(Id, !IsDedicated, AutoScaleEvaluationInterval, AutoPoolFormula, GetTaskCountAsync, cancellationToken);
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
                        _logger.LogInformation(@"Pool {PoolId} is back to normal resize and monitoring status.", Id);
                        break;
                }

                async ValueTask<int> GetTaskCountAsync(int @default) // Used to make reenabling auto-scale more performant by attempting to gather the current number of "pending" tasks, falling back on the current target.
                {
                    try
                    {
                        return await GetTasksAsync(includeCompleted: false).CountAsync(cancellationToken);
                    }
                    catch
                    {
                        return @default;
                    }
                }
            }

            IAsyncEnumerable<ComputeNode> GetNodesToRemove(bool withState)
                => _azureProxy.ListComputeNodesAsync(Id, new ODATADetailLevel(filterClause: @"state eq 'starttaskfailed' or state eq 'preempted' or state eq 'unusable'", selectClause: withState ? @"id,recentTasks,state,startTaskInfo" : @"id"));

            async Task SendNodeTaskInformation(string nodeId, IReadOnlyList<TaskInformation> content)
            {
                var url = new Uri(await _storageAccessProvider.GetInternalTesBlobUrlAsync(
                    $"nodeError/{nodeId}-{new Guid():N}",
                    Azure.Storage.Sas.BlobSasPermissions.Create,
                    cancellationToken));

                if (content is null || content!.Any())
                {
                    await _azureProxy.UploadBlobAsync(url, "No recent tasks found on node.", cancellationToken);
                }
                else
                {
                    await _azureProxy.UploadBlobAsync(
                        url,
                        System.Text.Json.JsonSerializer.Serialize(content,
                        new System.Text.Json.JsonSerializerOptions()
                        {
                            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingDefault,
                            Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter(System.Text.Json.JsonNamingPolicy.CamelCase) }
                        }),
                        cancellationToken);
                }
            }
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
                var (_, _, _, _, lowPriorityNodes, _, dedicatedNodes) = await _azureProxy.GetFullAllocationStateAsync(Id, cancellationToken);

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
        private bool _removedFromService = false;

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
        public string Id { get; private set; }

        /// <inheritdoc/>
        public async ValueTask<bool> CanBeDeletedAsync(CancellationToken cancellationToken = default)
        {
            if (_removedFromService)
            {
                return true;
            }

            if (await GetTasksAsync(includeCompleted: true).AnyAsync(cancellationToken))
            {
                return false;
            }

            await foreach (var node in _azureProxy.ListComputeNodesAsync(Id, new ODATADetailLevel(selectClause: "state")).WithCancellation(cancellationToken))
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
                if (!_removedFromService)
                {
                    await func(cancellationToken);
                }
            }
            finally
            {
                _ = lockObj.Release();
            }
        }

        /// <inheritdoc/>
        public async ValueTask ServicePoolAsync(CancellationToken cancellationToken)
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
                        return await RemoveMissingPoolsAsync(ex, cancellationToken);
                    }
                }

                return false;
            }

            // Returns false when pool/job was removed because it was not found. Returns true otherwise.
            async ValueTask<bool> RemoveMissingPoolsAsync(Exception ex, CancellationToken cancellationToken)
            {
                switch (ex)
                {
                    case AggregateException aggregateException:
                        var result = true;
                        foreach (var e in aggregateException.InnerExceptions)
                        {
                            result &= await RemoveMissingPoolsAsync(e, cancellationToken);
                        }
                        return result;

                    case BatchException batchException:
                        if (batchException.RequestInformation.BatchError.Code == BatchErrorCodeStrings.PoolNotFound ||
                            batchException.RequestInformation.BatchError.Code == BatchErrorCodeStrings.JobNotFound)
                        {
                            _logger.LogError(ex, "Batch pool and/or job {PoolId} is missing. Removing them from TES's active pool list.", Id);
                            _ = _batchPools.RemovePoolFromList(this);
                            await _batchPools.DeletePoolAsync(this, cancellationToken);
                            return false;
                        }
                        break;
                }
                return true;
            }
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudTaskBatchTaskState> GetTaskResizeFailuresAsync(CancellationToken cancellationToken)
        {
            return GetTasksAsync("id", "state eq 'active'", null).Zip(
                GetFailures(cancellationToken),
                (cloud, state) => new CloudTaskBatchTaskState(cloud.Id, state));

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
            async IAsyncEnumerable<AzureBatchTaskState> GetFailures([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
            {
                cancellationToken.ThrowIfCancellationRequested();

                for (var failure = PopNextStartTaskFailure(); failure is not null; failure = PopNextStartTaskFailure())
                {
                    yield return ConvertFromStartTask(failure);
                    cancellationToken.ThrowIfCancellationRequested();
                }

                cancellationToken.ThrowIfCancellationRequested();

                for (var failure = PopNextResizeError(); failure is not null; failure = PopNextResizeError())
                {
                    yield return ConvertFromResize(failure);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            }

            AzureBatchTaskState ConvertFromResize(ResizeError failure)
                => new(AzureBatchTaskState.TaskState.NodeAllocationFailed, Failure: new(failure.Code, Enumerable.Empty<string>()
                    .Append(failure.Message)
                    .Concat(failure.Values.Select(t => t.Value))));

            AzureBatchTaskState ConvertFromStartTask(TaskFailureInformation failure)
                => new(AzureBatchTaskState.TaskState.NodeFailedDuringStartupOrExecution, Failure: new(failure.Code, Enumerable.Empty<string>()
                    .Append(failure.Message)
                    .Append($"Start task failed ({failure.Category})")
                    .Concat(failure.Details.Select(t => t.Value))));

            ResizeError PopNextResizeError()
                => ResizeErrors.TryDequeue(out var resizeError) ? resizeError : default;

            TaskFailureInformation PopNextStartTaskFailure()
                => StartTaskFailures.TryDequeue(out var failure) ? failure : default;
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudTask> GetCompletedTasksAsync(CancellationToken _1)
            => GetTasksAsync("id,executionInfo", $"state eq 'completed' and stateTransitionTime lt DateTime'{DateTime.UtcNow - TimeSpan.FromMinutes(2):O}'", "executionInfo");

        /// <inheritdoc/>
        public async ValueTask<DateTime> GetAllocationStateTransitionTimeAsync(CancellationToken cancellationToken = default)
            => (await _azureProxy.GetFullAllocationStateAsync(Id, cancellationToken)).AllocationStateTransitionTime ?? DateTime.UtcNow;

        /// <inheritdoc/>
        public async ValueTask CreatePoolAndJobAsync(Microsoft.Azure.Management.Batch.Models.Pool poolModel, bool isPreemptible, CancellationToken cancellationToken)
        {
            try
            {
                CloudPool pool = default;
                await Task.WhenAll(
                    _azureProxy.CreateBatchJobAsync(poolModel.Name, cancellationToken),
                    Task.Run(async () => pool = await _azureProxy.CreateBatchPoolAsync(poolModel, isPreemptible, cancellationToken), cancellationToken));

                Configure(pool, false);
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
                Id ??= poolModel.Name;
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
        public async ValueTask AssignPoolAsync(CloudPool pool, bool forceRemove, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(pool);

            if (pool.Id is null || pool.CreationTime is null || pool.Metadata is null || !pool.Metadata.Any(m => BatchScheduler.PoolHostName.Equals(m.Name, StringComparison.Ordinal)) || !pool.Metadata.Any(m => BatchScheduler.PoolIsDedicated.Equals(m.Name, StringComparison.Ordinal)))
            {
                throw new ArgumentException("CloudPool is either not configured correctly or was not retrieved with all required metadata.", nameof(pool));
            }

            // Pool is "broken" if its associated job is missing/not active. Reject this pool via the side effect of the exception that is thrown.
            var job = (await _azureProxy.GetBatchJobAsync(pool.Id, cancellationToken, new ODATADetailLevel { SelectClause = "poolInfo,state" }));
            if (job.State != JobState.Active || !pool.Id.Equals(job.PoolInformation?.PoolId, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Active Job not found for Pool {pool.Id}");
            }

            Configure(pool, forceRemove);
        }

        private void Configure(CloudPool pool, bool forceRemove)
        {
            ArgumentNullException.ThrowIfNull(pool);

            Id = pool.Id;
            IsAvailable = !forceRemove && DetermineIsAvailable(pool.CreationTime);

            if (IsAvailable)
            {
                Creation = pool.CreationTime.Value;
            }

            IsDedicated = bool.Parse(pool.Metadata.First(m => BatchScheduler.PoolIsDedicated.Equals(m.Name, StringComparison.Ordinal)).Value);
            _ = _batchPools.AddPool(this);
        }

        /// <inheritdoc/>
        public void MarkRemovedFromService()
        {
            _removedFromService = true;
        }
    }

    /// <content>
    /// Used for unit/module testing.
    /// </content>
    public sealed partial class BatchPool
    {
        internal int TestPendingReservationsCount => GetTasksAsync(includeCompleted: false).CountAsync().AsTask().Result;

        internal int? TestTargetDedicated => _azureProxy.GetFullAllocationStateAsync(Id, CancellationToken.None).Result.TargetDedicated;
        internal int? TestTargetLowPriority => _azureProxy.GetFullAllocationStateAsync(Id, CancellationToken.None).Result.TargetLowPriority;

        internal TimeSpan TestRotatePoolTime
            => _forcePoolRotationAge;

        internal void TestSetAvailable(bool available)
            => IsAvailable = available;

        internal void TimeShift(TimeSpan shift)
            => Creation -= shift;
    }
}
