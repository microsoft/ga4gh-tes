// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.Models;
using TesApi.Web;
using TesApi.Web.Management;
using TesApi.Web.Management.Models.Quotas;

namespace TesApi.Tests
{
    [TestClass]
    public class BatchPoolTests
    {
        private const string AffinityPrefix = "AP-";

        [TestMethod]
        public async Task RotateDoesNothingWhenPoolIsNotAvailable()
        {
            var services = GetServiceProvider();
            var pool = await AddPool(services.GetT(), false);
            pool.TestSetAvailable(false);

            await pool.ServicePoolAsync(BatchPool.ServiceKind.Rotate);

            Assert.IsFalse(pool.IsAvailable);
        }

        [TestMethod]
        public async Task RotateMarksPoolUnavailableWhenRotateIntervalHasPassed()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyGetComputeNodeAllocationState = id => new(DateTime.UtcNow, true, 0, 0, 1, 1) { AllocationState = Microsoft.Azure.Batch.Common.AllocationState.Steady };
            azureProxy.AzureProxyListTasks = (jobId, detailLevel) => AsyncEnumerable.Empty<CloudTask>().Append(GenerateTask(jobId, "job1"));
            azureProxy.AzureProxyListComputeNodesAsync = (i, d) => AsyncEnumerable.Empty<ComputeNode>();
            var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT(), false);
            TimeShift(pool.TestRotatePoolTime, pool);

            await pool.ServicePoolAsync(BatchPool.ServiceKind.Rotate);

            Assert.IsFalse(pool.IsAvailable);
            Assert.AreNotEqual(0, pool.TestTargetDedicated + pool.TestTargetLowPriority);
        }

        [TestMethod]
        public async Task RemovePoolIfEmptyDoesNotDeletePoolIfPoolIsAvailable()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyDeleteBatchPool = (poolId, cancellationToken) => Assert.Fail();
            var services = GetServiceProvider(azureProxy);
            var pool = (BatchPool)await AddPool(services.GetT(), false);

            await pool.ServicePoolAsync(BatchPool.ServiceKind.RemovePoolIfEmpty);
        }

        [TestMethod]
        public async Task RemovePoolIfEmptyDoesNotDeletePoolIfPoolHasComputeNodes()
        {
            BatchPool pool = default;
            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyGetComputeNodeAllocationState = id => new(DateTime.UtcNow, true, 0, 0, 1, 1) { AllocationState = Microsoft.Azure.Batch.Common.AllocationState.Steady };
            azureProxy.AzureProxyDeleteBatchPool = (poolId, cancellationToken) => Assert.Fail();
            var services = GetServiceProvider(azureProxy);
            pool = await AddPool(services.GetT(), false);
            pool.TestSetAvailable(false);

            await pool.ServicePoolAsync(BatchPool.ServiceKind.RemovePoolIfEmpty);
        }

        [TestMethod]
        public async Task RemovePoolIfEmptyDeletesPoolIfPoolIsNotAvailableAndHasNoComputeNodes()
        {
            BatchPool pool = default;
            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyDeleteBatchPool = DeletePool;
            var services = GetServiceProvider(azureProxy);
            pool = await AddPool(services.GetT(), false);
            pool.TestSetAvailable(false);
            var isDeleted = false;

            await pool.ServicePoolAsync(BatchPool.ServiceKind.RemovePoolIfEmpty);

            Assert.IsTrue(isDeleted);

            void DeletePool(string poolId, System.Threading.CancellationToken cancellationToken)
            {
                Assert.AreEqual(poolId, pool.Pool.PoolId);
                isDeleted = true;
            }
        }

        [TestMethod]
        public async Task ServicePoolGetResizeErrorsResetsAutoScalingWhenBatchStopsEvaluatingAutoScaleAfterQuotaError()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT(), false);

            azureProxy.SetPoolState(
                pool.Pool.PoolId,
                enableAutoScale: true,
                autoScaleRun: new(DateTime.UtcNow - (6 * BatchPool.AutoScaleEvaluationInterval)));

            await pool.ServicePoolAsync(BatchPool.ServiceKind.GetResizeErrors);
            await pool.ServicePoolAsync(BatchPool.ServiceKind.ManagePoolScaling);

            services.AzureProxy.Verify(a => a.DisableBatchPoolAutoScaleAsync(pool.Pool.PoolId, It.IsAny<System.Threading.CancellationToken>()));
        }


        private static TestServices.TestServiceProvider<BatchScheduler> GetServiceProvider(AzureProxyReturnValues azureProxyReturn = default)
        {
            azureProxyReturn ??= AzureProxyReturnValues.Get();
            return new(
                wrapAzureProxy: true,
                configuration: GetMockConfig(),
                azureProxy: PrepareMockAzureProxy(azureProxyReturn),
                batchQuotaProvider: GetMockQuotaProvider(azureProxyReturn),
                batchSkuInformationProvider: GetMockSkuInfoProvider(azureProxyReturn),
                accountResourceInformation: new("defaultbatchaccount", "defaultresourcegroup", "defaultsubscription", "defaultregion"));
        }

        private static async Task<BatchPool> AddPool(BatchScheduler batchPools, bool isPreemtable)
            => (BatchPool)await batchPools.GetOrAddPoolAsync("key1", isPreemtable, (id, _1) => ValueTask.FromResult(new Pool(name: id, displayName: "display1", vmSize: "vmSize1")), System.Threading.CancellationToken.None);

        private static void TimeShift(TimeSpan shift, BatchPool pool)
            => pool.TimeShift(shift);

        private class AzureProxyReturnValues
        {
            internal static AzureProxyReturnValues Get()
                => new();

            internal AzureBatchAccountQuotas BatchQuotas { get; set; } = new() { PoolQuota = 1, ActiveJobAndJobScheduleQuota = 1, DedicatedCoreQuotaPerVMFamily = new List<VirtualMachineFamilyCoreQuota>() };
            internal int ActivePoolCount { get; set; } = 0;

            internal Func<string, ODATADetailLevel, IAsyncEnumerable<ComputeNode>> AzureProxyListComputeNodesAsync { get; set; } = (poolId, detailLevel) => AsyncEnumerable.Empty<ComputeNode>();
            internal Action<string, IEnumerable<ComputeNode>, System.Threading.CancellationToken> AzureProxyDeleteBatchComputeNodes { get; set; } = (poolId, computeNodes, cancellationToken) => { };
            internal Func<string, AzureBatchPoolAllocationState> AzureProxyGetComputeNodeAllocationState { get; set; } = null;
            internal Action<string, System.Threading.CancellationToken> AzureProxyDeleteBatchPool { get; set; } = (poolId, cancellationToken) => { };
            internal Func<string, ODATADetailLevel, IAsyncEnumerable<CloudTask>> AzureProxyListTasks { get; set; } = (jobId, detailLevel) => AsyncEnumerable.Empty<CloudTask>();
            internal List<VirtualMachineInformation> VmSizesAndPrices { get; set; } = new();

            internal static Func<string, AzureBatchPoolAllocationState> AzureProxyGetComputeNodeAllocationStateDefault = id =>
            {
                var now = DateTime.UtcNow;
                return new(now, true, 0, 0, 0, 0) { AllocationState = Microsoft.Azure.Batch.Common.AllocationState.Steady };
            };

            internal bool PoolStateExists(string poolId)
                => poolState.ContainsKey(poolId);

            private readonly Dictionary<string, (int? CurrentDedicatedNodes, int? CurrentLowPriorityNodes, int? TargetDedicatedNodes, int? TargetLowPriorityNodes, Microsoft.Azure.Batch.Common.AllocationState? AllocationState, DateTime? AllocationStateTransitionTime, Microsoft.Azure.Batch.Protocol.Models.AutoScaleRun AutoScaleRun, bool? EnableAutoScale, DateTime? CreationTime, IList<Microsoft.Azure.Batch.MetadataItem> PoolMetadata)> poolState = new();

            internal void SetPoolState(
                string id,
                int? currentDedicatedNodes = default,
                int? currentLowPriorityNodes = default,
                int? targetDedicatedNodes = default,
                int? targetLowPriorityNodes = default,
                Microsoft.Azure.Batch.Common.AllocationState? allocationState = default,
                DateTime? allocationStateTransitionTime = default,
                Microsoft.Azure.Batch.Protocol.Models.AutoScaleRun autoScaleRun = default,
                bool? enableAutoScale = default,
                DateTime? creationTime = default,
                IList<Microsoft.Azure.Batch.MetadataItem> poolMetadata = default)
            {
                if (poolState.TryGetValue(id, out var state))
                {
                    var metadata = state.PoolMetadata?.ToDictionary(p => p.Name, p => p.Value) ?? new Dictionary<string, string>();
                    foreach (var meta in poolMetadata ?? new List<Microsoft.Azure.Batch.MetadataItem>())
                    {
                        if (metadata.ContainsKey(meta.Name))
                        {
                            metadata[meta.Name] = meta.Value;
                        }
                        else
                        {
                            metadata.Add(meta.Name, meta.Value);
                        }
                    }

                    poolState[id] = (
                        currentDedicatedNodes ?? state.CurrentDedicatedNodes,
                        currentLowPriorityNodes ?? state.CurrentLowPriorityNodes,
                        targetDedicatedNodes ?? state.TargetDedicatedNodes,
                        targetLowPriorityNodes ?? state.TargetLowPriorityNodes,
                        allocationState ?? state.AllocationState,
                        allocationStateTransitionTime ?? state.AllocationStateTransitionTime,
                        autoScaleRun ?? state.AutoScaleRun,
                        enableAutoScale ?? state.EnableAutoScale,
                        creationTime ?? state.CreationTime,
                        metadata.Count == 0 ? null : metadata.Select(ConvertMetadata).ToList());

                    static Microsoft.Azure.Batch.MetadataItem ConvertMetadata(KeyValuePair<string, string> pair)
                        => new(pair.Key, pair.Value);
                }
                else
                {
                    poolState.Add(id, (currentDedicatedNodes, currentLowPriorityNodes, targetDedicatedNodes, targetLowPriorityNodes, allocationState, allocationStateTransitionTime, autoScaleRun, true, creationTime, poolMetadata));
                }
            }

            internal void AzureProxyDeleteBatchPoolImpl(string poolId, System.Threading.CancellationToken cancellationToken)
            {
                AzureProxyDeleteBatchPool(poolId, cancellationToken);
                _ = poolState.Remove(poolId);
            }

            internal PoolInformation CreateBatchPoolImpl(Pool pool)
            {
                var poolId = pool.Name;

                poolState.Add(poolId, (default, default, default, default, Microsoft.Azure.Batch.Common.AllocationState.Steady, default, default, true, default, pool.Metadata?.Select(ConvertMetadata).ToList()));
                return new() { PoolId = poolId };

                static Microsoft.Azure.Batch.MetadataItem ConvertMetadata(Microsoft.Azure.Management.Batch.Models.MetadataItem item)
                    => new(item.Name, item.Value);
            }

            internal CloudPool GetBatchPoolImpl(string poolId)
            {
                if (!poolState.TryGetValue(poolId, out var state))
                {
                    return GeneratePool(poolId);
                }

                return GeneratePool(
                    poolId,
                    currentDedicatedNodes: state.CurrentDedicatedNodes,
                    currentLowPriorityNodes: state.CurrentLowPriorityNodes,
                    targetDedicatedNodes: state.TargetDedicatedNodes,
                    targetLowPriorityNodes: state.TargetLowPriorityNodes,
                    allocationState: state.AllocationState,
                    allocationStateTransitionTime: state.AllocationStateTransitionTime,
                    autoScaleRun: state.AutoScaleRun,
                    enableAutoScale: state.EnableAutoScale,
                    creationTime: state.CreationTime,
                    metadata: state.PoolMetadata);
            }
        }

        private static Action<Mock<IBatchSkuInformationProvider>> GetMockSkuInfoProvider(AzureProxyReturnValues azureProxyReturnValues)
            => new(proxy =>
                proxy.Setup(p => p.GetVmSizesAndPricesAsync(It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>()))
                    .ReturnsAsync(azureProxyReturnValues.VmSizesAndPrices));

        private static Action<Mock<IBatchQuotaProvider>> GetMockQuotaProvider(AzureProxyReturnValues azureProxyReturnValues)
            => new(quotaProvider =>
            {
                var batchQuotas = azureProxyReturnValues.BatchQuotas;
                var vmFamilyQuota = batchQuotas.DedicatedCoreQuotaPerVMFamily?.FirstOrDefault(v => string.Equals(v.Name, "VmFamily1", StringComparison.InvariantCultureIgnoreCase))?.CoreQuota ?? 0;

                quotaProvider.Setup(p =>
                        p.GetQuotaForRequirementAsync(It.IsAny<string>(), It.Is<bool>(p => p == false), It.IsAny<int?>(), It.IsAny<System.Threading.CancellationToken>()))
                    .ReturnsAsync(() => new BatchVmFamilyQuotas(batchQuotas.DedicatedCoreQuota,
                        vmFamilyQuota,
                        batchQuotas.PoolQuota,
                        batchQuotas.ActiveJobAndJobScheduleQuota,
                        batchQuotas.DedicatedCoreQuotaPerVMFamilyEnforced, "VmSize1"));
                quotaProvider.Setup(p =>
                        p.GetQuotaForRequirementAsync(It.IsAny<string>(), It.Is<bool>(p => p == true), It.IsAny<int?>(), It.IsAny<System.Threading.CancellationToken>()))
                    .ReturnsAsync(() => new BatchVmFamilyQuotas(batchQuotas.LowPriorityCoreQuota,
                        vmFamilyQuota,
                        batchQuotas.PoolQuota,
                        batchQuotas.ActiveJobAndJobScheduleQuota,
                        batchQuotas.DedicatedCoreQuotaPerVMFamilyEnforced, "VmSize1"));

                quotaProvider.Setup(p =>
                        p.GetVmCoreQuotaAsync(It.Is<bool>(l => l == true), It.IsAny<System.Threading.CancellationToken>()))
                    .ReturnsAsync(new BatchVmCoreQuota(batchQuotas.LowPriorityCoreQuota,
                        true,
                        batchQuotas.DedicatedCoreQuotaPerVMFamilyEnforced,
                        batchQuotas.DedicatedCoreQuotaPerVMFamily?.Select(v => new BatchVmCoresPerFamily(v.Name, v.CoreQuota)).ToList(),
                        new(batchQuotas.ActiveJobAndJobScheduleQuota, batchQuotas.PoolQuota, batchQuotas.DedicatedCoreQuota, batchQuotas.LowPriorityCoreQuota)));
                quotaProvider.Setup(p =>
                        p.GetVmCoreQuotaAsync(It.Is<bool>(l => l == false), It.IsAny<System.Threading.CancellationToken>()))
                    .ReturnsAsync(new BatchVmCoreQuota(batchQuotas.DedicatedCoreQuota,
                        false,
                        batchQuotas.DedicatedCoreQuotaPerVMFamilyEnforced,
                        batchQuotas.DedicatedCoreQuotaPerVMFamily?.Select(v => new BatchVmCoresPerFamily(v.Name, v.CoreQuota)).ToList(),
                        new(batchQuotas.ActiveJobAndJobScheduleQuota, batchQuotas.PoolQuota, batchQuotas.DedicatedCoreQuota, batchQuotas.LowPriorityCoreQuota)));
            });

        private static Action<Mock<IAzureProxy>> PrepareMockAzureProxy(AzureProxyReturnValues azureProxyReturnValues)
            => azureProxy =>
            {
                azureProxy.Setup(a => a.GetActivePoolsAsync(It.IsAny<string>())).Returns(AsyncEnumerable.Empty<CloudPool>());
                azureProxy.Setup(a => a.GetBatchActivePoolCount()).Returns(azureProxyReturnValues.ActivePoolCount);
                azureProxy.Setup(a => a.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>(), It.IsAny<System.Threading.CancellationToken>())).Returns((Pool p, bool _1, System.Threading.CancellationToken _2) => Task.FromResult(azureProxyReturnValues.CreateBatchPoolImpl(p)));
                azureProxy.Setup(a => a.ListComputeNodesAsync(It.IsAny<string>(), It.IsAny<DetailLevel>())).Returns<string, ODATADetailLevel>((poolId, detailLevel) => azureProxyReturnValues.AzureProxyListComputeNodesAsync(poolId, detailLevel));
                azureProxy.Setup(a => a.ListTasksAsync(It.IsAny<string>(), It.IsAny<DetailLevel>())).Returns<string, ODATADetailLevel>((jobId, detailLevel) => azureProxyReturnValues.AzureProxyListTasks(jobId, detailLevel));
                azureProxy.Setup(a => a.DeleteBatchComputeNodesAsync(It.IsAny<string>(), It.IsAny<IEnumerable<ComputeNode>>(), It.IsAny<System.Threading.CancellationToken>())).Callback<string, IEnumerable<ComputeNode>, System.Threading.CancellationToken>((poolId, computeNodes, cancellationToken) => azureProxyReturnValues.AzureProxyDeleteBatchComputeNodes(poolId, computeNodes, cancellationToken)).Returns(Task.CompletedTask);
                azureProxy.Setup(a => a.GetBatchPoolAsync(It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>(), It.IsAny<DetailLevel>())).Returns((string id, System.Threading.CancellationToken cancellationToken, DetailLevel detailLevel) => Task.FromResult(azureProxyReturnValues.GetBatchPoolImpl(id)));
                azureProxy.Setup(a => a.GetFullAllocationStateAsync(It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>())).Returns((string poolId, System.Threading.CancellationToken _1) =>
                    Task.FromResult(GetPoolStateFromSettingStateOrDefault(poolId)));
                azureProxy.Setup(a => a.DeleteBatchPoolAsync(It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>())).Callback<string, System.Threading.CancellationToken>((poolId, cancellationToken) => azureProxyReturnValues.AzureProxyDeleteBatchPoolImpl(poolId, cancellationToken)).Returns(Task.CompletedTask);

                AzureBatchPoolAllocationState GetPoolStateFromSettingStateOrDefault(string poolId)
                {
                    if (azureProxyReturnValues.AzureProxyGetComputeNodeAllocationState is null)
                    {
                        if (azureProxyReturnValues.PoolStateExists(poolId))
                        {
                            var state = azureProxyReturnValues.GetBatchPoolImpl(poolId);
                            return new(state.AllocationStateTransitionTime, state.AutoScaleEnabled, state.TargetLowPriorityComputeNodes, state.CurrentLowPriorityComputeNodes, state.TargetDedicatedComputeNodes, state.CurrentDedicatedComputeNodes) { AllocationState = state.AllocationState};
                        }
                        else
                        {
                            return AzureProxyReturnValues.AzureProxyGetComputeNodeAllocationStateDefault(poolId);
                        }
                    }
                    else
                    {
                        return azureProxyReturnValues.AzureProxyGetComputeNodeAllocationState(poolId);
                    }
                }
            };

        private static IEnumerable<(string Key, string Value)> GetMockConfig()
            => Enumerable
                .Empty<(string Key, string Value)>()
                .Append(("BatchScheduling:PoolRotationForcedDays", "0.000694444"));

        private sealed class MockServiceClient : Microsoft.Azure.Batch.Protocol.BatchServiceClient
        {
            private readonly Microsoft.Azure.Batch.Protocol.IComputeNodeOperations computeNode;

            public MockServiceClient(Microsoft.Azure.Batch.Protocol.IComputeNodeOperations computeNode)
            {
                this.computeNode = computeNode ?? throw new ArgumentNullException(nameof(computeNode));
            }

            public override Microsoft.Azure.Batch.Protocol.IComputeNodeOperations ComputeNode => computeNode;
        }

        // Below this line we use reflection and internal details of the Azure libraries in order to generate Mocks of CloudPool and ComputeNode. A newer version of the library is supposed to enable this scenario, so hopefully we can soon drop this code.
        internal static CloudPool GeneratePool(
            string id,
            int? currentDedicatedNodes = default,
            int? currentLowPriorityNodes = default,
            int? targetDedicatedNodes = default,
            int? targetLowPriorityNodes = default,
            Microsoft.Azure.Batch.Common.AllocationState? allocationState = Microsoft.Azure.Batch.Common.AllocationState.Steady,
            DateTime? allocationStateTransitionTime = default,
            Microsoft.Azure.Batch.Protocol.Models.AutoScaleRun autoScaleRun = default,
            bool? enableAutoScale = default,
            DateTime? creationTime = default,
            IList<Microsoft.Azure.Batch.MetadataItem> metadata = default)
        {
            if (default == creationTime)
            {
                creationTime = DateTime.UtcNow;
            }

            metadata ??= new List<Microsoft.Azure.Batch.MetadataItem>();

            var computeNodeOperations = new Mock<Microsoft.Azure.Batch.Protocol.IComputeNodeOperations>();
            var batchServiceClient = new MockServiceClient(computeNodeOperations.Object);
            var protocolLayer = typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient).Assembly.GetType("Microsoft.Azure.Batch.ProtocolLayer").GetConstructor(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic, null, new Type[] { typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient) }, null)
                .Invoke(new object[] { batchServiceClient });
            var parentClient = (BatchClient)typeof(BatchClient).GetConstructor(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic, null, new Type[] { typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient).Assembly.GetType("Microsoft.Azure.Batch.IProtocolLayer") }, null)
                .Invoke(new object[] { protocolLayer });
            var modelPool = new Microsoft.Azure.Batch.Protocol.Models.CloudPool(
                id: id,
                currentDedicatedNodes: currentDedicatedNodes,
                currentLowPriorityNodes: currentLowPriorityNodes,
                targetDedicatedNodes: targetDedicatedNodes,
                targetLowPriorityNodes: targetLowPriorityNodes,
                allocationState: (Microsoft.Azure.Batch.Protocol.Models.AllocationState)allocationState,
                allocationStateTransitionTime: allocationStateTransitionTime,
                autoScaleRun: autoScaleRun,
                enableAutoScale: enableAutoScale,
                creationTime: creationTime,
                metadata: metadata.Select(ConvertMetadata).ToList());
            var pool = (CloudPool)typeof(CloudPool).GetConstructor(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance, default, new Type[] { typeof(BatchClient), typeof(Microsoft.Azure.Batch.Protocol.Models.CloudPool), typeof(IEnumerable<BatchClientBehavior>) }, default)
                .Invoke(new object[] { parentClient, modelPool, null });
            return pool;

            static Microsoft.Azure.Batch.Protocol.Models.MetadataItem ConvertMetadata(Microsoft.Azure.Batch.MetadataItem item)
                => item is null ? default : new(item.Name, item.Value);
        }

        internal static CloudTask GenerateTask(string jobId, string id, DateTime stateTransitionTime = default)
        {
            if (default == stateTransitionTime)
            {
                stateTransitionTime = DateTime.UtcNow;
            }

            var computeNodeOperations = new Mock<Microsoft.Azure.Batch.Protocol.IComputeNodeOperations>();
            var batchServiceClient = new MockServiceClient(computeNodeOperations.Object);
            var protocolLayer = typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient).Assembly.GetType("Microsoft.Azure.Batch.ProtocolLayer").GetConstructor(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic, null, new Type[] { typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient) }, null)
                .Invoke(new object[] { batchServiceClient });
            var parentClient = (BatchClient)typeof(BatchClient).GetConstructor(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic, null, new Type[] { typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient).Assembly.GetType("Microsoft.Azure.Batch.IProtocolLayer") }, null)
                .Invoke(new object[] { protocolLayer });
            var modelTask = new Microsoft.Azure.Batch.Protocol.Models.CloudTask(id: id, stateTransitionTime: stateTransitionTime, state: Microsoft.Azure.Batch.Protocol.Models.TaskState.Active);
            var task = (CloudTask)typeof(CloudTask).GetConstructor(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance, default, new Type[] { typeof(BatchClient), typeof(string), typeof(Microsoft.Azure.Batch.Protocol.Models.CloudTask), typeof(IEnumerable<BatchClientBehavior>) }, default)
                .Invoke(new object[] { parentClient, jobId, modelTask, Enumerable.Empty<BatchClientBehavior>() });
            return task;
        }

        internal static ComputeNode GenerateNode(string poolId, string id, bool isDedicated, bool isIdle, DateTime stateTransitionTime = default)
        {
            if (default == stateTransitionTime)
            {
                stateTransitionTime = DateTime.UtcNow;
            }

            var computeNodeOperations = new Mock<Microsoft.Azure.Batch.Protocol.IComputeNodeOperations>();
            var batchServiceClient = new MockServiceClient(computeNodeOperations.Object);
            var protocolLayer = typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient).Assembly.GetType("Microsoft.Azure.Batch.ProtocolLayer").GetConstructor(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic, null, new Type[] { typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient) }, null)
                .Invoke(new object[] { batchServiceClient });
            var parentClient = (BatchClient)typeof(BatchClient).GetConstructor(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic, null, new Type[] { typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient).Assembly.GetType("Microsoft.Azure.Batch.IProtocolLayer") }, null)
                .Invoke(new object[] { protocolLayer });
            var modelNode = new Microsoft.Azure.Batch.Protocol.Models.ComputeNode(stateTransitionTime: stateTransitionTime, id: id, affinityId: AffinityPrefix + id, isDedicated: isDedicated, state: isIdle ? Microsoft.Azure.Batch.Protocol.Models.ComputeNodeState.Idle : Microsoft.Azure.Batch.Protocol.Models.ComputeNodeState.Running);
            var node = (ComputeNode)typeof(ComputeNode).GetConstructor(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance, default, new Type[] { typeof(BatchClient), typeof(string), typeof(Microsoft.Azure.Batch.Protocol.Models.ComputeNode), typeof(IEnumerable<BatchClientBehavior>) }, default)
                .Invoke(new object[] { parentClient, poolId, modelNode, null });
            return node;
        }
    }
}
