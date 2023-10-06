// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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
            azureProxy.AzureProxyGetComputeNodeAllocationState = id => (Microsoft.Azure.Batch.Common.AllocationState.Steady, true, 0, 0, 1, 1);
            azureProxy.AzureProxyListTasks = (jobId, detailLevel) => AsyncEnumerable.Empty<CloudTask>().Append(GenerateTask(jobId, "task1-1"));
            azureProxy.AzureProxyListComputeNodesAsync = (i, d) => AsyncEnumerable.Empty<ComputeNode>();
            var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT(), false);
            TimeShift(pool.TestRotatePoolTime, pool);

            await pool.ServicePoolAsync(BatchPool.ServiceKind.Rotate);

            Assert.IsFalse(pool.IsAvailable);
            Assert.AreNotEqual(0, ((BatchPool)pool).TestTargetDedicated + ((BatchPool)pool).TestTargetLowPriority);
        }

        [TestMethod]
        public async Task RemovePoolIfEmptyDoesNotDeletePoolIfPoolIsAvailable()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyDeleteBatchPool = (poolId, cancellationToken) => Assert.Fail();
            var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT(), false);

            await pool.ServicePoolAsync(BatchPool.ServiceKind.RemovePoolIfEmpty);
        }

        [TestMethod]
        public async Task RemovePoolIfEmptyDoesNotDeletePoolIfPoolHasComputeNodes()
        {
            BatchPool pool = default;
            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyGetComputeNodeAllocationState = id => (Microsoft.Azure.Batch.Common.AllocationState.Steady, true, 0, 0, 1, 1);
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
                Assert.AreEqual(poolId, pool.Id);
                isDeleted = true;
            }
        }

        [TestMethod]
        public async Task ServicePoolGetResizeErrorsResetsAutoScalingWhenBatchStopsEvaluatingAutoScaleAfterAutoScaleFormulaError()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT(), false);

            azureProxy.SetPoolState(
                pool.Id,
                enableAutoScale: true,
                autoScaleRun: new(DateTime.UtcNow, error: new("ErrorCode", "Message")));

            await pool.ServicePoolAsync(BatchPool.ServiceKind.GetResizeErrors);
            await pool.ServicePoolAsync(BatchPool.ServiceKind.ManagePoolScaling);

            services.AzureProxy.Verify(a => a.DisableBatchPoolAutoScaleAsync(pool.Id, It.IsAny<System.Threading.CancellationToken>()));
        }

        [TestMethod]
        public async Task ServicePoolGetResizeErrorsResetsAutoScalingWhenBatchStopsEvaluatingAutoScaleAfterQuotaError()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT(), false);
            pool.TimeShift(7 * BatchPool.AutoScaleEvaluationInterval);

            azureProxy.SetPoolState(
                pool.Id,
                enableAutoScale: true,
                autoScaleRun: new(DateTime.UtcNow - (6 * BatchPool.AutoScaleEvaluationInterval)));

            await pool.ServicePoolAsync(BatchPool.ServiceKind.GetResizeErrors);
            await pool.ServicePoolAsync(BatchPool.ServiceKind.ManagePoolScaling);

            services.AzureProxy.Verify(a => a.DisableBatchPoolAutoScaleAsync(pool.Id, It.IsAny<System.Threading.CancellationToken>()));
        }

        [TestMethod]
        public async Task ServicePoolGetResizeErrorsResetsAutoScalingWhenBatchStopsEvaluatingAutoScaleLongAllocationStateTransitionDelay()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.EvaluateAutoScale = (id, formula) => GenerateAutoScaleRun(error: new());
            var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT(), false);
            pool.TimeShift(12 * BatchPool.AutoScaleEvaluationInterval);

            azureProxy.SetPoolState(
                pool.Id,
                enableAutoScale: true,
                resizeErrors: Enumerable.Repeat<Microsoft.Azure.Batch.Protocol.Models.ResizeError>(new(Microsoft.Azure.Batch.Common.PoolResizeErrorCodes.AccountCoreQuotaReached, "Core quota reached."), 1).ToList(),
                allocationStateTransitionTime: DateTime.UtcNow - (11 * BatchPool.AutoScaleEvaluationInterval));

            await pool.ServicePoolAsync(BatchPool.ServiceKind.GetResizeErrors);
            await pool.ServicePoolAsync(BatchPool.ServiceKind.ManagePoolScaling);

            services.AzureProxy.Verify(a => a.DisableBatchPoolAutoScaleAsync(pool.Id, It.IsAny<System.Threading.CancellationToken>()));
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
            internal Func<string, (Microsoft.Azure.Batch.Common.AllocationState? AllocationState, bool? AutoScaleEnabled, int? TargetLowPriority, int? CurrentLowPriority, int? TargetDedicated, int? CurrentDedicated)> AzureProxyGetComputeNodeAllocationState { get; set; } = null;
            internal Action<string, System.Threading.CancellationToken> AzureProxyDeleteBatchPool { get; set; } = (poolId, cancellationToken) => { };
            internal Func<string, ODATADetailLevel, IAsyncEnumerable<CloudTask>> AzureProxyListTasks { get; set; } = (jobId, detailLevel) => AsyncEnumerable.Empty<CloudTask>();
            internal Func<string, string, Microsoft.Azure.Batch.AutoScaleRun> EvaluateAutoScale { get; set; } //= new((poolId, autoscaleFormula) => AutoScaleRun);
            internal List<VirtualMachineInformation> VmSizesAndPrices { get; set; } = new();

            internal static Func<string, (Microsoft.Azure.Batch.Common.AllocationState? AllocationState, bool? AutoScaleEnabled, int? TargetLowPriority, int? CurrentLowPriority, int? TargetDedicated, int? CurrentDedicated)> AzureProxyGetComputeNodeAllocationStateDefault = id => (Microsoft.Azure.Batch.Common.AllocationState.Steady, true, 0, 0, 0, 0);

            internal bool PoolStateExists(string poolId)
                => poolState.ContainsKey(poolId);

            private record PoolState(int? CurrentDedicatedNodes, int? CurrentLowPriorityNodes, Microsoft.Azure.Batch.Common.AllocationState? AllocationState, Microsoft.Azure.Batch.Protocol.Models.AutoScaleRun AutoScaleRun, DateTime? AllocationStateTransitionTime, IList<Microsoft.Azure.Batch.Protocol.Models.ResizeError> ResizeErrors, IList<Microsoft.Azure.Batch.MetadataItem> PoolMetadata)
            {
                public int? TargetDedicatedNodes { get; set; }
                public int? TargetLowPriorityNodes { get; set; }
                public bool? EnableAutoScale { get; set; }
            }

            private readonly Dictionary<string, PoolState> poolState = new();

            internal void SetPoolState(
                string id,
                int? currentDedicatedNodes = default,
                int? currentLowPriorityNodes = default,
                int? targetDedicatedNodes = default,
                int? targetLowPriorityNodes = default,
                DateTime? allocationStateTransitionTime = default,
                Microsoft.Azure.Batch.Common.AllocationState? allocationState = default,
                IList<Microsoft.Azure.Batch.Protocol.Models.ResizeError> resizeErrors = default,
                Microsoft.Azure.Batch.Protocol.Models.AutoScaleRun autoScaleRun = default,
                bool? enableAutoScale = default,
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

                    poolState[id] = new(
                        currentDedicatedNodes ?? state.CurrentDedicatedNodes,
                        currentLowPriorityNodes ?? state.CurrentLowPriorityNodes,
                        allocationState ?? state.AllocationState,
                        autoScaleRun ?? state.AutoScaleRun,
                        allocationStateTransitionTime ?? state.AllocationStateTransitionTime,
                        resizeErrors ?? state.ResizeErrors,
                        metadata.Count == 0 ? null : metadata.Select(ConvertMetadata).ToList())
                    {
                        TargetDedicatedNodes = targetDedicatedNodes ?? state.TargetDedicatedNodes,
                        TargetLowPriorityNodes = targetLowPriorityNodes ?? state.TargetLowPriorityNodes,
                        EnableAutoScale = enableAutoScale ?? state.EnableAutoScale
                    };

                    static Microsoft.Azure.Batch.MetadataItem ConvertMetadata(KeyValuePair<string, string> pair)
                        => new(pair.Key, pair.Value);
                }
                else
                {
                    poolState.Add(id, new(currentDedicatedNodes, currentLowPriorityNodes, allocationState, autoScaleRun, allocationStateTransitionTime, resizeErrors, poolMetadata)
                        { TargetDedicatedNodes = targetDedicatedNodes, TargetLowPriorityNodes = targetLowPriorityNodes, EnableAutoScale = enableAutoScale });
                }
            }

            internal void AzureProxyDeleteBatchPoolImpl(string poolId, System.Threading.CancellationToken cancellationToken)
            {
                AzureProxyDeleteBatchPool(poolId, cancellationToken);
                _ = poolState.Remove(poolId);
            }

            internal void SetPoolAutoScaleTargets(string id, int? targetDedicatedNodes = default, int? targetLowPriorityNodes = default)
            {
                if (!poolState.TryGetValue(id, out var state))
                {
                    throw new InvalidOperationException("Pool not found");
                }

                state.EnableAutoScale = true;
                state.TargetDedicatedNodes = targetDedicatedNodes ?? state.TargetDedicatedNodes ?? 0;
                state.TargetLowPriorityNodes = targetLowPriorityNodes ?? state.TargetLowPriorityNodes ?? 0;
            }

            internal CloudPool CreateBatchPoolImpl(Pool pool)
            {
                PoolState state = new(
                    CurrentDedicatedNodes: 0,
                    CurrentLowPriorityNodes: 0,
                    AllocationState: Microsoft.Azure.Batch.Common.AllocationState.Steady,
                    AutoScaleRun: default,
                    AllocationStateTransitionTime: default,
                    ResizeErrors: default,
                    PoolMetadata: pool.Metadata?.Select(ConvertMetadata).ToList())
                    {
                        TargetDedicatedNodes = pool.ScaleSettings?.FixedScale?.TargetDedicatedNodes ?? 0,
                        TargetLowPriorityNodes = pool.ScaleSettings?.FixedScale?.TargetLowPriorityNodes ?? 0,
                        EnableAutoScale = pool.ScaleSettings?.AutoScale is not null
                    };

                    poolState.Add(pool.Name, state);

                return GetPoolFromState(pool.Name, state);

                static Microsoft.Azure.Batch.MetadataItem ConvertMetadata(Microsoft.Azure.Management.Batch.Models.MetadataItem item)
                    => new(item.Name, item.Value);
            }

            internal CloudPool GetBatchPoolImpl(string poolId)
            {
                if (!poolState.TryGetValue(poolId, out var state))
                {
                    return GeneratePool(poolId);
                }

                return GetPoolFromState(poolId, state);
            }

            private static CloudPool GetPoolFromState(string poolId, PoolState poolState)
                => GeneratePool(
                    id: poolId,
                    currentDedicatedNodes: poolState.CurrentDedicatedNodes,
                    currentLowPriorityNodes: poolState.CurrentLowPriorityNodes,
                    targetDedicatedNodes: poolState.TargetDedicatedNodes,
                    targetLowPriorityNodes: poolState.TargetLowPriorityNodes,
                    allocationState: poolState.AllocationState,
                    allocationStateTransitionTime: poolState.AllocationStateTransitionTime,
                    resizeErrors: poolState.ResizeErrors,
                    autoScaleRun: poolState.AutoScaleRun,
                    enableAutoScale: poolState.EnableAutoScale,
                    metadata: poolState.PoolMetadata);
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
                azureProxy.Setup(a => a.GetFullAllocationStateAsync(It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>())).Returns((string poolId, System.Threading.CancellationToken _1) => Task.FromResult(GetPoolStateFromSettingStateOrDefault(poolId)));
                azureProxy.Setup(a => a.DeleteBatchPoolAsync(It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>())).Callback<string, System.Threading.CancellationToken>((poolId, cancellationToken) => azureProxyReturnValues.AzureProxyDeleteBatchPoolImpl(poolId, cancellationToken)).Returns(Task.CompletedTask);
                azureProxy.Setup(a => a.EvaluateAutoScaleAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>())).Returns((string poolId, string autoscaleFormula, CancellationToken _1) => Task.FromResult(azureProxyReturnValues.EvaluateAutoScale(poolId, autoscaleFormula)));

                (Microsoft.Azure.Batch.Common.AllocationState? AllocationState, bool? AutoScaleEnabled, int? TargetLowPriority, int? CurrentLowPriority, int? TargetDedicated, int? CurrentDedicated) GetPoolStateFromSettingStateOrDefault(string poolId)
                {
                    if (azureProxyReturnValues.AzureProxyGetComputeNodeAllocationState is null)
                    {
                        if (azureProxyReturnValues.PoolStateExists(poolId))
                        {
                            var state = azureProxyReturnValues.GetBatchPoolImpl(poolId);
                            return (state.AllocationState, state.AutoScaleEnabled, state.TargetLowPriorityComputeNodes, state.CurrentLowPriorityComputeNodes, state.TargetDedicatedComputeNodes, state.CurrentDedicatedComputeNodes);
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
        internal static Microsoft.Azure.Batch.AutoScaleRun GenerateAutoScaleRun(Microsoft.Azure.Batch.Protocol.Models.AutoScaleRunError error = default, string results = default)
        {
            var protocolObject = new Microsoft.Azure.Batch.Protocol.Models.AutoScaleRun(DateTime.UtcNow, results, error);
            var autoScaleRun = (Microsoft.Azure.Batch.AutoScaleRun)typeof(Microsoft.Azure.Batch.AutoScaleRun).GetConstructor(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance, default, new Type[] { typeof(Microsoft.Azure.Batch.Protocol.Models.AutoScaleRun) }, default)
                .Invoke(new object[] { protocolObject });
            return autoScaleRun;
        }



        internal static CloudPool GeneratePool(
            string id,
            int? currentDedicatedNodes = default,
            int? currentLowPriorityNodes = default,
            int? targetDedicatedNodes = default,
            int? targetLowPriorityNodes = default,
            Microsoft.Azure.Batch.Common.AllocationState? allocationState = Microsoft.Azure.Batch.Common.AllocationState.Steady,
            DateTime? allocationStateTransitionTime = default,
            IList<Microsoft.Azure.Batch.Protocol.Models.ResizeError> resizeErrors = default,
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
                resizeErrors: resizeErrors,
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

        internal static CloudTask GenerateTask(string jobId, string id, DateTime stateTransitionTime = default, Microsoft.Azure.Batch.Protocol.Models.TaskExecutionInformation executionInfo = default)
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
            var modelTask = new Microsoft.Azure.Batch.Protocol.Models.CloudTask(id: id, stateTransitionTime: stateTransitionTime, executionInfo: executionInfo, state: Microsoft.Azure.Batch.Protocol.Models.TaskState.Active);
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
