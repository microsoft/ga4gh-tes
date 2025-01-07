// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.ResourceManager.Models;
using Microsoft.Azure.Batch;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.Models;
using TesApi.Web;
using TesApi.Web.Management;
using TesApi.Web.Management.Batch;
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
            azureProxy.AzureProxyGetComputeNodeAllocationState = id => new(Microsoft.Azure.Batch.Common.AllocationState.Steady, DateTime.MinValue.ToUniversalTime(), true, 0, 0, 1, 1);
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
            azureProxy.AzureProxyGetComputeNodeAllocationState = id => new(Microsoft.Azure.Batch.Common.AllocationState.Steady, DateTime.MinValue.ToUniversalTime(), true, 0, 0, 1, 1);
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
                Assert.AreEqual(poolId, pool.PoolId);
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
                pool.PoolId,
                enableAutoScale: true,
                autoScaleRun: new(DateTime.UtcNow, error: new("ErrorCode", "Message")));

            await pool.ServicePoolAsync(BatchPool.ServiceKind.GetResizeErrors);
            await pool.ServicePoolAsync(BatchPool.ServiceKind.ManagePoolScaling);

            services.AzureProxy.Verify(a => a.DisableBatchPoolAutoScaleAsync(pool.PoolId, It.IsAny<System.Threading.CancellationToken>()));
        }

        [TestMethod] // TODO: delete?
        public async Task ServicePoolGetResizeErrorsResetsAutoScalingWhenBatchStopsEvaluatingAutoScaleAfterQuotaError()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT(), false);
            pool.TimeShift(7 * BatchPool.AutoScaleEvaluationInterval);

            azureProxy.SetPoolState(
                pool.PoolId,
                enableAutoScale: true,
                autoScaleRun: new(DateTime.UtcNow - (6 * BatchPool.AutoScaleEvaluationInterval)));

            await pool.ServicePoolAsync(BatchPool.ServiceKind.GetResizeErrors);
            await pool.ServicePoolAsync(BatchPool.ServiceKind.ManagePoolScaling);

            services.AzureProxy.Verify(a => a.DisableBatchPoolAutoScaleAsync(pool.PoolId, It.IsAny<System.Threading.CancellationToken>()));
        }

        [TestMethod]
        public async Task ServicePoolGetResizeErrorsResetsAutoScalingWhenBatchReturnsAutoscaleEvaluationError()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT(), false);

            azureProxy.SetPoolState(
                pool.PoolId,
                enableAutoScale: true,
                autoScaleRun: new(DateTime.UtcNow, error: new(code: "InsufficientSampleData")));

            await pool.ServicePoolAsync(BatchPool.ServiceKind.GetResizeErrors);
            await pool.ServicePoolAsync(BatchPool.ServiceKind.ManagePoolScaling);

            services.AzureProxy.Verify(a => a.DisableBatchPoolAutoScaleAsync(pool.PoolId, It.IsAny<System.Threading.CancellationToken>()));
        }


        private static TestServices.TestServiceProvider<BatchScheduler> GetServiceProvider(AzureProxyReturnValues azureProxyReturn = default)
        {
            azureProxyReturn ??= AzureProxyReturnValues.Get();
            return new(
                wrapAzureProxy: true,
                configuration: GetMockConfig(),
                azureProxy: PrepareMockAzureProxy(azureProxyReturn),
                batchPoolManager: PrepareMockBatchPoolManager(azureProxyReturn),
                batchQuotaProvider: GetMockQuotaProvider(azureProxyReturn),
                batchSkuInformationProvider: GetMockSkuInfoProvider(azureProxyReturn),
                accountResourceInformation: new("defaultbatchaccount", "defaultresourcegroup", "defaultsubscription", "defaultregion", "defaultendpoint"));
        }

        private static async Task<BatchPool> AddPool(BatchScheduler batchPools, bool isPreemtable)
            => (BatchPool)await batchPools.GetOrAddPoolAsync("key1", isPreemtable, (id, _1) => ValueTask.FromResult(CreatePoolData(name: id, displayName: "display1", vmSize: "vmSize1")), System.Threading.CancellationToken.None);

        internal static Azure.ResourceManager.Batch.BatchAccountPoolData CreatePoolData(string name, string displayName = default, string vmSize = default)
        {
            Azure.ResourceManager.Batch.BatchAccountPoolData result = new() { DisplayName = displayName, VmSize = vmSize };
            result.Metadata.Add(new(string.Empty, name));
            return result;
        }

        private static void TimeShift(TimeSpan shift, BatchPool pool)
            => pool.TimeShift(shift);

        private class AzureProxyReturnValues
        {
            internal static AzureProxyReturnValues Get()
                => new();

            internal AzureBatchAccountQuotas BatchQuotas { get; set; } = new() { PoolQuota = 1, ActiveJobAndJobScheduleQuota = 1, DedicatedCoreQuotaPerVMFamily = [] };
            internal int ActivePoolCount { get; set; } = 0;

            internal Func<string, ODATADetailLevel, IAsyncEnumerable<ComputeNode>> AzureProxyListComputeNodesAsync { get; set; } = (poolId, detailLevel) => AsyncEnumerable.Empty<ComputeNode>();
            internal Action<string, IEnumerable<ComputeNode>, System.Threading.CancellationToken> AzureProxyDeleteBatchComputeNodes { get; set; } = (poolId, computeNodes, cancellationToken) => { };
            internal Func<string, FullBatchPoolAllocationState> AzureProxyGetComputeNodeAllocationState { get; set; } = null;
            internal Action<string, System.Threading.CancellationToken> AzureProxyDeleteBatchPool { get; set; } = (poolId, cancellationToken) => { };
            internal Func<string, ODATADetailLevel, IAsyncEnumerable<CloudTask>> AzureProxyListTasks { get; set; } = (jobId, detailLevel) => AsyncEnumerable.Empty<CloudTask>();
            internal List<VirtualMachineInformation> VmSizesAndPrices { get; set; } = new();

            internal static Func<string, FullBatchPoolAllocationState> AzureProxyGetComputeNodeAllocationStateDefault = id => new(Microsoft.Azure.Batch.Common.AllocationState.Steady, DateTime.MinValue.ToUniversalTime(), true, 0, 0, 0, 0);

            internal bool PoolStateExists(string poolId)
                => poolState.ContainsKey(poolId);

            private record PoolState(int? CurrentDedicatedNodes, int? CurrentLowPriorityNodes, Microsoft.Azure.Batch.Common.AllocationState? AllocationState, DateTime? AllocationStateTransitionTime, Microsoft.Azure.Batch.Protocol.Models.AutoScaleRun AutoScaleRun, DateTime? CreationTime, IList<Microsoft.Azure.Batch.Protocol.Models.ResizeError> ResizeErrors, IList<MetadataItem> PoolMetadata, IList<DataDisk> DataDisks)
            {
                public int? TargetDedicatedNodes { get; set; }
                public int? TargetLowPriorityNodes { get; set; }
                public bool? EnableAutoScale { get; set; }
            }

            private readonly Dictionary<string, PoolState> poolState = [];

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
                DateTime? creationTime = default,
                IList<MetadataItem> poolMetadata = default,
                IList<DataDisk> dataDisks = default)
            {
                if (poolState.TryGetValue(id, out var state))
                {
                    var metadata = state.PoolMetadata?.ToDictionary(p => p.Name, p => p.Value) ?? [];
                    foreach (var meta in poolMetadata ?? [])
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
                        allocationStateTransitionTime ?? state.AllocationStateTransitionTime,
                        autoScaleRun ?? state.AutoScaleRun,
                        creationTime ?? state.CreationTime,
                        resizeErrors ?? state.ResizeErrors,
                        metadata.Count == 0 ? null : metadata.Select(ConvertMetadata).ToList(), dataDisks ?? [])
                    {
                        TargetDedicatedNodes = targetDedicatedNodes ?? state.TargetDedicatedNodes,
                        TargetLowPriorityNodes = targetLowPriorityNodes ?? state.TargetLowPriorityNodes,
                        EnableAutoScale = enableAutoScale ?? state.EnableAutoScale,
                    };

                    static MetadataItem ConvertMetadata(KeyValuePair<string, string> pair)
                        => new(pair.Key, pair.Value);
                }
                else
                {
                    poolState.Add(id, new(currentDedicatedNodes, currentLowPriorityNodes, allocationState, allocationStateTransitionTime, autoScaleRun, creationTime, resizeErrors, poolMetadata, dataDisks ?? [])
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

            internal string CreateBatchPoolImpl(Azure.ResourceManager.Batch.BatchAccountPoolData pool)
            {
                var poolIdItem = pool.Metadata.Single(i => string.IsNullOrEmpty(i.Name));
                pool.Metadata.Remove(poolIdItem);

                poolState.Add(poolIdItem.Value, new(default, default, Microsoft.Azure.Batch.Common.AllocationState.Steady, default, default, default, default, pool.Metadata?.Select(ConvertMetadata).ToList(), (pool.DeploymentVmConfiguration ?? new(new(), "nodeAgentSkuId")).DataDisks.Select(ConvertDataDisk).ToList()) { EnableAutoScale = true });
                return poolIdItem.Value;

                static MetadataItem ConvertMetadata(Azure.ResourceManager.Batch.Models.BatchAccountPoolMetadataItem item)
                    => new(item.Name, item.Value);

                static DataDisk ConvertDataDisk(Azure.ResourceManager.Batch.Models.BatchVmDataDisk disk)
                    => new(disk.Lun, disk.DiskSizeInGB, (Microsoft.Azure.Batch.Common.CachingType)disk.Caching, (Microsoft.Azure.Batch.Common.StorageAccountType)disk.StorageAccountType);
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
            {
                return GeneratePool(
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
                    creationTime: poolState.CreationTime,
                    metadata: poolState.PoolMetadata,
                    dataDisks: poolState.DataDisks);
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
                        batchQuotas.DedicatedCoreQuotaPerVMFamily?.Select(v => new BatchVmCoresPerFamily(v.Name, v.CoreQuota ?? 0)).ToList(),
                        new(batchQuotas.ActiveJobAndJobScheduleQuota, batchQuotas.PoolQuota, batchQuotas.DedicatedCoreQuota, batchQuotas.LowPriorityCoreQuota)));
                quotaProvider.Setup(p =>
                        p.GetVmCoreQuotaAsync(It.Is<bool>(l => l == false), It.IsAny<System.Threading.CancellationToken>()))
                    .ReturnsAsync(new BatchVmCoreQuota(batchQuotas.DedicatedCoreQuota,
                        false,
                        batchQuotas.DedicatedCoreQuotaPerVMFamilyEnforced,
                        batchQuotas.DedicatedCoreQuotaPerVMFamily?.Select(v => new BatchVmCoresPerFamily(v.Name, v.CoreQuota ?? 0)).ToList(),
                        new(batchQuotas.ActiveJobAndJobScheduleQuota, batchQuotas.PoolQuota, batchQuotas.DedicatedCoreQuota, batchQuotas.LowPriorityCoreQuota)));
            });

        private static Action<Mock<IBatchPoolManager>> PrepareMockBatchPoolManager(AzureProxyReturnValues azureProxyReturnValues)
            => azureProxy =>
            {
                azureProxy.Setup(a => a.CreateBatchPoolAsync(It.IsAny<Azure.ResourceManager.Batch.BatchAccountPoolData>(), It.IsAny<bool>(), It.IsAny<System.Threading.CancellationToken>())).Returns((Azure.ResourceManager.Batch.BatchAccountPoolData p, bool _1, System.Threading.CancellationToken _2) => Task.FromResult(azureProxyReturnValues.CreateBatchPoolImpl(p)));
                azureProxy.Setup(a => a.DeleteBatchPoolAsync(It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>())).Callback<string, System.Threading.CancellationToken>((poolId, cancellationToken) => azureProxyReturnValues.AzureProxyDeleteBatchPoolImpl(poolId, cancellationToken)).Returns(Task.CompletedTask);
            };

        private static Action<Mock<IAzureProxy>> PrepareMockAzureProxy(AzureProxyReturnValues azureProxyReturnValues)
            => azureProxy =>
            {
                azureProxy.Setup(a => a.GetActivePoolsAsync(It.IsAny<string>())).Returns(AsyncEnumerable.Empty<CloudPool>());
                azureProxy.Setup(a => a.GetBatchActivePoolCount()).Returns(azureProxyReturnValues.ActivePoolCount);
                azureProxy.Setup(a => a.ListComputeNodesAsync(It.IsAny<string>(), It.IsAny<DetailLevel>())).Returns<string, ODATADetailLevel>((poolId, detailLevel) => azureProxyReturnValues.AzureProxyListComputeNodesAsync(poolId, detailLevel));
                azureProxy.Setup(a => a.ListTasksAsync(It.IsAny<string>(), It.IsAny<DetailLevel>())).Returns<string, ODATADetailLevel>((jobId, detailLevel) => azureProxyReturnValues.AzureProxyListTasks(jobId, detailLevel));
                azureProxy.Setup(a => a.DeleteBatchComputeNodesAsync(It.IsAny<string>(), It.IsAny<IEnumerable<ComputeNode>>(), It.IsAny<System.Threading.CancellationToken>())).Callback<string, IEnumerable<ComputeNode>, System.Threading.CancellationToken>((poolId, computeNodes, cancellationToken) => azureProxyReturnValues.AzureProxyDeleteBatchComputeNodes(poolId, computeNodes, cancellationToken)).Returns(Task.CompletedTask);
                azureProxy.Setup(a => a.GetBatchPoolAsync(It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>(), It.IsAny<DetailLevel>())).Returns((string id, System.Threading.CancellationToken cancellationToken, DetailLevel detailLevel) => Task.FromResult(azureProxyReturnValues.GetBatchPoolImpl(id)));
                azureProxy.Setup(a => a.GetFullAllocationStateAsync(It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>())).Returns((string poolId, System.Threading.CancellationToken _1) => Task.FromResult(GetPoolStateFromSettingStateOrDefault(poolId)));

                FullBatchPoolAllocationState GetPoolStateFromSettingStateOrDefault(string poolId)
                {
                    if (azureProxyReturnValues.AzureProxyGetComputeNodeAllocationState is null)
                    {
                        if (azureProxyReturnValues.PoolStateExists(poolId))
                        {
                            var state = azureProxyReturnValues.GetBatchPoolImpl(poolId);
                            return new(state.AllocationState, state.AllocationStateTransitionTime, state.AutoScaleEnabled, state.TargetLowPriorityComputeNodes, state.CurrentLowPriorityComputeNodes, state.TargetDedicatedComputeNodes, state.CurrentDedicatedComputeNodes);
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
                .Append(("BatchScheduling:PoolRotationForcedDays", "0.000694444"))
                .Append(("BatchScheduling:Prefix", "0123456789"));

        private sealed class MockServiceClient(Microsoft.Azure.Batch.Protocol.IComputeNodeOperations computeNode) : Microsoft.Azure.Batch.Protocol.BatchServiceClient
        {
            private readonly Microsoft.Azure.Batch.Protocol.IComputeNodeOperations computeNode = computeNode ?? throw new ArgumentNullException(nameof(computeNode));
            public override Microsoft.Azure.Batch.Protocol.IComputeNodeOperations ComputeNode => computeNode;
        }

        // Below this line we use reflection and internal details of the Azure libraries in order to generate Mocks of CloudPool and ComputeNode. A newer version of the library is supposed to enable this scenario, so hopefully we can soon drop this code.
        internal static AutoScaleRun GenerateAutoScaleRun(Microsoft.Azure.Batch.Protocol.Models.AutoScaleRunError error = default, string results = default)
        {
            var protocolObject = new Microsoft.Azure.Batch.Protocol.Models.AutoScaleRun(DateTime.UtcNow, results, error);
            var autoScaleRun = (AutoScaleRun)typeof(AutoScaleRun).GetConstructor(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance, default, [typeof(Microsoft.Azure.Batch.Protocol.Models.AutoScaleRun)], default)
                .Invoke([protocolObject]);
            return autoScaleRun;
        }

        internal static CloudJob GenerateJob(string id, Microsoft.Azure.Batch.Protocol.Models.JobState state, Microsoft.Azure.Batch.Protocol.Models.PoolInformation poolInfo)
        {
            var parentClient = CreateMockBatchClient();
            Microsoft.Azure.Batch.Protocol.Models.CloudJob modelJob = new(id: id, state: state, poolInfo: poolInfo);
            var job = (CloudJob)typeof(CloudJob).GetConstructor(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance, default, [typeof(BatchClient), typeof(Microsoft.Azure.Batch.Protocol.Models.CloudJob), typeof(IEnumerable<BatchClientBehavior>)], default)
                .Invoke([parentClient, modelJob, null]);
            return job;
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
            IList<MetadataItem> metadata = default,
            IList<DataDisk> dataDisks = default,
            Microsoft.Azure.Batch.Protocol.Models.BatchPoolIdentity identity = default)
        {
            if (default == creationTime)
            {
                creationTime = DateTime.UtcNow;
            }

            metadata ??= [];

            var parentClient = CreateMockBatchClient();
            Microsoft.Azure.Batch.Protocol.Models.CloudPool modelPool = new(
                id: id,
                creationTime: creationTime,
                allocationState: (Microsoft.Azure.Batch.Protocol.Models.AllocationState)allocationState,
                allocationStateTransitionTime: allocationStateTransitionTime,
                virtualMachineConfiguration: new() { DataDisks = dataDisks?.Select(ConvertDataDisk).ToList() ?? [] },
                resizeErrors: resizeErrors,
                currentDedicatedNodes: currentDedicatedNodes,
                currentLowPriorityNodes: currentLowPriorityNodes,
                targetDedicatedNodes: targetDedicatedNodes,
                targetLowPriorityNodes: targetLowPriorityNodes,
                enableAutoScale: enableAutoScale,
                autoScaleRun: autoScaleRun,
                metadata: metadata.Select(ConvertMetadata).ToList(),
                identity: identity);
            var pool = (CloudPool)typeof(CloudPool).GetConstructor(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance, default, [typeof(BatchClient), typeof(Microsoft.Azure.Batch.Protocol.Models.CloudPool), typeof(IEnumerable<BatchClientBehavior>)], default)
                .Invoke([parentClient, modelPool, null]);
            return pool;

            static Microsoft.Azure.Batch.Protocol.Models.MetadataItem ConvertMetadata(MetadataItem item)
                => item is null ? default : new(item.Name, item.Value);

            static Microsoft.Azure.Batch.Protocol.Models.DataDisk ConvertDataDisk(DataDisk disk)
                => disk is null ? default : new(disk.Lun, disk.DiskSizeGB, (Microsoft.Azure.Batch.Protocol.Models.CachingType)disk.Caching, (Microsoft.Azure.Batch.Protocol.Models.StorageAccountType)disk.StorageAccountType);
        }

        internal static CloudTask GenerateTask(string jobId, string id, DateTime stateTransitionTime = default, Microsoft.Azure.Batch.Protocol.Models.TaskExecutionInformation executionInfo = default)
        {
            if (default == stateTransitionTime)
            {
                stateTransitionTime = DateTime.UtcNow;
            }

            var parentClient = CreateMockBatchClient();
            Microsoft.Azure.Batch.Protocol.Models.CloudTask modelTask = new(id: id, stateTransitionTime: stateTransitionTime, state: Microsoft.Azure.Batch.Protocol.Models.TaskState.Active);
            var task = (CloudTask)typeof(CloudTask).GetConstructor(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance, default, [typeof(BatchClient), typeof(string), typeof(Microsoft.Azure.Batch.Protocol.Models.CloudTask), typeof(IEnumerable<BatchClientBehavior>)], default)
                .Invoke([parentClient, jobId, modelTask, Enumerable.Empty<BatchClientBehavior>()]);
            return task;
        }

        internal static ComputeNode GenerateNode(string poolId, string id, bool isDedicated, bool isIdle, DateTime stateTransitionTime = default)
        {
            if (default == stateTransitionTime)
            {
                stateTransitionTime = DateTime.UtcNow;
            }

            var parentClient = CreateMockBatchClient();
            Microsoft.Azure.Batch.Protocol.Models.ComputeNode modelNode = new(stateTransitionTime: stateTransitionTime, id: id, affinityId: AffinityPrefix + id, isDedicated: isDedicated, state: isIdle ? Microsoft.Azure.Batch.Protocol.Models.ComputeNodeState.Idle : Microsoft.Azure.Batch.Protocol.Models.ComputeNodeState.Running);
            var node = (ComputeNode)typeof(ComputeNode).GetConstructor(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance, default, [typeof(BatchClient), typeof(string), typeof(Microsoft.Azure.Batch.Protocol.Models.ComputeNode), typeof(IEnumerable<BatchClientBehavior>)], default)
                .Invoke([parentClient, poolId, modelNode, null]);
            return node;
        }

        private static BatchClient CreateMockBatchClient()
        {
            Mock<Microsoft.Azure.Batch.Protocol.IComputeNodeOperations> computeNodeOperations = new();
            MockServiceClient batchServiceClient = new(computeNodeOperations.Object);
            var protocolLayer = typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient).Assembly.GetType("Microsoft.Azure.Batch.ProtocolLayer").GetConstructor(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic, null, [typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient)], null)
                .Invoke([batchServiceClient]);
            return (BatchClient)typeof(BatchClient).GetConstructor(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic, null, [typeof(Microsoft.Azure.Batch.Protocol.BatchServiceClient).Assembly.GetType("Microsoft.Azure.Batch.IProtocolLayer")], null)
                .Invoke([protocolLayer]);
        }
    }
}
