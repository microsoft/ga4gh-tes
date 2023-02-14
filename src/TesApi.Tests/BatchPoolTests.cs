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
            ((BatchPool)pool).TestSetAvailable(false);

            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Rotate);

            Assert.IsFalse(pool.IsAvailable);
        }

        [TestMethod]
        public async Task RotateMarksPoolUnavailableWhenRotateIntervalHasPassed()
        {
            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyGetComputeNodeAllocationState = id => (Microsoft.Azure.Batch.Common.AllocationState.Steady, true, 0, 0, 1, 1);
            azureProxy.AzureProxyListTasks = (jobId, detailLevel) => AsyncEnumerable.Empty<CloudTask>().Append(GenerateTask(jobId, "job1"));
            azureProxy.AzureProxyListComputeNodesAsync = (i, d) => AsyncEnumerable.Empty<ComputeNode>();
            var services = GetServiceProvider(azureProxy);
            var pool = await AddPool(services.GetT(), false);
            TimeShift(((BatchPool)pool).TestRotatePoolTime, pool);

            await pool.ServicePoolAsync(IBatchPool.ServiceKind.Rotate);

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

            await pool.ServicePoolAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty);
        }

        [TestMethod]
        public async Task RemovePoolIfEmptyDoesNotDeletePoolIfPoolHasComputeNodes()
        {
            IBatchPool pool = default;
            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyGetComputeNodeAllocationState = id => (Microsoft.Azure.Batch.Common.AllocationState.Steady, true, 0, 0, 1, 1);
            azureProxy.AzureProxyDeleteBatchPool = (poolId, cancellationToken) => Assert.Fail();
            var services = GetServiceProvider(azureProxy);
            pool = await AddPool(services.GetT(), false);
            ((BatchPool)pool).TestSetAvailable(false);

            await pool.ServicePoolAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty);
        }

        [TestMethod]
        public async Task RemovePoolIfEmptyDeletesPoolIfPoolIsNotAvailableAndHasNoComputeNodes()
        {
            IBatchPool pool = default;
            var azureProxy = AzureProxyReturnValues.Get();
            azureProxy.AzureProxyDeleteBatchPool = DeletePool;
            var services = GetServiceProvider(azureProxy);
            pool = await AddPool(services.GetT(), false);
            ((BatchPool)pool).TestSetAvailable(false);
            var isDeleted = false;

            await pool.ServicePoolAsync(IBatchPool.ServiceKind.RemovePoolIfEmpty);

            Assert.IsTrue(isDeleted);

            void DeletePool(string poolId, CancellationToken cancellationToken)
            {
                Assert.AreEqual(poolId, pool.Pool.PoolId);
                isDeleted = true;
            }
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
                accountResourceInformation: new("defaultbatchaccount", "defaultresourcegroup", "defaultsubscription", "defaultregion"),
                batchPoolRepositoryArgs: ("endpoint", "key", "databaseId", "containerId", "partitionKeyValue"));
        }

        private static async Task<IBatchPool> AddPool(BatchScheduler batchPools, bool isPreemtable)
            => await batchPools.GetOrAddPoolAsync("key1", isPreemtable, id => ValueTask.FromResult(new Pool(name: id, displayName: "display1", vmSize: "vmSize1")));

        private static void TimeShift(TimeSpan shift, IBatchPool pool)
            => ((BatchPool)pool).TimeShift(shift);

        private class AzureProxyReturnValues
        {
            internal static AzureProxyReturnValues Get()
                => new();

            internal AzureBatchAccountQuotas BatchQuotas { get; set; } = new() { PoolQuota = 1, ActiveJobAndJobScheduleQuota = 1, DedicatedCoreQuotaPerVMFamily = new List<VirtualMachineFamilyCoreQuota>() };
            internal int ActivePoolCount { get; set; } = 0;

            internal Func<string, ODATADetailLevel, IAsyncEnumerable<ComputeNode>> AzureProxyListComputeNodesAsync { get; set; } = (poolId, detailLevel) => AsyncEnumerable.Empty<ComputeNode>();
            internal Action<string, IEnumerable<ComputeNode>, CancellationToken> AzureProxyDeleteBatchComputeNodes { get; set; } = (poolId, computeNodes, cancellationToken) => { };
            internal Func<string, (Microsoft.Azure.Batch.Common.AllocationState? AllocationState, bool? AutoScaleEnabled, int? TargetLowPriority, int? CurrentLowPriority, int? TargetDedicated, int? CurrentDedicated)> AzureProxyGetComputeNodeAllocationState { get; set; } = id => (Microsoft.Azure.Batch.Common.AllocationState.Steady, true, 0, 0, 0, 0);
            internal Action<string, CancellationToken> AzureProxyDeleteBatchPool { get; set; } = (poolId, cancellationToken) => { };
            internal Func<string, ODATADetailLevel, IAsyncEnumerable<CloudTask>> AzureProxyListTasks { get; set; } = (jobId, detailLevel) => AsyncEnumerable.Empty<CloudTask>();
            internal List<VirtualMachineInformation> VmSizesAndPrices { get; set; } = new();

            private readonly Dictionary<string, IList<Microsoft.Azure.Batch.MetadataItem>> poolMetadata = new();

            internal void AzureProxyDeleteBatchPoolImpl(string poolId, CancellationToken cancellationToken)
            {
                _ = poolMetadata.Remove(poolId);
                AzureProxyDeleteBatchPool(poolId, cancellationToken);
            }

            internal PoolInformation CreateBatchPoolImpl(Pool pool)
            {
                var poolId = pool.Name;

                poolMetadata.Add(poolId, pool.Metadata?.Select(Convert).ToList());
                return new() { PoolId = poolId };

                static Microsoft.Azure.Batch.MetadataItem Convert(Microsoft.Azure.Management.Batch.Models.MetadataItem item)
                    => new(item.Name, item.Value);
            }

            internal CloudPool GetBatchPoolImpl(string poolId)
            {
                if (!poolMetadata.TryGetValue(poolId, out var items))
                {
                    items = null;
                }

                return GeneratePool(poolId, metadata: items);
            }
        }

        private static Action<Mock<IBatchSkuInformationProvider>> GetMockSkuInfoProvider(AzureProxyReturnValues azureProxyReturnValues)
            => new(proxy =>
                proxy.Setup(p => p.GetVmSizesAndPricesAsync(It.IsAny<string>()))
                    .ReturnsAsync(azureProxyReturnValues.VmSizesAndPrices));

        private static Action<Mock<IBatchQuotaProvider>> GetMockQuotaProvider(AzureProxyReturnValues azureProxyReturnValues)
            => new(quotaProvider =>
            {
                var batchQuotas = azureProxyReturnValues.BatchQuotas;
                var vmFamilyQuota = batchQuotas.DedicatedCoreQuotaPerVMFamily?.FirstOrDefault(v => string.Equals(v.Name, "VmFamily1", StringComparison.InvariantCultureIgnoreCase))?.CoreQuota ?? 0;

                quotaProvider.Setup(p =>
                        p.GetQuotaForRequirementAsync(It.IsAny<string>(), It.Is<bool>(p => p == false), It.IsAny<int?>()))
                    .ReturnsAsync(() => new BatchVmFamilyQuotas(batchQuotas.DedicatedCoreQuota,
                        vmFamilyQuota,
                        batchQuotas.PoolQuota,
                        batchQuotas.ActiveJobAndJobScheduleQuota,
                        batchQuotas.DedicatedCoreQuotaPerVMFamilyEnforced, "VmSize1"));
                quotaProvider.Setup(p =>
                        p.GetQuotaForRequirementAsync(It.IsAny<string>(), It.Is<bool>(p => p == true), It.IsAny<int?>()))
                    .ReturnsAsync(() => new BatchVmFamilyQuotas(batchQuotas.LowPriorityCoreQuota,
                        vmFamilyQuota,
                        batchQuotas.PoolQuota,
                        batchQuotas.ActiveJobAndJobScheduleQuota,
                        batchQuotas.DedicatedCoreQuotaPerVMFamilyEnforced, "VmSize1"));

                quotaProvider.Setup(p =>
                        p.GetVmCoreQuotaAsync(It.Is<bool>(l => l == true)))
                    .ReturnsAsync(new BatchVmCoreQuota(batchQuotas.LowPriorityCoreQuota,
                        true,
                        batchQuotas.DedicatedCoreQuotaPerVMFamilyEnforced,
                        batchQuotas.DedicatedCoreQuotaPerVMFamily?.Select(v => new BatchVmCoresPerFamily(v.Name, v.CoreQuota)).ToList(),
                        new(batchQuotas.ActiveJobAndJobScheduleQuota, batchQuotas.PoolQuota, batchQuotas.DedicatedCoreQuota, batchQuotas.LowPriorityCoreQuota)));
                quotaProvider.Setup(p =>
                        p.GetVmCoreQuotaAsync(It.Is<bool>(l => l == false)))
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
                azureProxy.Setup(a => a.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>())).Returns((Pool p, bool _1) => Task.FromResult(azureProxyReturnValues.CreateBatchPoolImpl(p)));
                azureProxy.Setup(a => a.ListComputeNodesAsync(It.IsAny<string>(), It.IsAny<DetailLevel>())).Returns<string, ODATADetailLevel>((poolId, detailLevel) => azureProxyReturnValues.AzureProxyListComputeNodesAsync(poolId, detailLevel));
                azureProxy.Setup(a => a.ListTasksAsync(It.IsAny<string>(), It.IsAny<DetailLevel>())).Returns<string, ODATADetailLevel>((jobId, detailLevel) => azureProxyReturnValues.AzureProxyListTasks(jobId, detailLevel));
                azureProxy.Setup(a => a.DeleteBatchComputeNodesAsync(It.IsAny<string>(), It.IsAny<IEnumerable<ComputeNode>>(), It.IsAny<CancellationToken>())).Callback<string, IEnumerable<ComputeNode>, CancellationToken>((poolId, computeNodes, cancellationToken) => azureProxyReturnValues.AzureProxyDeleteBatchComputeNodes(poolId, computeNodes, cancellationToken)).Returns(Task.CompletedTask);
                azureProxy.Setup(a => a.GetBatchPoolAsync(It.IsAny<string>(), It.IsAny<DetailLevel>(), It.IsAny<CancellationToken>())).Returns((string id, DetailLevel detailLevel, CancellationToken cancellationToken) => Task.FromResult(azureProxyReturnValues.GetBatchPoolImpl(id)));
                azureProxy.Setup(a => a.GetFullAllocationStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns((string poolId, CancellationToken _1) => Task.FromResult(azureProxyReturnValues.AzureProxyGetComputeNodeAllocationState(poolId)));
                azureProxy.Setup(a => a.DeleteBatchPoolAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Callback<string, CancellationToken>((poolId, cancellationToken) => azureProxyReturnValues.AzureProxyDeleteBatchPoolImpl(poolId, cancellationToken)).Returns(Task.CompletedTask);
            };

        private static IEnumerable<(string Key, string Value)> GetMockConfig()
            => Enumerable
                .Empty<(string Key, string Value)>()
                .Append(("BatchPoolIdlePoolDays", "0.000416667"))
                .Append(("BatchPoolRotationForcedDays", "0.000694444"));

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
        internal static CloudPool GeneratePool(string id, int currentDedicatedNodes = default, int currentLowPriorityNodes = default, int targetDedicatedNodes = default, int targetLowPriorityNodes = default, Microsoft.Azure.Batch.Common.AllocationState allocationState = Microsoft.Azure.Batch.Common.AllocationState.Steady, IList<Microsoft.Azure.Batch.MetadataItem> metadata = default, DateTime creationTime = default)
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
            var modelPool = new Microsoft.Azure.Batch.Protocol.Models.CloudPool(id: id, currentDedicatedNodes: currentDedicatedNodes, currentLowPriorityNodes: currentLowPriorityNodes, targetDedicatedNodes: targetDedicatedNodes, targetLowPriorityNodes: targetLowPriorityNodes, allocationState: (Microsoft.Azure.Batch.Protocol.Models.AllocationState)allocationState, metadata: metadata.Select(ConvertMetadata).ToList(), creationTime: creationTime);
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
