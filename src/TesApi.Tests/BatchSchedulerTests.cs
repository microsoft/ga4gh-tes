// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;
using Tes.Extensions;
using Tes.Models;
using TesApi.Web;
using TesApi.Web.Management;
using TesApi.Web.Management.Models.Quotas;
using TesApi.Web.Storage;

namespace TesApi.Tests
{
    [TestClass]
    public class BatchSchedulerTests
    {
        [TestMethod]
        public async Task LocalPoolCacheAccessesNewPoolsAfterAllPoolsRemovedWithSameKey()
        {
            using var serviceProvider = GetServiceProvider();
            var batchScheduler = serviceProvider.GetT() as BatchScheduler;
            var pool = await AddPool(batchScheduler);
            Assert.IsNotNull(pool);
            var key = batchScheduler.GetPoolGroupKeys().First();
            Assert.IsTrue(batchScheduler.RemovePoolFromList(pool));
            Assert.AreEqual(0, batchScheduler.GetPoolGroupKeys().Count());

            pool = (BatchPool)await batchScheduler.GetOrAddPoolAsync(key, false, (id, cancellationToken) => ValueTask.FromResult(new Pool(name: id)), CancellationToken.None);

            Assert.IsNotNull(pool);
            Assert.AreEqual(1, batchScheduler.GetPoolGroupKeys().Count());
            Assert.IsTrue(batchScheduler.TryGetPool(pool.Id, out var pool1));
            Assert.AreSame(pool, pool1);
        }

        [TestMethod]
        public async Task GetOrAddDoesNotAddExistingAvailablePool()
        {
            using var serviceProvider = GetServiceProvider();
            var batchScheduler = serviceProvider.GetT() as BatchScheduler;
            var info = await AddPool(batchScheduler);
            var keyCount = batchScheduler.GetPoolGroupKeys().Count();
            var key = batchScheduler.GetPoolGroupKeys().First();
            var count = batchScheduler.GetPools().Count();
            serviceProvider.AzureProxy.Verify(mock => mock.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Once);

            var pool = await batchScheduler.GetOrAddPoolAsync(key, false, (id, cancellationToken) => ValueTask.FromResult(new Pool(name: id)), CancellationToken.None);
            await pool.ServicePoolAsync();

            Assert.AreEqual(count, batchScheduler.GetPools().Count());
            Assert.AreEqual(keyCount, batchScheduler.GetPoolGroupKeys().Count());
            //Assert.AreSame(info, pool);
            Assert.AreEqual(info.Id, pool.Id);
            serviceProvider.AzureProxy.Verify(mock => mock.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public async Task GetOrAddDoesAddWithExistingUnavailablePool()
        {
            using var serviceProvider = GetServiceProvider();
            var batchScheduler = serviceProvider.GetT() as BatchScheduler;
            var info = await AddPool(batchScheduler);
            info.TestSetAvailable(false);
            //await info.ServicePoolAsync(BatchPool.ServiceKind.Update);
            var keyCount = batchScheduler.GetPoolGroupKeys().Count();
            var key = batchScheduler.GetPoolGroupKeys().First();
            var count = batchScheduler.GetPools().Count();

            var pool = await batchScheduler.GetOrAddPoolAsync(key, false, (id, cancellationToken) => ValueTask.FromResult(new Pool(name: id)), CancellationToken.None);
            await pool.ServicePoolAsync();

            Assert.AreNotEqual(count, batchScheduler.GetPools().Count());
            Assert.AreEqual(keyCount, batchScheduler.GetPoolGroupKeys().Count());
            //Assert.AreNotSame(info, pool);
            Assert.AreNotEqual(info.Id, pool.Id);
        }


        [TestMethod]
        public async Task TryGetReturnsTrueAndCorrectPool()
        {
            using var serviceProvider = GetServiceProvider();
            var batchScheduler = serviceProvider.GetT() as BatchScheduler;
            var info = await AddPool(batchScheduler);

            var result = batchScheduler.TryGetPool(info.Id, out var pool);

            Assert.IsTrue(result);
            //Assert.AreSame(infoPoolId, pool);
            Assert.AreEqual(info.Id, pool.Id);
        }

        [TestMethod]
        public async Task TryGetReturnsFalseWhenPoolIdNotPresent()
        {
            using var serviceProvider = GetServiceProvider();
            var batchScheduler = serviceProvider.GetT() as BatchScheduler;
            _ = await AddPool(batchScheduler);

            var result = batchScheduler.TryGetPool("key2", out _);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public async Task TryGetReturnsFalseWhenNoPoolIsAvailable()
        {
            using var serviceProvider = GetServiceProvider();
            var batchScheduler = serviceProvider.GetT() as BatchScheduler;
            var pool = await AddPool(batchScheduler);
            ((BatchPool)pool).TestSetAvailable(false);

            var result = batchScheduler.TryGetPool("key1", out _);

            Assert.IsFalse(result);
        }

        [TestMethod]
        public Task TryGetReturnsFalseWhenPoolIdIsNull()
        {
            using var serviceProvider = GetServiceProvider();
            var batchScheduler = serviceProvider.GetT() as BatchScheduler;

            var result = batchScheduler.TryGetPool(null, out _);

            Assert.IsFalse(result);
            return Task.CompletedTask;
        }

        [TestMethod]
        public async Task UnavailablePoolsAreRemoved()
        {
            var poolId = string.Empty;
            var azureProxyMock = AzureProxyReturnValues.Defaults;
            azureProxyMock.AzureProxyDeleteBatchPool = (id, token) => poolId = id;

            using var serviceProvider = GetServiceProvider(azureProxyMock);
            var batchScheduler = serviceProvider.GetT() as BatchScheduler;
            var pool = await AddPool(batchScheduler);
            Assert.IsTrue(batchScheduler.IsPoolAvailable("key1"));
            pool.TestSetAvailable(false);
            Assert.IsFalse(batchScheduler.IsPoolAvailable("key1"));
            Assert.IsTrue(batchScheduler.GetPools().Any());

            await pool.ServicePoolAsync(BatchPool.ServiceKind.RemovePoolIfEmpty);

            Assert.AreEqual(pool.Id, poolId);
            Assert.IsFalse(batchScheduler.IsPoolAvailable("key1"));
            Assert.IsFalse(batchScheduler.GetPools().Any());
        }

        [TestCategory("TES 1.1")]
        [TestMethod]
        public async Task BackendParametersVmSizeShallOverrideVmSelection()
        {
            // "vmsize" is not case sensitive
            // If vmsize is specified, (numberofcores, memoryingb, resourcedisksizeingb) are ignored

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            azureProxyReturnValues.VmSizesAndPrices = new() {
                new() { VmSize = "VmSize1", LowPriority = true, VCpusAvailable = 1, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 1 },
                new() { VmSize = "VmSize2", LowPriority = true, VCpusAvailable = 2, MemoryInGiB = 8, ResourceDiskSizeInGiB = 40, PricePerHour = 2 }};

            var state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = true, BackendParameters = new() { { "vm_size", "VmSize1" } } }, azureProxyReturnValues);
            Assert.AreEqual(TesState.INITIALIZINGEnum, state);

            state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = true, BackendParameters = new() { { "vm_size", "VMSIZE1" } } }, azureProxyReturnValues);
            Assert.AreEqual(TesState.INITIALIZINGEnum, state);

            state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = true, BackendParameters = new() { { "vm_size", "VmSize1" } }, CpuCores = 1000, RamGb = 100000, DiskGb = 1000000 }, azureProxyReturnValues);
            Assert.AreEqual(TesState.INITIALIZINGEnum, state);

            state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = true, BackendParameters = new(), CpuCores = 1000, RamGb = 100000, DiskGb = 1000000 }, azureProxyReturnValues);
            Assert.AreEqual(TesState.SYSTEMERROREnum, state);

            state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = false, BackendParameters = new() { { "vm_size", "VmSize1" } } }, azureProxyReturnValues);
            Assert.AreEqual(TesState.SYSTEMERROREnum, state);

            state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = true, BackendParameters = new() { { "vm_size", "VmSize3" } } }, azureProxyReturnValues);
            Assert.AreEqual(TesState.SYSTEMERROREnum, state);
        }


        [TestCategory("TES 1.1")]
        [DataRow("VmSizeLowPri1", true)]
        [DataRow("VmSizeLowPri2", true)]
        [DataRow("VmSizeDedicated1", false)]
        [DataRow("VmSizeDedicated2", false)]
        [TestMethod]
        public async Task TestIfVmSizeIsAvailable(string vmSize, bool preemptible)
        {
            var task = GetTesTask();
            task.Resources.Preemptible = preemptible;
            task.Resources.BackendParameters = new() { { "vm_size", vmSize } };

            var config = GetMockConfig()();
            using var serviceProvider = GetServiceProvider(
                config,
                GetMockAzureProxy(AzureProxyReturnValues.Defaults),
                GetMockQuotaProvider(AzureProxyReturnValues.Defaults),
                GetMockSkuInfoProvider(AzureProxyReturnValues.Defaults),
                GetMockAllowedVms(config));
            var batchScheduler = serviceProvider.GetT();

            var size = await ((BatchScheduler)batchScheduler).GetVmSizeAsync(task, CancellationToken.None);
            GuardAssertsWithTesTask(task, () => Assert.AreEqual(vmSize, size.VmSize));
        }

        private static BatchAccountResourceInformation GetNewBatchResourceInfo()
          => new("batchAccount", "mrg", "sub-id", "eastus", "batchAccount/endpoint");

        [TestMethod]
        public async Task TesTaskFailsWithSystemErrorWhenNoSuitableVmExists()
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            azureProxyReturnValues.VmSizesAndPrices = new() {
                new() { VmSize = "VmSize1", LowPriority = true, VCpusAvailable = 1, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 1 },
                new() { VmSize = "VmSize2", LowPriority = true, VCpusAvailable = 2, MemoryInGiB = 8, ResourceDiskSizeInGiB = 40, PricePerHour = 2 }};

            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 1, DiskGb = 10, Preemptible = false }, azureProxyReturnValues));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 4, RamGb = 1, DiskGb = 10, Preemptible = true }, azureProxyReturnValues));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 10, DiskGb = 10, Preemptible = true }, azureProxyReturnValues));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 1, DiskGb = 50, Preemptible = true }, azureProxyReturnValues));
        }

        [TestMethod]
        public async Task TesTaskFailsWithSystemErrorWhenTotalBatchQuotaIsSetTooLow()
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchQuotas = new() { ActiveJobAndJobScheduleQuota = 1, PoolQuota = 1, DedicatedCoreQuota = 1, LowPriorityCoreQuota = 10 };

            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 2, RamGb = 1, Preemptible = false }, azureProxyReturnValues));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 11, RamGb = 1, Preemptible = true }, azureProxyReturnValues));

            var dedicatedCoreQuotaPerVMFamily = new List<VirtualMachineFamilyCoreQuota> { new("VmFamily2", 1) };
            azureProxyReturnValues.BatchQuotas = new()
            {
                ActiveJobAndJobScheduleQuota = 1,
                PoolQuota = 1,
                DedicatedCoreQuota = 100,
                LowPriorityCoreQuota = 100,
                DedicatedCoreQuotaPerVMFamilyEnforced = true,
                DedicatedCoreQuotaPerVMFamily = dedicatedCoreQuotaPerVMFamily
            };

            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 2, RamGb = 1, Preemptible = false }, azureProxyReturnValues));
        }

        [TestMethod]
        public async Task TesTaskFailsWhenBatchNodeDiskIsFull()
        {
            var tesTask = GetTesTask();
            tesTask.State = TesState.INITIALIZINGEnum;

            (var failureReason, var systemLog) = await ProcessTesTaskAndGetFailureReasonAndSystemLogAsync(tesTask, BatchTaskStates.NodeDiskFull[0]);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(TesState.SYSTEMERROREnum, tesTask.State); // TODO: Should be ExecutorError, but this currently falls into the bucket of NodeFailedDuringStartupOrExecution, which also covers StartTask failures, which are more correctly SystemError.
                Assert.AreEqual("DiskFull", failureReason);
                Assert.AreEqual("DiskFull", systemLog[0]);
                Assert.AreEqual("DiskFull", tesTask.FailureReason);
            });
        }

        //TODO: This test (and potentially others) must be reviewed and see if they are necessary considering that the quota verification logic is its own class.
        // There are a couple of issues: a similar validation already exists in the quota verifier class, and in order to run this test a complex set up is required, which is hard to maintain.
        // Instead, this test should be refactor to validate if transitions occur in the scheduler when specific exceptions are thrown.  
        [TestMethod]
        public async Task TesTaskRemainsQueuedWhenBatchQuotaIsTemporarilyUnavailable()
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            azureProxyReturnValues.VmSizesAndPrices = new() {
                new() { VmSize = "VmSize1", VmFamily = "VmFamily1", LowPriority = false, VCpusAvailable = 2, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 1 },
                new() { VmSize = "VmSize1", VmFamily = "VmFamily1", LowPriority = true, VCpusAvailable = 2, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 2 }};

            azureProxyReturnValues.BatchQuotas = new() { ActiveJobAndJobScheduleQuota = 1, PoolQuota = 1, DedicatedCoreQuota = 9, LowPriorityCoreQuota = 17 };

            azureProxyReturnValues.ActiveNodeCountByVmSize = new List<AzureBatchNodeCount> {
                new() { VirtualMachineSize = "VmSize1", DedicatedNodeCount = 4, LowPriorityNodeCount = 8 }  // 8 (4 * 2) dedicated and 16 (8 * 2) low pri cores are in use, there is no more room for 2 cores
            };

            // The actual CPU core count (2) of the selected VM is used for quota calculation, not the TesResources CpuCores requirement
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 1, Preemptible = false }, azureProxyReturnValues));
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 1, Preemptible = true }, azureProxyReturnValues));

            azureProxyReturnValues.ActiveNodeCountByVmSize = new List<AzureBatchNodeCount> {
                new() { VirtualMachineSize = "VmSize1", DedicatedNodeCount = 4, LowPriorityNodeCount = 7 }  // 8 dedicated and 14 low pri cores are in use
            };

            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 1, Preemptible = true }, azureProxyReturnValues));

            var dedicatedCoreQuotaPerVMFamily = new List<VirtualMachineFamilyCoreQuota> { new("VmFamily1", 100) };
            azureProxyReturnValues.BatchQuotas = new() { ActiveJobAndJobScheduleQuota = 1, PoolQuota = 1, DedicatedCoreQuota = 9, LowPriorityCoreQuota = 17, DedicatedCoreQuotaPerVMFamilyEnforced = true, DedicatedCoreQuotaPerVMFamily = dedicatedCoreQuotaPerVMFamily };

            azureProxyReturnValues.ActiveNodeCountByVmSize = new List<AzureBatchNodeCount> {
                new() { VirtualMachineSize = "VmSize1", DedicatedNodeCount = 4, LowPriorityNodeCount = 8 }  // 8 (4 * 2) dedicated and 16 (8 * 2) low pri cores are in use, there is no more room for 2 cores
            };

            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 1, Preemptible = false }, azureProxyReturnValues));
        }

        private async Task AddBatchTasksHandlesExceptions(TesState? newState, Func<AzureProxyReturnValues, (Action<IServiceCollection>, Action<Mock<IAzureProxy>>)> testArranger, Action<TesTask, IEnumerable<(LogLevel, Exception, string)>> resultValidator, int numberOfTasks = 1)
        {
            var logger = new Mock<ILogger<BatchScheduler>>();
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            var (providerModifier, azureProxyModifier) = testArranger?.Invoke(azureProxyReturnValues) ?? (default, default);
            var azureProxy = new Action<Mock<IAzureProxy>>(mock =>
            {
                GetMockAzureProxy(azureProxyReturnValues)(mock);
                azureProxyModifier?.Invoke(mock);
            });

            var tasks = Enumerable.Repeat(0, numberOfTasks).Select((_, index) =>
            {
                var task = GetTesTask();
                task.State = TesState.QUEUEDEnum;

                if (numberOfTasks > 1)
                {
                    task.Id = Guid.NewGuid().ToString("D");
                    task.Resources.BackendParameters ??= new();
                    task.Resources.BackendParameters.Add("vm_size", index % 2 == 1 ? "VmSizeDedicated1" : "VmSizeDedicated2");
                }

                return task;
            }).ToArray();

            _ = await ProcessTesTasksAndGetBatchJobArgumentsAsync(
                tasks,
                GetMockConfig()(),
                azureProxy,
                azureProxyReturnValues,
                s =>
                {
                    providerModifier?.Invoke(s);
                    s.AddTransient(p => logger.Object);
                });

            foreach (var task in tasks)
            {
                GuardAssertsWithTesTask(task, () =>
                {
                    if (newState.HasValue)
                    {
                        Assert.AreEqual(newState, task.State);
                    }

                    resultValidator?.Invoke(task, logger.Invocations.Where(i => nameof(ILogger.Log).Equals(i.Method.Name)).Select(i => (((LogLevel?)i.Arguments[0]) ?? LogLevel.None, i.Arguments[3] as Exception, i.Arguments[2].ToString())));
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesAzureBatchPoolCreationExceptionViaJobCreation()
        {
            return AddBatchTasksHandlesExceptions(TesState.QUEUEDEnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (default, azureProxy => azureProxy.Setup(b => b.CreateBatchJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                    .Callback<string, CancellationToken>((_1, _2)
                        => throw new Microsoft.Rest.Azure.CloudException("No job for you.") { Body = new() { Code = BatchErrorCodeStrings.OperationTimedOut } }));

            void Validator(TesTask tesTask, IEnumerable<(LogLevel, Exception, string)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    var (logLevel, exception, _) = log;
                    Assert.AreEqual(LogLevel.Warning, logLevel);
                    Assert.IsInstanceOfType<AzureBatchPoolCreationException>(exception);
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesAzureBatchPoolCreationExceptionViaPoolCreation()
        {
            return AddBatchTasksHandlesExceptions(TesState.QUEUEDEnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (default, azureProxy => azureProxy.Setup(b => b.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                    .Callback<Pool, bool, CancellationToken>((_1, _2, _3)
                        => throw new Microsoft.Rest.Azure.CloudException("No job for you.") { Body = new() { Code = BatchErrorCodeStrings.OperationTimedOut } }));

            void Validator(TesTask tesTask, IEnumerable<(LogLevel, Exception, string)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    var (logLevel, exception, _) = log;
                    Assert.AreEqual(LogLevel.Warning, logLevel);
                    Assert.IsInstanceOfType<AzureBatchPoolCreationException>(exception);
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesAzureBatchQuotaMaxedOutException()
        {
            return AddBatchTasksHandlesExceptions(TesState.QUEUEDEnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (services => services.AddSingleton<IBatchQuotaVerifier, TestBatchQuotaVerifierQuotaMaxedOut>(), default);

            void Validator(TesTask tesTask, IEnumerable<(LogLevel logLevel, Exception, string)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    Assert.AreEqual(LogLevel.Warning, log.logLevel);
                });
            }
        }

        [TestMethod]
        public async Task MultipleTaskAddBatchTaskHandlesAzureBatchQuotaMaxedOutException()
        {
            var quotaDelayedTasks = 0;
            var queuedTasks = 0;

            await AddBatchTasksHandlesExceptions(null, Arranger, Validator, 4);

            Assert.AreEqual(2, queuedTasks);
            Assert.AreEqual(2, quotaDelayedTasks);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (services => services.AddSingleton<IBatchQuotaVerifier, TestMultitaskBatchQuotaVerifierQuotaMaxedOut>(), default);

            void Validator(TesTask tesTask, IEnumerable<(LogLevel logLevel, Exception exception, string message)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    switch (tesTask.State)
                    {
                        case TesState.QUEUEDEnum:
                            {
                                var log = logs.LastOrDefault(l => l.message.Contains(tesTask.Id));
                                Assert.IsNotNull(log);
                                Assert.AreEqual(LogLevel.Warning, log.logLevel);
                                Assert.IsNull(log.exception);
                                Assert.IsTrue(log.message.Contains(nameof(AzureBatchQuotaMaxedOutException)));
                            }
                            ++quotaDelayedTasks;
                            break;

                        case TesState.INITIALIZINGEnum:
                            {
                                var log = tesTask.Logs?.LastOrDefault();
                                Assert.IsNotNull(log);
                                Assert.IsNotNull(log.VirtualMachineInfo);
                                Assert.IsNotNull(log.VirtualMachineInfo.VmSize);
                            }
                            ++queuedTasks;
                            break;

                        default:
                            Assert.Fail();
                            break;
                    }
                });
            }
        }

        [TestMethod]
        public async Task MultipleTaskAddBatchTaskMultiplePoolsAdded()
        {
            var quotaDelayedTasks = 0;
            var queuedTasks = 0;

            await AddBatchTasksHandlesExceptions(null, Arranger, Validator, 4);

            Assert.AreEqual(4, queuedTasks);
            Assert.AreEqual(0, quotaDelayedTasks);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (services => services.AddSingleton<IBatchQuotaVerifier, TestMultitaskBatchQuotaVerifierQuotaAllAllowed>(), default);

            void Validator(TesTask tesTask, IEnumerable<(LogLevel logLevel, Exception exception, string message)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    switch (tesTask.State)
                    {
                        case TesState.QUEUEDEnum:
                            {
                                var log = logs.LastOrDefault(l => l.message.Contains(tesTask.Id));
                                Assert.IsNotNull(log);
                                Assert.AreEqual(LogLevel.Warning, log.logLevel);
                                Assert.IsNull(log.exception);
                                Assert.IsTrue(log.message.Contains(nameof(AzureBatchQuotaMaxedOutException)));
                            }
                            ++quotaDelayedTasks;
                            break;

                        case TesState.INITIALIZINGEnum:
                            {
                                var log = tesTask.Logs?.LastOrDefault();
                                Assert.IsNotNull(log);
                                Assert.IsNotNull(log.VirtualMachineInfo);
                                Assert.IsNotNull(log.VirtualMachineInfo.VmSize);
                            }
                            ++queuedTasks;
                            break;

                        default:
                            Assert.Fail();
                            break;
                    }
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesAzureBatchLowQuotaException()
        {
            return AddBatchTasksHandlesExceptions(TesState.SYSTEMERROREnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (services => services.AddSingleton<IBatchQuotaVerifier, TestBatchQuotaVerifierLowQuota>(), default);

            void Validator(TesTask tesTask, IEnumerable<(LogLevel, Exception, string)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    var (logLevel, exception, _) = log;
                    Assert.AreEqual(LogLevel.Error, logLevel);
                    Assert.IsInstanceOfType<AzureBatchLowQuotaException>(exception);
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesAzureBatchVirtualMachineAvailabilityException()
        {
            return AddBatchTasksHandlesExceptions(TesState.SYSTEMERROREnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues proxy)
            {
                proxy.VmSizesAndPrices = Enumerable.Empty<VirtualMachineInformation>().ToList();
                return (default, default);
            }

            void Validator(TesTask tesTask, IEnumerable<(LogLevel, Exception, string)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    var (logLevel, exception, _) = log;
                    Assert.AreEqual(LogLevel.Error, logLevel);
                    Assert.IsInstanceOfType<AzureBatchVirtualMachineAvailabilityException>(exception);
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesTesException()
        {
            return AddBatchTasksHandlesExceptions(TesState.SYSTEMERROREnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (default, azureProxy => azureProxy.Setup(b => b.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                    .Callback<Pool, bool, CancellationToken>((poolInfo, isPreemptible, cancellationToken)
                        => throw new TesException("TestFailureReason")));

            void Validator(TesTask tesTask, IEnumerable<(LogLevel, Exception, string)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    var (logLevel, exception, _) = log;
                    Assert.AreEqual(LogLevel.Error, logLevel);
                    Assert.IsInstanceOfType<TesException>(exception);
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesBatchClientException()
        {
            return AddBatchTasksHandlesExceptions(TesState.SYSTEMERROREnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (default, azureProxy => azureProxy.Setup(b => b.AddBatchTaskAsync(It.IsAny<string>(), It.IsAny<CloudTask>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                    .Callback<string, CloudTask, string, CancellationToken>((_1, _2, _3, _4)
                        => throw typeof(BatchClientException)
                                .GetConstructor(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance,
                                    new[] { typeof(string), typeof(Exception) })
                                .Invoke(new object[] { null, null }) as Exception));

            void Validator(TesTask tesTask, IEnumerable<(LogLevel, Exception, string)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    var (logLevel, exception, _) = log;
                    Assert.AreEqual(LogLevel.Error, logLevel);
                    Assert.IsInstanceOfType<BatchClientException>(exception);
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesBatchExceptionForJobQuota()
        {
            return AddBatchTasksHandlesExceptions(TesState.QUEUEDEnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (default, azureProxy => azureProxy.Setup(b => b.CreateBatchJobAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                    .Callback<string, CancellationToken>((_1, _2)
                        => throw new BatchException(
                            new Mock<RequestInformation>().Object,
                            default,
                            new Microsoft.Azure.Batch.Protocol.Models.BatchErrorException() { Body = new() { Code = "ActiveJobAndScheduleQuotaReached", Message = new(value: "No job for you.") } })));

            void Validator(TesTask task, IEnumerable<(LogLevel logLevel, Exception, string)> logs)
            {
                GuardAssertsWithTesTask(task, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    Assert.AreEqual(LogLevel.Warning, log.logLevel);
                    Assert.IsNotNull(task.Logs?.Last().Warning);
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesBatchExceptionForPoolQuota()
        {
            return AddBatchTasksHandlesExceptions(TesState.QUEUEDEnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (default, azureProxy => azureProxy.Setup(b => b.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                    .Callback<Pool, bool, CancellationToken>((poolInfo, isPreemptible, cancellationToken)
                        => throw new BatchException(
                            new Mock<RequestInformation>().Object,
                            default,
                            new Microsoft.Azure.Batch.Protocol.Models.BatchErrorException() { Body = new() { Code = "PoolQuotaReached", Message = new(value: "No pool for you.") } })));

            void Validator(TesTask task, IEnumerable<(LogLevel logLevel, Exception, string)> logs)
            {
                GuardAssertsWithTesTask(task, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    Assert.AreEqual(LogLevel.Warning, log.logLevel);
                    Assert.IsNotNull(task.Logs?.Last().Warning);
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesCloudExceptionForPoolQuota()
        {
            return AddBatchTasksHandlesExceptions(TesState.QUEUEDEnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (default, azureProxy => azureProxy.Setup(b => b.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                    .Callback<Pool, bool, CancellationToken>((poolInfo, isPreemptible, cancellationToken)
                        => throw new Microsoft.Rest.Azure.CloudException() { Body = new() { Code = "AutoPoolCreationFailedWithQuotaReached", Message = "No autopool for you." } }));

            void Validator(TesTask task, IEnumerable<(LogLevel logLevel, Exception, string)> logs)
            {
                GuardAssertsWithTesTask(task, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    Assert.AreEqual(LogLevel.Warning, log.logLevel);
                    Assert.IsNotNull(task.Logs?.Last().Warning);
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesUnknownException()
        {
            var exceptionMsg = "Successful Test";
            var batchQuotaProvider = new Mock<IBatchQuotaProvider>();
            batchQuotaProvider.Setup(p => p.GetVmCoreQuotaAsync(It.IsAny<bool>(), It.IsAny<CancellationToken>())).Callback<bool, CancellationToken>((lowPriority, _1) => throw new InvalidOperationException(exceptionMsg));
            return AddBatchTasksHandlesExceptions(TesState.SYSTEMERROREnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (services => services.AddTransient(p => batchQuotaProvider.Object), default);

            void Validator(TesTask tesTask, IEnumerable<(LogLevel, Exception, string)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    var (logLevel, exception, _) = log;
                    Assert.AreEqual(LogLevel.Error, logLevel);
                    Assert.IsInstanceOfType<InvalidOperationException>(exception);
                    Assert.AreEqual(exceptionMsg, exception.Message);
                });
            }
        }

        [TestMethod]
        public async Task BatchJobContainsExpectedBatchPoolInformation()
        {
            var tesTask = GetTesTask();
            var config = GetMockConfig()();
            using var serviceProvider = GetServiceProvider(
                config,
                GetMockAzureProxy(AzureProxyReturnValues.Defaults),
                GetMockQuotaProvider(AzureProxyReturnValues.Defaults),
                GetMockSkuInfoProvider(AzureProxyReturnValues.Defaults),
                GetMockAllowedVms(config));
            var batchScheduler = serviceProvider.GetT();

            await foreach (var _ in batchScheduler.ProcessQueuedTesTasksAsync(new[] { tesTask }, CancellationToken.None)) { }

            var createBatchPoolAsyncInvocation = serviceProvider.AzureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.CreateBatchPoolAsync));
            var addBatchTaskAsyncInvocation = serviceProvider.AzureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.AddBatchTaskAsync));

            var cloudTask = addBatchTaskAsyncInvocation?.Arguments[1] as CloudTask;
            var poolId = addBatchTaskAsyncInvocation?.Arguments[2] as string;
            var pool = createBatchPoolAsyncInvocation?.Arguments[0] as Pool;

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.IsNotNull(poolId);
                Assert.AreEqual("TES-hostname-edicated1-lwsfq7ml3bfuzw3kjwvp55cpykgfdtpm-", poolId[0..^8]);
                Assert.AreEqual("VmSizeDedicated1", pool.VmSize);
                Assert.IsTrue(((BatchScheduler)batchScheduler).TryGetPool(poolId, out _));
            });
        }

        //[TestCategory("TES 1.1")]
        //[TestMethod]
        //public async Task BatchJobContainsExpectedManualPoolInformation()
        //{
        //    var task = GetTesTask();
        //    task.Resources.BackendParameters = new()
        //    {
        //        { "workflow_execution_identity", "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/coa/providers/Microsoft.ManagedIdentity/userAssignedIdentities/coa-test-uami" }
        //    };

        //    (_, _, var poolInformation, var pool) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(task, GetMockConfig(true)(), GetMockAzureProxy(AzureProxyReturnValues.Defaults), AzureProxyReturnValues.Defaults);

        //    GuardAssertsWithTesTask(task, () =>
        //    {
        //        Assert.IsNotNull(poolInformation.PoolId);
        //        Assert.IsNull(poolInformation.AutoPoolSpecification);
        //        Assert.AreEqual("TES_JobId-1", poolInformation.PoolId);
        //        Assert.AreEqual("VmSizeDedicated1", pool.VmSize);
        //        Assert.AreEqual(1, pool.ScaleSettings.FixedScale.TargetDedicatedNodes);
        //    });
        //}

        [TestMethod]
        public async Task NewTesTaskGetsScheduledSuccessfully()
        {
            var tesTask = GetTesTask();

            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(AzureProxyReturnValues.Defaults), AzureProxyReturnValues.Defaults);

            GuardAssertsWithTesTask(tesTask, () => Assert.AreEqual(TesState.INITIALIZINGEnum, tesTask.State));
        }

        [TestMethod]
        public async Task PreemptibleTesTaskGetsScheduledToLowPriorityVm()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = true;

            (_, _, _, var pool) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(AzureProxyReturnValues.Defaults), AzureProxyReturnValues.Defaults);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual("VmSizeLowPri1", pool.VmSize);
                Assert.IsTrue(pool.ScaleSettings.AutoScale.Formula.Contains("\n$TargetLowPriorityNodes = "));
                Assert.IsFalse(pool.ScaleSettings.AutoScale.Formula.Contains("\n$TargetDedicated = "));
            });
        }

        [TestMethod]
        public async Task NonPreemptibleTesTaskGetsScheduledToDedicatedVm()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = false;

            (_, _, _, var pool) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(AzureProxyReturnValues.Defaults), AzureProxyReturnValues.Defaults);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual("VmSizeDedicated1", pool.VmSize);
                Assert.IsTrue(pool.ScaleSettings.AutoScale.Formula.Contains("\n$TargetDedicated = "));
                Assert.IsFalse(pool.ScaleSettings.AutoScale.Formula.Contains("\n$TargetLowPriorityNodes = "));
            });
        }

        [TestMethod]
        public async Task PreemptibleTesTaskGetsScheduledToLowPriorityVm_PerVMFamilyEnforced()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = true;

            (_, _, _, var pool) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(AzureProxyReturnValues.DefaultsPerVMFamilyEnforced), AzureProxyReturnValues.DefaultsPerVMFamilyEnforced);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual("VmSizeLowPri1", pool.VmSize);
                Assert.IsTrue(pool.ScaleSettings.AutoScale.Formula.Contains("\n$TargetLowPriorityNodes = "));
                Assert.IsFalse(pool.ScaleSettings.AutoScale.Formula.Contains("\n$TargetDedicated = "));
            });
        }

        [TestMethod]
        public async Task NonPreemptibleTesTaskGetsScheduledToDedicatedVm_PerVMFamilyEnforced()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = false;

            (_, _, _, var pool) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(AzureProxyReturnValues.DefaultsPerVMFamilyEnforced), AzureProxyReturnValues.DefaultsPerVMFamilyEnforced);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual("VmSizeDedicated1", pool.VmSize);
                Assert.IsTrue(pool.ScaleSettings.AutoScale.Formula.Contains("\n$TargetDedicated = "));
                Assert.IsFalse(pool.ScaleSettings.AutoScale.Formula.Contains("\n$TargetLowPriorityNodes = "));
            });
        }

        [TestMethod]
        public async Task NonPreemptibleTesTaskGetsWarningAndIsScheduledToLowPriorityVmIfPriceIsDoubleIdeal()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = false;
            tesTask.Resources.CpuCores = 2;

            var azureProxyReturnValues = AzureProxyReturnValues.DefaultsPerVMFamilyEnforced;
            azureProxyReturnValues.VmSizesAndPrices.First(vm => vm.VmSize.Equals("VmSize3", StringComparison.OrdinalIgnoreCase)).PricePerHour = 44;

            (_, _, _, var pool) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(azureProxyReturnValues), azureProxyReturnValues);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.IsTrue(tesTask.Logs.Any(l => "UsedLowPriorityInsteadOfDedicatedVm".Equals(l.Warning)));
                Assert.IsTrue(pool.ScaleSettings.AutoScale.Formula.Contains("\n$TargetLowPriorityNodes = "));
            });
        }

        [TestMethod]
        public async Task TesTaskGetsScheduledToLowPriorityVmIfSettingUsePreemptibleVmsOnlyIsSet()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = false;

            var config = GetMockConfig()()
                .Append(("BatchScheduling:UsePreemptibleVmsOnly", "true"));

            (_, _, _, var pool) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, GetMockAzureProxy(AzureProxyReturnValues.Defaults), AzureProxyReturnValues.Defaults);

            GuardAssertsWithTesTask(tesTask, () => Assert.IsTrue(pool.ScaleSettings.AutoScale.Formula.Contains("\n$TargetLowPriorityNodes = ")));
        }

        [TestMethod]
        public async Task TesTaskGetsScheduledToAllowedVmSizeOnly()
        {
            static async Task RunTest(string allowedVmSizes, TesState expectedTaskState, string expectedSelectedVmSize = null)
            {
                var tesTask = GetTesTask();
                tesTask.Resources.Preemptible = true;

                var config = GetMockConfig()()
                    .Append(("AllowedVmSizes", allowedVmSizes));

                (_, _, _, var pool) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, GetMockAzureProxy(AzureProxyReturnValues.Defaults), AzureProxyReturnValues.Defaults);

                GuardAssertsWithTesTask(tesTask, () =>
                {
                    Assert.AreEqual(expectedTaskState, tesTask.State);

                    if (expectedSelectedVmSize is not null)
                    {
                        Assert.AreEqual(expectedSelectedVmSize, pool.VmSize);
                    }
                });
            }

            await RunTest(null, TesState.INITIALIZINGEnum, "VmSizeLowPri1");
            await RunTest(string.Empty, TesState.INITIALIZINGEnum, "VmSizeLowPri1");
            await RunTest("VmSizeLowPri1", TesState.INITIALIZINGEnum, "VmSizeLowPri1");
            await RunTest("VmSizeLowPri1,VmSizeLowPri2", TesState.INITIALIZINGEnum, "VmSizeLowPri1");
            await RunTest("VmSizeLowPri2", TesState.INITIALIZINGEnum, "VmSizeLowPri2");
            await RunTest("VmSizeLowPriNonExistent", TesState.SYSTEMERROREnum);
            await RunTest("VmSizeLowPriNonExistent,VmSizeLowPri1", TesState.INITIALIZINGEnum, "VmSizeLowPri1");
            await RunTest("VmFamily2", TesState.INITIALIZINGEnum, "VmSizeLowPri2");
        }

        [TestMethod]
        public async Task TaskStateTransitionsFromRunningState()
        {
            Assert.AreEqual(TesState.RUNNINGEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchTaskStates.TaskActive));
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchTaskStates.TaskPreparing));
            Assert.AreEqual(TesState.RUNNINGEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchTaskStates.TaskRunning));
            Assert.AreEqual(TesState.COMPLETEEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchTaskStates.TaskCompletedSuccessfully));
            Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchTaskStates.TaskFailed));
            Assert.AreEqual(TesState.CANCELEDEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchTaskStates.CancellationRequested));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchTaskStates.NodeDiskFull));
            Assert.AreEqual(TesState.RUNNINGEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchTaskStates.UploadOrDownloadFailed));
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchTaskStates.NodePreempted));
        }

        [TestMethod]
        public async Task TaskStateTransitionsFromInitializingState()
        {
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchTaskStates.TaskActive));
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchTaskStates.TaskPreparing));
            Assert.AreEqual(TesState.RUNNINGEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchTaskStates.TaskRunning));
            Assert.AreEqual(TesState.COMPLETEEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchTaskStates.TaskCompletedSuccessfully));
            Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchTaskStates.TaskFailed));
            Assert.AreEqual(TesState.CANCELEDEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchTaskStates.CancellationRequested));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchTaskStates.NodeDiskFull));
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchTaskStates.UploadOrDownloadFailed));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchTaskStates.NodeStartTaskFailed));
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchTaskStates.NodeAllocationFailed));
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchTaskStates.NodePreempted));
        }

        [TestMethod]
        public async Task TaskStateTransitionsFromQueuedState()
        {
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, new AzureBatchTaskState[] { default }));
            Assert.AreEqual(TesState.CANCELEDEnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, BatchTaskStates.CancellationRequested));
        }

        [TestMethod]
        public async Task TaskIsRequeuedUpToThreeTimesForTransientErrors()
        {
            var tesTask = GetTesTask();

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            azureProxyReturnValues.VmSizesAndPrices = new() {
                new() { VmSize = "VmSize1", LowPriority = false, VCpusAvailable = 2, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 1 },
                new() { VmSize = "VmSize2", LowPriority = false, VCpusAvailable = 2, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 2 },
                new() { VmSize = "VmSize3", LowPriority = false, VCpusAvailable = 2, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 3 },
                new() { VmSize = "VmSize4", LowPriority = false, VCpusAvailable = 2, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 4 },
                new() { VmSize = "VmSize5", LowPriority = false, VCpusAvailable = 2, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 5 }
            };

            await GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
            await GuardAssertsWithTesTask(tesTask, async () => Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(tesTask, BatchTaskStates.NodeAllocationFailed[0])));
            await GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
            await GuardAssertsWithTesTask(tesTask, async () => Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(tesTask, BatchTaskStates.NodeAllocationFailed[0])));
            await GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
            await GuardAssertsWithTesTask(tesTask, async () => Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(tesTask, BatchTaskStates.NodeAllocationFailed[0])));
            await GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
            await GuardAssertsWithTesTask(tesTask, async () => Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(tesTask, BatchTaskStates.NodeAllocationFailed[0])));
        }

        [TestMethod]
        public async Task TaskThatFailsWithNodeAllocationErrorIsRequeuedOnDifferentVmSize()
        {
            var tesTask = GetTesTask();

            await GetNewTesTaskStateAsync(tesTask);
            await GetNewTesTaskStateAsync(tesTask, BatchTaskStates.NodeAllocationFailed[0]);
            var firstAttemptVmSize = tesTask.Logs[0].VirtualMachineInfo.VmSize;

            await GetNewTesTaskStateAsync(tesTask);
            await GetNewTesTaskStateAsync(tesTask, BatchTaskStates.NodeAllocationFailed[0]);
            var secondAttemptVmSize = tesTask.Logs[1].VirtualMachineInfo.VmSize;

            GuardAssertsWithTesTask(tesTask, () => Assert.AreNotEqual(firstAttemptVmSize, secondAttemptVmSize));

            // There are only two suitable VMs, and both have been excluded because of the NodeAllocationFailed error on the two earlier attempts
            _ = await GetNewTesTaskStateAsync(tesTask);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(TesState.SYSTEMERROREnum, tesTask.State);
                Assert.AreEqual("NoVmSizeAvailable", tesTask.FailureReason);
            });
        }

        [TestMethod]
        public async Task TaskGetsCancelled()
        {
            var tesTask = new TesTask { Id = "test", PoolId = "pool1", State = TesState.CANCELINGEnum, Logs = new() { new() } };

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchTaskState = BatchTaskStates.CancellationRequested[0];
            Mock<IAzureProxy> azureProxy = default;
            var azureProxySetter = new Action<Mock<IAzureProxy>>(mock =>
            {
                GetMockAzureProxy(azureProxyReturnValues)(mock);
                azureProxy = mock;
            });

            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), azureProxySetter, azureProxyReturnValues);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(TesState.CANCELEDEnum, tesTask.State);
                azureProxy.Verify(i => i.TerminateBatchTaskAsync(tesTask.Id, It.IsAny<string>(), It.IsAny<CancellationToken>()));
            });
        }

        //[TestMethod]
        //public async Task CancelledTaskGetsDeleted()
        //{
        //    var tesTask = new TesTask
        //    {
        //        Id = "test", PoolId = "pool1", State = TesState.CANCELEDEnum, Logs = new()
        //        {
        //            new()
        //            {
        //                StartTime = DateTimeOffset.UtcNow - TimeSpan.FromMinutes(11), Logs = new()
        //                {
        //                    new() { IsCloudTaskDeletionRequired = true, TaskId = "cloudTest" }
        //                }
        //            }
        //        }
        //    };

        //    var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
        //    azureProxyReturnValues.BatchTaskState = BatchTaskStates.Terminated;
        //    Mock<IAzureProxy> azureProxy = default;
        //    var azureProxySetter = new Action<Mock<IAzureProxy>>(mock =>
        //    {
        //        GetMockAzureProxy(azureProxyReturnValues)(mock);
        //        azureProxy = mock;
        //    });

        //    _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), azureProxySetter, azureProxyReturnValues);

        //    GuardAssertsWithTesTask(tesTask, () =>
        //    {
        //        var executorLog = tesTask.Logs.Last().Logs.Last();
        //        Assert.IsFalse(executorLog.IsCloudTaskDeletionRequired);
        //        azureProxy.Verify(i => i.DeleteBatchTaskAsync(executorLog.TaskId, It.IsAny<string>(), It.IsAny<CancellationToken>()));
        //    });
        //}

        [TestMethod]
        public async Task SuccessfullyCompletedTaskContainsBatchNodeMetrics()
        {
            var tesTask = GetTesTask();
            tesTask.State = TesState.INITIALIZINGEnum;

            var metricsFileContent = @"
                BlobXferPullStart=2020-10-08T02:30:39+00:00
                BlobXferPullEnd=2020-10-08T02:31:39+00:00
                ExecutorPullStart=2020-10-08T02:32:39+00:00
                ExecutorImageSizeInBytes=3000000000
                ExecutorPullEnd=2020-10-08T02:34:39+00:00
                DownloadStart=2020-10-08T02:35:39+00:00
                FileDownloadSizeInBytes=2000000000
                DownloadEnd=2020-10-08T02:38:39+00:00
                ExecutorStart=2020-10-08T02:39:39+00:00
                ExecutorEnd=2020-10-08T02:43:39+00:00
                UploadStart=2020-10-08T02:44:39+00:00
                FileUploadSizeInBytes=4000000000
                UploadEnd=2020-10-08T02:49:39+00:00
                DiskSizeInKiB=8000000
                DiskUsedInKiB=1000000".Replace(" ", string.Empty);

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchTaskState = BatchTaskStates.TaskCompletedSuccessfully[0];
            azureProxyReturnValues.DownloadedBlobContent = metricsFileContent;

            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(azureProxyReturnValues), azureProxyReturnValues);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(TesState.COMPLETEEnum, tesTask.State);

                var batchNodeMetrics = tesTask.GetOrAddTesTaskLog().BatchNodeMetrics;
                Assert.IsNotNull(batchNodeMetrics);
                Assert.AreEqual(60, batchNodeMetrics.BlobXferImagePullDurationInSeconds);
                Assert.AreEqual(120, batchNodeMetrics.ExecutorImagePullDurationInSeconds);
                Assert.AreEqual(3, batchNodeMetrics.ExecutorImageSizeInGB);
                Assert.AreEqual(180, batchNodeMetrics.FileDownloadDurationInSeconds);
                Assert.AreEqual(240, batchNodeMetrics.ExecutorDurationInSeconds);
                Assert.AreEqual(300, batchNodeMetrics.FileUploadDurationInSeconds);
                Assert.AreEqual(1.024, batchNodeMetrics.DiskUsedInGB);
                Assert.AreEqual(12.5f, batchNodeMetrics.DiskUsedPercent);
                Assert.AreEqual(2, batchNodeMetrics.FileDownloadSizeInGB);
                Assert.AreEqual(4, batchNodeMetrics.FileUploadSizeInGB);

                var taskLog = tesTask.GetOrAddTesTaskLog();
                Assert.AreEqual(DateTimeOffset.Parse("2020-10-08T02:30:39+00:00"), taskLog.StartTime);
                Assert.AreEqual(DateTimeOffset.Parse("2020-10-08T02:49:39+00:00"), taskLog.EndTime);
            });
        }

        [TestMethod]
        public async Task SuccessfullyCompletedTaskContainsCromwellResultCode()
        {
            var tesTask = GetTesTask();
            tesTask.State = TesState.INITIALIZINGEnum;

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchTaskState = BatchTaskStates.TaskCompletedSuccessfully[0];
            azureProxyReturnValues.DownloadedBlobContent = "2";
            var azureProxy = GetMockAzureProxy(azureProxyReturnValues);

            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), azureProxy, azureProxyReturnValues);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(TesState.COMPLETEEnum, tesTask.State);
                Assert.AreEqual(2, tesTask.GetOrAddTesTaskLog().CromwellResultCode);
                Assert.AreEqual(2, tesTask.CromwellResultCode);
            });
        }

        [TestMethod]
        public async Task TesInputFilePathMustStartWithCromwellExecutions()
        {
            var tesTask = GetTesTask();

            tesTask.Inputs.Add(new()
            {
                Path = "xyz/path"
            });

            (var failureReason, var systemLog) = await ProcessTesTaskAndGetFailureReasonAndSystemLogAsync(tesTask);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(TesState.SYSTEMERROREnum, tesTask.State);
                Assert.AreEqual($"InvalidInputFilePath", failureReason);
                Assert.AreEqual($"InvalidInputFilePath", systemLog[0]);
                Assert.AreEqual($"Unsupported input path 'xyz/path' for task Id {tesTask.Id}. Must start with '/'.", systemLog[1]);
            });
        }

        [TestMethod]
        public async Task TesInputFileMustHaveEitherUrlOrContent()
        {
            var tesTask = GetTesTask();

            tesTask.Inputs.Add(new()
            {
                Url = null,
                Content = null,
                Path = "/file1.txt"
            });

            (var failureReason, var systemLog) = await ProcessTesTaskAndGetFailureReasonAndSystemLogAsync(tesTask);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(TesState.SYSTEMERROREnum, tesTask.State);
                Assert.AreEqual($"InvalidInputFilePath", failureReason);
                Assert.AreEqual($"InvalidInputFilePath", systemLog[0]);
                Assert.AreEqual($"One of Input Url or Content must be set", systemLog[1]);
            });
        }

        [TestMethod]
        public async Task TesInputFileMustNotHaveBothUrlAndContent()
        {
            var tesTask = GetTesTask();

            tesTask.Inputs.Add(new()
            {
                Url = "/storageaccount1/container1/file1.txt",
                Content = "test content",
                Path = "/file1.txt"
            });

            (var failureReason, var systemLog) = await ProcessTesTaskAndGetFailureReasonAndSystemLogAsync(tesTask);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(TesState.SYSTEMERROREnum, tesTask.State);
                Assert.AreEqual($"InvalidInputFilePath", failureReason);
                Assert.AreEqual($"InvalidInputFilePath", systemLog[0]);
                Assert.AreEqual($"Input Url and Content cannot be both set", systemLog[1]);
            });
        }

        [TestMethod]
        [Ignore("Not applicable in the new design")]
        public async Task TesInputFileTypeMustNotBeDirectory()
        {
            var tesTask = GetTesTask();

            tesTask.Inputs.Add(new()
            {
                Url = "/storageaccount1/container1/directory",
                Type = TesFileType.DIRECTORYEnum,
                Path = "/directory"
            });

            (var failureReason, var systemLog) = await ProcessTesTaskAndGetFailureReasonAndSystemLogAsync(tesTask);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(TesState.SYSTEMERROREnum, tesTask.State);
                Assert.AreEqual($"InvalidInputFilePath", failureReason);
                Assert.AreEqual($"InvalidInputFilePath", systemLog[0]);
                Assert.AreEqual($"Directory input is not supported.", systemLog[1]);
            });
        }

        [TestMethod]
        [Ignore("Not applicable in the new design")]
        public async Task QueryStringsAreRemovedFromLocalFilePathsWhenCommandScriptIsProvidedAsFile()
        {
            var tesTask = GetTesTask();

            var originalCommandScript = "cat /cromwell-executions/workflowpath/inputs/host/path?param=2";

            tesTask.Inputs = new()
            {
                new() { Url = "/cromwell-executions/workflowpath/execution/script", Path = "/cromwell-executions/workflowpath/execution/script", Type = TesFileType.FILEEnum, Name = "commandScript", Description = "test.commandScript", Content = null },
                new() { Url = "http://host/path?param=1", Path = "/cromwell-executions/workflowpath/inputs/host/path?param=2", Type = TesFileType.FILEEnum, Name = "file1", Content = null }
            };

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.DownloadedBlobContent = originalCommandScript;
            Mock<IAzureProxy> azureProxy = default;
            var azureProxySetter = new Action<Mock<IAzureProxy>>(mock =>
            {
                GetMockAzureProxy(azureProxyReturnValues)(mock);
                azureProxy = mock;
            });

            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), azureProxySetter, azureProxyReturnValues);

            var modifiedCommandScript = (string)azureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.UploadBlobAsync) && Guid.TryParseExact(Path.GetFileName(new Uri(i.Arguments[0].ToString()).AbsolutePath), "D", out _))?.Arguments[1];
            var filesToDownload = GetFilesToDownload(azureProxy);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(TesState.INITIALIZINGEnum, tesTask.State);
                Assert.IsFalse(filesToDownload.Any(f => f.LocalPath.Contains('?') || f.LocalPath.Contains("param=1") || f.LocalPath.Contains("param=2")), "Query string was not removed from local file path");
                Assert.AreEqual(1, filesToDownload.Count(f => f.StorageUrl.Contains("?param=1")), "Query string was removed from blob URL");
                Assert.IsFalse(modifiedCommandScript.Contains("?param=2"), "Query string was not removed from local file path in command script");
            });
        }

        [TestMethod]
        [Ignore("Not applicable in the new design")]
        public async Task QueryStringsAreRemovedFromLocalFilePathsWhenCommandScriptIsProvidedAsContent()
        {
            var tesTask = GetTesTask();

            var originalCommandScript = "cat /cromwell-executions/workflowpath/inputs/host/path?param=2";

            tesTask.Inputs = new()
            {
                new() { Url = null, Path = "/cromwell-executions/workflowpath/execution/script", Type = TesFileType.FILEEnum, Name = "commandScript", Description = "test.commandScript", Content = originalCommandScript },
                new() { Url = "http://host/path?param=1", Path = "/cromwell-executions/workflowpath/inputs/host/path?param=2", Type = TesFileType.FILEEnum, Name = "file1", Content = null }
            };

            Mock<IAzureProxy> azureProxy = default;
            var azureProxySetter = new Action<Mock<IAzureProxy>>(mock =>
            {
                GetMockAzureProxy(AzureProxyReturnValues.Defaults)(mock);
                azureProxy = mock;
            });

            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), azureProxySetter, AzureProxyReturnValues.Defaults);

            var modifiedCommandScript = (string)azureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.UploadBlobAsync) && Guid.TryParseExact(Path.GetFileName(new Uri(i.Arguments[0].ToString()).AbsolutePath), "D", out _))?.Arguments[1];
            var filesToDownload = GetFilesToDownload(azureProxy);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(TesState.INITIALIZINGEnum, tesTask.State);
                Assert.AreEqual(2, filesToDownload.Count());
                Assert.IsFalse(filesToDownload.Any(f => f.LocalPath.Contains('?') || f.LocalPath.Contains("param=1") || f.LocalPath.Contains("param=2")), "Query string was not removed from local file path");
                Assert.AreEqual(1, filesToDownload.Count(f => f.StorageUrl.Contains("?param=1")), "Query string was removed from blob URL");
                Assert.IsFalse(modifiedCommandScript.Contains("?param=2"), "Query string was not removed from local file path in command script");
            });
        }

        [TestMethod]
        [Ignore("Not applicable in the new design")]
        public async Task PublicHttpUrlsAreKeptIntact()
        {
            var config = GetMockConfig()()
                .Append(("Storage:ExternalStorageContainers", "https://externalaccount1.blob.core.windows.net/container1?sas1; https://externalaccount2.blob.core.windows.net/container2/?sas2; https://externalaccount2.blob.core.windows.net?accountsas;"));

            var tesTask = GetTesTask();

            tesTask.Inputs = new()
            {
                new() { Url = null, Path = "/cromwell-executions/workflowpath/execution/script", Type = TesFileType.FILEEnum, Name = "commandScript", Description = "test.commandScript", Content = "echo hello" },
                new() { Url = "https://storageaccount1.blob.core.windows.net/container1/blob1?sig=sassignature", Path = "/cromwell-executions/workflowpath/inputs/blob1", Type = TesFileType.FILEEnum, Name = "blob1", Content = null },
                new() { Url = "https://externalaccount1.blob.core.windows.net/container1/blob2?sig=sassignature", Path = "/cromwell-executions/workflowpath/inputs/blob2", Type = TesFileType.FILEEnum, Name = "blob2", Content = null },
                new() { Url = "https://publicaccount1.blob.core.windows.net/container1/blob3", Path = "/cromwell-executions/workflowpath/inputs/blob3", Type = TesFileType.FILEEnum, Name = "blob3", Content = null }
            };

            Mock<IAzureProxy> azureProxy = default;
            var azureProxySetter = new Action<Mock<IAzureProxy>>(mock =>
            {
                GetMockAzureProxy(AzureProxyReturnValues.Defaults)(mock);
                azureProxy = mock;
            });

            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, azureProxySetter, AzureProxyReturnValues.Defaults);

            var filesToDownload = GetFilesToDownload(azureProxy);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(4, filesToDownload.Count());
                Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://storageaccount1.blob.core.windows.net/container1/blob1?sig=sassignature")));
                Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount1.blob.core.windows.net/container1/blob2?sig=sassignature")));
                Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://publicaccount1.blob.core.windows.net/container1/blob3")));
            });
        }

        [TestMethod]
        [Ignore("Not applicable in new design.")]
        public async Task PrivatePathsAndUrlsGetSasToken()
        {
            var config = GetMockConfig()()
                .Append(("Storage:ExternalStorageContainers", "https://externalaccount1.blob.core.windows.net/container1?sas1; https://externalaccount2.blob.core.windows.net/container2/?sas2; https://externalaccount2.blob.core.windows.net?accountsas;"));

            var tesTask = GetTesTask();

            tesTask.Inputs = new()
            {
                // defaultstorageaccount and storageaccount1 are accessible to TES identity
                new() { Url = null, Path = "/cromwell-executions/workflowpath/execution/script", Type = TesFileType.FILEEnum, Name = "commandScript", Description = "test.commandScript", Content = "echo hello" },

                new() { Url = "/defaultstorageaccount/container1/blob1", Path = "/cromwell-executions/workflowpath/inputs/blob1", Type = TesFileType.FILEEnum, Name = "blob1", Content = null },
                new() { Url = "/storageaccount1/container1/blob2", Path = "/cromwell-executions/workflowpath/inputs/blob2", Type = TesFileType.FILEEnum, Name = "blob2", Content = null },
                new() { Url = "/externalaccount1/container1/blob3", Path = "/cromwell-executions/workflowpath/inputs/blob3", Type = TesFileType.FILEEnum, Name = "blob3", Content = null },
                new() { Url = "/externalaccount2/container2/blob4", Path = "/cromwell-executions/workflowpath/inputs/blob4", Type = TesFileType.FILEEnum, Name = "blob4", Content = null },

                new() { Url = "file:///defaultstorageaccount/container1/blob5", Path = "/cromwell-executions/workflowpath/inputs/blob5", Type = TesFileType.FILEEnum, Name = "blob5", Content = null },
                new() { Url = "file:///storageaccount1/container1/blob6", Path = "/cromwell-executions/workflowpath/inputs/blob6", Type = TesFileType.FILEEnum, Name = "blob6", Content = null },
                new() { Url = "file:///externalaccount1/container1/blob7", Path = "/cromwell-executions/workflowpath/inputs/blob7", Type = TesFileType.FILEEnum, Name = "blob7", Content = null },
                new() { Url = "file:///externalaccount2/container2/blob8", Path = "/cromwell-executions/workflowpath/inputs/blob8", Type = TesFileType.FILEEnum, Name = "blob8", Content = null },

                new() { Url = "https://defaultstorageaccount.blob.core.windows.net/container1/blob9", Path = "/cromwell-executions/workflowpath/inputs/blob9", Type = TesFileType.FILEEnum, Name = "blob9", Content = null },
                new() { Url = "https://storageaccount1.blob.core.windows.net/container1/blob10", Path = "/cromwell-executions/workflowpath/inputs/blob10", Type = TesFileType.FILEEnum, Name = "blob10", Content = null },
                new() { Url = "https://externalaccount1.blob.core.windows.net/container1/blob11", Path = "/cromwell-executions/workflowpath/inputs/blob11", Type = TesFileType.FILEEnum, Name = "blob11", Content = null },
                new() { Url = "https://externalaccount2.blob.core.windows.net/container2/blob12", Path = "/cromwell-executions/workflowpath/inputs/blob12", Type = TesFileType.FILEEnum, Name = "blob12", Content = null },

                // ExternalStorageContainers entry exists for externalaccount2/container2 and for externalaccount2 (account level SAS), so this uses account SAS:
                new() { Url = "https://externalaccount2.blob.core.windows.net/container3/blob13", Path = "/cromwell-executions/workflowpath/inputs/blob13", Type = TesFileType.FILEEnum, Name = "blob13", Content = null },

                // ExternalStorageContainers entry exists for externalaccount1/container1, but not for externalaccount1/publiccontainer, so this is treated as public URL:
                new() { Url = "https://externalaccount1.blob.core.windows.net/publiccontainer/blob14", Path = "/cromwell-executions/workflowpath/inputs/blob14", Type = TesFileType.FILEEnum, Name = "blob14", Content = null }
            };

            Mock<IAzureProxy> azureProxy = default;
            var azureProxySetter = new Action<Mock<IAzureProxy>>(mock =>
            {
                GetMockAzureProxy(AzureProxyReturnValues.Defaults)(mock);
                azureProxy = mock;
            });

            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, azureProxySetter, AzureProxyReturnValues.Defaults);

            var filesToDownload = GetFilesToDownload(azureProxy);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(15, filesToDownload.Count());

                Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.StartsWith("https://defaultstorageaccount.blob.core.windows.net/container1/blob1?sv=")));
                Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.StartsWith("https://storageaccount1.blob.core.windows.net/container1/blob2?sv=")));
                Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount1.blob.core.windows.net/container1/blob3?sas1")));
                Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount2.blob.core.windows.net/container2/blob4?sas2")));

                Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.StartsWith("https://defaultstorageaccount.blob.core.windows.net/container1/blob5?sv=")));
                Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.StartsWith("https://storageaccount1.blob.core.windows.net/container1/blob6?sv=")));
                Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount1.blob.core.windows.net/container1/blob7?sas1")));
                Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount2.blob.core.windows.net/container2/blob8?sas2")));

                Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.StartsWith("https://defaultstorageaccount.blob.core.windows.net/container1/blob9?sv=")));
                Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.StartsWith("https://storageaccount1.blob.core.windows.net/container1/blob10?sv=")));
                Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount1.blob.core.windows.net/container1/blob11?sas1")));
                Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount2.blob.core.windows.net/container2/blob12?sas2")));

                Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount2.blob.core.windows.net/container3/blob13?accountsas")));

                Assert.IsNotNull(filesToDownload.SingleOrDefault(f => f.StorageUrl.Equals("https://externalaccount1.blob.core.windows.net/publiccontainer/blob14")));
            });
        }

        [TestMethod]
        [Ignore("Not applicable in the new design")]
        public async Task PrivateImagesArePulledUsingPoolConfiguration()
        {
            var tesTask = GetTesTask();

            Mock<IAzureProxy> azureProxy = default;
            var azureProxySetter = new Action<Mock<IAzureProxy>>(mock =>
            {
                GetMockAzureProxy(AzureProxyReturnValues.Defaults)(mock);
                azureProxy = mock;
            });
            (_, var cloudTask, _, var pool) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), azureProxySetter, AzureProxyReturnValues.Defaults);
            var batchScript = (string)azureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.UploadBlobAsync) && i.Arguments[0].ToString().Contains("/batch_script"))?.Arguments[1];

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.IsNotNull(pool.DeploymentConfiguration.VirtualMachineConfiguration.ContainerConfiguration);
                Assert.AreEqual("registryServer1.io", pool.DeploymentConfiguration.VirtualMachineConfiguration.ContainerConfiguration.ContainerRegistries.FirstOrDefault()?.RegistryServer);
                Assert.AreEqual(2, Regex.Matches(batchScript, tesTask.Executors.First().Image, RegexOptions.IgnoreCase).Count);
                Assert.IsFalse(batchScript.Contains($"docker pull --quiet {tesTask.Executors.First().Image}"));
            });
        }

        [TestMethod]
        [Ignore("Not applicable in the new design")]
        public async Task PublicImagesArePulledInTaskCommand()
        {
            var tesTask = GetTesTask();
            tesTask.Executors.First().Image = "ubuntu";

            Mock<IAzureProxy> azureProxy = default;
            var azureProxySetter = new Action<Mock<IAzureProxy>>(mock =>
            {
                GetMockAzureProxy(AzureProxyReturnValues.Defaults)(mock);
                azureProxy = mock;
            });
            (_, var cloudTask, _, var pool) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), azureProxySetter, AzureProxyReturnValues.Defaults);
            var batchScript = (string)azureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.UploadBlobAsync) && i.Arguments[0].ToString().Contains("/batch_script"))?.Arguments[1];

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.IsNull(pool.DeploymentConfiguration.VirtualMachineConfiguration.ContainerConfiguration);
                Assert.AreEqual(3, Regex.Matches(batchScript, tesTask.Executors.First().Image, RegexOptions.IgnoreCase).Count);
                Assert.IsTrue(batchScript.Contains("docker pull --quiet ubuntu"));
            });
        }

        [TestMethod]
        [Ignore("Not applicable in the new design")]
        public async Task PrivateContainersRunInsideDockerInDockerContainer()
        {
            var tesTask = GetTesTask();

            (_, var cloudTask, _, _) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(AzureProxyReturnValues.Defaults), AzureProxyReturnValues.Defaults);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.IsNotNull(cloudTask.ContainerSettings);
                Assert.AreEqual("docker", cloudTask.ContainerSettings.ImageName);
            });
        }

        [TestMethod]
        public async Task PublicContainersRunInsideRegularTaskCommand()
        {
            var tesTask = GetTesTask();
            tesTask.Executors.First().Image = "ubuntu";

            (_, var cloudTask, _, _) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(AzureProxyReturnValues.Defaults), AzureProxyReturnValues.Defaults);

            GuardAssertsWithTesTask(tesTask, () => Assert.IsNull(cloudTask.ContainerSettings));
        }

        [DataTestMethod]
        [DataRow(new string[] { null, "/cromwell-executions/workflowpath/execution/script", "echo hello" }, "blob1.tmp", false)]
        [DataRow(new string[] { "https://defaultstorageaccount.blob.core.windows.net/cromwell-executions/workflowpath/execution/script", "/cromwell-executions/workflowpath/execution/script", null }, "blob1.tmp", false)]
        [DataRow(new string[] { "https://defaultstorageaccount.blob.core.windows.net/cromwell-executions/workflowpath/execution/script", "/cromwell-executions/workflowpath/execution/script", null }, "blob1.tmp", true)]
        [DataRow(new string[] { "https://defaultstorageaccount.blob.core.windows.net/privateworkspacecontainer/cromwell-executions/workflowpath/execution/script", "/cromwell-executions/workflowpath/execution/script", null }, "blob1.tmp", false)]
        [DataRow(new string[] { "https://defaultstorageaccount.blob.core.windows.net/privateworkspacecontainer/cromwell-executions/workflowpath/execution/script", "/cromwell-executions/workflowpath/execution/script", null }, "blob1.tmp", true)]
        public async Task CromwellWriteFilesAreDiscoveredAndAddedIfMissedWithContentScript(string[] script, string fileName, bool fileIsInInputs)
        {
            var tesTask = GetTesTask();

            tesTask.Inputs = new()
            {
                new() { Url = script[0], Path = script[1], Type = TesFileType.FILEEnum, Name = "commandScript", Description = "test.commandScript", Content = script[2] },
            };

            var commandScriptUri = UriFromTesInput(tesTask.Inputs[0]);
            var executionDirectoryBlobs = tesTask.Inputs.Select(BlobNameUriFromTesInput).ToList();

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            Mock<IAzureProxy> azureProxy = default;
            var azureProxySetter = new Action<Mock<IAzureProxy>>(mock =>
            {
                GetMockAzureProxy(azureProxyReturnValues)(mock);
                azureProxy = mock;
            });

            Uri executionDirectoryUri = default;

            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), azureProxySetter, azureProxyReturnValues, serviceProviderActions: serviceProvider =>
            {
                var storageAccessProvider = serviceProvider.GetServiceOrCreateInstance<IStorageAccessProvider>();

                var commandScriptDir = new UriBuilder(commandScriptUri) { Path = Path.GetDirectoryName(commandScriptUri.AbsolutePath).Replace('\\', '/') }.Uri;
                executionDirectoryUri = UrlMutableSASEqualityComparer.TrimUri(storageAccessProvider.MapLocalPathToSasUrlAsync(commandScriptDir.IsFile ? commandScriptDir.AbsolutePath : commandScriptDir.AbsoluteUri, Azure.Storage.Sas.BlobSasPermissions.List, CancellationToken.None).Result);

                serviceProvider.AzureProxy.Setup(p => p.ListBlobsAsync(It.Is(executionDirectoryUri, new UrlMutableSASEqualityComparer()), It.IsAny<CancellationToken>())).Returns(executionDirectoryBlobs.ToAsyncEnumerable());

                var uri = new UriBuilder(executionDirectoryUri) { Query = null };
                uri.Path = uri.Path.TrimEnd('/') + $"/{fileName}";

                TesInput writeInput = new() { Url = uri.Uri.AbsoluteUri, Path = Path.Combine(Path.GetDirectoryName(script[1]), fileName).Replace('\\', '/'), Type = TesFileType.FILEEnum, Name = "write_", Content = null };
                executionDirectoryBlobs.Add(BlobNameUriFromTesInput(writeInput));

                if (fileIsInInputs)
                {
                    tesTask.Inputs.Add(writeInput);
                }
            });

            var filesToDownload = GetFilesToDownload(azureProxy).ToArray();

            GuardAssertsWithTesTask(tesTask, () =>
            {
                var inputFileUrl = filesToDownload.SingleOrDefault(f => f.LocalPath.EndsWith(fileName))?.StorageUrl;
                Assert.IsNotNull(inputFileUrl);
                Assert.AreEqual(2, filesToDownload.Length);
            });

            static BlobNameAndUri BlobNameUriFromTesInput(TesInput input)
                => new(BlobNameFromTesInput(input), UriFromTesInput(input));

            static string BlobNameFromTesInput(TesInput input)
            {
                var uri = UriFromTesInput(input);

                if (uri.IsFile)
                {
                    var trimmedPath = input.Path.TrimStart('/');
                    return trimmedPath[trimmedPath.IndexOf('/')..].TrimStart('/');
                }

                return new Azure.Storage.Blobs.BlobUriBuilder(uri).BlobName;
            }

            static Uri UriFromTesInput(TesInput input)
            {
                if (Uri.IsWellFormedUriString(input.Url, UriKind.Absolute))
                {
                    return new Uri(input.Url);
                }

                if (Uri.IsWellFormedUriString(input.Url, UriKind.Relative))
                {
                    var uri = new UriBuilder
                    {
                        Scheme = "file",
                        Path = input.Url
                    };
                    return uri.Uri;
                }

                return new UriBuilder
                {
                    Scheme = "file",
                    Path = input.Path
                }.Uri;
            }
        }

        [TestMethod]
        public async Task PoolIsCreatedInSubnetWhenBatchNodesSubnetIdIsSet()
        {
            var config = GetMockConfig()()
                .Append(("BatchNodes:SubnetId", "subnet1"));

            var tesTask = GetTesTask();
            var azureProxy = GetMockAzureProxy(AzureProxyReturnValues.Defaults);

            (_, _, _, var pool) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, azureProxy, AzureProxyReturnValues.Defaults);

            var poolNetworkConfiguration = pool.NetworkConfiguration;

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(Microsoft.Azure.Management.Batch.Models.IPAddressProvisioningType.BatchManaged, poolNetworkConfiguration?.PublicIPAddressConfiguration?.Provision);
                Assert.AreEqual("subnet1", poolNetworkConfiguration?.SubnetId);
            });
        }

        [TestMethod]
        public async Task PoolIsCreatedWithoutPublicIpWhenSubnetAndDisableBatchNodesPublicIpAddressAreSet()
        {
            var config = GetMockConfig()()
                .Append(("BatchNodes:SubnetId", "subnet1"))
                .Append(("BatchNodes:DisablePublicIpAddress", "true"));

            var tesTask = GetTesTask();
            var azureProxy = GetMockAzureProxy(AzureProxyReturnValues.Defaults);

            (_, _, _, var pool) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, azureProxy, AzureProxyReturnValues.Defaults);

            var poolNetworkConfiguration = pool.NetworkConfiguration;

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(Microsoft.Azure.Management.Batch.Models.IPAddressProvisioningType.NoPublicIPAddresses, poolNetworkConfiguration?.PublicIPAddressConfiguration?.Provision);
                Assert.AreEqual("subnet1", poolNetworkConfiguration?.SubnetId);
            });
        }

        private static async Task<(string FailureReason, string[] SystemLog)> ProcessTesTaskAndGetFailureReasonAndSystemLogAsync(TesTask tesTask, AzureBatchTaskState azureBatchTaskState = null)
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchTaskState = azureBatchTaskState ?? azureProxyReturnValues.BatchTaskState;

            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(azureProxyReturnValues), azureProxyReturnValues);

            return (tesTask.Logs?.LastOrDefault()?.FailureReason, tesTask.Logs?.LastOrDefault()?.SystemLogs?.ToArray());
        }

        private static Task<(string JobId, CloudTask CloudTask, PoolInformation PoolInformation, Pool batchModelsPool)> ProcessTesTaskAndGetBatchJobArgumentsAsync()
            => ProcessTesTaskAndGetBatchJobArgumentsAsync(GetTesTask(), GetMockConfig()(), GetMockAzureProxy(AzureProxyReturnValues.Defaults), AzureProxyReturnValues.Defaults);

        private static Task<(string JobId, CloudTask CloudTask, PoolInformation PoolInformation, Pool batchModelsPool)> ProcessTesTaskAndGetBatchJobArgumentsAsync(TesTask tesTask, IEnumerable<(string Key, string Value)> configuration, Action<Mock<IAzureProxy>> azureProxy, AzureProxyReturnValues azureProxyReturnValues, Action<IServiceCollection> additionalActions = default, Action<TestServices.TestServiceProvider<IBatchScheduler>> serviceProviderActions = default)
            => ProcessTesTasksAndGetBatchJobArgumentsAsync(new[] { tesTask }, configuration, azureProxy, azureProxyReturnValues, additionalActions, serviceProviderActions);

        private static async Task<(string JobId, CloudTask CloudTask, PoolInformation PoolInformation, Pool batchModelsPool)> ProcessTesTasksAndGetBatchJobArgumentsAsync(TesTask[] tesTasks, IEnumerable<(string Key, string Value)> configuration, Action<Mock<IAzureProxy>> azureProxy, AzureProxyReturnValues azureProxyReturnValues, Action<IServiceCollection> additionalActions = default, Action<TestServices.TestServiceProvider<IBatchScheduler>> serviceProviderActions = default)
        {
            using var serviceProvider = GetServiceProvider(
                configuration,
                azureProxy,
                GetMockQuotaProvider(azureProxyReturnValues),
                GetMockSkuInfoProvider(azureProxyReturnValues),
                GetMockAllowedVms(configuration),
                additionalActions: additionalActions);
            var batchScheduler = serviceProvider.GetT();
            serviceProviderActions?.Invoke(serviceProvider);

            if (azureProxyReturnValues.BatchTaskState is null)
            {
                await foreach (var _ in batchScheduler.ProcessQueuedTesTasksAsync(tesTasks, CancellationToken.None)) { }
            }
            else
            {
                await foreach (var _ in batchScheduler.ProcessTesTaskBatchStatesAsync(tesTasks, Enumerable.Repeat(azureProxyReturnValues.BatchTaskState, tesTasks.Length).ToArray(), CancellationToken.None)) { }
            }

            var createBatchPoolAsyncInvocation = serviceProvider.AzureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.CreateBatchPoolAsync));
            var addBatchTaskAsyncInvocation = serviceProvider.AzureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.AddBatchTaskAsync));

            var jobId = (addBatchTaskAsyncInvocation?.Arguments[0]) as string;
            var cloudTask = (addBatchTaskAsyncInvocation?.Arguments[1]) as CloudTask;
            var poolInformation = (addBatchTaskAsyncInvocation?.Arguments[2]) as PoolInformation;
            var batchPoolsModel = createBatchPoolAsyncInvocation?.Arguments[0] as Pool;

            return (jobId, cloudTask, poolInformation, batchPoolsModel);
        }

        private static Action<Mock<IAllowedVmSizesService>> GetMockAllowedVms(IEnumerable<(string Key, string Value)> configuration)
            => new(proxy =>
            {
                var allowedVmsConfig = configuration.FirstOrDefault(x => x.Key == "AllowedVmSizes").Value;
                var allowedVms = new List<string>();
                if (!string.IsNullOrWhiteSpace(allowedVmsConfig))
                {
                    allowedVms = allowedVmsConfig.Split(",").ToList();
                }
                proxy.Setup(p => p.GetAllowedVmSizes(It.IsAny<CancellationToken>()))
                    .ReturnsAsync(allowedVms);
            });


        private static Action<Mock<IBatchSkuInformationProvider>> GetMockSkuInfoProvider(AzureProxyReturnValues azureProxyReturnValues)
            => new(proxy =>
                proxy.Setup(p => p.GetVmSizesAndPricesAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(azureProxyReturnValues.VmSizesAndPrices));

        private static Action<Mock<IBatchQuotaProvider>> GetMockQuotaProvider(AzureProxyReturnValues azureProxyReturnValues)
            => new(quotaProvider =>
            {
                var batchQuotas = azureProxyReturnValues.BatchQuotas;
                var vmFamilyQuota = batchQuotas.DedicatedCoreQuotaPerVMFamily?.FirstOrDefault(v => string.Equals(v.Name, "VmFamily1", StringComparison.InvariantCultureIgnoreCase))?.CoreQuota ?? 0;

                quotaProvider.Setup(p =>
                        p.GetQuotaForRequirementAsync(It.IsAny<string>(), It.Is<bool>(p => p == false), It.IsAny<int?>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(() => new BatchVmFamilyQuotas(batchQuotas.DedicatedCoreQuota,
                        vmFamilyQuota,
                        batchQuotas.PoolQuota,
                        batchQuotas.ActiveJobAndJobScheduleQuota,
                        batchQuotas.DedicatedCoreQuotaPerVMFamilyEnforced, "VmSize1"));
                quotaProvider.Setup(p =>
                        p.GetQuotaForRequirementAsync(It.IsAny<string>(), It.Is<bool>(p => p == true), It.IsAny<int?>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(() => new BatchVmFamilyQuotas(batchQuotas.LowPriorityCoreQuota,
                        vmFamilyQuota,
                        batchQuotas.PoolQuota,
                        batchQuotas.ActiveJobAndJobScheduleQuota,
                        batchQuotas.DedicatedCoreQuotaPerVMFamilyEnforced, "VmSize1"));

                quotaProvider.Setup(p =>
                        p.GetVmCoreQuotaAsync(It.Is<bool>(l => l == true), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(new BatchVmCoreQuota(batchQuotas.LowPriorityCoreQuota,
                        true,
                        batchQuotas.DedicatedCoreQuotaPerVMFamilyEnforced,
                        batchQuotas.DedicatedCoreQuotaPerVMFamily?.Select(v => new BatchVmCoresPerFamily(v.Name, v.CoreQuota)).ToList(),
                        new(batchQuotas.ActiveJobAndJobScheduleQuota, batchQuotas.PoolQuota, batchQuotas.DedicatedCoreQuota, batchQuotas.LowPriorityCoreQuota)));
                quotaProvider.Setup(p =>
                        p.GetVmCoreQuotaAsync(It.Is<bool>(l => l == false), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(new BatchVmCoreQuota(batchQuotas.DedicatedCoreQuota,
                        false,
                        batchQuotas.DedicatedCoreQuotaPerVMFamilyEnforced,
                        batchQuotas.DedicatedCoreQuotaPerVMFamily?.Select(v => new BatchVmCoresPerFamily(v.Name, v.CoreQuota)).ToList(),
                        new(batchQuotas.ActiveJobAndJobScheduleQuota, batchQuotas.PoolQuota, batchQuotas.DedicatedCoreQuota, batchQuotas.LowPriorityCoreQuota)));
            });

        private static TestServices.TestServiceProvider<IBatchScheduler> GetServiceProvider(IEnumerable<(string Key, string Value)> configuration, Action<Mock<IAzureProxy>> azureProxy, Action<Mock<IBatchQuotaProvider>> quotaProvider, Action<Mock<IBatchSkuInformationProvider>> skuInfoProvider, Action<Mock<IAllowedVmSizesService>> allowedVmSizesServiceSetup, Action<IServiceCollection> additionalActions = default)
            => new(wrapAzureProxy: true, configuration: configuration, azureProxy: azureProxy, batchQuotaProvider: quotaProvider, batchSkuInformationProvider: skuInfoProvider, accountResourceInformation: GetNewBatchResourceInfo(), allowedVmSizesServiceSetup: allowedVmSizesServiceSetup, additionalActions: additionalActions);

        private static async Task<TesState> GetNewTesTaskStateAsync(TesTask tesTask, AzureProxyReturnValues azureProxyReturnValues)
        {
            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(azureProxyReturnValues), azureProxyReturnValues);

            return tesTask.State;
        }

        private static async Task<TesState> GetNewTesTaskStateAsync(TesState currentTesTaskState, IEnumerable<AzureBatchTaskState> batchTaskStates)
        {
            var tesTask = new TesTask { Id = "test", State = currentTesTaskState, Executors = new() { new() { Image = "imageName1", Command = new() { "command" } } } };
            TesState result = default;

            foreach (var batchTaskState in batchTaskStates)
            {
                result = await GetNewTesTaskStateAsync(tesTask, batchTaskState);
            }

            return result;
        }

        private static Task<TesState> GetNewTesTaskStateAsync(TesTask tesTask, AzureBatchTaskState batchTaskState = default)
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchTaskState = batchTaskState ?? azureProxyReturnValues.BatchTaskState;

            return GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
        }

        private static Task<TesState> GetNewTesTaskStateAsync(TesResources resources, AzureProxyReturnValues proxyReturnValues)
        {
            var tesTask = GetTesTask();
            tesTask.Resources = resources;

            return GetNewTesTaskStateAsync(tesTask, proxyReturnValues);
        }

        private static TesTask GetTesTask()
            => JsonConvert.DeserializeObject<TesTask>(File.ReadAllText("testask1.json"));

        private static Action<Mock<IAzureProxy>> GetMockAzureProxy(AzureProxyReturnValues azureProxyReturnValues)
            => azureProxy =>
            {
                azureProxy.Setup(a => a.BlobExistsAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(true);

                azureProxy.Setup(a => a.GetActivePoolsAsync(It.IsAny<string>()))
                    .Returns(AsyncEnumerable.Empty<CloudPool>());

                azureProxy.Setup(a => a.GetStorageAccountInfoAsync("defaultstorageaccount", It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(azureProxyReturnValues.StorageAccountInfos["defaultstorageaccount"]));

                azureProxy.Setup(a => a.GetStorageAccountInfoAsync("storageaccount1", It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(azureProxyReturnValues.StorageAccountInfos["storageaccount1"]));

                azureProxy.Setup(a => a.GetStorageAccountKeyAsync(It.IsAny<StorageAccountInfo>(), It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(azureProxyReturnValues.StorageAccountKey));

                azureProxy.Setup(a => a.GetBatchActiveNodeCountByVmSize())
                    .Returns(azureProxyReturnValues.ActiveNodeCountByVmSize);

                azureProxy.Setup(a => a.GetBatchActiveJobCount())
                    .Returns(azureProxyReturnValues.ActiveJobCount);

                azureProxy.Setup(a => a.GetBatchActivePoolCount())
                    .Returns(azureProxyReturnValues.ActivePoolCount);

                azureProxy.Setup(a => a.GetBatchPoolAsync(It.IsAny<string>(), It.IsAny<CancellationToken>(), It.IsAny<DetailLevel>()))
                    .Returns((string id, CancellationToken cancellationToken, DetailLevel detailLevel) => Task.FromResult(azureProxyReturnValues.GetBatchPoolImpl(id)));

                azureProxy.Setup(a => a.DownloadBlobAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(azureProxyReturnValues.DownloadedBlobContent));

                azureProxy.Setup(a => a.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                    .Returns((Pool p, bool _1, CancellationToken _2) => Task.FromResult(azureProxyReturnValues.CreateBatchPoolImpl(p)));

                azureProxy.Setup(a => a.GetFullAllocationStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(azureProxyReturnValues.AzureProxyGetFullAllocationState?.Invoke() ?? new(null, null, null, null, null, null, null)));

                azureProxy.Setup(a => a.ListComputeNodesAsync(It.IsAny<string>(), It.IsAny<DetailLevel>()))
                    .Returns(new Func<string, DetailLevel, IAsyncEnumerable<ComputeNode>>((string poolId, DetailLevel _1)
                        => AsyncEnumerable.Empty<ComputeNode>()
                            .Append(BatchPoolTests.GenerateNode(poolId, "ComputeNodeDedicated1", true, true))));

                azureProxy.Setup(a => a.DeleteBatchPoolAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                    .Callback<string, CancellationToken>((poolId, cancellationToken) => azureProxyReturnValues.AzureProxyDeleteBatchPoolImpl(poolId, cancellationToken))
                    .Returns(Task.CompletedTask);

                azureProxy.Setup(a => a.ListTasksAsync(It.IsAny<string>(), It.IsAny<DetailLevel>()))
                    .Returns(azureProxyReturnValues.AzureProxyListTasks);

                azureProxy.Setup(a => a.ListBlobsAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()))
                    .Returns(AsyncEnumerable.Empty<BlobNameAndUri>());
            };

        private static Func<IEnumerable<(string Key, string Value)>> GetMockConfig()
            => new(() =>
            {
                var config = Enumerable.Empty<(string Key, string Value)>()
                .Append(("Storage:DefaultAccountName", "defaultstorageaccount"))
                .Append(("BatchScheduling:Prefix", "hostname"))
                .Append(("BatchImageGen1:Offer", "ubuntu-server-container"))
                .Append(("BatchImageGen1:Publisher", "microsoft-azure-batch"))
                .Append(("BatchImageGen1:Sku", "20-04-lts"))
                .Append(("BatchImageGen1:Version", "latest"))
                .Append(("BatchImageGen1:NodeAgentSkuId", "batch.node.ubuntu 20.04"))
                .Append(("BatchImageGen2:Offer", "ubuntu-hpc"))
                .Append(("BatchImageGen2:Publisher", "microsoft-dsvm"))
                .Append(("BatchImageGen2:Sku", "2004"))
                .Append(("BatchImageGen2:Version", "latest"))
                .Append(("BatchImageGen2:NodeAgentSkuId", "batch.node.ubuntu 20.04"));

                return config;
            });

        private static IEnumerable<FileToDownload> GetFilesToDownload(Mock<IAzureProxy> azureProxy)
        {
            var downloadFilesScriptContent = (string)azureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.UploadBlobAsync) && i.Arguments[0].ToString().Contains("/runner-task.json"))?.Arguments[1];

            if (string.IsNullOrEmpty(downloadFilesScriptContent))
            {
                return new List<FileToDownload>();
            }

            var fileInputs = JsonConvert.DeserializeObject<Tes.Runner.Models.NodeTask>(downloadFilesScriptContent)?.Inputs ?? Enumerable.Empty<Tes.Runner.Models.FileInput>().ToList();

            return fileInputs
                .Select(f => new FileToDownload { LocalPath = f.Path, StorageUrl = f.SourceUrl });
        }

        private static TestServices.TestServiceProvider<IBatchScheduler> GetServiceProvider(AzureProxyReturnValues azureProxyReturn = default)
        {
            azureProxyReturn ??= AzureProxyReturnValues.Defaults;
            var config = GetMockConfig()();
            return new(
                wrapAzureProxy: true,
                accountResourceInformation: new("defaultbatchaccount", "defaultresourcegroup", "defaultsubscription", "defaultregion", "defaultendpoint"),
                configuration: config,
                azureProxy: GetMockAzureProxy(azureProxyReturn),
                batchQuotaProvider: GetMockQuotaProvider(azureProxyReturn),
                batchSkuInformationProvider: GetMockSkuInfoProvider(azureProxyReturn),
                allowedVmSizesServiceSetup: GetMockAllowedVms(config));
        }

        private static async Task<BatchPool> AddPool(BatchScheduler batchScheduler)
            => (BatchPool)await batchScheduler.GetOrAddPoolAsync("key1", false, (id, cancellationToken) => ValueTask.FromResult<Pool>(new(name: id, displayName: "display1", vmSize: "vmSize1")), CancellationToken.None);

        internal static void GuardAssertsWithTesTask(TesTask tesTask, Action assertBlock)
        {
            ArgumentNullException.ThrowIfNull(tesTask);
            ArgumentNullException.ThrowIfNull(assertBlock);

            try
            {
                assertBlock();
            }
            catch (AssertFailedException)
            {
                foreach (var log in tesTask.Logs ?? Enumerable.Empty<TesTaskLog>())
                {
                    Console.WriteLine("Task failure: State: {0}: FailureReason: {1} SystemLogs: {2}", tesTask.State, log.FailureReason, string.Join(Environment.NewLine, log.SystemLogs));
                }

                throw;
            }
        }

        internal static async ValueTask GuardAssertsWithTesTask(TesTask tesTask, Func<ValueTask> assertBlock)
        {
            ArgumentNullException.ThrowIfNull(tesTask);
            ArgumentNullException.ThrowIfNull(assertBlock);

            try
            {
                await assertBlock();
            }
            catch (AssertFailedException)
            {
                foreach (var log in tesTask.Logs)
                {
                    Console.WriteLine("Task failure: State: {0}: FailureReason: {1} SystemLogs: {2}", tesTask.State, log.FailureReason, string.Join(Environment.NewLine, log.SystemLogs));
                }

                throw;
            }
        }


        private struct BatchTaskStates
        {
            public static AzureBatchTaskState[] TaskActive => new[] { new AzureBatchTaskState(AzureBatchTaskState.TaskState.InfoUpdate) };
            public static AzureBatchTaskState[] TaskPreparing => new[] { new AzureBatchTaskState(AzureBatchTaskState.TaskState.Initializing, CloudTaskCreationTime: DateTimeOffset.UtcNow) };
            public static AzureBatchTaskState[] TaskRunning => new[] { new AzureBatchTaskState(AzureBatchTaskState.TaskState.Running, CloudTaskCreationTime: DateTimeOffset.UtcNow - TimeSpan.FromMinutes(6)) };
            public static AzureBatchTaskState[] TaskCompletedSuccessfully => new[] { new AzureBatchTaskState(AzureBatchTaskState.TaskState.CompletedSuccessfully, BatchTaskExitCode: 0) };
            public static AzureBatchTaskState[] TaskFailed => new[]
            {
                new AzureBatchTaskState(AzureBatchTaskState.TaskState.InfoUpdate, Failure: new(AzureBatchTaskState.ExecutorError, new[] { TaskFailureInformationCodes.FailureExitCode, @"1" }), ExecutorExitCode: 1),
                new AzureBatchTaskState(AzureBatchTaskState.TaskState.CompletedWithErrors, Failure: new(AzureBatchTaskState.SystemError, new[] { TaskFailureInformationCodes.FailureExitCode, @"1" }), BatchTaskExitCode: 1)
            };
            public static AzureBatchTaskState[] NodeDiskFull => new[] { new AzureBatchTaskState(AzureBatchTaskState.TaskState.NodeFailedDuringStartupOrExecution, Failure: new("DiskFull", new[] { "Error message." })) };
            public static AzureBatchTaskState[] UploadOrDownloadFailed => new[] { new AzureBatchTaskState(AzureBatchTaskState.TaskState.NodeFilesUploadOrDownloadFailed) };
            public static AzureBatchTaskState[] NodeAllocationFailed => new[] { new AzureBatchTaskState(AzureBatchTaskState.TaskState.NodeAllocationFailed, Failure: new(AzureBatchTaskState.TaskState.NodeAllocationFailed.ToString(), new[] { "Error message." })) };
            public static AzureBatchTaskState[] NodePreempted => new[] { new AzureBatchTaskState(AzureBatchTaskState.TaskState.NodePreempted) };
            public static AzureBatchTaskState[] NodeStartTaskFailed => new[] { new AzureBatchTaskState(AzureBatchTaskState.TaskState.NodeStartTaskFailed) };
            public static AzureBatchTaskState[] CancellationRequested => new[] { new AzureBatchTaskState(AzureBatchTaskState.TaskState.CancellationRequested, CloudTaskCreationTime: DateTimeOffset.UtcNow - TimeSpan.FromMinutes(12)) };
        }

        private class AzureProxyReturnValues
        {
            internal Func<FullBatchPoolAllocationState> AzureProxyGetFullAllocationState { get; set; }
            internal Action<string, CancellationToken> AzureProxyDeleteBatchPoolIfExists { get; set; }
            internal Action<string, CancellationToken> AzureProxyDeleteBatchPool { get; set; }
            internal Func<string, ODATADetailLevel, IAsyncEnumerable<CloudTask>> AzureProxyListTasks { get; set; } = (jobId, detail) => AsyncEnumerable.Empty<CloudTask>();
            public Dictionary<string, StorageAccountInfo> StorageAccountInfos { get; set; }
            public List<VirtualMachineInformation> VmSizesAndPrices { get; set; }
            public AzureBatchAccountQuotas BatchQuotas { get; set; }
            public IEnumerable<AzureBatchNodeCount> ActiveNodeCountByVmSize { get; set; }
            public int ActiveJobCount { get; set; }
            public int ActivePoolCount { get; set; }
            public AzureBatchTaskState BatchTaskState { get; set; }
            public string StorageAccountKey { get; set; }
            public string DownloadedBlobContent { get; set; }

            public static AzureProxyReturnValues Defaults => new()
            {
                AzureProxyGetFullAllocationState = () => new(Microsoft.Azure.Batch.Common.AllocationState.Steady, DateTime.MinValue.ToUniversalTime(), true, 0, 0, 0, 0),
                AzureProxyDeleteBatchPoolIfExists = (poolId, cancellationToken) => { },
                AzureProxyDeleteBatchPool = (poolId, cancellationToken) => { },
                StorageAccountInfos = new() {
                    { "defaultstorageaccount", new() { Name = "defaultstorageaccount", Id = "Id", BlobEndpoint = new("https://defaultstorageaccount.blob.core.windows.net/"), SubscriptionId = "SubId" } },
                    { "storageaccount1", new() { Name = "storageaccount1", Id = "Id", BlobEndpoint = new("https://storageaccount1.blob.core.windows.net/"), SubscriptionId = "SubId" } }
                },
                VmSizesAndPrices = new() {
                    new() { VmSize = "VmSizeLowPri1", VmFamily = "VmFamily1", LowPriority = true, VCpusAvailable = 1, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 1 },
                    new() { VmSize = "VmSizeLowPri2", VmFamily = "VmFamily2", LowPriority = true, VCpusAvailable = 2, MemoryInGiB = 8, ResourceDiskSizeInGiB = 40, PricePerHour = 2 },
                    new() { VmSize = "VmSizeDedicated1", VmFamily = "VmFamily1", LowPriority = false, VCpusAvailable = 1, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 11 },
                    new() { VmSize = "VmSizeDedicated2", VmFamily = "VmFamily2", LowPriority = false, VCpusAvailable = 2, MemoryInGiB = 8, ResourceDiskSizeInGiB = 40, PricePerHour = 22 }
                },
                BatchQuotas = new() { ActiveJobAndJobScheduleQuota = 1, PoolQuota = 1, DedicatedCoreQuota = 5, LowPriorityCoreQuota = 10, DedicatedCoreQuotaPerVMFamily = new List<VirtualMachineFamilyCoreQuota>() },
                ActiveNodeCountByVmSize = new List<AzureBatchNodeCount>(),
                ActiveJobCount = 0,
                ActivePoolCount = 0,
                BatchTaskState = default,
                StorageAccountKey = "Key1",
                DownloadedBlobContent = string.Empty,
            };

            public static AzureProxyReturnValues DefaultsPerVMFamilyEnforced => DefaultsPerVMFamilyEnforcedImpl();

            private static AzureProxyReturnValues DefaultsPerVMFamilyEnforcedImpl()
            {
                var proxy = Defaults;
                proxy.VmSizesAndPrices.Add(new() { VmSize = "VmSize3", VmFamily = "VmFamily3", LowPriority = false, VCpusAvailable = 4, MemoryInGiB = 12, ResourceDiskSizeInGiB = 80, PricePerHour = 33 });
                proxy.BatchQuotas = new()
                {
                    DedicatedCoreQuotaPerVMFamilyEnforced = true,
                    DedicatedCoreQuotaPerVMFamily = new VirtualMachineFamilyCoreQuota[] { new("VmFamily1", proxy.BatchQuotas.DedicatedCoreQuota), new("VmFamily2", 0), new("VmFamily3", 4) },
                    DedicatedCoreQuota = proxy.BatchQuotas.DedicatedCoreQuota,
                    ActiveJobAndJobScheduleQuota = proxy.BatchQuotas.ActiveJobAndJobScheduleQuota,
                    LowPriorityCoreQuota = proxy.BatchQuotas.LowPriorityCoreQuota,
                    PoolQuota = proxy.BatchQuotas.PoolQuota
                };
                return proxy;
            }

            private readonly Dictionary<string, IList<Microsoft.Azure.Batch.MetadataItem>> poolMetadata = new();

            internal void AzureProxyDeleteBatchPoolImpl(string poolId, CancellationToken cancellationToken)
            {
                _ = poolMetadata.Remove(poolId);
                AzureProxyDeleteBatchPool(poolId, cancellationToken);
            }

            internal CloudPool CreateBatchPoolImpl(Pool pool)
            {
                var poolId = pool.Name;
                var metadata = pool.Metadata?.Select(Convert).ToList();

                poolMetadata.Add(poolId, metadata);
                return BatchPoolTests.GeneratePool(id: poolId, creationTime: DateTime.UtcNow, metadata: metadata);

                static Microsoft.Azure.Batch.MetadataItem Convert(Microsoft.Azure.Management.Batch.Models.MetadataItem item)
                    => new(item.Name, item.Value);
            }

            internal CloudPool GetBatchPoolImpl(string poolId)
            {
                if (!poolMetadata.TryGetValue(poolId, out var items))
                {
                    items = null;
                }

                return BatchPoolTests.GeneratePool(poolId, metadata: items);
            }
        }

        private class TestMultitaskBatchQuotaVerifierQuotaMaxedOut : TestBatchQuotaVerifierBase
        {
            public TestMultitaskBatchQuotaVerifierQuotaMaxedOut(IBatchQuotaProvider batchQuotaProvider) : base(batchQuotaProvider) { }

            public override Task<CheckGroupPoolAndJobQuotaResult> CheckBatchAccountPoolAndJobQuotasAsync(int required, CancellationToken cancellationToken)
                => Task.FromResult(new CheckGroupPoolAndJobQuotaResult(required / 2, new AzureBatchQuotaMaxedOutException("Test AzureBatchQuotaMaxedOutException")));

            public override Task CheckBatchAccountQuotasAsync(VirtualMachineInformation _1, bool _2, CancellationToken cancellationToken)
                => Task.CompletedTask;
        }

        private class TestMultitaskBatchQuotaVerifierQuotaAllAllowed : TestBatchQuotaVerifierBase
        {
            public TestMultitaskBatchQuotaVerifierQuotaAllAllowed(IBatchQuotaProvider batchQuotaProvider) : base(batchQuotaProvider) { }

            public override Task<CheckGroupPoolAndJobQuotaResult> CheckBatchAccountPoolAndJobQuotasAsync(int required, CancellationToken cancellationToken)
                => Task.FromResult(new CheckGroupPoolAndJobQuotaResult(0, null));

            public override Task CheckBatchAccountQuotasAsync(VirtualMachineInformation _1, bool _2, CancellationToken cancellationToken)
                => Task.CompletedTask;
        }

        private class TestBatchQuotaVerifierQuotaMaxedOut : TestBatchQuotaVerifierBase
        {
            public TestBatchQuotaVerifierQuotaMaxedOut(IBatchQuotaProvider batchQuotaProvider) : base(batchQuotaProvider) { }

            public override Task<CheckGroupPoolAndJobQuotaResult> CheckBatchAccountPoolAndJobQuotasAsync(int required, CancellationToken cancellationToken)
                => Task.FromResult(new CheckGroupPoolAndJobQuotaResult(required / 2, new AzureBatchQuotaMaxedOutException("Test AzureBatchQuotaMaxedOutException")));

            public override Task CheckBatchAccountQuotasAsync(VirtualMachineInformation _1, bool _2, CancellationToken cancellationToken)
                => throw new AzureBatchQuotaMaxedOutException("Test AzureBatchQuotaMaxedOutException");
        }

        private class TestBatchQuotaVerifierLowQuota : TestBatchQuotaVerifierBase
        {
            public TestBatchQuotaVerifierLowQuota(IBatchQuotaProvider batchQuotaProvider) : base(batchQuotaProvider) { }

            public override Task<CheckGroupPoolAndJobQuotaResult> CheckBatchAccountPoolAndJobQuotasAsync(int required, CancellationToken cancellationToken)
                => throw new NotSupportedException();

            public override Task CheckBatchAccountQuotasAsync(VirtualMachineInformation _1, bool _2, CancellationToken cancellationToken)
                => throw new AzureBatchLowQuotaException("Test AzureBatchLowQuotaException");
        }

        private abstract class TestBatchQuotaVerifierBase : IBatchQuotaVerifier
        {
            private readonly IBatchQuotaProvider batchQuotaProvider;

            protected TestBatchQuotaVerifierBase(IBatchQuotaProvider batchQuotaProvider)
                => this.batchQuotaProvider = batchQuotaProvider;

            public abstract Task<CheckGroupPoolAndJobQuotaResult> CheckBatchAccountPoolAndJobQuotasAsync(int required, CancellationToken cancellationToken);

            public abstract Task CheckBatchAccountQuotasAsync(VirtualMachineInformation virtualMachineInformation, bool needPoolOrJobQuotaCheck, CancellationToken cancellationToken);

            public IBatchQuotaProvider GetBatchQuotaProvider()
                => batchQuotaProvider;
        }

        private sealed class UrlMutableSASEqualityComparer : IEqualityComparer<Uri>
        {
            internal static Uri TrimUri(Uri uri)
            {
                var builder = new UriBuilder(uri);
                builder.Query = builder.Query[0..24];
                return builder.Uri;
            }

            public bool Equals(Uri x, Uri y)
            {
                if (x is null && y is null) return true; // TODO: verify
                if (x is null || y is null) return false;
                return EqualityComparer<Uri>.Default.Equals(TrimUri(x), TrimUri(y));
            }

            public int GetHashCode([DisallowNull] Uri uri)
            {
                ArgumentNullException.ThrowIfNull(uri);
                return TrimUri(uri).GetHashCode();
            }
        }

        private class FileToDownload
        {
            public string StorageUrl { get; set; }
            public string LocalPath { get; set; }
        }
    }
}
