// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.ResourceManager.Batch;
using Azure.ResourceManager.Batch.Models;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;
using Tes.Extensions;
using Tes.Models;
using Tes.TaskSubmitters;
using TesApi.Tests.Storage;
using TesApi.Web;
using TesApi.Web.Management;
using TesApi.Web.Management.Batch;
using TesApi.Web.Management.Models.Quotas;
using TesApi.Web.Storage;

namespace TesApi.Tests
{
    [TestClass]
    public class BatchSchedulerTests
    {
        private const string GlobalManagedIdentity = "/subscriptions/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/resourceGroups/SomeResourceGroup/providers/Microsoft.ManagedIdentity/userAssignedIdentities/GlobalManagedIdentity";

        [TestMethod]
        public void GetOrAddExecutorLogCreatesLogAtIndex()
        {
            var index = 3;
            TesTask tesTask = new() { Logs = [new()] };

            var log = tesTask.Logs.Last().GetOrAddExecutorLog(index);

            Assert.IsNotNull(log);
            Assert.AreSame(log, tesTask.Logs.Last().Logs[index]);
        }

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

            pool = (BatchPool)await batchScheduler.GetOrAddPoolAsync(key, false, (id, cancellationToken) => ValueTask.FromResult(BatchPoolTests.CreatePoolData(name: id)), CancellationToken.None);

            Assert.IsNotNull(pool);
            Assert.AreEqual(1, batchScheduler.GetPoolGroupKeys().Count());
            Assert.IsTrue(batchScheduler.TryGetPool(pool.PoolId, out var pool1));
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
            serviceProvider.BatchPoolManager.Verify(mock => mock.CreateBatchPoolAsync(It.IsAny<BatchAccountPoolData>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Once);

            var pool = await batchScheduler.GetOrAddPoolAsync(key, false, (id, cancellationToken) => ValueTask.FromResult(BatchPoolTests.CreatePoolData(name: id)), CancellationToken.None);
            await pool.ServicePoolAsync();

            Assert.AreEqual(count, batchScheduler.GetPools().Count());
            Assert.AreEqual(keyCount, batchScheduler.GetPoolGroupKeys().Count());
            //Assert.AreSame(info, pool);
            Assert.AreEqual(info.PoolId, pool.PoolId);
            serviceProvider.BatchPoolManager.Verify(mock => mock.CreateBatchPoolAsync(It.IsAny<BatchAccountPoolData>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Once);
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

            var pool = await batchScheduler.GetOrAddPoolAsync(key, false, (id, cancellationToken) => ValueTask.FromResult(BatchPoolTests.CreatePoolData(name: id)), CancellationToken.None);
            await pool.ServicePoolAsync();

            Assert.AreNotEqual(count, batchScheduler.GetPools().Count());
            Assert.AreEqual(keyCount, batchScheduler.GetPoolGroupKeys().Count());
            //Assert.AreNotSame(info, pool);
            Assert.AreNotEqual(info.PoolId, pool.PoolId);
        }


        [TestMethod]
        public async Task TryGetReturnsTrueAndCorrectPool()
        {
            using var serviceProvider = GetServiceProvider();
            var batchScheduler = serviceProvider.GetT() as BatchScheduler;
            var info = await AddPool(batchScheduler);

            var result = batchScheduler.TryGetPool(info.PoolId, out var pool);

            Assert.IsTrue(result);
            //Assert.AreSame(infoPoolId, pool);
            Assert.AreEqual(info.PoolId, pool.PoolId);
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

            Assert.AreEqual(pool.PoolId, poolId);
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

            azureProxyReturnValues.VmSizesAndPrices =
            [
                new() { VmSize = "VmSize1", VmFamily = "VmFamily1", LowPriority = true, VCpusAvailable = 1, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 1 },
                new() { VmSize = "VmSize2", VmFamily = "VmFamily1", LowPriority = true, VCpusAvailable = 2, MemoryInGiB = 8, ResourceDiskSizeInGiB = 40, PricePerHour = 2 }
            ];

            var state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = true, BackendParameters = new() { { "vm_size", "VmSize1" } } }, azureProxyReturnValues);
            Assert.AreEqual(TesState.INITIALIZING, state);

            state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = true, BackendParameters = new() { { "vm_size", "VMSIZE1" } } }, azureProxyReturnValues);
            Assert.AreEqual(TesState.INITIALIZING, state);

            state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = true, BackendParameters = new() { { "vm_size", "VmSize1" } }, CpuCores = 1000, RamGb = 100000, DiskGb = 1000000 }, azureProxyReturnValues);
            Assert.AreEqual(TesState.INITIALIZING, state);

            state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = true, BackendParameters = [], CpuCores = 1000, RamGb = 100000, DiskGb = 1000000 }, azureProxyReturnValues);
            Assert.AreEqual(TesState.SYSTEM_ERROR, state);

            state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = false, BackendParameters = new() { { "vm_size", "VmSize1" } } }, azureProxyReturnValues);
            Assert.AreEqual(TesState.SYSTEM_ERROR, state);

            state = await GetNewTesTaskStateAsync(new TesResources { Preemptible = true, BackendParameters = new() { { "vm_size", "VmSize3" } } }, azureProxyReturnValues);
            Assert.AreEqual(TesState.SYSTEM_ERROR, state);
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
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            using var serviceProvider = GetServiceProvider(
                config,
                GetMockAzureProxy(azureProxyReturnValues),
                GetMockBatchPoolManager(azureProxyReturnValues),
                GetMockQuotaProvider(azureProxyReturnValues),
                GetMockSkuInfoProvider(azureProxyReturnValues),
                GetMockAllowedVms(config));
            var batchScheduler = serviceProvider.GetT();

            var size = await ((BatchScheduler)batchScheduler).GetVmSizeAsync(task, CancellationToken.None);
            GuardAssertsWithTesTask(task, () => Assert.AreEqual(vmSize, size.VM.VmSize));
        }

        private static BatchAccountResourceInformation GetNewBatchResourceInfo()
          => new("batchAccount", "mrg", "sub-id", "eastus", "batchAccount/endpoint");

        [TestMethod]
        public async Task TesTaskFailsWithSystemErrorWhenNoSuitableVmExists()
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            azureProxyReturnValues.VmSizesAndPrices =
            [
                new() { VmSize = "VmSize1", LowPriority = true, VCpusAvailable = 1, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 1 },
                new() { VmSize = "VmSize2", LowPriority = true, VCpusAvailable = 2, MemoryInGiB = 8, ResourceDiskSizeInGiB = 40, PricePerHour = 2 }
            ];

            Assert.AreEqual(TesState.SYSTEM_ERROR, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 1, DiskGb = 10, Preemptible = false }, azureProxyReturnValues));
            Assert.AreEqual(TesState.SYSTEM_ERROR, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 4, RamGb = 1, DiskGb = 10, Preemptible = true }, azureProxyReturnValues));
            Assert.AreEqual(TesState.SYSTEM_ERROR, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 10, DiskGb = 10, Preemptible = true }, azureProxyReturnValues));
            Assert.AreEqual(TesState.SYSTEM_ERROR, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 1, RamGb = 1, DiskGb = 50, Preemptible = true }, azureProxyReturnValues));
        }

        [TestMethod]
        public async Task TesTaskFailsWithSystemErrorWhenTotalBatchQuotaIsSetTooLow()
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchQuotas = new() { ActiveJobAndJobScheduleQuota = 1, PoolQuota = 1, DedicatedCoreQuota = 1, LowPriorityCoreQuota = 10 };

            Assert.AreEqual(TesState.SYSTEM_ERROR, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 2, RamGb = 1, Preemptible = false }, azureProxyReturnValues));
            Assert.AreEqual(TesState.SYSTEM_ERROR, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 11, RamGb = 1, Preemptible = true }, azureProxyReturnValues));

            List<BatchVmFamilyCoreQuota> dedicatedCoreQuotaPerVMFamily = [CreateBatchVmFamilyCoreQuota("VmFamily2", 1)];
            azureProxyReturnValues.BatchQuotas = new()
            {
                ActiveJobAndJobScheduleQuota = 1,
                PoolQuota = 1,
                DedicatedCoreQuota = 100,
                LowPriorityCoreQuota = 100,
                DedicatedCoreQuotaPerVMFamilyEnforced = true,
                DedicatedCoreQuotaPerVMFamily = dedicatedCoreQuotaPerVMFamily
            };

            Assert.AreEqual(TesState.SYSTEM_ERROR, await GetNewTesTaskStateAsync(new TesResources { CpuCores = 2, RamGb = 1, Preemptible = false }, azureProxyReturnValues));
        }

        [TestMethod]
        public async Task TesTaskFailsWhenBatchNodeDiskIsFull()
        {
            var tesTask = GetTesTask();
            tesTask.State = TesState.INITIALIZING;

            (var failureReason, var systemLog) = await ProcessTesTaskAndGetFailureReasonAndSystemLogAsync(tesTask, BatchTaskStates.NodeDiskFull[0]);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(TesState.SYSTEM_ERROR, tesTask.State); // TODO: Should be ExecutorError, but this currently falls into the bucket of NodeFailedDuringStartupOrExecution, which also covers StartTask failures, which are more correctly SystemError.
                Assert.AreEqual("DiskFull", failureReason);
                Assert.AreEqual("DiskFull", systemLog[0]);
                Assert.AreEqual("DiskFull", tesTask.FailureReason);
            });
        }

        private async Task AddBatchTasksHandlesExceptions(TesState? newState, Func<AzureProxyReturnValues, (Action<IServiceCollection>, Action<Mock<IAzureProxy>>, Action<Mock<IBatchPoolManager>>)> testArranger, Action<TesTask, IEnumerable<(LogLevel, Exception, string)>> resultValidator, int numberOfTasks = 1)
        {
            var logger = new Mock<ILogger<BatchScheduler>>();
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            var (providerModifier, azureProxyModifier, batchPoolManagerModifier) = testArranger?.Invoke(azureProxyReturnValues) ?? (default, default, default);
            var azureProxy = new Action<Mock<IAzureProxy>>(mock =>
            {
                GetMockAzureProxy(azureProxyReturnValues)(mock);
                azureProxyModifier?.Invoke(mock);
            });

            var batchPoolManager = new Action<Mock<IBatchPoolManager>>(mock =>
            {
                GetMockBatchPoolManager(azureProxyReturnValues)(mock);
                batchPoolManagerModifier?.Invoke(mock);
            });

            var tasks = Enumerable.Repeat(0, numberOfTasks).Select((_, index) =>
            {
                var task = GetTesTask();
                task.State = TesState.QUEUED;

                if (numberOfTasks > 1)
                {
                    task.Id = Guid.NewGuid().ToString("D");
                    task.Resources.BackendParameters ??= [];
                    task.Resources.BackendParameters.Add("vm_size", index % 2 == 1 ? "VmSizeDedicated1" : "VmSizeDedicated2");
                }

                return task;
            }).ToArray();

            _ = await ProcessTesTasksAndGetBatchJobArgumentsAsync(
                tasks,
                GetMockConfig()(),
                azureProxy,
                batchPoolManager,
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
            return AddBatchTasksHandlesExceptions(TesState.QUEUED, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>, Action<Mock<IBatchPoolManager>>) Arranger(AzureProxyReturnValues _1)
                => (default,
                    azureProxy => azureProxy.Setup(b => b.CreateBatchJobAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                    .Callback<string, string, CancellationToken>((_, _, _)
                        => throw new Microsoft.Rest.Azure.CloudException("No job for you.") { Body = new() { Code = BatchErrorCodeStrings.OperationTimedOut } }),
                    default);

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
            return AddBatchTasksHandlesExceptions(TesState.QUEUED, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>, Action<Mock<IBatchPoolManager>>) Arranger(AzureProxyReturnValues _1)
                => (default,
                    default,
                    azureProxy => azureProxy.Setup(b => b.CreateBatchPoolAsync(It.IsAny<BatchAccountPoolData>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                        .Callback<BatchAccountPoolData, bool, CancellationToken>((poolInfo, isPreemptible, cancellationToken)
                            => throw new Microsoft.Rest.Azure.CloudException("No pool for you.") { Body = new() { Code = BatchErrorCodeStrings.OperationTimedOut } }));

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
            return AddBatchTasksHandlesExceptions(TesState.QUEUED, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>, Action<Mock<IBatchPoolManager>>) Arranger(AzureProxyReturnValues _1)
                => (services => services.AddSingleton<IBatchQuotaVerifier, TestBatchQuotaVerifierQuotaMaxedOut>(),
                    default,
                    default);

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

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>, Action<Mock<IBatchPoolManager>>) Arranger(AzureProxyReturnValues _1)
                => (services => services.AddSingleton<IBatchQuotaVerifier, TestMultitaskBatchQuotaVerifierQuotaMaxedOut>(), default, default);

            void Validator(TesTask tesTask, IEnumerable<(LogLevel logLevel, Exception exception, string message)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    switch (tesTask.State)
                    {
                        case TesState.QUEUED:
                            {
                                var log = logs.LastOrDefault(l => l.message.Contains(tesTask.Id));
                                Assert.IsNotNull(log);
                                Assert.AreEqual(LogLevel.Warning, log.logLevel);
                                Assert.IsNull(log.exception);
                                Assert.IsTrue(log.message.Contains(nameof(AzureBatchQuotaMaxedOutException)));
                            }
                            ++quotaDelayedTasks;
                            break;

                        case TesState.INITIALIZING:
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

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>, Action<Mock<IBatchPoolManager>>) Arranger(AzureProxyReturnValues _1)
                => (services => services.AddSingleton<IBatchQuotaVerifier, TestMultitaskBatchQuotaVerifierQuotaAllAllowed>(), default, default);

            void Validator(TesTask tesTask, IEnumerable<(LogLevel logLevel, Exception exception, string message)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    switch (tesTask.State)
                    {
                        case TesState.QUEUED:
                            {
                                var log = logs.LastOrDefault(l => l.message.Contains(tesTask.Id));
                                Assert.IsNotNull(log);
                                Assert.AreEqual(LogLevel.Warning, log.logLevel);
                                Assert.IsNull(log.exception);
                                Assert.IsTrue(log.message.Contains(nameof(AzureBatchQuotaMaxedOutException)));
                            }
                            ++quotaDelayedTasks;
                            break;

                        case TesState.INITIALIZING:
                            {
                                var log = tesTask.Logs?.LastOrDefault();
                                Assert.IsNotNull(log);
                                Assert.IsNotNull(log.VirtualMachineInfo);
                                Assert.IsNotNull(log.VirtualMachineInfo.VmSize);
                            }
                            ++queuedTasks;
                            break;

                        default:
                            Assert.Fail($"Unexpected TesState: {tesTask.State}.");
                            break;
                    }
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesAzureBatchLowQuotaException()
        {
            return AddBatchTasksHandlesExceptions(TesState.SYSTEM_ERROR, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>, Action<Mock<IBatchPoolManager>>) Arranger(AzureProxyReturnValues _1)
                => (services => services.AddSingleton<IBatchQuotaVerifier, TestBatchQuotaVerifierLowQuota>(),
                    default,
                    default);

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
            return AddBatchTasksHandlesExceptions(TesState.SYSTEM_ERROR, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>, Action<Mock<IBatchPoolManager>>) Arranger(AzureProxyReturnValues proxy)
            {
                proxy.VmSizesAndPrices = Enumerable.Empty<VirtualMachineInformation>().ToList();
                return (default, default, default);
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
            return AddBatchTasksHandlesExceptions(TesState.SYSTEM_ERROR, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>, Action<Mock<IBatchPoolManager>>) Arranger(AzureProxyReturnValues _1)
                => (default,
                    default,
                    azureProxy => azureProxy.Setup(b => b.CreateBatchPoolAsync(It.IsAny<BatchAccountPoolData>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                        .Callback<BatchAccountPoolData, bool, CancellationToken>((poolInfo, isPreemptible, cancellationToken)
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
            return AddBatchTasksHandlesExceptions(TesState.SYSTEM_ERROR, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>, Action<Mock<IBatchPoolManager>>) Arranger(AzureProxyReturnValues _1)
                => (default,
                    default,
                    azureProxy => azureProxy.Setup(b => b.CreateBatchPoolAsync(It.IsAny<BatchAccountPoolData>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                        .Callback<BatchAccountPoolData, bool, CancellationToken>((poolInfo, isPreemptible, cancellationToken)
                            => throw typeof(BatchClientException)
                                .GetConstructor(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance,
                                    [typeof(string), typeof(Exception)])
                                .Invoke([null, null]) as Exception));

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
            return AddBatchTasksHandlesExceptions(TesState.QUEUED, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>, Action<Mock<IBatchPoolManager>>) Arranger(AzureProxyReturnValues _1)
                => (default,
                    default,
                    azureProxy => azureProxy.Setup(b => b.CreateBatchPoolAsync(It.IsAny<BatchAccountPoolData>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                        .Callback<BatchAccountPoolData, bool, CancellationToken>((poolInfo, isPreemptible, cancellationToken)
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
            return AddBatchTasksHandlesExceptions(TesState.QUEUED, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>, Action<Mock<IBatchPoolManager>>) Arranger(AzureProxyReturnValues _1)
                => (default,
                    default,
                    azureProxy => azureProxy.Setup(b => b.CreateBatchPoolAsync(It.IsAny<BatchAccountPoolData>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                        .Callback<BatchAccountPoolData, bool, CancellationToken>((poolInfo, isPreemptible, cancellationToken)
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
            return AddBatchTasksHandlesExceptions(TesState.QUEUED, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>, Action<Mock<IBatchPoolManager>>) Arranger(AzureProxyReturnValues _1)
                => (default,
                    default,
                    azureProxy => azureProxy.Setup(b => b.CreateBatchPoolAsync(It.IsAny<BatchAccountPoolData>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                        .Callback<BatchAccountPoolData, bool, CancellationToken>((poolInfo, isPreemptible, cancellationToken)
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
            return AddBatchTasksHandlesExceptions(TesState.SYSTEM_ERROR, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>, Action<Mock<IBatchPoolManager>>) Arranger(AzureProxyReturnValues _1)
                => (services => services.AddTransient(p => batchQuotaProvider.Object), default, default);

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
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            using var serviceProvider = GetServiceProvider(
                config,
                GetMockAzureProxy(azureProxyReturnValues),
                GetMockBatchPoolManager(azureProxyReturnValues),
                GetMockQuotaProvider(azureProxyReturnValues),
                GetMockSkuInfoProvider(azureProxyReturnValues),
                GetMockAllowedVms(config));
            var batchScheduler = serviceProvider.GetT();

            {
                await using PerformBatchSchedulerBackgroundTasks _1 = new(batchScheduler);
                _ = await batchScheduler.ProcessQueuedTesTaskAsync(tesTask, CancellationToken.None);
            }

            var createBatchPoolAsyncInvocation = serviceProvider.BatchPoolManager.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IBatchPoolManager.CreateBatchPoolAsync));
            var pool = createBatchPoolAsyncInvocation?.Arguments[0] as BatchAccountPoolData;

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual("TES-hostname-edicated1-woy4muc7mxr23jl23zwg4kn2ynz4kvzo-", tesTask.PoolId[0..^8]);
                Assert.AreEqual("VmSizeDedicated1", pool.VmSize);
                Assert.IsTrue(((BatchScheduler)batchScheduler).TryGetPool(tesTask.PoolId, out _));
            });
        }

        [TestCategory("TES 1.1")]
        [TestMethod]
        public async Task BatchPoolContainsExpectedIdentity()
        {
            var identity = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/coa/providers/Microsoft.ManagedIdentity/userAssignedIdentities/coa-test-uami";
            var task = GetTesTask();
            task.Resources.BackendParameters = new()
            {
                { "workflow_execution_identity", identity }
            };
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(task, GetMockConfig()(), GetMockAzureProxy(azureProxyReturnValues), GetMockBatchPoolManager(azureProxyReturnValues), azureProxyReturnValues);

            GuardAssertsWithTesTask(task, () =>
            {
                Assert.AreEqual("VmSizeDedicated1", poolSpec.VmSize);
                Assert.IsTrue(poolSpec.ScaleSettings.AutoScale.Formula.Contains("TargetDedicated"));
                Assert.AreEqual(2, poolSpec.Identity.UserAssignedIdentities.Count);
                Assert.AreEqual(identity, poolSpec.Identity.UserAssignedIdentities.Keys.Skip(1).First());
            });
        }

        [TestMethod]
        public async Task NewTesTaskGetsScheduledSuccessfully()
        {
            var tesTask = GetTesTask();
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(azureProxyReturnValues), GetMockBatchPoolManager(azureProxyReturnValues), azureProxyReturnValues);

            GuardAssertsWithTesTask(tesTask, () => Assert.AreEqual(TesState.INITIALIZING, tesTask.State));
        }

        [TestMethod]
        public async Task PreemptibleTesTaskGetsScheduledToLowPriorityVm()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = true;
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(azureProxyReturnValues), GetMockBatchPoolManager(azureProxyReturnValues), azureProxyReturnValues);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual("VmSizeLowPri1", poolSpec.VmSize);
                Assert.IsTrue(poolSpec.ScaleSettings.AutoScale.Formula.Contains("\n$TargetLowPriorityNodes = "));
                Assert.IsFalse(poolSpec.ScaleSettings.AutoScale.Formula.Contains("\n$TargetDedicated = "));
            });
        }

        [TestMethod]
        public async Task NonPreemptibleTesTaskGetsScheduledToDedicatedVm()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = false;
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(azureProxyReturnValues), GetMockBatchPoolManager(azureProxyReturnValues), azureProxyReturnValues);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual("VmSizeDedicated1", poolSpec.VmSize);
                Assert.IsTrue(poolSpec.ScaleSettings.AutoScale.Formula.Contains("\n$TargetDedicated = "));
                Assert.IsFalse(poolSpec.ScaleSettings.AutoScale.Formula.Contains("\n$TargetLowPriorityNodes = "));
            });
        }

        [TestMethod]
        public async Task PreemptibleTesTaskGetsScheduledToLowPriorityVm_PerVMFamilyEnforced()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = true;
            var azureProxyReturnValues = AzureProxyReturnValues.DefaultsPerVMFamilyEnforced;

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(azureProxyReturnValues), GetMockBatchPoolManager(azureProxyReturnValues), azureProxyReturnValues);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual("VmSizeLowPri1", poolSpec.VmSize);
                Assert.IsTrue(poolSpec.ScaleSettings.AutoScale.Formula.Contains("\n$TargetLowPriorityNodes = "));
                Assert.IsFalse(poolSpec.ScaleSettings.AutoScale.Formula.Contains("\n$TargetDedicated = "));
            });
        }

        [TestMethod]
        public async Task NonPreemptibleTesTaskGetsScheduledToDedicatedVm_PerVMFamilyEnforced()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = false;
            var azureProxyReturnValues = AzureProxyReturnValues.DefaultsPerVMFamilyEnforced;

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(azureProxyReturnValues), GetMockBatchPoolManager(azureProxyReturnValues), azureProxyReturnValues);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual("VmSizeDedicated1", poolSpec.VmSize);
                Assert.IsTrue(poolSpec.ScaleSettings.AutoScale.Formula.Contains("\n$TargetDedicated = "));
                Assert.IsFalse(poolSpec.ScaleSettings.AutoScale.Formula.Contains("\n$TargetLowPriorityNodes = "));
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

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(azureProxyReturnValues), GetMockBatchPoolManager(azureProxyReturnValues), azureProxyReturnValues);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.IsTrue(tesTask.Logs.Any(l => "UsedLowPriorityInsteadOfDedicatedVm".Equals(l.Warning)));
                Assert.IsTrue(poolSpec.ScaleSettings.AutoScale.Formula.Contains("\n$TargetLowPriorityNodes = "));
            });
        }

        [TestMethod]
        public async Task TesTaskGetsScheduledToLowPriorityVmIfSettingUsePreemptibleVmsOnlyIsSet()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = false;

            var config = GetMockConfig()()
                .Append(("BatchScheduling:UsePreemptibleVmsOnly", "true"));
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, GetMockAzureProxy(azureProxyReturnValues), GetMockBatchPoolManager(azureProxyReturnValues), azureProxyReturnValues);

            GuardAssertsWithTesTask(tesTask, () => Assert.IsTrue(poolSpec.ScaleSettings.AutoScale.Formula.Contains("\n$TargetLowPriorityNodes = ")));
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
                var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

                (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, GetMockAzureProxy(azureProxyReturnValues), GetMockBatchPoolManager(azureProxyReturnValues), azureProxyReturnValues);

                GuardAssertsWithTesTask(tesTask, () =>
                {
                    Assert.AreEqual(expectedTaskState, tesTask.State);

                    if (expectedSelectedVmSize is not null)
                    {
                        Assert.AreEqual(expectedSelectedVmSize, poolSpec.VmSize);
                    }
                });
            }

            await RunTest(null, TesState.INITIALIZING, "VmSizeLowPri1");
            await RunTest(string.Empty, TesState.INITIALIZING, "VmSizeLowPri1");
            await RunTest("VmSizeLowPri1", TesState.INITIALIZING, "VmSizeLowPri1");
            await RunTest("VmSizeLowPri1,VmSizeLowPri2", TesState.INITIALIZING, "VmSizeLowPri1");
            await RunTest("VmSizeLowPri2", TesState.INITIALIZING, "VmSizeLowPri2");
            await RunTest("VmSizeLowPriNonExistent", TesState.SYSTEM_ERROR);
            await RunTest("VmSizeLowPriNonExistent,VmSizeLowPri1", TesState.INITIALIZING, "VmSizeLowPri1");
            await RunTest("VmFamily2", TesState.INITIALIZING, "VmSizeLowPri2");
        }

        [TestMethod]
        public async Task TaskStateTransitionsFromRunningState()
        {
            Assert.AreEqual(TesState.RUNNING, await GetNewTesTaskStateAsync(TesState.RUNNING, BatchTaskStates.TaskActive));
            Assert.AreEqual(TesState.INITIALIZING, await GetNewTesTaskStateAsync(TesState.RUNNING, BatchTaskStates.TaskPreparing));
            Assert.AreEqual(TesState.RUNNING, await GetNewTesTaskStateAsync(TesState.RUNNING, BatchTaskStates.TaskRunning));
            Assert.AreEqual(TesState.COMPLETE, await GetNewTesTaskStateAsync(TesState.RUNNING, BatchTaskStates.TaskCompletedSuccessfully));
            Assert.AreEqual(TesState.EXECUTOR_ERROR, await GetNewTesTaskStateAsync(TesState.RUNNING, BatchTaskStates.TaskFailed));
            Assert.AreEqual(TesState.CANCELED, await GetNewTesTaskStateAsync(TesState.RUNNING, BatchTaskStates.CancellationRequested));
            Assert.AreEqual(TesState.SYSTEM_ERROR, await GetNewTesTaskStateAsync(TesState.RUNNING, BatchTaskStates.NodeDiskFull));
            Assert.AreEqual(TesState.RUNNING, await GetNewTesTaskStateAsync(TesState.RUNNING, BatchTaskStates.UploadOrDownloadFailed));
            Assert.AreEqual(TesState.INITIALIZING, await GetNewTesTaskStateAsync(TesState.RUNNING, BatchTaskStates.NodePreempted));
        }

        [TestMethod]
        public async Task TaskStateTransitionsFromInitializingState()
        {
            Assert.AreEqual(TesState.INITIALIZING, await GetNewTesTaskStateAsync(TesState.INITIALIZING, BatchTaskStates.TaskActive));
            Assert.AreEqual(TesState.INITIALIZING, await GetNewTesTaskStateAsync(TesState.INITIALIZING, BatchTaskStates.TaskPreparing));
            Assert.AreEqual(TesState.RUNNING, await GetNewTesTaskStateAsync(TesState.INITIALIZING, BatchTaskStates.TaskRunning));
            Assert.AreEqual(TesState.COMPLETE, await GetNewTesTaskStateAsync(TesState.INITIALIZING, BatchTaskStates.TaskCompletedSuccessfully));
            Assert.AreEqual(TesState.EXECUTOR_ERROR, await GetNewTesTaskStateAsync(TesState.INITIALIZING, BatchTaskStates.TaskFailed));
            Assert.AreEqual(TesState.CANCELED, await GetNewTesTaskStateAsync(TesState.INITIALIZING, BatchTaskStates.CancellationRequested));
            Assert.AreEqual(TesState.SYSTEM_ERROR, await GetNewTesTaskStateAsync(TesState.INITIALIZING, BatchTaskStates.NodeDiskFull));
            Assert.AreEqual(TesState.INITIALIZING, await GetNewTesTaskStateAsync(TesState.INITIALIZING, BatchTaskStates.UploadOrDownloadFailed));
            Assert.AreEqual(TesState.SYSTEM_ERROR, await GetNewTesTaskStateAsync(TesState.INITIALIZING, BatchTaskStates.NodeStartTaskFailed));
            Assert.AreEqual(TesState.QUEUED, await GetNewTesTaskStateAsync(TesState.INITIALIZING, BatchTaskStates.NodeAllocationFailed));
            Assert.AreEqual(TesState.INITIALIZING, await GetNewTesTaskStateAsync(TesState.INITIALIZING, BatchTaskStates.NodePreempted));
        }

        [TestMethod]
        public async Task TaskStateTransitionsFromQueuedState()
        {
            Assert.AreEqual(TesState.INITIALIZING, await GetNewTesTaskStateAsync(TesState.QUEUED, [default]));
            Assert.AreEqual(TesState.CANCELED, await GetNewTesTaskStateAsync(TesState.QUEUED, BatchTaskStates.CancellationRequested));
        }

        [TestMethod]
        public async Task TaskIsRequeuedUpToThreeTimesForTransientErrors()
        {
            var tesTask = GetTesTask();

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            azureProxyReturnValues.VmSizesAndPrices =
            [
                new() { VmSize = "VmSize1", VmFamily = "VmFamily1", LowPriority = false, VCpusAvailable = 2, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 1 },
                new() { VmSize = "VmSize2", VmFamily = "VmFamily2", LowPriority = false, VCpusAvailable = 2, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 2 },
                new() { VmSize = "VmSize3", VmFamily = "VmFamily3", LowPriority = false, VCpusAvailable = 2, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 3 },
                new() { VmSize = "VmSize4", VmFamily = "VmFamily4", LowPriority = false, VCpusAvailable = 2, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 4 },
                new() { VmSize = "VmSize5", VmFamily = "VmFamily5", LowPriority = false, VCpusAvailable = 2, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 5 }
            ];

            await GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
            await GuardAssertsWithTesTask(tesTask, async () => Assert.AreEqual(TesState.QUEUED, await GetNewTesTaskStateAsync(tesTask, BatchTaskStates.NodeAllocationFailed[0])));
            await GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
            await GuardAssertsWithTesTask(tesTask, async () => Assert.AreEqual(TesState.QUEUED, await GetNewTesTaskStateAsync(tesTask, BatchTaskStates.NodeAllocationFailed[0])));
            await GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
            await GuardAssertsWithTesTask(tesTask, async () => Assert.AreEqual(TesState.QUEUED, await GetNewTesTaskStateAsync(tesTask, BatchTaskStates.NodeAllocationFailed[0])));
            await GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
            await GuardAssertsWithTesTask(tesTask, async () => Assert.AreEqual(TesState.SYSTEM_ERROR, await GetNewTesTaskStateAsync(tesTask, BatchTaskStates.NodeAllocationFailed[0])));
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
                Assert.AreEqual(TesState.SYSTEM_ERROR, tesTask.State);
                Assert.AreEqual("NoVmSizeAvailable", tesTask.FailureReason);
            });
        }

        [TestMethod]
        public async Task TaskGetsCancelled()
        {
            var poolId = "pool1-1";
            var tesTask = new TesTask { Id = "test", State = TesState.CANCELING, Logs = [new()] };

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchTaskState = BatchTaskStates.CancellationRequested[0];

            BatchAccountPoolData poolInfo = new()
            {
                DeploymentConfiguration = new()
                {
                    VmConfiguration = new BatchVmConfiguration(new BatchImageReference(), "batchNodeAgent"),
                },
                Identity = new(Azure.ResourceManager.Models.ManagedServiceIdentityType.UserAssigned),
            };

            poolInfo.Identity.UserAssignedIdentities.Add(new(new(GlobalManagedIdentity), new()));
            poolInfo.Metadata.Add(new("CoA-TES-Metadata", @"{""hostName"":""hostname"",""isDedicated"":true,""RunnerMD5"":""MD5"",""eventsVersion"":{""EventVersion"":""1.0"",""TesTaskRunnerEntityType"":""TesRunnerTask"",""EventDataVersion"":""1.0""}}"));
            poolInfo.Metadata.Add(new(string.Empty, poolId));
            tesTask.PoolId = azureProxyReturnValues.CreateBatchPoolImpl(poolInfo);
            var job = BatchPoolTests.GenerateJob(tesTask.PoolId, Microsoft.Azure.Batch.Protocol.Models.JobState.Active, new(tesTask.PoolId));

            Mock<IAzureProxy> azureProxy = default;
            var azureProxySetter = new Action<Mock<IAzureProxy>>(mock =>
            {
                GetMockAzureProxy(azureProxyReturnValues)(mock);
                mock.Setup(m => m.GetBatchJobAsync(poolId, It.IsAny<CancellationToken>(), It.IsAny<DetailLevel>()))
                    .Returns(Task.FromResult(job));
                azureProxy = mock;
            });

            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(
                tesTask,
                GetMockConfig()(),
                azureProxySetter,
                GetMockBatchPoolManager(azureProxyReturnValues),
                azureProxyReturnValues,
                serviceProviderActions: (_, scheduler) => scheduler.LoadExistingPoolsAsync(CancellationToken.None).GetAwaiter().GetResult());

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(TesState.CANCELED, tesTask.State);
                azureProxy.Verify(i => i.TerminateBatchTaskAsync(tesTask.Id, It.IsAny<string>(), It.IsAny<CancellationToken>()));
            });
        }

        //[TestMethod]
        //public async Task CancelledTaskGetsDeleted()
        //{
        //    var tesTask = new TesTask
        //    {
        //        Id = "test", PoolId = "pool1", State = TesState.CANCELED, Logs = new()
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
            tesTask.State = TesState.INITIALIZING;

            var metricsFileContent = @"
                ExecutorPullStart=2020-10-08T02:35:39+00:00
                ExecutorImageSizeInBytes=3000000000
                ExecutorPullEnd=2020-10-08T02:37:39+00:00
                DownloadStart=2020-10-08T02:30:39+00:00
                FileDownloadSizeInBytes=2000000000
                DownloadEnd=2020-10-08T02:33:39+00:00
                ExecutorStart=2020-10-08T02:39:39+00:00
                ExecutorEnd=2020-10-08T02:43:39+00:00
                UploadStart=2020-10-08T02:44:39+00:00
                FileUploadSizeInBytes=4000000000
                UploadEnd=2020-10-08T02:49:39+00:00
                DiskSizeInKiB=8000000
                DiskUsedInKiB=1000000".Replace(" ", string.Empty);

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.DownloadedBlobContent = metricsFileContent;

            foreach (var batchTaskState in BatchTaskStates.TaskCompletedSuccessfully)
            {
                azureProxyReturnValues.BatchTaskState = batchTaskState;
                _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(azureProxyReturnValues), GetMockBatchPoolManager(azureProxyReturnValues), azureProxyReturnValues);
            }

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(TesState.COMPLETE, tesTask.State);

                var batchNodeMetrics = tesTask.GetOrAddTesTaskLog().BatchNodeMetrics;
                Assert.IsNotNull(batchNodeMetrics);
                Assert.AreEqual(120, batchNodeMetrics.ExecutorImagePullDurationInSeconds[0]);
                Assert.AreEqual(3, batchNodeMetrics.ExecutorImageSizeInGB[0]);
                Assert.AreEqual(180, batchNodeMetrics.FileDownloadDurationInSeconds);
                Assert.AreEqual(240, batchNodeMetrics.ExecutorDurationInSeconds[0]);
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
            tesTask.State = TesState.INITIALIZING;

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.DownloadedBlobContent = "2";
            var azureProxy = GetMockAzureProxy(azureProxyReturnValues);
            var batchPoolManager = GetMockBatchPoolManager(azureProxyReturnValues);

            foreach (var batchTaskState in BatchTaskStates.TaskCompletedSuccessfully)
            {
                azureProxyReturnValues.BatchTaskState = batchTaskState;
                _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), azureProxy, batchPoolManager, azureProxyReturnValues);
            }

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(TesState.COMPLETE, tesTask.State);
                Assert.AreEqual(2, tesTask.GetOrAddTesTaskLog().CromwellResultCode);
                Assert.AreEqual(2, tesTask.CromwellResultCode);
            });
        }

        [DataTestMethod]
        [DataRow(new string[] { null, "echo hello" }, "blob1.tmp", false, DisplayName = "commandScript via content")]
        [DataRow(new string[] { "https://defaultstorageaccount.blob.core.windows.net/cromwell-executions/workflow1/0fbdb535-4afd-45e3-a8a8-c8e50585ee4e/call-Task1/execution/script", null }, "blob1.tmp", false, DisplayName = "default url with file missing")]
        [DataRow(new string[] { "https://defaultstorageaccount.blob.core.windows.net/cromwell-executions/workflow1/0fbdb535-4afd-45e3-a8a8-c8e50585ee4e/call-Task1/execution/script", null }, "blob1.tmp", true, DisplayName = "default url with file present")]
        [DataRow(new string[] { "https://defaultstorageaccount.blob.core.windows.net/privateworkspacecontainer/cromwell-executions/workflow1/0fbdb535-4afd-45e3-a8a8-c8e50585ee4e/call-Task1/execution/script", null }, "blob1.tmp", false, DisplayName = "custom container with file missing")]
        [DataRow(new string[] { "https://defaultstorageaccount.blob.core.windows.net/privateworkspacecontainer/cromwell-executions/workflow1/0fbdb535-4afd-45e3-a8a8-c8e50585ee4e/call-Task1/execution/script", null }, "blob1.tmp", true, DisplayName = "custom container with file present")]
        public async Task CromwellWriteFilesAreDiscoveredAndAddedIfMissedWithContentScript(string[] script, string fileName, bool fileIsInInputs)
        {
            var tesTask = GetTesTask();
            var scriptPath = $"{tesTask.GetCromwellMetadata().CromwellExecutionDir}/script";

            tesTask.Inputs =
            [
                new() { Url = script[0], Path = scriptPath, Type = TesFileType.FILE, Name = "commandScript", Description = "test.commandScript", Content = script[1] },
            ];

            // fixup output urls
            {
                var executionDirectoryUrl = script[0] ?? tesTask.Outputs.First(o => o.Name.Equals("commandScript", StringComparison.Ordinal)).Url;
                executionDirectoryUrl = executionDirectoryUrl[..^"/script".Length];
                var textToReplace = "/cromwell-executions/workflow1/0fbdb535-4afd-45e3-a8a8-c8e50585ee4e/call-Task1/execution/";
                tesTask.Outputs.ForEach(o => o.Url = o.Url.Replace(textToReplace, executionDirectoryUrl + "/"));
                tesTask.TaskSubmitter = TaskSubmitter.Parse(tesTask);
            }

            var commandScriptUri = UriFromTesOutput(tesTask.Outputs.First(o => o.Name.Equals("commandScript", StringComparison.Ordinal)));
            var executionDirectoryBlobs = tesTask.Inputs.Select(BlobNameUriFromTesInput).ToList();

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;

            Mock<IAzureProxy> azureProxy = default;
            var azureProxySetter = new Action<Mock<IAzureProxy>>(mock =>
            {
                GetMockAzureProxy(azureProxyReturnValues)(mock);
                azureProxy = mock;
            });

            Uri executionDirectoryUri = default;

            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), azureProxySetter, GetMockBatchPoolManager(azureProxyReturnValues), azureProxyReturnValues, serviceProviderActions: (serviceProvider, _) =>
            {
                var storageAccessProvider = serviceProvider.GetServiceOrCreateInstance<IStorageAccessProvider>();

                var commandScriptDir = new UriBuilder(commandScriptUri) { Path = Path.GetDirectoryName(commandScriptUri.AbsolutePath).Replace('\\', '/') }.Uri;
                executionDirectoryUri = UrlMutableSASEqualityComparer.TrimUri(storageAccessProvider.MapLocalPathToSasUrlAsync(commandScriptDir.IsFile ? commandScriptDir.AbsolutePath : commandScriptDir.AbsoluteUri, Azure.Storage.Sas.BlobSasPermissions.List, CancellationToken.None).Result);

                serviceProvider.AzureProxy.Setup(p => p.ListBlobsAsync(It.Is(executionDirectoryUri, new UrlMutableSASEqualityComparer()), It.IsAny<CancellationToken>())).Returns(executionDirectoryBlobs.ToAsyncEnumerable());

                var uri = new UriBuilder(executionDirectoryUri) { Query = null };
                uri.Path = uri.Path.TrimEnd('/') + $"/{fileName}";

                TesInput writeInput = new() { Url = uri.Uri.AbsoluteUri, Path = Path.Combine(Path.GetDirectoryName(scriptPath), fileName).Replace('\\', '/'), Type = TesFileType.FILE, Name = "write_", Content = null };
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

            static Uri UriFromTesOutput(TesOutput output)
            {
                if (Uri.IsWellFormedUriString(output.Url, UriKind.Absolute))
                {
                    return new Uri(output.Url);
                }

                if (Uri.IsWellFormedUriString(output.Url, UriKind.Relative))
                {
                    var uri = new UriBuilder
                    {
                        Scheme = "file",
                        Path = output.Url
                    };
                    return uri.Uri;
                }

                return new UriBuilder
                {
                    Scheme = "file",
                    Path = output.Path
                }.Uri;
            }
        }

        [TestMethod]
        public async Task PoolIsCreatedInSubnetWhenBatchNodesSubnetIdIsSet()
        {
            var config = GetMockConfig()()
                .Append(("BatchNodes:SubnetId", "subnet1"));

            var tesTask = GetTesTask();
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            var azureProxy = GetMockAzureProxy(azureProxyReturnValues);
            var batchPoolManager = GetMockBatchPoolManager(azureProxyReturnValues);

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, azureProxy, batchPoolManager, azureProxyReturnValues);

            var poolNetworkConfiguration = poolSpec.NetworkConfiguration;

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(BatchIPAddressProvisioningType.BatchManaged, poolNetworkConfiguration?.PublicIPAddressConfiguration?.Provision);
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
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            var azureProxy = GetMockAzureProxy(azureProxyReturnValues);
            var batchPoolManager = GetMockBatchPoolManager(azureProxyReturnValues);

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, azureProxy, batchPoolManager, azureProxyReturnValues);

            var poolNetworkConfiguration = poolSpec.NetworkConfiguration;

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(BatchIPAddressProvisioningType.NoPublicIPAddresses, poolNetworkConfiguration?.PublicIPAddressConfiguration?.Provision);
                Assert.AreEqual("subnet1", poolNetworkConfiguration?.SubnetId);
            });
        }

        private static async Task<(string FailureReason, string[] SystemLog)> ProcessTesTaskAndGetFailureReasonAndSystemLogAsync(TesTask tesTask, AzureBatchTaskState azureBatchTaskState = null)
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchTaskState = azureBatchTaskState ?? azureProxyReturnValues.BatchTaskState;

            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(azureProxyReturnValues), GetMockBatchPoolManager(azureProxyReturnValues), azureProxyReturnValues);

            return (tesTask.Logs?.LastOrDefault()?.FailureReason, tesTask.Logs?.LastOrDefault()?.SystemLogs?.ToArray());
        }

        private static Task<(string JobId, IEnumerable<CloudTask> CloudTask, BatchAccountPoolData batchModelsPool)> ProcessTesTaskAndGetBatchJobArgumentsAsync(TesTask tesTask, IEnumerable<(string Key, string Value)> configuration, Action<Mock<IAzureProxy>> azureProxy, Action<Mock<IBatchPoolManager>> batchPoolManager, AzureProxyReturnValues azureProxyReturnValues, Action<IServiceCollection> additionalActions = default, Action<TestServices.TestServiceProvider<IBatchScheduler>, IBatchScheduler> serviceProviderActions = default)
            => ProcessTesTasksAndGetBatchJobArgumentsAsync([tesTask], configuration, azureProxy, batchPoolManager, azureProxyReturnValues, additionalActions, serviceProviderActions);

        private static async Task<(string JobId, IEnumerable<CloudTask> CloudTasks, BatchAccountPoolData batchModelsPool)> ProcessTesTasksAndGetBatchJobArgumentsAsync(TesTask[] tesTasks, IEnumerable<(string Key, string Value)> configuration, Action<Mock<IAzureProxy>> azureProxy, Action<Mock<IBatchPoolManager>> batchPoolManager, AzureProxyReturnValues azureProxyReturnValues, Action<IServiceCollection> additionalActions = default, Action<TestServices.TestServiceProvider<IBatchScheduler>, IBatchScheduler> serviceProviderActions = default)
        {
            using var serviceProvider = GetServiceProvider(
                configuration,
                azureProxy,
                batchPoolManager,
                GetMockQuotaProvider(azureProxyReturnValues),
                GetMockSkuInfoProvider(azureProxyReturnValues),
                GetMockAllowedVms(configuration),
                additionalActions: additionalActions);
            var batchScheduler = serviceProvider.GetT();
            serviceProviderActions?.Invoke(serviceProvider, batchScheduler);

            if (azureProxyReturnValues.BatchTaskState is null)
            {
                await using PerformBatchSchedulerBackgroundTasks _1 = new(batchScheduler);
                await Parallel.ForEachAsync(tesTasks, async (task, token) => _ = await batchScheduler.ProcessQueuedTesTaskAsync(task, token));
            }
            else
            {
                await Parallel.ForEachAsync(tesTasks, async (task, token) => _ = await batchScheduler.ProcessTesTaskBatchStateAsync(task, azureProxyReturnValues.BatchTaskState, token));
            }

            var createBatchPoolAsyncInvocation = serviceProvider.BatchPoolManager.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IBatchPoolManager.CreateBatchPoolAsync));
            var addBatchTaskAsyncInvocation = serviceProvider.AzureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.AddBatchTasksAsync));

            var jobId = (addBatchTaskAsyncInvocation?.Arguments[1]) as string;
            var cloudTask = (addBatchTaskAsyncInvocation?.Arguments[0]) as IEnumerable<CloudTask>;
            var batchPoolsModel = createBatchPoolAsyncInvocation?.Arguments[0] as BatchAccountPoolData;

            return (jobId, cloudTask, batchPoolsModel);
        }

        private static Action<Mock<IAllowedVmSizesService>> GetMockAllowedVms(IEnumerable<(string Key, string Value)> configuration)
            => new(proxy =>
            {
                var allowedVmsConfig = configuration.FirstOrDefault(x => x.Key == "AllowedVmSizes").Value;
                var allowedVms = new List<string>();
                if (!string.IsNullOrWhiteSpace(allowedVmsConfig))
                {
                    allowedVms = [.. allowedVmsConfig.Split(",")];
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
                        batchQuotas.DedicatedCoreQuotaPerVMFamily?.Select(v => new BatchVmCoresPerFamily(v.Name, v.CoreQuota ?? 0)).ToList(),
                        new(batchQuotas.ActiveJobAndJobScheduleQuota, batchQuotas.PoolQuota, batchQuotas.DedicatedCoreQuota, batchQuotas.LowPriorityCoreQuota)));
                quotaProvider.Setup(p =>
                        p.GetVmCoreQuotaAsync(It.Is<bool>(l => l == false), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(new BatchVmCoreQuota(batchQuotas.DedicatedCoreQuota,
                        false,
                        batchQuotas.DedicatedCoreQuotaPerVMFamilyEnforced,
                        batchQuotas.DedicatedCoreQuotaPerVMFamily?.Select(v => new BatchVmCoresPerFamily(v.Name, v.CoreQuota ?? 0)).ToList(),
                        new(batchQuotas.ActiveJobAndJobScheduleQuota, batchQuotas.PoolQuota, batchQuotas.DedicatedCoreQuota, batchQuotas.LowPriorityCoreQuota)));
            });

        private static TestServices.TestServiceProvider<IBatchScheduler> GetServiceProvider(IEnumerable<(string Key, string Value)> configuration, Action<Mock<IAzureProxy>> azureProxy, Action<Mock<IBatchPoolManager>> batchPoolManager, Action<Mock<IBatchQuotaProvider>> quotaProvider, Action<Mock<IBatchSkuInformationProvider>> skuInfoProvider, Action<Mock<IAllowedVmSizesService>> allowedVmSizesServiceSetup, Action<IServiceCollection> additionalActions = default)
            => new(wrapAzureProxy: true, configuration: configuration, azureProxy: azureProxy, batchPoolManager: batchPoolManager, batchQuotaProvider: quotaProvider, batchSkuInformationProvider: skuInfoProvider, accountResourceInformation: GetNewBatchResourceInfo(), allowedVmSizesServiceSetup: allowedVmSizesServiceSetup, additionalActions: additionalActions);

        private static async Task<TesState> GetNewTesTaskStateAsync(TesTask tesTask, AzureProxyReturnValues azureProxyReturnValues)
        {
            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(azureProxyReturnValues), GetMockBatchPoolManager(azureProxyReturnValues), azureProxyReturnValues);

            return tesTask.State;
        }

        private static async Task<TesState> GetNewTesTaskStateAsync(TesState currentTesTaskState, IEnumerable<AzureBatchTaskState> batchTaskStates)
        {
            var tesTask = new TesTask { Id = "test", State = currentTesTaskState, Executors = [new() { Image = "imageName1", Command = ["command"] }] };
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
        {
            var task = JsonConvert.DeserializeObject<TesTask>(File.ReadAllText("testask1.json"));
            task.TaskSubmitter = TaskSubmitter.Parse(task);
            return task;
        }

        private static Action<Mock<IBatchPoolManager>> GetMockBatchPoolManager(AzureProxyReturnValues azureProxyReturnValues)
            => azureProxy =>
            {
                azureProxy.Setup(a => a.CreateBatchPoolAsync(It.IsAny<BatchAccountPoolData>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                    .Returns((BatchAccountPoolData p, bool _1, CancellationToken _2) => Task.FromResult(azureProxyReturnValues.CreateBatchPoolImpl(p)));
                azureProxy.Setup(a => a.DeleteBatchPoolAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                    .Callback<string, CancellationToken>((poolId, cancellationToken) => azureProxyReturnValues.DeleteBatchPoolImpl(poolId, cancellationToken))
                    .Returns(Task.CompletedTask);
            };

        private static Action<Mock<IAzureProxy>> GetMockAzureProxy(AzureProxyReturnValues azureProxyReturnValues)
            => azureProxy =>
            {
                azureProxy.Setup(a => a.GetManagedIdentityInBatchAccountResourceGroup(It.IsAny<string>()))
                    .Returns<string>(name => $"/subscriptions/defaultsubscription/resourceGroups/defaultresourcegroup/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{name}");

                azureProxy.Setup(a => a.BlobExistsAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(true);

                azureProxy.Setup(a => a.GetActivePoolsAsync(It.IsAny<string>()))
                    .Returns(azureProxyReturnValues.GetCloudPools().ToAsyncEnumerable());

                azureProxy.Setup(a => a.GetStorageAccountInfoAsync("defaultstorageaccount", It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(azureProxyReturnValues.StorageAccountInfos["defaultstorageaccount"]));

                azureProxy.Setup(a => a.GetStorageAccountInfoAsync("storageaccount1", It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(azureProxyReturnValues.StorageAccountInfos["storageaccount1"]));

                azureProxy.Setup(a => a.GetStorageAccountUserKeyAsync(It.IsAny<StorageAccountInfo>(), It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(DefaultStorageAccessProviderTests.GenerateTestAzureStorageKey(azureProxyReturnValues.StorageAccountKey)));

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

                azureProxy.Setup(a => a.GetFullAllocationStateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(azureProxyReturnValues.AzureProxyGetFullAllocationState?.Invoke() ?? new(null, null, null, null, null, null, null)));

                azureProxy.Setup(a => a.ListComputeNodesAsync(It.IsAny<string>(), It.IsAny<DetailLevel>()))
                    .Returns(new Func<string, DetailLevel, IAsyncEnumerable<ComputeNode>>((string poolId, DetailLevel _1)
                        => AsyncEnumerable.Empty<ComputeNode>()
                            .Append(BatchPoolTests.GenerateNode(poolId, "ComputeNodeDedicated1", true, true))));

                azureProxy.Setup(a => a.ListTasksAsync(It.IsAny<string>(), It.IsAny<DetailLevel>()))
                    .Returns(azureProxyReturnValues.AzureProxyListTasks);

                azureProxy.Setup(a => a.ListBlobsAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()))
                    .Returns(AsyncEnumerable.Empty<BlobNameAndUri>());
            };

        private static Func<IEnumerable<(string Key, string Value)>> GetMockConfig()
            => new(() =>
            [
                ("Storage:DefaultAccountName", "defaultstorageaccount"),
                ("BatchNodes:GlobalManagedIdentity", GlobalManagedIdentity),
                ("BatchScheduling:Prefix", "hostname"),
                ("BatchImageGen1:Offer", "ubuntu-server-container"),
                ("BatchImageGen1:Publisher", "microsoft-azure-batch"),
                ("BatchImageGen1:Sku", "20-04-lts"),
                ("BatchImageGen1:Version", "latest"),
                ("BatchImageGen1:NodeAgentSkuId", "batch.node.ubuntu 20.04"),
                ("BatchImageGen2:Offer", "ubuntu-hpc"),
                ("BatchImageGen2:Publisher", "microsoft-dsvm"),
                ("BatchImageGen2:Sku", "2004"),
                ("BatchImageGen2:Version", "latest"),
                ("BatchImageGen2:NodeAgentSkuId", "batch.node.ubuntu 20.04"),
            ]);

        private static IEnumerable<FileToDownload> GetFilesToDownload(Mock<IAzureProxy> azureProxy)
        {
            var downloadFilesScriptContent = (string)azureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.UploadBlobAsync) && i.Arguments[0].ToString().Contains("/runner-task.json"))?.Arguments[1];

            if (string.IsNullOrEmpty(downloadFilesScriptContent))
            {
                return [];
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
                batchPoolManager: GetMockBatchPoolManager(azureProxyReturn),
                batchQuotaProvider: GetMockQuotaProvider(azureProxyReturn),
                batchSkuInformationProvider: GetMockSkuInfoProvider(azureProxyReturn),
                allowedVmSizesServiceSetup: GetMockAllowedVms(config));
        }

        private static async Task<BatchPool> AddPool(BatchScheduler batchScheduler)
            => (BatchPool)await batchScheduler.GetOrAddPoolAsync("key1", false, (id, _1) => ValueTask.FromResult(BatchPoolTests.CreatePoolData(id, "display1", "vmSize1")), CancellationToken.None);

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

        internal readonly struct PerformBatchSchedulerBackgroundTasks : IAsyncDisposable
        {
            private readonly CancellationTokenSource cancellationToken = new();
            private readonly IBatchScheduler batchScheduler;
            private readonly Task task;

            public PerformBatchSchedulerBackgroundTasks(IBatchScheduler batchScheduler) : this()
            {
                BatchScheduler.QueuedTesTaskPoolGroupGatherWindow = TimeSpan.FromSeconds(0.5);
                BatchScheduler.QueuedTesTaskTaskGroupGatherWindow = TimeSpan.FromSeconds(0.5);
                this.batchScheduler = batchScheduler;
                this.task = RepeatedlyCallPerformBackgroundTasksAsync();
            }

            private readonly async Task RepeatedlyCallPerformBackgroundTasksAsync()
            {
                using PeriodicTimer timer = new(TimeSpan.FromMilliseconds(750));

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        await batchScheduler.PerformShortBackgroundTasksAsync(CancellationToken.None);
                        await Task.WhenAll(batchScheduler.PerformLongBackgroundTasksAsync(CancellationToken.None).ToBlockingEnumerable(CancellationToken.None));
                        await Task.WhenAll(batchScheduler.PerformLongBackgroundTasksAsync(CancellationToken.None).ToBlockingEnumerable(CancellationToken.None));
                        await timer.WaitForNextTickAsync(cancellationToken.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                }
            }

            readonly async ValueTask IAsyncDisposable.DisposeAsync()
            {
                cancellationToken.Cancel();
                await task;
                cancellationToken.Dispose();
            }
        }


        private struct BatchTaskStates
        {
            public static AzureBatchTaskState[] TaskActive => [new AzureBatchTaskState(AzureBatchTaskState.TaskState.InfoUpdate)];
            public static AzureBatchTaskState[] TaskPreparing => [new AzureBatchTaskState(AzureBatchTaskState.TaskState.Initializing, CloudTaskCreationTime: DateTimeOffset.UtcNow)];
            public static AzureBatchTaskState[] TaskRunning => [new AzureBatchTaskState(AzureBatchTaskState.TaskState.Running, CloudTaskCreationTime: DateTimeOffset.UtcNow - TimeSpan.FromMinutes(6))];
            public static AzureBatchTaskState[] TaskCompletedSuccessfully =>
            [
                new AzureBatchTaskState(AzureBatchTaskState.TaskState.InfoUpdate, ExecutorExitCode: 0),
                new AzureBatchTaskState(AzureBatchTaskState.TaskState.CompletedSuccessfully, BatchTaskExitCode: 0),
            ];
            public static AzureBatchTaskState[] TaskFailed =>
            [
                new AzureBatchTaskState(AzureBatchTaskState.TaskState.InfoUpdate, Failure: new(AzureBatchTaskState.ExecutorError, [TaskFailureInformationCodes.FailureExitCode, @"1"]), ExecutorExitCode: 1),
                new AzureBatchTaskState(AzureBatchTaskState.TaskState.CompletedWithErrors, Failure: new(AzureBatchTaskState.SystemError, [TaskFailureInformationCodes.FailureExitCode, @"1"]), BatchTaskExitCode: 1)
            ];
            public static AzureBatchTaskState[] NodeDiskFull => [new AzureBatchTaskState(AzureBatchTaskState.TaskState.NodeFailedDuringStartupOrExecution, Failure: new("DiskFull", ["Error message."]))];
            public static AzureBatchTaskState[] UploadOrDownloadFailed => [new AzureBatchTaskState(AzureBatchTaskState.TaskState.NodeFilesUploadOrDownloadFailed)];
            public static AzureBatchTaskState[] NodeAllocationFailed => [new AzureBatchTaskState(AzureBatchTaskState.TaskState.NodeAllocationFailed, Failure: new(AzureBatchTaskState.TaskState.NodeAllocationFailed.ToString(), ["Error message."]))];
            public static AzureBatchTaskState[] NodePreempted => [new AzureBatchTaskState(AzureBatchTaskState.TaskState.NodePreempted)];
            public static AzureBatchTaskState[] NodeStartTaskFailed => [new AzureBatchTaskState(AzureBatchTaskState.TaskState.NodeStartTaskFailed)];
            public static AzureBatchTaskState[] CancellationRequested => [new AzureBatchTaskState(AzureBatchTaskState.TaskState.CancellationRequested, CloudTaskCreationTime: DateTimeOffset.UtcNow - TimeSpan.FromMinutes(12))];
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
                AzureProxyGetFullAllocationState = () => new(AllocationState.Steady, DateTime.MinValue.ToUniversalTime(), true, 0, 0, 0, 0),
                AzureProxyDeleteBatchPoolIfExists = (poolId, cancellationToken) => { },
                AzureProxyDeleteBatchPool = (poolId, cancellationToken) => { },
                StorageAccountInfos = new() {
                    { "defaultstorageaccount", new() { Name = "defaultstorageaccount", Id = "Id", BlobEndpoint = new("https://defaultstorageaccount.blob.core.windows.net/"), SubscriptionId = "SubId" } },
                    { "storageaccount1", new() { Name = "storageaccount1", Id = "Id", BlobEndpoint = new("https://storageaccount1.blob.core.windows.net/"), SubscriptionId = "SubId" } }
                },
                VmSizesAndPrices =
                [
                    new() { VmSize = "VmSizeLowPri1", VmFamily = "VmFamily1", LowPriority = true, VCpusAvailable = 1, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 1 },
                    new() { VmSize = "VmSizeLowPri2", VmFamily = "VmFamily2", LowPriority = true, VCpusAvailable = 2, MemoryInGiB = 8, ResourceDiskSizeInGiB = 40, PricePerHour = 2 },
                    new() { VmSize = "VmSizeDedicated1", VmFamily = "VmFamily1", LowPriority = false, VCpusAvailable = 1, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 11 },
                    new() { VmSize = "VmSizeDedicated2", VmFamily = "VmFamily2", LowPriority = false, VCpusAvailable = 2, MemoryInGiB = 8, ResourceDiskSizeInGiB = 40, PricePerHour = 22 }
                ],
                BatchQuotas = new() { ActiveJobAndJobScheduleQuota = 1, PoolQuota = 1, DedicatedCoreQuota = 5, LowPriorityCoreQuota = 10, DedicatedCoreQuotaPerVMFamily = [] },
                ActiveNodeCountByVmSize = [],
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
                    DedicatedCoreQuotaPerVMFamily = [CreateBatchVmFamilyCoreQuota("VmFamily1", proxy.BatchQuotas.DedicatedCoreQuota), CreateBatchVmFamilyCoreQuota("VmFamily2", 0), CreateBatchVmFamilyCoreQuota("VmFamily3", 4)],
                    DedicatedCoreQuota = proxy.BatchQuotas.DedicatedCoreQuota,
                    ActiveJobAndJobScheduleQuota = proxy.BatchQuotas.ActiveJobAndJobScheduleQuota,
                    LowPriorityCoreQuota = proxy.BatchQuotas.LowPriorityCoreQuota,
                    PoolQuota = proxy.BatchQuotas.PoolQuota
                };
                return proxy;
            }

            private readonly System.Collections.Concurrent.ConcurrentDictionary<string, IList<MetadataItem>> poolMetadata = [];

            internal void DeleteBatchPoolImpl(string poolId, CancellationToken cancellationToken)
            {
                _ = poolMetadata.TryRemove(poolId, out _);
                AzureProxyDeleteBatchPool(poolId, cancellationToken);
            }

            internal string CreateBatchPoolImpl(BatchAccountPoolData pool)
            {
                var poolNameItem = pool.Metadata.Single(i => string.IsNullOrEmpty(i.Name));
                pool.Metadata.Remove(poolNameItem);
                var poolId = poolNameItem.Value;

                if (pool.Identity is not null)
                {
                    PoolIdentityValues identityItem = new(pool.Identity.ManagedServiceIdentityType switch
                    {
                        var x when x == Azure.ResourceManager.Models.ManagedServiceIdentityType.None => Microsoft.Azure.Batch.Protocol.Models.PoolIdentityType.None,
                        var x when x == Azure.ResourceManager.Models.ManagedServiceIdentityType.UserAssigned => Microsoft.Azure.Batch.Protocol.Models.PoolIdentityType.UserAssigned,
                        _ => throw new ArgumentOutOfRangeException(nameof(pool), $"{nameof(BatchAccountPoolData.Identity)}.{nameof(BatchAccountPoolData.Identity.ManagedServiceIdentityType)} has an unsupported value.")
                    }, pool.Identity.UserAssignedIdentities?.Select(item => new UserAssignedIdentityValues(item.Key.ToString(), item.Value?.ClientId.ToString(), item.Value?.PrincipalId.ToString())).ToArray());

                    pool.Metadata.Add(new(nameof(BatchAccountPoolData.Identity), System.Text.Json.JsonSerializer.Serialize(identityItem, serializerOptions.Value)));
                }

                poolMetadata.AddOrUpdate(poolId, _ => pool.Metadata?.Select(Convert).ToList(), (_, _) => throw new Exception("Unexpected attempt to modify pool."));
                return poolId;

                static MetadataItem Convert(BatchAccountPoolMetadataItem item)
                    => new(item.Name, item.Value);
            }

            internal CloudPool GetBatchPoolImpl(string poolId)
            {
                if (!poolMetadata.TryGetValue(poolId, out var items))
                {
                    items = null;
                }

                items = items?.ToList();

                var identityItem = items?.SingleOrDefault(item => nameof(BatchAccountPoolData.Identity).Equals(item.Name, StringComparison.Ordinal));

                if (identityItem is not null)
                {
                    items.Remove(identityItem);
                }

                return BatchPoolTests.GeneratePool(poolId, metadata: items, identity: GetIdentity(identityItem?.Value));

                static Microsoft.Azure.Batch.Protocol.Models.BatchPoolIdentity GetIdentity(string item)
                {
                    if (item is null)
                    {
                        return default;
                    }

                    var identityItem = System.Text.Json.JsonSerializer.Deserialize<PoolIdentityValues>(item, serializerOptions.Value);

                    return new()
                    {
                        Type = identityItem.Type,
                        UserAssignedIdentities = identityItem.UserAssignedIdentites.Select(uai => new Microsoft.Azure.Batch.Protocol.Models.UserAssignedIdentity(uai.ResourceId, uai.ClientId, uai.PrincipalId)).ToList()
                    };
                }
            }

            private static readonly Lazy<System.Text.Json.JsonSerializerOptions> serializerOptions = new(() =>
            {
                System.Text.Json.JsonSerializerOptions options = new()
                {
                    //DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingDefault,
                    PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase
                };

                options.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter(System.Text.Json.JsonNamingPolicy.CamelCase, true));

                return options;
            });

            internal IEnumerable<CloudPool> GetCloudPools()
                => poolMetadata.Select(pool => pool.Key).Select(pool => GetBatchPoolImpl(pool));

            private record struct UserAssignedIdentityValues(string ResourceId, string ClientId, string PrincipalId);
            private record struct PoolIdentityValues(Microsoft.Azure.Batch.Protocol.Models.PoolIdentityType Type, UserAssignedIdentityValues[] UserAssignedIdentites);
        }

        private static BatchVmFamilyCoreQuota CreateBatchVmFamilyCoreQuota(string name, int? quota)
        {
            return ArmBatchModelFactory.BatchVmFamilyCoreQuota(name, quota);
        }

        private class TestMultitaskBatchQuotaVerifierQuotaMaxedOut(IBatchQuotaProvider batchQuotaProvider) : TestBatchQuotaVerifierBase(batchQuotaProvider)
        {
            public override Task<CheckGroupPoolAndJobQuotaResult> CheckBatchAccountPoolAndJobQuotasAsync(int required, CancellationToken cancellationToken)
                => Task.FromResult(new CheckGroupPoolAndJobQuotaResult(required / 2, new AzureBatchQuotaMaxedOutException("Test AzureBatchQuotaMaxedOutException")));

            public override Task CheckBatchAccountQuotasAsync(VirtualMachineInformation _1, bool _2, CancellationToken cancellationToken)
                => Task.CompletedTask;
        }

        private class TestMultitaskBatchQuotaVerifierQuotaAllAllowed(IBatchQuotaProvider batchQuotaProvider) : TestBatchQuotaVerifierBase(batchQuotaProvider)
        {
            public override Task<CheckGroupPoolAndJobQuotaResult> CheckBatchAccountPoolAndJobQuotasAsync(int required, CancellationToken cancellationToken)
                => Task.FromResult(new CheckGroupPoolAndJobQuotaResult(0, null));

            public override Task CheckBatchAccountQuotasAsync(VirtualMachineInformation _1, bool _2, CancellationToken cancellationToken)
                => Task.CompletedTask;
        }

        private class TestBatchQuotaVerifierQuotaMaxedOut(IBatchQuotaProvider batchQuotaProvider) : TestBatchQuotaVerifierBase(batchQuotaProvider)
        {
            public override Task<CheckGroupPoolAndJobQuotaResult> CheckBatchAccountPoolAndJobQuotasAsync(int required, CancellationToken cancellationToken)
                => Task.FromResult(new CheckGroupPoolAndJobQuotaResult(required / 2, new AzureBatchQuotaMaxedOutException("Test AzureBatchQuotaMaxedOutException")));

            public override Task CheckBatchAccountQuotasAsync(VirtualMachineInformation _1, bool _2, CancellationToken cancellationToken)
                => throw new AzureBatchQuotaMaxedOutException("Test AzureBatchQuotaMaxedOutException");
        }

        private class TestBatchQuotaVerifierLowQuota(IBatchQuotaProvider batchQuotaProvider) : TestBatchQuotaVerifierBase(batchQuotaProvider)
        {
            public override Task<CheckGroupPoolAndJobQuotaResult> CheckBatchAccountPoolAndJobQuotasAsync(int required, CancellationToken cancellationToken)
                => throw new NotSupportedException();

            public override Task CheckBatchAccountQuotasAsync(VirtualMachineInformation _1, bool _2, CancellationToken cancellationToken)
                => throw new AzureBatchLowQuotaException("Test AzureBatchLowQuotaException");
        }

        private abstract class TestBatchQuotaVerifierBase(IBatchQuotaProvider batchQuotaProvider) : IBatchQuotaVerifier
        {
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
