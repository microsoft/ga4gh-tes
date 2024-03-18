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
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Blob;
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
        [TestCategory("Batch Pools")]
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

            pool = (BatchPool)await batchScheduler.GetOrAddPoolAsync(key, false, (id, cancellationToken) => ValueTask.FromResult(new Pool(name: id)), System.Threading.CancellationToken.None);

            Assert.IsNotNull(pool);
            Assert.AreEqual(1, batchScheduler.GetPoolGroupKeys().Count());
            Assert.IsTrue(batchScheduler.TryGetPool(pool.PoolId, out var pool1));
            Assert.AreSame(pool, pool1);
        }

        [TestCategory("Batch Pools")]
        [TestMethod]
        public async Task GetOrAddDoesNotAddExistingAvailablePool()
        {
            using var serviceProvider = GetServiceProvider();
            var batchScheduler = serviceProvider.GetT() as BatchScheduler;
            var info = await AddPool(batchScheduler);
            var keyCount = batchScheduler.GetPoolGroupKeys().Count();
            var key = batchScheduler.GetPoolGroupKeys().First();
            var count = batchScheduler.GetPools().Count();
            serviceProvider.AzureProxy.Verify(mock => mock.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>(), It.IsAny<System.Threading.CancellationToken>()), Times.Once);

            var pool = await batchScheduler.GetOrAddPoolAsync(key, false, (id, cancellationToken) => ValueTask.FromResult(new Pool(name: id)), System.Threading.CancellationToken.None);
            await pool.ServicePoolAsync();

            Assert.AreEqual(count, batchScheduler.GetPools().Count());
            Assert.AreEqual(keyCount, batchScheduler.GetPoolGroupKeys().Count());
            //Assert.AreSame(info, pool);
            Assert.AreEqual(info.PoolId, pool.PoolId);
            serviceProvider.AzureProxy.Verify(mock => mock.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>(), It.IsAny<System.Threading.CancellationToken>()), Times.Once);
        }

        [TestCategory("Batch Pools")]
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

            var pool = await batchScheduler.GetOrAddPoolAsync(key, false, (id, cancellationToken) => ValueTask.FromResult(new Pool(name: id)), System.Threading.CancellationToken.None);
            await pool.ServicePoolAsync();

            Assert.AreNotEqual(count, batchScheduler.GetPools().Count());
            Assert.AreEqual(keyCount, batchScheduler.GetPoolGroupKeys().Count());
            //Assert.AreNotSame(info, pool);
            Assert.AreNotEqual(info.PoolId, pool.PoolId);
        }


        [TestCategory("Batch Pools")]
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

        [TestCategory("Batch Pools")]
        [TestMethod]
        public async Task TryGetReturnsFalseWhenPoolIdNotPresent()
        {
            using var serviceProvider = GetServiceProvider();
            var batchScheduler = serviceProvider.GetT() as BatchScheduler;
            _ = await AddPool(batchScheduler);

            var result = batchScheduler.TryGetPool("key2", out _);

            Assert.IsFalse(result);
        }

        [TestCategory("Batch Pools")]
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

        [TestCategory("Batch Pools")]
        [TestMethod]
        public Task TryGetReturnsFalseWhenPoolIdIsNull()
        {
            using var serviceProvider = GetServiceProvider();
            var batchScheduler = serviceProvider.GetT() as BatchScheduler;

            var result = batchScheduler.TryGetPool(null, out _);

            Assert.IsFalse(result);
            return Task.CompletedTask;
        }

        [TestCategory("Batch Pools")]
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

            var size = await ((BatchScheduler)batchScheduler).GetVmSizeAsync(task, System.Threading.CancellationToken.None);
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

            (var failureReason, var systemLog) = await ProcessTesTaskAndGetFailureReasonAndSystemLogAsync(tesTask, BatchJobAndTaskStates.NodeDiskFull);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(TesState.EXECUTORERROREnum, tesTask.State);
                Assert.AreEqual("DiskFull", failureReason);
                Assert.AreEqual("DiskFull", systemLog[0]);
                Assert.AreEqual("DiskFull", tesTask.FailureReason);
            });
        }

        private async Task AddBatchTaskHandlesExceptions(TesState newState, Func<AzureProxyReturnValues, (Action<IServiceCollection>, Action<Mock<IAzureProxy>>)> testArranger, Action<TesTask, IEnumerable<(LogLevel, Exception)>> resultValidator)
        {
            var logger = new Mock<ILogger<BatchScheduler>>();
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            var (providerModifier, azureProxyModifier) = testArranger?.Invoke(azureProxyReturnValues) ?? (default, default);
            var azureProxy = new Action<Mock<IAzureProxy>>(mock =>
            {
                GetMockAzureProxy(azureProxyReturnValues)(mock);
                azureProxyModifier?.Invoke(mock);
            });
            var task = GetTesTask();
            task.State = TesState.QUEUEDEnum;

            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(
                task,
                GetMockConfig()(),
                azureProxy,
                azureProxyReturnValues,
                s =>
                {
                    providerModifier?.Invoke(s);
                    s.AddTransient(p => logger.Object);
                });

            GuardAssertsWithTesTask(task, () =>
            {
                Assert.AreEqual(newState, task.State);
                resultValidator?.Invoke(task, logger.Invocations.Where(i => nameof(ILogger.Log).Equals(i.Method.Name)).Select(i => (((LogLevel?)i.Arguments[0]) ?? LogLevel.None, (Exception)i.Arguments[3])));
            });
        }

        [TestMethod]
        public Task AddBatchTaskHandlesAzureBatchPoolCreationExceptionViaJobCreation()
        {
            return AddBatchTaskHandlesExceptions(TesState.QUEUEDEnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (default, azureProxy => azureProxy.Setup(b => b.CreateBatchJobAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>()))
                    .Callback<string, string, System.Threading.CancellationToken>((_, _, _)
                        => throw new Microsoft.Rest.Azure.CloudException("No job for you.") { Body = new() { Code = BatchErrorCodeStrings.OperationTimedOut } }));

            void Validator(TesTask tesTask, IEnumerable<(LogLevel logLevel, Exception exception)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    var (logLevel, exception) = log;
                    Assert.AreEqual(LogLevel.Warning, logLevel);
                    Assert.IsInstanceOfType<AzureBatchPoolCreationException>(exception);
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesAzureBatchPoolCreationExceptionViaPoolCreation()
        {
            return AddBatchTaskHandlesExceptions(TesState.QUEUEDEnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (default, azureProxy => azureProxy.Setup(b => b.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>(), It.IsAny<System.Threading.CancellationToken>()))
                    .Callback<Pool, bool, System.Threading.CancellationToken>((poolInfo, isPreemptible, cancellationToken)
                        => throw new Microsoft.Rest.Azure.CloudException("No job for you.") { Body = new() { Code = BatchErrorCodeStrings.OperationTimedOut } }));

            void Validator(TesTask tesTask, IEnumerable<(LogLevel logLevel, Exception exception)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    var (logLevel, exception) = log;
                    Assert.AreEqual(LogLevel.Warning, logLevel);
                    Assert.IsInstanceOfType<AzureBatchPoolCreationException>(exception);
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesAzureBatchQuotaMaxedOutException()
        {
            var quotaVerifier = new Mock<IBatchQuotaVerifier>();
            return AddBatchTaskHandlesExceptions(TesState.QUEUEDEnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (services => services.AddSingleton<IBatchQuotaVerifier, TestBatchQuotaVerifierQuotaMaxedOut>(), default);

            void Validator(TesTask tesTask, IEnumerable<(LogLevel logLevel, Exception exception)> logs)
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
        public Task AddBatchTaskHandlesAzureBatchLowQuotaException()
        {
            var quotaVerifier = new Mock<IBatchQuotaVerifier>();
            return AddBatchTaskHandlesExceptions(TesState.SYSTEMERROREnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (services => services.AddSingleton<IBatchQuotaVerifier, TestBatchQuotaVerifierLowQuota>(), default);

            void Validator(TesTask tesTask, IEnumerable<(LogLevel logLevel, Exception exception)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    var (logLevel, exception) = log;
                    Assert.AreEqual(LogLevel.Error, logLevel);
                    Assert.IsInstanceOfType<AzureBatchLowQuotaException>(exception);
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesAzureBatchVirtualMachineAvailabilityException()
        {
            return AddBatchTaskHandlesExceptions(TesState.SYSTEMERROREnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues proxy)
            {
                proxy.VmSizesAndPrices = Enumerable.Empty<VirtualMachineInformation>().ToList();
                return (default, default);
            }

            void Validator(TesTask tesTask, IEnumerable<(LogLevel logLevel, Exception exception)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    var (logLevel, exception) = log;
                    Assert.AreEqual(LogLevel.Error, logLevel);
                    Assert.IsInstanceOfType<AzureBatchVirtualMachineAvailabilityException>(exception);
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesTesException()
        {
            return AddBatchTaskHandlesExceptions(TesState.SYSTEMERROREnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (default, azureProxy => azureProxy.Setup(b => b.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>(), It.IsAny<System.Threading.CancellationToken>()))
                    .Callback<Pool, bool, System.Threading.CancellationToken>((poolInfo, isPreemptible, cancellationToken)
                        => throw new TesException("TestFailureReason")));

            void Validator(TesTask tesTask, IEnumerable<(LogLevel logLevel, Exception exception)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    var (logLevel, exception) = log;
                    Assert.AreEqual(LogLevel.Error, logLevel);
                    Assert.IsInstanceOfType<TesException>(exception);
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesBatchClientException()
        {
            return AddBatchTaskHandlesExceptions(TesState.SYSTEMERROREnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (default, azureProxy => azureProxy.Setup(b => b.AddBatchTaskAsync(It.IsAny<string>(), It.IsAny<CloudTask>(), It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>()))
                    .Callback<string, CloudTask, string, System.Threading.CancellationToken>((_, _, _, _)
                        => throw typeof(BatchClientException)
                                .GetConstructor(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance,
                                    new[] { typeof(string), typeof(Exception) })
                                .Invoke(new object[] { null, null }) as Exception));

            void Validator(TesTask tesTask, IEnumerable<(LogLevel logLevel, Exception exception)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    var (logLevel, exception) = log;
                    Assert.AreEqual(LogLevel.Error, logLevel);
                    Assert.IsInstanceOfType<BatchClientException>(exception);
                });
            }
        }

        [TestMethod]
        public Task AddBatchTaskHandlesBatchExceptionForJobQuota()
        {
            return AddBatchTaskHandlesExceptions(TesState.QUEUEDEnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (default, azureProxy => azureProxy.Setup(b => b.CreateBatchJobAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>()))
                    .Callback<string, string, System.Threading.CancellationToken>((_, _, _)
                        => throw new BatchException(
                            new Mock<RequestInformation>().Object,
                            default,
                            new Microsoft.Azure.Batch.Protocol.Models.BatchErrorException() { Body = new() { Code = "ActiveJobAndScheduleQuotaReached", Message = new(value: "No job for you.") } })));

            void Validator(TesTask task, IEnumerable<(LogLevel logLevel, Exception exception)> logs)
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
            return AddBatchTaskHandlesExceptions(TesState.QUEUEDEnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (default, azureProxy => azureProxy.Setup(b => b.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>(), It.IsAny<System.Threading.CancellationToken>()))
                    .Callback<Pool, bool, System.Threading.CancellationToken>((poolInfo, isPreemptible, cancellationToken)
                        => throw new BatchException(
                            new Mock<RequestInformation>().Object,
                            default,
                            new Microsoft.Azure.Batch.Protocol.Models.BatchErrorException() { Body = new() { Code = "PoolQuotaReached", Message = new(value: "No pool for you.") } })));

            void Validator(TesTask task, IEnumerable<(LogLevel logLevel, Exception exception)> logs)
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
            return AddBatchTaskHandlesExceptions(TesState.QUEUEDEnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (default, azureProxy => azureProxy.Setup(b => b.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>(), It.IsAny<System.Threading.CancellationToken>()))
                    .Callback<Pool, bool, System.Threading.CancellationToken>((poolInfo, isPreemptible, cancellationToken)
                        => throw new Microsoft.Rest.Azure.CloudException() { Body = new() { Code = "AutoPoolCreationFailedWithQuotaReached", Message = "No autopool for you." } }));

            void Validator(TesTask task, IEnumerable<(LogLevel logLevel, Exception exception)> logs)
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
            batchQuotaProvider.Setup(p => p.GetVmCoreQuotaAsync(It.IsAny<bool>(), It.IsAny<System.Threading.CancellationToken>())).Callback<bool, System.Threading.CancellationToken>((lowPriority, _1) => throw new InvalidOperationException(exceptionMsg));
            return AddBatchTaskHandlesExceptions(TesState.SYSTEMERROREnum, Arranger, Validator);

            (Action<IServiceCollection>, Action<Mock<IAzureProxy>>) Arranger(AzureProxyReturnValues _1)
                => (services => services.AddTransient(p => batchQuotaProvider.Object), default);

            void Validator(TesTask tesTask, IEnumerable<(LogLevel logLevel, Exception exception)> logs)
            {
                GuardAssertsWithTesTask(tesTask, () =>
                {
                    var log = logs.LastOrDefault();
                    Assert.IsNotNull(log);
                    var (logLevel, exception) = log;
                    Assert.AreEqual(LogLevel.Error, logLevel);
                    Assert.IsInstanceOfType<InvalidOperationException>(exception);
                    Assert.AreEqual(exceptionMsg, exception.Message);
                });
            }
        }

        [TestCategory("Batch Pools")]
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

            await batchScheduler.ProcessTesTaskAsync(tesTask, System.Threading.CancellationToken.None);

            var createBatchPoolAsyncInvocation = serviceProvider.AzureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.CreateBatchPoolAsync));
            var pool = createBatchPoolAsyncInvocation?.Arguments[0] as Pool;

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual("TES-hostname-edicated1-rpsd645merzfkqmdnj7pkqrase2ancnh-", pool.Name[0..^8]);
                Assert.AreEqual("VmSizeDedicated1", pool.VmSize);
                Assert.IsTrue(((BatchScheduler)batchScheduler).TryGetPool(pool.Name, out _));
            });
        }

        [TestCategory("TES 1.1")]
        [TestMethod]
        public async Task BatchJobContainsExpectedManualPoolInformation()
        {
            var identity = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/coa/providers/Microsoft.ManagedIdentity/userAssignedIdentities/coa-test-uami";
            var task = GetTesTask();
            task.Resources.BackendParameters = new()
            {
                { "workflow_execution_identity", identity }
            };

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(task, GetMockConfig()(), GetMockAzureProxy(AzureProxyReturnValues.Defaults), AzureProxyReturnValues.Defaults);

            GuardAssertsWithTesTask(task, () =>
            {
                Assert.AreEqual("VmSizeDedicated1", poolSpec.VmSize);
                Assert.IsTrue(poolSpec.ScaleSettings.AutoScale.Formula.Contains("TargetDedicated"));
                Assert.AreEqual(1, poolSpec.Identity.UserAssignedIdentities.Count);
                Assert.AreEqual(identity, poolSpec.Identity.UserAssignedIdentities.Keys.First());
            });
        }

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

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(AzureProxyReturnValues.Defaults), AzureProxyReturnValues.Defaults);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual("VmSizeLowPri1", poolSpec.VmSize);
                Assert.IsTrue(poolSpec.ScaleSettings.AutoScale.Formula.Contains("$TargetLowPriorityNodes"));
                Assert.IsFalse(poolSpec.ScaleSettings.AutoScale.Formula.Contains("TargetDedicated"));
            });
        }

        [TestMethod]
        public async Task NonPreemptibleTesTaskGetsScheduledToDedicatedVm()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = false;

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(AzureProxyReturnValues.Defaults), AzureProxyReturnValues.Defaults);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual("VmSizeDedicated1", poolSpec.VmSize);
                Assert.IsTrue(poolSpec.ScaleSettings.AutoScale.Formula.Contains("TargetDedicated"));
                Assert.IsFalse(poolSpec.ScaleSettings.AutoScale.Formula.Contains("$TargetLowPriorityNodes"));
            });
        }

        [TestMethod]
        public async Task PreemptibleTesTaskGetsScheduledToLowPriorityVm_PerVMFamilyEnforced()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = true;

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(AzureProxyReturnValues.DefaultsPerVMFamilyEnforced), AzureProxyReturnValues.DefaultsPerVMFamilyEnforced);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual("VmSizeLowPri1", poolSpec.VmSize);
                Assert.IsTrue(poolSpec.ScaleSettings.AutoScale.Formula.Contains("$TargetLowPriorityNodes"));
                Assert.IsFalse(poolSpec.ScaleSettings.AutoScale.Formula.Contains("TargetDedicated"));
            });
        }

        [TestMethod]
        public async Task NonPreemptibleTesTaskGetsScheduledToDedicatedVm_PerVMFamilyEnforced()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = false;

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(AzureProxyReturnValues.DefaultsPerVMFamilyEnforced), AzureProxyReturnValues.DefaultsPerVMFamilyEnforced);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual("VmSizeDedicated1", poolSpec.VmSize);
                Assert.IsTrue(poolSpec.ScaleSettings.AutoScale.Formula.Contains("TargetDedicated"));
                Assert.IsFalse(poolSpec.ScaleSettings.AutoScale.Formula.Contains("$TargetLowPriorityNodes"));
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

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(azureProxyReturnValues), azureProxyReturnValues);

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.IsTrue(tesTask.Logs.Any(l => "UsedLowPriorityInsteadOfDedicatedVm".Equals(l.Warning)));
                Assert.IsTrue(poolSpec.ScaleSettings.AutoScale.Formula.Contains("$TargetLowPriorityNodes"));
            });
        }

        [TestMethod]
        public async Task TesTaskGetsScheduledToLowPriorityVmIfSettingUsePreemptibleVmsOnlyIsSet()
        {
            var tesTask = GetTesTask();
            tesTask.Resources.Preemptible = false;

            var config = GetMockConfig()()
                .Append(("BatchScheduling:UsePreemptibleVmsOnly", "true"));

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, GetMockAzureProxy(AzureProxyReturnValues.Defaults), AzureProxyReturnValues.Defaults);

            GuardAssertsWithTesTask(tesTask, () => Assert.IsTrue(poolSpec.ScaleSettings.AutoScale.Formula.Contains("$TargetLowPriorityNodes")));
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

                (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, GetMockAzureProxy(AzureProxyReturnValues.Defaults), AzureProxyReturnValues.Defaults);

                GuardAssertsWithTesTask(tesTask, () =>
                {
                    Assert.AreEqual(expectedTaskState, tesTask.State);

                    if (expectedSelectedVmSize is not null)
                    {
                        Assert.AreEqual(expectedSelectedVmSize, poolSpec.VmSize);
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
            Assert.AreEqual(TesState.RUNNINGEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.TaskActive));
            Assert.AreEqual(TesState.RUNNINGEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.TaskPreparing));
            Assert.AreEqual(TesState.RUNNINGEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.TaskRunning));
            Assert.AreEqual(TesState.COMPLETEEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.TaskCompletedSuccessfully));
            Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.TaskFailed));
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.JobNotFound));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.TaskNotFound));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.MoreThanOneJobFound));
            Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.NodeDiskFull));
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.ActiveJobWithMissingAutoPool));
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.RUNNINGEnum, BatchJobAndTaskStates.NodePreempted));
        }

        [TestMethod]
        public async Task TaskStateTransitionsFromInitializingState()
        {
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.TaskActive));
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.TaskPreparing));
            Assert.AreEqual(TesState.RUNNINGEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.TaskRunning));
            Assert.AreEqual(TesState.COMPLETEEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.TaskCompletedSuccessfully));
            Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.TaskFailed));
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.JobNotFound));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.TaskNotFound));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.MoreThanOneJobFound));
            Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.NodeDiskFull));
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.NodeAllocationFailed));
            Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.ImageDownloadFailed));
            Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.ActiveJobWithMissingAutoPool));
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.INITIALIZINGEnum, BatchJobAndTaskStates.NodePreempted));
        }

        [TestMethod]
        public async Task TaskStateTransitionsFromQueuedState()
        {
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, BatchJobAndTaskStates.TaskActive));
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, BatchJobAndTaskStates.TaskPreparing));
            Assert.AreEqual(TesState.RUNNINGEnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, BatchJobAndTaskStates.TaskRunning));
            Assert.AreEqual(TesState.COMPLETEEnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, BatchJobAndTaskStates.TaskCompletedSuccessfully));
            Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, BatchJobAndTaskStates.TaskFailed));
            Assert.AreEqual(TesState.SYSTEMERROREnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, BatchJobAndTaskStates.MoreThanOneJobFound));
            Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, BatchJobAndTaskStates.NodeDiskFull));
            Assert.AreEqual(TesState.INITIALIZINGEnum, await GetNewTesTaskStateAsync(TesState.QUEUEDEnum, BatchJobAndTaskStates.TaskNotFound));
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
            await GuardAssertsWithTesTask(tesTask, async () => Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(tesTask, BatchJobAndTaskStates.NodeAllocationFailed)));
            await GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
            await GuardAssertsWithTesTask(tesTask, async () => Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(tesTask, BatchJobAndTaskStates.NodeAllocationFailed)));
            await GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
            await GuardAssertsWithTesTask(tesTask, async () => Assert.AreEqual(TesState.QUEUEDEnum, await GetNewTesTaskStateAsync(tesTask, BatchJobAndTaskStates.NodeAllocationFailed)));
            await GetNewTesTaskStateAsync(tesTask, azureProxyReturnValues);
            await GuardAssertsWithTesTask(tesTask, async () => Assert.AreEqual(TesState.EXECUTORERROREnum, await GetNewTesTaskStateAsync(tesTask, BatchJobAndTaskStates.NodeAllocationFailed)));
        }

        [TestMethod]
        public async Task TaskThatFailsWithNodeAllocationErrorIsRequeuedOnDifferentVmSize()
        {
            var tesTask = GetTesTask();

            await GetNewTesTaskStateAsync(tesTask);
            await GetNewTesTaskStateAsync(tesTask, BatchJobAndTaskStates.NodeAllocationFailed);
            var firstAttemptVmSize = tesTask.Logs[0].VirtualMachineInfo.VmSize;

            await GetNewTesTaskStateAsync(tesTask);
            await GetNewTesTaskStateAsync(tesTask, BatchJobAndTaskStates.NodeAllocationFailed);
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
            var tesTask = new TesTask { Id = "test", PoolId = "pool1", State = TesState.CANCELEDEnum, IsCancelRequested = true };

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchJobAndTaskState = BatchJobAndTaskStates.TaskActive;
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
                Assert.IsFalse(tesTask.IsCancelRequested);
                azureProxy.Verify(i => i.DeleteBatchTaskAsync(tesTask.Id, It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>()));
            });
        }

        [TestMethod]
        public async Task SuccessfullyCompletedTaskContainsBatchNodeMetrics()
        {
            var tesTask = GetTesTask();

            var metricsFileContent = @"
                BlobXferPullStart=2020-10-08T02:30:39+00:00
                BlobXferPullEnd=2020-10-08T02:31:39+00:00
                ExecutorPullStart=2020-10-08T02:32:39+00:00
                ExecutorPullEnd=2020-10-08T02:34:39+00:00
                ExecutorImageSizeInBytes=3000000000
                DownloadStart=2020-10-08T02:35:39+00:00
                DownloadEnd=2020-10-08T02:38:39+00:00
                ExecutorStart=2020-10-08T02:39:39+00:00
                ExecutorEnd=2020-10-08T02:43:39+00:00
                UploadStart=2020-10-08T02:44:39+00:00
                UploadEnd=2020-10-08T02:49:39+00:00
                DiskSizeInKiB=8000000
                DiskUsedInKiB=1000000
                FileDownloadSizeInBytes=2000000000
                FileUploadSizeInBytes=4000000000".Replace(" ", string.Empty);

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchJobAndTaskState = BatchJobAndTaskStates.TaskCompletedSuccessfully;
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

                var executorLog = tesTask.GetOrAddTesTaskLog().GetOrAddExecutorLog();
                Assert.IsNotNull(executorLog);
                Assert.AreEqual(0, executorLog.ExitCode);
                Assert.AreEqual(DateTimeOffset.Parse("2020-10-08T02:30:39+00:00"), executorLog.StartTime);
                Assert.AreEqual(DateTimeOffset.Parse("2020-10-08T02:49:39+00:00"), executorLog.EndTime);
            });
        }

        [TestMethod]
        public async Task SuccessfullyCompletedTaskContainsCromwellResultCode()
        {
            var tesTask = GetTesTask();

            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchJobAndTaskState = BatchJobAndTaskStates.TaskCompletedSuccessfully;
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
            var executionDirectoryBlobs = tesTask.Inputs.Select(CloudBlobFromTesInput).ToList();

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
                executionDirectoryUri = UrlMutableSASEqualityComparer.TrimUri(storageAccessProvider.MapLocalPathToSasUrlAsync(commandScriptDir.IsFile ? commandScriptDir.AbsolutePath : commandScriptDir.AbsoluteUri, CancellationToken.None, getContainerSas: true).Result);

                serviceProvider.AzureProxy.Setup(p => p.ListBlobsAsync(It.Is(executionDirectoryUri, new UrlMutableSASEqualityComparer()), It.IsAny<CancellationToken>())).Returns(Task.FromResult<IEnumerable<CloudBlob>>(executionDirectoryBlobs));

                var uri = new UriBuilder(executionDirectoryUri);
                uri.Path = uri.Path.TrimEnd('/') + $"/{fileName}";

                TesInput writeInput = new() { Url = uri.Uri.AbsoluteUri, Path = Path.Combine(Path.GetDirectoryName(script[1]), fileName).Replace('\\', '/'), Type = TesFileType.FILEEnum, Name = "write_", Content = null };
                executionDirectoryBlobs.Add(CloudBlobFromTesInput(writeInput));

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

            static CloudBlob CloudBlobFromTesInput(TesInput input)
                => new(UriFromTesInput(input));

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

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, azureProxy, AzureProxyReturnValues.Defaults);

            var poolNetworkConfiguration = poolSpec.NetworkConfiguration;

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

            (_, _, var poolSpec) = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, config, azureProxy, AzureProxyReturnValues.Defaults);

            var poolNetworkConfiguration = poolSpec.NetworkConfiguration;

            GuardAssertsWithTesTask(tesTask, () =>
            {
                Assert.AreEqual(Microsoft.Azure.Management.Batch.Models.IPAddressProvisioningType.NoPublicIPAddresses, poolNetworkConfiguration?.PublicIPAddressConfiguration?.Provision);
                Assert.AreEqual("subnet1", poolNetworkConfiguration?.SubnetId);
            });
        }

        private static async Task<(string FailureReason, string[] SystemLog)> ProcessTesTaskAndGetFailureReasonAndSystemLogAsync(TesTask tesTask, AzureBatchJobAndTaskState? azureBatchJobAndTaskState = null)
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchJobAndTaskState = azureBatchJobAndTaskState ?? azureProxyReturnValues.BatchJobAndTaskState;

            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(azureProxyReturnValues), azureProxyReturnValues);

            return (tesTask.Logs?.LastOrDefault()?.FailureReason, tesTask.Logs?.LastOrDefault()?.SystemLogs?.ToArray());
        }

        private static async Task<(string JobId, CloudTask CloudTask, Pool batchModelsPool)> ProcessTesTaskAndGetBatchJobArgumentsAsync(TesTask tesTask, IEnumerable<(string Key, string Value)> configuration, Action<Mock<IAzureProxy>> azureProxy, AzureProxyReturnValues azureProxyReturnValues, Action<IServiceCollection> additionalActions = default, Action<TestServices.TestServiceProvider<IBatchScheduler>> serviceProviderActions = default)
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

            await batchScheduler.ProcessTesTaskAsync(tesTask, System.Threading.CancellationToken.None);

            var createBatchPoolAsyncInvocation = serviceProvider.AzureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.CreateBatchPoolAsync));
            var addBatchTaskAsyncInvocation = serviceProvider.AzureProxy.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IAzureProxy.AddBatchTaskAsync));

            var jobId = addBatchTaskAsyncInvocation?.Arguments[2] as string;
            var cloudTask = addBatchTaskAsyncInvocation?.Arguments[1] as CloudTask;
            var batchPoolsModel = createBatchPoolAsyncInvocation?.Arguments[0] as Pool;

            return (jobId, cloudTask, batchPoolsModel);
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
                proxy.Setup(p => p.GetAllowedVmSizes(It.IsAny<System.Threading.CancellationToken>()))
                    .ReturnsAsync(allowedVms);
            });


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

        private static TestServices.TestServiceProvider<IBatchScheduler> GetServiceProvider(IEnumerable<(string Key, string Value)> configuration, Action<Mock<IAzureProxy>> azureProxy, Action<Mock<IBatchQuotaProvider>> quotaProvider, Action<Mock<IBatchSkuInformationProvider>> skuInfoProvider, Action<Mock<IAllowedVmSizesService>> allowedVmSizesServiceSetup, Action<IServiceCollection> additionalActions = default)
            => new(wrapAzureProxy: true, configuration: configuration, azureProxy: azureProxy, batchQuotaProvider: quotaProvider, batchSkuInformationProvider: skuInfoProvider, accountResourceInformation: GetNewBatchResourceInfo(), allowedVmSizesServiceSetup: allowedVmSizesServiceSetup, additionalActions: additionalActions);

        private static async Task<TesState> GetNewTesTaskStateAsync(TesTask tesTask, AzureProxyReturnValues azureProxyReturnValues)
        {
            _ = await ProcessTesTaskAndGetBatchJobArgumentsAsync(tesTask, GetMockConfig()(), GetMockAzureProxy(azureProxyReturnValues), azureProxyReturnValues);

            return tesTask.State;
        }

        private static Task<TesState> GetNewTesTaskStateAsync(TesState currentTesTaskState, AzureBatchJobAndTaskState azureBatchJobAndTaskState)
            => GetNewTesTaskStateAsync(new TesTask { Id = "test", State = currentTesTaskState, Executors = Enumerable.Empty<TesExecutor>().Append(new() { Image = "image", Command = Enumerable.Empty<string>().Append("command").ToList() }).ToList() }, azureBatchJobAndTaskState);

        private static Task<TesState> GetNewTesTaskStateAsync(TesTask tesTask, AzureBatchJobAndTaskState? azureBatchJobAndTaskState = null)
        {
            var azureProxyReturnValues = AzureProxyReturnValues.Defaults;
            azureProxyReturnValues.BatchJobAndTaskState = azureBatchJobAndTaskState ?? azureProxyReturnValues.BatchJobAndTaskState;

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

                azureProxy.Setup(a => a.GetBatchJobAndTaskStateAsync(It.IsAny<TesTask>(), It.IsAny<System.Threading.CancellationToken>()))
                    .Returns(Task.FromResult(azureProxyReturnValues.BatchJobAndTaskState));

                azureProxy.Setup(a => a.GetStorageAccountInfoAsync("defaultstorageaccount", It.IsAny<System.Threading.CancellationToken>()))
                    .Returns(Task.FromResult(azureProxyReturnValues.StorageAccountInfos["defaultstorageaccount"]));

                azureProxy.Setup(a => a.GetStorageAccountInfoAsync("storageaccount1", It.IsAny<System.Threading.CancellationToken>()))
                    .Returns(Task.FromResult(azureProxyReturnValues.StorageAccountInfos["storageaccount1"]));

                azureProxy.Setup(a => a.GetStorageAccountKeyAsync(It.IsAny<StorageAccountInfo>(), It.IsAny<System.Threading.CancellationToken>()))
                    .Returns(Task.FromResult(azureProxyReturnValues.StorageAccountKey));

                azureProxy.Setup(a => a.GetBatchActiveNodeCountByVmSize())
                    .Returns(azureProxyReturnValues.ActiveNodeCountByVmSize);

                azureProxy.Setup(a => a.GetBatchActiveJobCount())
                    .Returns(azureProxyReturnValues.ActiveJobCount);

                azureProxy.Setup(a => a.GetBatchActivePoolCount())
                    .Returns(azureProxyReturnValues.ActivePoolCount);

                azureProxy.Setup(a => a.GetBatchPoolAsync(It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>(), It.IsAny<DetailLevel>()))
                    .Returns((string id, System.Threading.CancellationToken cancellationToken, DetailLevel detailLevel) => Task.FromResult(azureProxyReturnValues.GetBatchPoolImpl(id)));

                azureProxy.Setup(a => a.DownloadBlobAsync(It.IsAny<Uri>(), It.IsAny<System.Threading.CancellationToken>()))
                    .Returns(Task.FromResult(azureProxyReturnValues.DownloadedBlobContent));

                azureProxy.Setup(a => a.CreateBatchPoolAsync(It.IsAny<Pool>(), It.IsAny<bool>(), It.IsAny<System.Threading.CancellationToken>()))
                    .Returns((Pool p, bool _1, System.Threading.CancellationToken _2) => Task.FromResult(azureProxyReturnValues.CreateBatchPoolImpl(p)));

                azureProxy.Setup(a => a.GetFullAllocationStateAsync(It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>()))
                    .Returns(Task.FromResult(azureProxyReturnValues.AzureProxyGetFullAllocationState?.Invoke() ?? new(null, null, null, null, null, null, null)));

                azureProxy.Setup(a => a.ListComputeNodesAsync(It.IsAny<string>(), It.IsAny<DetailLevel>()))
                    .Returns(new Func<string, DetailLevel, IAsyncEnumerable<ComputeNode>>((string poolId, DetailLevel _1)
                        => AsyncEnumerable.Empty<ComputeNode>()
                            .Append(BatchPoolTests.GenerateNode(poolId, "ComputeNodeDedicated1", true, true))));

                azureProxy.Setup(a => a.DeleteBatchPoolAsync(It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>()))
                    .Callback<string, System.Threading.CancellationToken>((poolId, cancellationToken) => azureProxyReturnValues.AzureProxyDeleteBatchPoolImpl(poolId, cancellationToken))
                    .Returns(Task.CompletedTask);

                azureProxy.Setup(a => a.ListTasksAsync(It.IsAny<string>(), It.IsAny<DetailLevel>()))
                    .Returns(azureProxyReturnValues.AzureProxyListTasks);
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
            => (BatchPool)await batchScheduler.GetOrAddPoolAsync("key1", false, (id, cancellationToken) => ValueTask.FromResult<Pool>(new(name: id, displayName: "display1", vmSize: "vmSize1")), System.Threading.CancellationToken.None);

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
                foreach (var log in tesTask.Logs)
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

        private struct BatchJobAndTaskStates
        {
            public static AzureBatchJobAndTaskState TaskActive => new() { JobState = JobState.Active, TaskState = TaskState.Active };
            public static AzureBatchJobAndTaskState TaskPreparing => new() { JobState = JobState.Active, TaskState = TaskState.Preparing };
            public static AzureBatchJobAndTaskState TaskRunning => new() { JobState = JobState.Active, TaskState = TaskState.Running };
            public static AzureBatchJobAndTaskState TaskCompletedSuccessfully => new() { JobState = JobState.Completed, TaskState = TaskState.Completed, TaskExitCode = 0 };
            public static AzureBatchJobAndTaskState TaskFailed => new() { JobState = JobState.Completed, TaskState = TaskState.Completed, TaskExitCode = -1 };
            public static AzureBatchJobAndTaskState JobNotFound => new() { JobState = null };
            public static AzureBatchJobAndTaskState TaskNotFound => new() { JobState = JobState.Active, TaskState = null };
            public static AzureBatchJobAndTaskState MoreThanOneJobFound => new() { MoreThanOneActiveJobOrTaskFound = true };
            public static AzureBatchJobAndTaskState NodeAllocationFailed => new() { JobState = JobState.Active, NodeAllocationFailed = true };
            public static AzureBatchJobAndTaskState NodePreempted => new() { JobState = JobState.Active, NodeState = ComputeNodeState.Preempted };
            public static AzureBatchJobAndTaskState NodeDiskFull => new() { JobState = JobState.Active, NodeErrorCode = "DiskFull" };
            public static AzureBatchJobAndTaskState ActiveJobWithMissingAutoPool => new() { ActiveJobWithMissingAutoPool = true };
            public static AzureBatchJobAndTaskState ImageDownloadFailed => new() { JobState = JobState.Active, NodeErrorCode = "ContainerInvalidImage" };
        }

        private class AzureProxyReturnValues
        {
            internal Func<FullBatchPoolAllocationState> AzureProxyGetFullAllocationState { get; set; }
            internal Action<string, System.Threading.CancellationToken> AzureProxyDeleteBatchPoolIfExists { get; set; }
            internal Action<string, System.Threading.CancellationToken> AzureProxyDeleteBatchPool { get; set; }
            internal Func<string, ODATADetailLevel, IAsyncEnumerable<CloudTask>> AzureProxyListTasks { get; set; } = (jobId, detail) => AsyncEnumerable.Empty<CloudTask>();
            public Dictionary<string, StorageAccountInfo> StorageAccountInfos { get; set; }
            public List<VirtualMachineInformation> VmSizesAndPrices { get; set; }
            public AzureBatchAccountQuotas BatchQuotas { get; set; }
            public IEnumerable<AzureBatchNodeCount> ActiveNodeCountByVmSize { get; set; }
            public int ActiveJobCount { get; set; }
            public int ActivePoolCount { get; set; }
            public AzureBatchJobAndTaskState BatchJobAndTaskState { get; set; }
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
                BatchJobAndTaskState = BatchJobAndTaskStates.JobNotFound,
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

            private readonly Dictionary<string, IList<Microsoft.Azure.Batch.MetadataItem>> poolMetadata = [];

            internal void AzureProxyDeleteBatchPoolImpl(string poolId, System.Threading.CancellationToken cancellationToken)
            {
                _ = poolMetadata.Remove(poolId);
                AzureProxyDeleteBatchPool(poolId, cancellationToken);
            }

            internal string CreateBatchPoolImpl(Pool pool)
            {
                var poolId = pool.Name;

                poolMetadata.Add(poolId, pool.Metadata?.Select(Convert).ToList());
                return poolId;

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

        private class TestBatchQuotaVerifierQuotaMaxedOut : TestBatchQuotaVerifierBase
        {
            public TestBatchQuotaVerifierQuotaMaxedOut(IBatchQuotaProvider batchQuotaProvider) : base(batchQuotaProvider) { }

            public override Task CheckBatchAccountQuotasAsync(VirtualMachineInformation _1, bool _2, System.Threading.CancellationToken cancellationToken)
                => throw new AzureBatchQuotaMaxedOutException("Test AzureBatchQuotaMaxedOutException");
        }

        private class TestBatchQuotaVerifierLowQuota : TestBatchQuotaVerifierBase
        {
            public TestBatchQuotaVerifierLowQuota(IBatchQuotaProvider batchQuotaProvider) : base(batchQuotaProvider) { }

            public override Task CheckBatchAccountQuotasAsync(VirtualMachineInformation _1, bool _2, System.Threading.CancellationToken cancellationToken)
                => throw new AzureBatchLowQuotaException("Test AzureBatchLowQuotaException");
        }

        private abstract class TestBatchQuotaVerifierBase : IBatchQuotaVerifier
        {
            private readonly IBatchQuotaProvider batchQuotaProvider;

            protected TestBatchQuotaVerifierBase(IBatchQuotaProvider batchQuotaProvider)
                => this.batchQuotaProvider = batchQuotaProvider;

            public abstract Task CheckBatchAccountQuotasAsync(VirtualMachineInformation virtualMachineInformation, bool needPoolOrJobQuotaCheck, System.Threading.CancellationToken cancellationToken);

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
