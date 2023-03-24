// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Polly.Utilities;
using TesApi.Web;
using TesApi.Web.Storage;

namespace TesApi.Tests
{
    [TestClass]
    public class CachingWithRetriesAzureProxyTests
    {
        [TestMethod]
        public async Task GetStorageAccountKeyAsync_UsesCache()
        {
            var storageAccountInfo = new StorageAccountInfo { Name = "defaultstorageaccount", Id = "Id", BlobEndpoint = "https://defaultstorageaccount/", SubscriptionId = "SubId" };
            var storageAccountKey = "key";
            using var serviceProvider = new TestServices.TestServiceProvider<CachingWithRetriesAzureProxy>(azureProxy: a =>
            {
                PrepareAzureProxy(a);
                a.Setup(a => a.GetStorageAccountKeyAsync(It.IsAny<StorageAccountInfo>(), It.IsAny<CancellationToken>())).Returns(Task.FromResult(storageAccountKey));
            });
            var cachingAzureProxy = serviceProvider.GetT();

            var key1 = await cachingAzureProxy.GetStorageAccountKeyAsync(storageAccountInfo, CancellationToken.None);
            var key2 = await cachingAzureProxy.GetStorageAccountKeyAsync(storageAccountInfo, CancellationToken.None);

            serviceProvider.AzureProxy.Verify(mock => mock.GetStorageAccountKeyAsync(storageAccountInfo, CancellationToken.None), Times.Once());
            Assert.AreEqual(storageAccountKey, key1);
            Assert.AreEqual(key1, key2);
        }

        [TestMethod]
        public async Task GetStorageAccountInfoAsync_UsesCache()
        {
            var storageAccountInfo = new StorageAccountInfo { Name = "defaultstorageaccount", Id = "Id", BlobEndpoint = "https://defaultstorageaccount/", SubscriptionId = "SubId" };
            using var serviceProvider = new TestServices.TestServiceProvider<CachingWithRetriesAzureProxy>(azureProxy: a =>
            {
                PrepareAzureProxy(a);
                a.Setup(a => a.GetStorageAccountInfoAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns(Task.FromResult(storageAccountInfo));
            });
            var cachingAzureProxy = serviceProvider.GetT();

            var info1 = await cachingAzureProxy.GetStorageAccountInfoAsync("defaultstorageaccount", CancellationToken.None);
            var info2 = await cachingAzureProxy.GetStorageAccountInfoAsync("defaultstorageaccount", CancellationToken.None);

            serviceProvider.AzureProxy.Verify(mock => mock.GetStorageAccountInfoAsync("defaultstorageaccount", CancellationToken.None), Times.Once());
            Assert.AreEqual(storageAccountInfo, info1);
            Assert.AreEqual(info1, info2);
        }

        [TestMethod]
        public async Task GetStorageAccountInfoAsync_NullInfo_DoesNotSetCache()
        {
            using var serviceProvider = new TestServices.TestServiceProvider<CachingWithRetriesAzureProxy>(azureProxy: a =>
            {
                PrepareAzureProxy(a);
                a.Setup(a => a.GetStorageAccountInfoAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns(Task.FromResult((StorageAccountInfo)null));
            });
            var cachingAzureProxy = serviceProvider.GetT();
            var info1 = await cachingAzureProxy.GetStorageAccountInfoAsync("defaultstorageaccount", CancellationToken.None);

            var storageAccountInfo = new StorageAccountInfo { Name = "defaultstorageaccount", Id = "Id", BlobEndpoint = "https://defaultstorageaccount/", SubscriptionId = "SubId" };
            serviceProvider.AzureProxy.Setup(a => a.GetStorageAccountInfoAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns(Task.FromResult(storageAccountInfo));
            var info2 = await cachingAzureProxy.GetStorageAccountInfoAsync("defaultstorageaccount", CancellationToken.None);

            serviceProvider.AzureProxy.Verify(mock => mock.GetStorageAccountInfoAsync("defaultstorageaccount", CancellationToken.None), Times.Exactly(2));
            Assert.IsNull(info1);
            Assert.AreEqual(storageAccountInfo, info2);
        }

        [TestMethod]
        public void GetBatchActivePoolCount_ThrowsException_RetriesThreeTimes()
        {
            SystemClock.SleepAsync = (_, __) => Task.FromResult(true);
            SystemClock.Sleep = (_, __) => { };
            using var serviceProvider = new TestServices.TestServiceProvider<CachingWithRetriesAzureProxy>(azureProxy: a =>
            {
                PrepareAzureProxy(a);
                a.Setup(a => a.GetBatchActivePoolCount()).Throws<Exception>();
            });
            var cachingAzureProxy = serviceProvider.GetT();

            Assert.ThrowsException<Exception>(() => cachingAzureProxy.GetBatchActivePoolCount());
            serviceProvider.AzureProxy.Verify(mock => mock.GetBatchActivePoolCount(), Times.Exactly(4));
        }

        [TestMethod]
        public void GetBatchActiveJobCount_ThrowsException_RetriesThreeTimes()
        {
            SystemClock.SleepAsync = (_, __) => Task.FromResult(true);
            SystemClock.Sleep = (_, __) => { };
            using var serviceProvider = new TestServices.TestServiceProvider<CachingWithRetriesAzureProxy>(azureProxy: a =>
            {
                PrepareAzureProxy(a);
                a.Setup(a => a.GetBatchActiveJobCount()).Throws<Exception>();
            });
            var cachingAzureProxy = serviceProvider.GetT();

            Assert.ThrowsException<Exception>(() => cachingAzureProxy.GetBatchActiveJobCount());
            serviceProvider.AzureProxy.Verify(mock => mock.GetBatchActiveJobCount(), Times.Exactly(4));
        }

        private static void PrepareAzureProxy(Mock<IAzureProxy> azureProxy)
        {
            //azureProxy.Setup(a => a.GetVmSizesAndPricesAsync()).Returns(Task.FromResult(
            //    new List<VirtualMachineInformation> {
            //        new() { VmSize = "VmSizeLowPri1", LowPriority = true, VCpusAvailable = 1, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 1 },
            //        new() { VmSize = "VmSizeLowPri2", LowPriority = true, VCpusAvailable = 2, MemoryInGiB = 8, ResourceDiskSizeInGiB = 40, PricePerHour = 2 },
            //        new() { VmSize = "VmSizeDedicated1", LowPriority = false, VCpusAvailable = 1, MemoryInGiB = 4, ResourceDiskSizeInGiB = 20, PricePerHour = 11 },
            //        new() { VmSize = "VmSizeDedicated2", LowPriority = false, VCpusAvailable = 2, MemoryInGiB = 8, ResourceDiskSizeInGiB = 40, PricePerHour = 22 }
            //    }));
        }
    }
}
