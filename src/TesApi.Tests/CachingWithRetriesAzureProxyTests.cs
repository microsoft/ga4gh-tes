// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.


using System;
using System.Threading.Tasks;
using LazyCache;
using LazyCache.Providers;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Polly.Utilities;
using TesApi.Web;

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
                a.Setup(a => a.GetStorageAccountKeyAsync(It.IsAny<StorageAccountInfo>())).Returns(Task.FromResult(storageAccountKey));
            });
            var cachingAzureProxy = serviceProvider.GetT();

            var key1 = await cachingAzureProxy.GetStorageAccountKeyAsync(storageAccountInfo);
            var key2 = await cachingAzureProxy.GetStorageAccountKeyAsync(storageAccountInfo);

            serviceProvider.AzureProxy.Verify(mock => mock.GetStorageAccountKeyAsync(storageAccountInfo), Times.Once());
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
                a.Setup(a => a.GetStorageAccountInfoAsync(It.IsAny<string>())).Returns(Task.FromResult(storageAccountInfo));
            });
            var cachingAzureProxy = serviceProvider.GetT();

            var info1 = await cachingAzureProxy.GetStorageAccountInfoAsync("defaultstorageaccount");
            var info2 = await cachingAzureProxy.GetStorageAccountInfoAsync("defaultstorageaccount");

            serviceProvider.AzureProxy.Verify(mock => mock.GetStorageAccountInfoAsync("defaultstorageaccount"), Times.Once());
            Assert.AreEqual(storageAccountInfo, info1);
            Assert.AreEqual(info1, info2);
        }

        [TestMethod]
        public async Task GetStorageAccountInfoAsync_NullInfo_DoesNotSetCache()
        {
            using var serviceProvider = new TestServices.TestServiceProvider<CachingWithRetriesAzureProxy>(azureProxy: a =>
            {
                PrepareAzureProxy(a);
                a.Setup(a => a.GetStorageAccountInfoAsync(It.IsAny<string>())).Returns(Task.FromResult((StorageAccountInfo)null));
            });
            var cachingAzureProxy = serviceProvider.GetT();
            var info1 = await cachingAzureProxy.GetStorageAccountInfoAsync("defaultstorageaccount");

            var storageAccountInfo = new StorageAccountInfo { Name = "defaultstorageaccount", Id = "Id", BlobEndpoint = "https://defaultstorageaccount/", SubscriptionId = "SubId" };
            serviceProvider.AzureProxy.Setup(a => a.GetStorageAccountInfoAsync(It.IsAny<string>())).Returns(Task.FromResult(storageAccountInfo));
            var info2 = await cachingAzureProxy.GetStorageAccountInfoAsync("defaultstorageaccount");

            serviceProvider.AzureProxy.Verify(mock => mock.GetStorageAccountInfoAsync("defaultstorageaccount"), Times.Exactly(2));
            Assert.IsNull(info1);
            Assert.AreEqual(storageAccountInfo, info2);
        }

        [TestMethod]
        public async Task GetContainerRegistryInfoAsync_UsesCache()
        {
            var containerRegistryInfo = new ContainerRegistryInfo { RegistryServer = "registryServer1", Username = "default", Password = "placeholder" };

            using var serviceProvider = new TestServices.TestServiceProvider<CachingWithRetriesAzureProxy>(azureProxy: a =>
            {
                PrepareAzureProxy(a);
                a.Setup(a => a.GetContainerRegistryInfoAsync(It.IsAny<string>())).Returns(Task.FromResult(containerRegistryInfo));
            });
            var cachingAzureProxy = serviceProvider.GetT();

            var info1 = await cachingAzureProxy.GetContainerRegistryInfoAsync("registryServer1/imageName1:tag1");
            var info2 = await cachingAzureProxy.GetContainerRegistryInfoAsync("registryServer1/imageName1:tag1");

            serviceProvider.AzureProxy.Verify(mock => mock.GetContainerRegistryInfoAsync("registryServer1/imageName1:tag1"), Times.Once());
            Assert.AreEqual(containerRegistryInfo, info1);
            Assert.AreEqual(info1, info2);
        }

        [TestMethod]
        public async Task GetContainerRegistryInfoAsync_NullInfo_DoesNotSetCache()
        {
            using var serviceProvider = new TestServices.TestServiceProvider<CachingWithRetriesAzureProxy>(azureProxy: a =>
            {
                PrepareAzureProxy(a);
                a.Setup(a => a.GetContainerRegistryInfoAsync(It.IsAny<string>())).Returns(Task.FromResult((ContainerRegistryInfo)null));
            });
            var cachingAzureProxy = serviceProvider.GetT();
            var info1 = await cachingAzureProxy.GetContainerRegistryInfoAsync("registryServer1/imageName1:tag1");

            var containerRegistryInfo = new ContainerRegistryInfo { RegistryServer = "registryServer1", Username = "default", Password = "placeholder" };
            serviceProvider.AzureProxy.Setup(a => a.GetContainerRegistryInfoAsync(It.IsAny<string>())).Returns(Task.FromResult(containerRegistryInfo));
            var info2 = await cachingAzureProxy.GetContainerRegistryInfoAsync("registryServer1/imageName1:tag1");

            serviceProvider.AzureProxy.Verify(mock => mock.GetContainerRegistryInfoAsync("registryServer1/imageName1:tag1"), Times.Exactly(2));
            Assert.IsNull(info1);
            Assert.AreEqual(containerRegistryInfo, info2);
        }

        [TestMethod]
        public async Task GetContainerRegistryInfoAsync_ThrowsException_DoesNotSetCache()
        {
            SystemClock.SleepAsync = (_, __) => Task.FromResult(true);
            SystemClock.Sleep = (_, __) => { };
            using var serviceProvider = new TestServices.TestServiceProvider<CachingWithRetriesAzureProxy>(azureProxy: a =>
            {
                PrepareAzureProxy(a);
                a.Setup(a => a.GetContainerRegistryInfoAsync("throw/exception:tag1")).Throws<Exception>();
            });
            var cachingAzureProxy = serviceProvider.GetT();

            await Assert.ThrowsExceptionAsync<Exception>(async () => await cachingAzureProxy.GetContainerRegistryInfoAsync("throw/exception:tag1"));
            serviceProvider.AzureProxy.Verify(mock => mock.GetContainerRegistryInfoAsync("throw/exception:tag1"), Times.Exactly(4));
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
            //        new() { VmSize = "VmSizeLowPri1", LowPriority = true, NumberOfCores = 1, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 1 },
            //        new() { VmSize = "VmSizeLowPri2", LowPriority = true, NumberOfCores = 2, MemoryInGB = 8, ResourceDiskSizeInGB = 40, PricePerHour = 2 },
            //        new() { VmSize = "VmSizeDedicated1", LowPriority = false, NumberOfCores = 1, MemoryInGB = 4, ResourceDiskSizeInGB = 20, PricePerHour = 11 },
            //        new() { VmSize = "VmSizeDedicated2", LowPriority = false, NumberOfCores = 2, MemoryInGB = 8, ResourceDiskSizeInGB = 40, PricePerHour = 22 }
            //    }));
        }
    }
}
