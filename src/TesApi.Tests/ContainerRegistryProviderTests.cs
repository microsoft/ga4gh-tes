// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.ApiClients;
using TesApi.Web;
using TesApi.Web.Management;
using TesApi.Web.Management.Configuration;

namespace TesApi.Tests
{
    [TestClass]
    [TestCategory("unit")]
    public class ContainerRegistryProviderTests
    {
        private ContainerRegistryProvider containerRegistryProvider;
        private ContainerRegistryOptions containerRegistryOptions;
        private Mock<CachingRetryHandler> retryHandlerMock;
        private Mock<IMemoryCache> appCacheMock;
        private Mock<IOptions<ContainerRegistryOptions>> containerRegistryOptionsMock;
        private Mock<ILogger<ContainerRegistryProvider>> loggerMock;
        private Mock<AzureManagementClientsFactory> clientFactoryMock;



        [TestInitialize]
        public void Setup()
        {
            appCacheMock = new Mock<IMemoryCache>();
            retryHandlerMock = new Mock<CachingRetryHandler>();
            retryHandlerMock.Setup(r => r.AppCache).Returns(appCacheMock.Object);
            clientFactoryMock = new Mock<AzureManagementClientsFactory>();
            containerRegistryOptionsMock = new Mock<IOptions<ContainerRegistryOptions>>();
            containerRegistryOptions = new ContainerRegistryOptions();
            containerRegistryOptionsMock.Setup(o => o.Value).Returns(containerRegistryOptions);
            loggerMock = new Mock<ILogger<ContainerRegistryProvider>>();
            containerRegistryProvider = new ContainerRegistryProvider(containerRegistryOptionsMock.Object,
                retryHandlerMock.Object, clientFactoryMock.Object, loggerMock.Object);
        }

        [TestMethod]
        public async Task GetContainerRegistryInfoAsync_ServerIsAccessible_ReturnsAndAddsToCacheRegistryInformation()
        {
            var server = "registry.com";
            var image = $"{server}/image";
            retryHandlerMock.Setup(r =>
                    r.ExecuteWithRetryAsync(It.IsAny<Func<System.Threading.CancellationToken, Task<IEnumerable<ContainerRegistryInfo>>>>(), It.IsAny<System.Threading.CancellationToken>()))
                .ReturnsAsync(new List<ContainerRegistryInfo>()
                {
                    new ContainerRegistryInfo() { RegistryServer = server }
                });
            appCacheMock.Setup(c => c.CreateEntry(It.IsAny<object>()))
                .Returns(new Mock<ICacheEntry>().Object);

            var container = await containerRegistryProvider.GetContainerRegistryInfoAsync(image, System.Threading.CancellationToken.None);

            Assert.IsNotNull(container);
            Assert.AreEqual(server, container.RegistryServer);
            appCacheMock.Verify(
                c => c.CreateEntry(It.Is<string>(v => v.Equals($"{nameof(ContainerRegistryProvider)}:{image}"))), Times.Once());
        }

        [TestMethod]
        public async Task GetContainerRegistryInfoAsync_ServerInCache_ReturnsRegistryInformationFromCacheAndNoListingOfRegistries()
        {
            var server = "registry";
            var image = $"{server}/image";
            appCacheMock.Setup(c => c.TryGetValue(It.Is<object>(v => $"{nameof(ContainerRegistryProvider)}:{image}".Equals(v)), out It.Ref<object>.IsAny))
                .Returns((object _1, out ContainerRegistryInfo registryInfo) => { registryInfo = new ContainerRegistryInfo() { RegistryServer = server }; return true; });

            var container = await containerRegistryProvider.GetContainerRegistryInfoAsync(image, System.Threading.CancellationToken.None);

            Assert.IsNotNull(container);
            Assert.AreEqual(server, container.RegistryServer);
            appCacheMock.Verify(c => c.TryGetValue(It.Is<object>(v => $"{nameof(ContainerRegistryProvider)}:{image}".Equals(v)), out It.Ref<object>.IsAny), Times.Once());
            retryHandlerMock.Verify(r =>
                r.ExecuteWithRetryAsync(It.IsAny<Func<Task<IEnumerable<ContainerRegistryInfo>>>>()), Times.Never);
        }

        [TestMethod]
        [DataRow("mcr.microsoft.com")]
        [DataRow("mcr.microsoft.com/blobxfer")]
        [DataRow("docker")]
        public async Task GetContainerRegistryInfoAsync_DoesNotLogWarningWhenKnownImage(string imageName)
        {
            await containerRegistryProvider.GetContainerRegistryInfoAsync(imageName, System.Threading.CancellationToken.None);

            loggerMock.Verify(logger => logger.Log(LogLevel.Warning, It.IsAny<EventId>(), It.IsAny<object>(), It.IsAny<Exception>(), (Func<object, Exception, string>)It.IsAny<object>()), Times.Never);
        }

        [TestMethod]
        public async Task GetContainerRegistryInfoAsync_NoAccessibleServerNoServerCached_ReturnsNullNotAddedToCache()
        {
            var server = "registry";
            var image = $"{server}_other/image";
            retryHandlerMock.Setup(r =>
                    r.ExecuteWithRetryAsync(It.IsAny<Func<Task<IEnumerable<ContainerRegistryInfo>>>>()))
                .ReturnsAsync(new List<ContainerRegistryInfo>()
                {
                    new ContainerRegistryInfo() { RegistryServer = server }
                });

            var container = await containerRegistryProvider.GetContainerRegistryInfoAsync(image, System.Threading.CancellationToken.None);

            Assert.IsNull(container);
            appCacheMock.Verify(
                c => c.CreateEntry(It.Is<string>(v => v.Equals($"{nameof(ContainerRegistryProvider)}:{image}"))), Times.Never);
        }
    }
}
