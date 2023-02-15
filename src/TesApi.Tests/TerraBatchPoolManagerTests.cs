// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AutoMapper;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using TesApi.Web.Management.Batch;
using TesApi.Web.Management.Clients;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Management.Models.Terra;

namespace TesApi.Tests
{
    [TestClass]
    public class TerraBatchPoolManagerTests
    {
        private TerraBatchPoolManager terraBatchPoolManager;
        private Mock<TerraWsmApiClient> wsmApiClientMock;
        private Mock<IOptions<TerraOptions>> terraOptionsMock;
        private Mock<IOptions<BatchAccountOptions>> batchAccountOptionsMock;
        private TerraApiStubData terraApiStubData;
        private ApiCreateBatchPoolRequest capturedApiCreateBatchPoolRequest;

        [TestInitialize]
        public void SetUp()
        {
            terraApiStubData = new TerraApiStubData();
            wsmApiClientMock = new Mock<TerraWsmApiClient>();
            terraOptionsMock = new Mock<IOptions<TerraOptions>>();
            terraOptionsMock.Setup(x => x.Value).Returns(terraApiStubData.GetTerraOptions());
            batchAccountOptionsMock = new Mock<IOptions<BatchAccountOptions>>();
            batchAccountOptionsMock.Setup(x => x.Value).Returns(terraApiStubData.GetBatchAccountOptions());
            wsmApiClientMock.Setup(x => x.CreateBatchPool(It.IsAny<Guid>(), It.IsAny<ApiCreateBatchPoolRequest>()))
                .Callback<Guid, ApiCreateBatchPoolRequest>((arg1, arg2) => capturedApiCreateBatchPoolRequest = arg2)
                .ReturnsAsync(terraApiStubData.GetApiCreateBatchPoolResponse());

            var mapperCfg = new MapperConfiguration(cfg => cfg.AddProfile(typeof(MappingProfilePoolToWsmRequest)));

            terraBatchPoolManager = new TerraBatchPoolManager(wsmApiClientMock.Object, mapperCfg.CreateMapper(),
                terraOptionsMock.Object, batchAccountOptionsMock.Object, NullLogger<TerraBatchPoolManager>.Instance);
        }


        [TestMethod]
        public void BatchPoolToWsmRequestMappingProfileIsValid()
        {
            var configuration = new MapperConfiguration(cfg =>
            {
                cfg.AddProfile(typeof(MappingProfilePoolToWsmRequest));
            });

            configuration.AssertConfigurationIsValid();
        }

        [TestMethod]
        public async Task CreateBatchPoolAsync_ValidResponse()
        {
            var poolInfo = new Pool()
            {
                DeploymentConfiguration = new DeploymentConfiguration()
                {
                    CloudServiceConfiguration = new CloudServiceConfiguration("osfamily", "osversion"),
                    VirtualMachineConfiguration = new VirtualMachineConfiguration()
                },
                UserAccounts = new List<UserAccount>() { new UserAccount("name", "password") }
            };

            var pool = await terraBatchPoolManager.CreateBatchPoolAsync(poolInfo, false);

            Assert.IsNotNull(pool);
            Assert.AreEqual(terraApiStubData.PoolId, pool.PoolId);
        }

        [TestMethod]
        [DataRow(false, 1)]
        [DataRow(true, 2)]
        public async Task CreateBatchPoolAsync_AddsResourceIdToMetadata(bool addPoolMetadata, int expectedMetadataLength)
        {
            var poolInfo = new Pool()
            {
                DeploymentConfiguration = new DeploymentConfiguration()
                {
                    CloudServiceConfiguration = new CloudServiceConfiguration("osfamily", "osversion"),
                    VirtualMachineConfiguration = new VirtualMachineConfiguration()
                },
                UserAccounts = new List<UserAccount>() { new UserAccount("name", "password") }
            };

            if (addPoolMetadata)
            {
                poolInfo.Metadata = new List<MetadataItem>() { new MetadataItem("name", "value") };
            }

            var pool = await terraBatchPoolManager.CreateBatchPoolAsync(poolInfo, false);

            Assert.IsNotNull(pool);
            Assert.AreEqual(expectedMetadataLength, capturedApiCreateBatchPoolRequest.AzureBatchPool.Metadata.Length);
            Assert.IsNotNull(capturedApiCreateBatchPoolRequest.AzureBatchPool.Metadata.SingleOrDefault(m => m.Name.Equals(TerraBatchPoolManager.TerraResourceIdMetadataKey)));
        }

        [TestMethod]
        public async Task CreateBatchPoolAsync_MultipleCallsHaveDifferentNameAndResourceId()
        {
            var poolInfo = new Pool()
            {
                DeploymentConfiguration = new DeploymentConfiguration()
                {
                    CloudServiceConfiguration = new CloudServiceConfiguration("osfamily", "osversion"),
                    VirtualMachineConfiguration = new VirtualMachineConfiguration()
                },
            };

            var pool = await terraBatchPoolManager.CreateBatchPoolAsync(poolInfo, false);

            var name = capturedApiCreateBatchPoolRequest.Common.Name;
            var resourceId = capturedApiCreateBatchPoolRequest.Common.ResourceId;

            pool = await terraBatchPoolManager.CreateBatchPoolAsync(poolInfo, false);

            Assert.AreNotEqual(name, capturedApiCreateBatchPoolRequest.Common.Name);
            Assert.AreNotEqual(resourceId, capturedApiCreateBatchPoolRequest.Common.ResourceId);
        }
    }
}
