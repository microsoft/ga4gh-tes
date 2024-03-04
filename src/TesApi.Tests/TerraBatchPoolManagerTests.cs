// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AutoMapper;
using Azure.Core;
using Azure.ResourceManager.Batch;
using Azure.ResourceManager.Batch.Models;
using Azure.ResourceManager.Models;
using CommonUtilities;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.ApiClients;
using TesApi.Web.Management.Batch;
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
        private Mock<PoolMetadataReader> poolMetadataReaderMock;
        private TerraApiStubData terraApiStubData;
        private ApiCreateBatchPoolRequest capturedApiCreateBatchPoolRequest;

        [TestInitialize]
        public void SetUp()
        {
            terraApiStubData = new();
            wsmApiClientMock = new();
            terraOptionsMock = new();
            terraOptionsMock.Setup(x => x.Value).Returns(terraApiStubData.GetTerraOptions());
            poolMetadataReaderMock = new();
            wsmApiClientMock.Setup(x => x.CreateBatchPool(It.IsAny<Guid>(), It.IsAny<ApiCreateBatchPoolRequest>(), It.IsAny<System.Threading.CancellationToken>()))
                .Callback<Guid, ApiCreateBatchPoolRequest, System.Threading.CancellationToken>((arg1, arg2, arg3) => capturedApiCreateBatchPoolRequest = arg2)
                .ReturnsAsync(terraApiStubData.GetApiCreateBatchPoolResponse());

            var mapperCfg = new MapperConfiguration(cfg => cfg.AddProfile(typeof(MappingProfilePoolToWsmRequest)));

            terraBatchPoolManager = new TerraBatchPoolManager(wsmApiClientMock.Object, mapperCfg.CreateMapper(),
                poolMetadataReaderMock.Object, terraOptionsMock.Object, NullLogger<TerraBatchPoolManager>.Instance);
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
            var poolInfo = new BatchAccountPoolData()
            {
                DeploymentConfiguration = new BatchDeploymentConfiguration()
                {
                    VmConfiguration = new BatchVmConfiguration(new BatchImageReference(), "batchNodeAgent")
                },
            };
            poolInfo.UserAccounts.Add(new("name", "password"));
            poolInfo.Metadata.Add(new(string.Empty, terraApiStubData.PoolId));

            var poolId = await terraBatchPoolManager.CreateBatchPoolAsync(poolInfo, false, System.Threading.CancellationToken.None);

            Assert.IsFalse(string.IsNullOrWhiteSpace(poolId));
            Assert.AreEqual(terraApiStubData.PoolId, poolId);
        }

        [TestMethod]
        [DataRow(false, 1)]
        [DataRow(true, 2)]
        public async Task CreateBatchPoolAsync_AddsResourceIdToMetadata(bool addPoolMetadata, int expectedMetadataLength)
        {
            var poolInfo = new BatchAccountPoolData()
            {
                DeploymentConfiguration = new BatchDeploymentConfiguration()
                {
                    VmConfiguration = new BatchVmConfiguration(new BatchImageReference(), "batchNodeAgent")
                },
            };
            poolInfo.UserAccounts.Add(new("name", "password"));
            poolInfo.Metadata.Add(new(string.Empty, terraApiStubData.PoolId));

            if (addPoolMetadata)
            {
                poolInfo.Metadata.Add(new("name", "value"));
            }

            var pool = await terraBatchPoolManager.CreateBatchPoolAsync(poolInfo, false, System.Threading.CancellationToken.None);

            Assert.IsNotNull(pool);
            Assert.AreEqual(expectedMetadataLength, capturedApiCreateBatchPoolRequest.AzureBatchPool.Metadata.Length);
            Assert.IsNotNull(capturedApiCreateBatchPoolRequest.AzureBatchPool.Metadata.SingleOrDefault(m => m.Name.Equals(TerraBatchPoolManager.TerraResourceIdMetadataKey)));
        }

        [TestMethod]
        public async Task CreateBatchPoolAsync_MultipleCallsHaveDifferentNameAndResourceId()
        {
            var poolInfo = new BatchAccountPoolData()
            {
                DeploymentConfiguration = new BatchDeploymentConfiguration()
                {
                    VmConfiguration = new BatchVmConfiguration(new BatchImageReference(), "batchNodeAgent")
                },
            };
            poolInfo.Metadata.Add(new(string.Empty, terraApiStubData.PoolId));

            await terraBatchPoolManager.CreateBatchPoolAsync(poolInfo, false, System.Threading.CancellationToken.None);

            var name = capturedApiCreateBatchPoolRequest.Common.Name;
            var resourceId = capturedApiCreateBatchPoolRequest.Common.ResourceId;

            poolInfo.Metadata.Add(new(string.Empty, terraApiStubData.PoolId));
            await terraBatchPoolManager.CreateBatchPoolAsync(poolInfo, false, System.Threading.CancellationToken.None);

            Assert.AreNotEqual(name, capturedApiCreateBatchPoolRequest.Common.Name);
            Assert.AreNotEqual(resourceId, capturedApiCreateBatchPoolRequest.Common.ResourceId);
        }

        [TestMethod]
        public async Task CreateBatchPoolAsync_ValidUserIdentityResourceIdProvided_UserIdentityNameIsMapped()
        {
            var identities = new Dictionary<ResourceIdentifier, UserAssignedIdentity>();

            var identityName = @"bar-identity";
            var identityResourceId = $@"/subscriptions/aaaaa450-5f22-4b20-9326-b5852bb89d90/resourcegroups/foo/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{identityName}";


            identities.Add(new(identityResourceId), new UserAssignedIdentity());

            var poolInfo = new BatchAccountPoolData()
            {
                DeploymentConfiguration = new BatchDeploymentConfiguration()
                {
                    VmConfiguration = new BatchVmConfiguration(new BatchImageReference(), "batchNodeAgent")
                },
                Identity = new(ManagedServiceIdentityType.UserAssigned)
            };
            poolInfo.Identity.UserAssignedIdentities.AddRange(identities);
            poolInfo.Metadata.Add(new(string.Empty, terraApiStubData.PoolId));

            await terraBatchPoolManager.CreateBatchPoolAsync(poolInfo, false, System.Threading.CancellationToken.None);

            Assert.AreEqual(identityName, capturedApiCreateBatchPoolRequest.AzureBatchPool.UserAssignedIdentities.SingleOrDefault()!.Name);
        }

        [DataTestMethod]
        [DataRow("/subscription/foo/bar-identity")]
        [DataRow("bar-identity")]
        //[DataRow("")]
        public async Task CreateBatchPoolAsync_InvalidUserIdentityResourceIdProvided_ReturnsValueProvided(string identityName)
        {
            var identities = new Dictionary<ResourceIdentifier, UserAssignedIdentity>();

            identities.Add(new(identityName), new UserAssignedIdentity());

            var poolInfo = new BatchAccountPoolData()
            {
                DeploymentConfiguration = new BatchDeploymentConfiguration()
                {
                    VmConfiguration = new BatchVmConfiguration(new BatchImageReference(), "batchNodeAgent")
                },
                Identity = new(ManagedServiceIdentityType.UserAssigned)
            };
            poolInfo.Identity.UserAssignedIdentities.AddRange(identities);
            poolInfo.Metadata.Add(new(string.Empty, terraApiStubData.PoolId));

            await terraBatchPoolManager.CreateBatchPoolAsync(poolInfo, false, System.Threading.CancellationToken.None);

            Assert.AreEqual(identityName, capturedApiCreateBatchPoolRequest.AzureBatchPool.UserAssignedIdentities.SingleOrDefault()!.Name);
        }
        [TestMethod]
        public async Task CreateBatchPoolAsync_NoUserIdentityResourceIdProvided_NoIdentitiesMapped()
        {
            var poolInfo = new BatchAccountPoolData()
            {
                DeploymentConfiguration = new BatchDeploymentConfiguration()
                {
                    VmConfiguration = new BatchVmConfiguration(new BatchImageReference(), "batchNodeAgent")
                },
            };
            poolInfo.Metadata.Add(new(string.Empty, terraApiStubData.PoolId));

            await terraBatchPoolManager.CreateBatchPoolAsync(poolInfo, false, System.Threading.CancellationToken.None);

            Assert.IsFalse(capturedApiCreateBatchPoolRequest.AzureBatchPool.UserAssignedIdentities.Any());
        }

        [TestMethod]
        public async Task CreateBatchPoolAsync_UserIdentityInStartTaskMapsCorrectly()
        {

            var poolInfo = new BatchAccountPoolData()
            {

                StartTask = new()
                {
                    UserIdentity = new() { UserName = "user", AutoUser = new() { Scope = BatchAutoUserScope.Pool, ElevationLevel = BatchUserAccountElevationLevel.Admin } }
                }
            };
            poolInfo.Metadata.Add(new(string.Empty, terraApiStubData.PoolId));

            await terraBatchPoolManager.CreateBatchPoolAsync(poolInfo, false, System.Threading.CancellationToken.None);

            var captureUserIdentity = capturedApiCreateBatchPoolRequest.AzureBatchPool.StartTask.UserIdentity;

            Assert.AreEqual(poolInfo.StartTask.UserIdentity.UserName, captureUserIdentity.UserName);
            Assert.AreEqual(poolInfo.StartTask.UserIdentity.AutoUser.Scope.ToString(), captureUserIdentity.AutoUser.Scope.ToString());
            Assert.AreEqual(poolInfo.StartTask.UserIdentity.AutoUser.ElevationLevel.ToString(), captureUserIdentity.AutoUser.ElevationLevel.ToString());

        }
    }
}
