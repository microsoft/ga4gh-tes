// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.ApiClients;
using Tes.ApiClients.Models.Terra;
using TesApi.Web;
using TesApi.Web.Management.Configuration;

namespace TesApi.Tests
{
    [TestClass]
    public class TerraActionIdentityProviderTests
    {
        private Mock<TerraSamApiClient> terraSamApiClientMock;
        private TerraApiStubData terraApiStubData;

        private Guid notFoundGuid = Guid.NewGuid();
        private Guid errorGuid = Guid.NewGuid();

        [TestInitialize]
        public void SetUp()
        {
            terraApiStubData = new();
            terraSamApiClientMock = new();

            terraSamApiClientMock
                .Setup(t => t.GetActionManagedIdentityForACRPullAsync(It.Is<Guid>(g => g.Equals(terraApiStubData.AcrPullIdentitySamResourceId)), It.IsAny<TimeSpan>(), It.IsAny<System.Threading.CancellationToken>()))
                .ReturnsAsync(terraApiStubData.GetSamActionManagedIdentityApiResponse());
            terraSamApiClientMock
                .Setup(t => t.GetActionManagedIdentityForACRPullAsync(It.Is<Guid>(g => g.Equals(notFoundGuid)), It.IsAny<TimeSpan>(), It.IsAny<System.Threading.CancellationToken>()))
                .ReturnsAsync((SamActionManagedIdentityApiResponse)null);
            terraSamApiClientMock
                .Setup(t => t.GetActionManagedIdentityForACRPullAsync(It.Is<Guid>(g => g.Equals(errorGuid)), It.IsAny<TimeSpan>(), It.IsAny<System.Threading.CancellationToken>()))
                .Throws(new HttpRequestException("Timeout!!", null, HttpStatusCode.GatewayTimeout));
        }

        private TerraActionIdentityProvider ActionIdentityProviderWithSamResourceId(Guid resourceId)
        {
            var optionsMock = new Mock<IOptions<TerraOptions>>();
            optionsMock.Setup(o => o.Value).Returns(new TerraOptions() { SamApiHost = TerraApiStubData.SamApiHost, SamResourceIdForAcrPull = resourceId.ToString() });
            return new TerraActionIdentityProvider(terraSamApiClientMock.Object, optionsMock.Object, NullLogger<TerraActionIdentityProvider>.Instance);
        }

        [TestMethod]
        public async Task GetAcrPullActionIdentity_Success()
        {
            var actionIdentityProvider = ActionIdentityProviderWithSamResourceId(terraApiStubData.AcrPullIdentitySamResourceId);
            var actionIdentity = await actionIdentityProvider.GetAcrPullActionIdentity(cancellationToken: System.Threading.CancellationToken.None);

            Assert.AreEqual(actionIdentity, terraApiStubData.ManagedIdentityObjectId);
        }

        [TestMethod]
        public async Task GetAcrPullActionIdentity_NotFound()
        {
            var actionIdentityProvider = ActionIdentityProviderWithSamResourceId(notFoundGuid);
            var actionIdentity = await actionIdentityProvider.GetAcrPullActionIdentity(cancellationToken: System.Threading.CancellationToken.None);

            Assert.IsNull(actionIdentity);
        }

        [TestMethod]
        public async Task GetAcrPullActionIdentity_Error()
        {
            var actionIdentityProvider = ActionIdentityProviderWithSamResourceId(errorGuid);
            var actionIdentity = await actionIdentityProvider.GetAcrPullActionIdentity(cancellationToken: System.Threading.CancellationToken.None);

            Assert.IsNull(actionIdentity);
        }
    }
}
