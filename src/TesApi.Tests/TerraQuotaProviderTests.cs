// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.ApiClients;
using TesApi.Web.Management;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Management.Models.Terra;

namespace TesApi.Tests
{
    [TestClass]
    public class TerraQuotaProviderTests
    {
        private Mock<TerraLandingZoneApiClient> terraLandingZoneApiClientMock;
        private TerraQuotaProvider terraQuotaProvider;
        private TerraApiStubData terraApiStubData;
        private LandingZoneResourcesApiResponse resourcesApiResponse;
        private QuotaApiResponse quotaApiResponse;

        [TestInitialize]
        public void SetUp()
        {
            terraApiStubData = new();
            terraLandingZoneApiClientMock = new();
            resourcesApiResponse = terraApiStubData.GetResourceApiResponse();
            quotaApiResponse = terraApiStubData.GetResourceQuotaApiResponse();

            var optionsMock = new Mock<IOptions<TerraOptions>>();
            optionsMock.Setup(o => o.Value).Returns(new TerraOptions() { LandingZoneApiHost = TerraApiStubData.LandingZoneApiHost, LandingZoneId = terraApiStubData.LandingZoneId.ToString() });

            terraLandingZoneApiClientMock
                .Setup(t => t.GetLandingZoneResourcesAsync(It.Is<Guid>(g => g.Equals(terraApiStubData.LandingZoneId)), It.IsAny<System.Threading.CancellationToken>(), It.Is<bool>(c => c == true)))
                .ReturnsAsync(resourcesApiResponse);
            terraLandingZoneApiClientMock
                .Setup(t => t.GetResourceQuotaAsync(It.Is<Guid>(g => g.Equals(terraApiStubData.LandingZoneId)), It.Is<string>(b => string.Equals(b, terraApiStubData.BatchAccountId, StringComparison.OrdinalIgnoreCase)), It.Is<bool>(c => c == true), It.IsAny<System.Threading.CancellationToken>()))
                .ReturnsAsync(quotaApiResponse);
            terraQuotaProvider = new(terraLandingZoneApiClientMock.Object, optionsMock.Object);
        }

        [TestMethod]
        public async Task GetVmCoreQuotaAsync_LowPriorityFalseReturnsQuotaInformationForDedicatedVms()
        {
            var quota = await terraQuotaProvider.GetVmCoreQuotaAsync(lowPriority: false, cancellationToken: System.Threading.CancellationToken.None);

            Assert.IsFalse(quota.IsLowPriority);
            Assert.AreEqual(quotaApiResponse.QuotaValues.DedicatedCoreQuota, quota.NumberOfCores);
            Assert.AreEqual(quotaApiResponse.QuotaValues.DedicatedCoreQuotaPerVmFamilyEnforced, quota.IsDedicatedAndPerVmFamilyCoreQuotaEnforced);
            Assert.AreEqual(quotaApiResponse.QuotaValues.DedicatedCoreQuotaPerVmFamily.Count, quota.DedicatedCoreQuotas.Count);
        }

        [TestMethod]
        public async Task GetVmCoreQuotaAsync_LowPriorityTrueReturnsQuotaInformationForLowPriorityOnly()
        {
            var quota = await terraQuotaProvider.GetVmCoreQuotaAsync(lowPriority: true, cancellationToken: System.Threading.CancellationToken.None);

            Assert.IsTrue(quota.IsLowPriority);
            Assert.AreEqual(quotaApiResponse.QuotaValues.LowPriorityCoreQuota, quota.NumberOfCores);
            Assert.IsFalse(quota.IsDedicatedAndPerVmFamilyCoreQuotaEnforced);
            Assert.IsNull(quota.DedicatedCoreQuotas);
        }

        [TestMethod]
        public async Task GetQuotaForRequirementAsync_VmFamilyMatchesAndDedicatedQuotaIsEnforced()
        {
            var vmFamily = "standardDSv2Family";
            var vmFamilyQuota = 350;
            var quota = await terraQuotaProvider.GetQuotaForRequirementAsync(vmFamily, lowPriority: false, coresRequirement: 10, cancellationToken: System.Threading.CancellationToken.None);

            Assert.AreEqual(vmFamily, quota.VmFamily);
            Assert.AreEqual(vmFamilyQuota, quota.VmFamilyQuota);
            Assert.AreEqual(quotaApiResponse.QuotaValues.PoolQuota, quota.PoolQuota);
            Assert.AreEqual(quotaApiResponse.QuotaValues.DedicatedCoreQuota, quota.TotalCoreQuota);
            Assert.AreEqual(quotaApiResponse.QuotaValues.ActiveJobAndJobScheduleQuota, quota.ActiveJobAndJobScheduleQuota);
        }

        [TestMethod]
        public async Task GetQuotaForRequirementAsync_DedicatedQuotaIsNotEnforcedReturnsCoreRequirements()
        {
            var vmFamily = "standardDSv2Family";
            quotaApiResponse.QuotaValues.DedicatedCoreQuotaPerVmFamilyEnforced = false;
            var quota = await terraQuotaProvider.GetQuotaForRequirementAsync(vmFamily, lowPriority: false, coresRequirement: 10, cancellationToken: System.Threading.CancellationToken.None);

            Assert.AreEqual(vmFamily, quota.VmFamily);
            Assert.AreEqual(10, quota.VmFamilyQuota);
            Assert.AreEqual(quotaApiResponse.QuotaValues.DedicatedCoreQuota, quota.TotalCoreQuota);
        }
    }
}
