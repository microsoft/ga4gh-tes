// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using CommonUtilities.AzureCloud;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using TesApi.Web;
using TesApi.Web.Management;
using TesApi.Web.Management.Batch;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Options;
using TesApi.Web.Storage;

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Tests
{
    [TestClass, TestCategory("Unit")]
    public class StartupTests
    {
        private Startup startup;
        private Mock<IConfiguration> configurationMock;
        private Mock<IWebHostEnvironment> hostingEnvMock;
        private ServiceCollection services;
        private TerraApiStubData terraApiStubData;

        [TestInitialize]
        public void SetUp()
        {
            terraApiStubData = new TerraApiStubData();


            services = new ServiceCollection();

            services.Configure<BatchAccountOptions>(options =>
            {
                options.AccountName = terraApiStubData.BatchAccountName;
                options.AppKey = "APP_KEY";
                options.BaseUrl = "https://batch.foo";
                options.ResourceGroup = TerraApiStubData.ResourceGroup;
                options.SubscriptionId = terraApiStubData.SubscriptionId.ToString();
            });

            services.Configure<BatchSchedulingOptions>(options =>
            {
                options.Prefix = "TES-prefix";
            });

            configurationMock = new Mock<IConfiguration>();
            configurationMock.Setup(c => c.GetSection(It.IsAny<string>())).Returns(new Mock<IConfigurationSection>().Object);
            hostingEnvMock = new Mock<IWebHostEnvironment>();
            hostingEnvMock.Setup(e => e.EnvironmentName).Returns("Development");

#pragma warning disable CS0618 // app insights has this dependency
            var hostEnv = new Mock<IHostingEnvironment>();
            hostEnv.Setup(e => e.EnvironmentName).Returns("Development");
            services.AddSingleton(hostEnv.Object);
#pragma warning restore CS0618

            Startup.AzureCloudConfig = AzureCloudConfig.CreateAsync().Result;
            startup = new Startup(configurationMock.Object, NullLogger<Startup>.Instance, hostingEnvMock.Object);
        }

        private void ConfigureTerraOptions()
        {
            services.Configure<TerraOptions>(options =>
            {
                options.LandingZoneId = terraApiStubData.LandingZoneId.ToString();
                options.LandingZoneApiHost = TerraApiStubData.LandingZoneApiHost;
                options.WorkspaceId = terraApiStubData.WorkspaceId.ToString();
                options.WsmApiHost = TerraApiStubData.WsmApiHost;
                options.WorkspaceStorageAccountName = TerraApiStubData.WorkspaceAccountName;
                options.WorkspaceStorageContainerName = TerraApiStubData.WorkspaceStorageContainerName;
                options.WorkspaceStorageContainerResourceId = terraApiStubData.ContainerResourceId.ToString();
                options.SamApiHost = TerraApiStubData.SamApiHost;
                options.SamResourceIdForAcrPull = terraApiStubData.AcrPullIdentitySamResourceId.ToString();
            });
        }

        [TestMethod]
        public void ConfigureServices_TerraOptionsAreConfigured_TerraStorageProviderIsResolved()
        {
            ConfigureTerraOptions();

            startup.ConfigureServices(services);

            var serviceProvider = services.BuildServiceProvider();

            var terraStorageProvider = serviceProvider.GetService<IStorageAccessProvider>();

            Assert.IsNotNull(terraStorageProvider);
            Assert.IsInstanceOfType(terraStorageProvider, typeof(TerraStorageAccessProvider));
        }

        [TestMethod]
        public void ConfigureServices_TerraOptionsAreConfigured_TerraActionIdentityProviderIsResolved()
        {
            ConfigureTerraOptions();

            startup.ConfigureServices(services);

            var serviceProvider = services.BuildServiceProvider();

            var terraActionIdentityProvider = serviceProvider.GetService<IActionIdentityProvider>();

            Assert.IsNotNull(terraActionIdentityProvider);
            Assert.IsInstanceOfType(terraActionIdentityProvider, typeof(TerraActionIdentityProvider));
        }

        [TestMethod]
        public void ConfigureServices_TerraOptionsAreNotConfigured_DefaultActionIdentityProviderIsResolved()
        {
            startup.ConfigureServices(services);

            var serviceProvider = services.BuildServiceProvider();

            var actionIdentityProvider = serviceProvider.GetService<IActionIdentityProvider>();

            Assert.IsNotNull(actionIdentityProvider);
            Assert.IsInstanceOfType(actionIdentityProvider, typeof(DefaultActionIdentityProvider));
        }

        [TestMethod]
        public void ConfigureServices_TerraOptionsAreConfigured_TerraBatchPoolManagerIsResolved()
        {
            ConfigureTerraOptions();

            startup.ConfigureServices(services);

            var serviceProvider = services.BuildServiceProvider();

            var poolManager = serviceProvider.GetService<IBatchPoolManager>();

            Assert.IsNotNull(poolManager);
            Assert.IsInstanceOfType(poolManager, typeof(TerraBatchPoolManager));
        }

        [TestMethod]
        public void ConfigureServices_TerraOptionsAreConfigured_TerraQuotaVerifierIsResolved()
        {
            ConfigureTerraOptions();

            startup.ConfigureServices(services);

            var serviceProvider = services.BuildServiceProvider();

            var quotaProvider = serviceProvider.GetService<IBatchQuotaProvider>();

            Assert.IsNotNull(quotaProvider);
            Assert.IsInstanceOfType(quotaProvider, typeof(TerraQuotaProvider));
        }
    }
}
