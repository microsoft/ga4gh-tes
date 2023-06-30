// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TesApi.Web.Management;
using TesApi.Web.Management.Clients;
using TesApi.Web.Management.Configuration;

namespace TesApi.Tests.Integration
{
    [TestClass, TestCategory("TerraIntegration")]
    public class TerraWsmApiClientIntegrationTests
    {
        private TerraWsmApiClient wsmApiClient = null!;
        private TestTerraEnvInfo envInfo = null!;

        [TestInitialize]
        public void Setup()
        {
            envInfo = new TestTerraEnvInfo();

            var terraOptions = Options.Create(new TerraOptions()
            {
                WsmApiHost = envInfo.WsmApiHost
            });
            var retryOptions = Options.Create(new RetryPolicyOptions());
            var memoryCache = new MemoryCache(new MemoryCacheOptions());

            wsmApiClient = new TerraWsmApiClient(new TestEnvTokenCredential(), terraOptions,
                new CacheAndRetryHandler(memoryCache,retryOptions), TestLoggerFactory.Create<TerraWsmApiClient>());

        }

        [TestMethod]
        public async Task GetContainerResourcesAsync_CallsUsingTheWSIdFromContainerName_ReturnsContainerInformation()
        {
            var workspaceId = Guid.Parse(envInfo.WorkspaceContainerName.Replace("sc-", ""));

            var results = await wsmApiClient.GetContainerResourcesAsync(workspaceId, 0, 100, CancellationToken.None);

            Assert.IsNotNull(results);
            Assert.IsTrue(results.Resources.Any(i=>i.ResourceAttributes.AzureStorageContainer.StorageContainerName.Equals(envInfo.WorkspaceContainerName,StringComparison.OrdinalIgnoreCase)));
        }
    }

    public static class TestLoggerFactory
    {
        private static readonly ILoggerFactory SLogFactory = LoggerFactory.Create(builder =>
        {
            builder
                .AddSystemdConsole(options =>
                {
                    options.IncludeScopes = true;
                    options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff ";
                    options.UseUtcTimestamp = true;
                });
            
            builder.SetMinimumLevel(LogLevel.Trace);
        });


        public static ILogger<T> Create<T>()
        {
            return SLogFactory.CreateLogger<T>();
        }
    }
}
