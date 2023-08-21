// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Tes.ApiClients.Options;

namespace Tes.ApiClients.Tests.TerraIntegration
{
    [TestClass, TestCategory("TerraIntegration")]
    [Ignore]
    public class TerraWsmApiClientIntegrationTests
    {
        private TerraWsmApiClient wsmApiClient = null!;
        private TestTerraEnvInfo envInfo = null!;

        [TestInitialize]
        public void Setup()
        {
            envInfo = new TestTerraEnvInfo();


            var retryOptions = Microsoft.Extensions.Options.Options.Create(new RetryPolicyOptions());
            var memoryCache = new MemoryCache(new MemoryCacheOptions());

            wsmApiClient = new TerraWsmApiClient(envInfo.WsmApiHost, new TestEnvTokenCredential(),
                new CachingRetryHandler(memoryCache, retryOptions), TestLoggerFactory.Create<TerraWsmApiClient>());

        }

        [TestMethod]
        public async Task GetContainerResourcesAsync_CallsUsingTheWSIdFromContainerName_ReturnsContainerInformation()
        {
            var workspaceId = Guid.Parse(envInfo.WorkspaceContainerName.Replace("sc-", ""));

            var results = await wsmApiClient.GetContainerResourcesAsync(workspaceId, 0, 100, CancellationToken.None);

            Assert.IsNotNull(results);
            Assert.IsTrue(results.Resources.Any(i => i.ResourceAttributes.AzureStorageContainer.StorageContainerName.Equals(envInfo.WorkspaceContainerName, StringComparison.OrdinalIgnoreCase)));
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
