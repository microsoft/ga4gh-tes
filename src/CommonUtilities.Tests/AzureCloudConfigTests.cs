// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;
using Moq;

namespace CommonUtilities.AzureCloud.Tests
{
    [TestClass]
    public class AzureCloudConfigTests
    {
        [TestMethod]
        public async Task AzureCloudConfigCanBeRetrievedAndDeserialized()
        {
            var logger = new Mock<ILogger<AzureCloudConfig>>();
            var config = await AzureCloudConfig.CreateAsync(logger.Object);
            Assert.AreEqual(config!.Authentication!.LoginEndpointUrl, "https://login.microsoftonline.com", true);

            config = await AzureCloudConfig.CreateAsync(logger.Object, "https://management.usgovcloudapi.net/metadata/endpoints?api-version=2023-11-01");
            Assert.AreEqual(config!.Authentication!.LoginEndpointUrl, "https://login.microsoftonline.us", true);

            config = await AzureCloudConfig.CreateAsync(logger.Object, "https://management.chinacloudapi.cn/metadata/endpoints?api-version=2023-11-01");
            Assert.AreEqual(config!.Authentication!.LoginEndpointUrl, "https://login.chinacloudapi.cn", true);
        }
    }
}
