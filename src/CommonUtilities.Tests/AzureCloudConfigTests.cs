// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace CommonUtilities.AzureCloud.Tests
{
    [TestClass]
    public class AzureCloudConfigTests
    {
        [TestMethod]
        [Ignore]
        public async Task AzureCloudConfigCanBeRetrievedAndDeserialized()
        {
            var config = await AzureCloudConfig.CreateAsync();
            Assert.AreEqual(config!.Authentication!.LoginEndpointUrl, "https://login.microsoftonline.com", true);
            Assert.AreEqual(config!.DefaultTokenScope, "https://management.azure.com//.default", true);

            config = await AzureCloudConfig.CreateAsync("AzureUSGovernment");
            Assert.AreEqual(config!.Authentication!.LoginEndpointUrl, "https://login.microsoftonline.us", true);
            Assert.AreEqual(config!.DefaultTokenScope, "https://management.usgovcloudapi.net/.default", true);

            config = await AzureCloudConfig.CreateAsync("AzureChinaCloud");
            Assert.AreEqual(config!.Authentication!.LoginEndpointUrl, "https://login.chinacloudapi.cn", true);
            Assert.AreEqual(config!.DefaultTokenScope, "https://management.chinacloudapi.cn/.default", true);
        }
    }
}
