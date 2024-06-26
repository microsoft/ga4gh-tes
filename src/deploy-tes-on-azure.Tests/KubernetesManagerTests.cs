// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Tasks;
using CommonUtilities;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TesDeployer.Tests
{
    [TestClass]
    public class KubernetesManagerTests
    {
        [TestMethod]
        public async Task ValuesTemplateSuccessfullyDeserializesTesdatabaseToYaml()
        {
            var azureConfig = ExpensiveObjectTestUtility.AzureCloudConfig;
            var manager = new KubernetesManager(null, null, azureConfig, System.Threading.CancellationToken.None);
            var helmValues = await manager.GetHelmValuesAsync(@"./cromwell-on-azure/helm/values-template.yaml");
            Assert.IsNotNull(helmValues.TesDatabase);
        }
    }
}
