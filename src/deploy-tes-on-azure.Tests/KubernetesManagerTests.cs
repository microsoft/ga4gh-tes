// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Tasks;
using CommonUtilities;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace TesDeployer.Tests
{
    [TestClass]
    public class KubernetesManagerTests
    {
        [TestMethod]
        public async Task ValuesTemplateSuccessfullyDeserializesTesdatabaseToYaml()
        {
            var manager = new KubernetesManager(new(), ExpensiveObjectTestUtility.AzureCloudConfig, (_, _, _) => throw new System.NotImplementedException(), System.Threading.CancellationToken.None);
            var helmValues = await manager.GetHelmValuesAsync(@"./cromwell-on-azure/helm/values-template.yaml");
            Assert.IsNotNull(helmValues.TesDatabase);
        }
    }
}
