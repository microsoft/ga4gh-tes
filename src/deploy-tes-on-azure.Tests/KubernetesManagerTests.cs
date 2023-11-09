// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TesDeployer.Tests
{
    [TestClass]
    public class KubernetesManagerTests
    {
        [TestMethod]
        public async Task ValuesTemplateSuccessfullyDeserializesTesdatabaseToYaml()
        {
            _ = new KubernetesManager(null, null, System.Threading.CancellationToken.None);
            var helmValues = await KubernetesManager.GetHelmValuesAsync(@"./cromwell-on-azure/helm/values-template.yaml");
            Assert.IsNotNull(helmValues.TesDatabase);
        }
    }
}
