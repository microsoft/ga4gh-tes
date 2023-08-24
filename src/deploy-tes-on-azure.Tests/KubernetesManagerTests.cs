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
            var helmValues = await new KubernetesManager(null, null, System.Threading.CancellationToken.None)
                .GetHelmValuesAsync(@"./cromwell-on-azure/helm/values-template.yaml", System.Threading.CancellationToken.None);
            Assert.IsNotNull(helmValues.TesDatabase);
        }
    }
}
