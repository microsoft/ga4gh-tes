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
            var helmValues = await KubernetesManager.GetHelmValuesAsync(@"./cromwell-on-azure/helm/values-template.yaml");
            Assert.IsNotNull(helmValues.TesDatabase);
        }
    }
}
