// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Containers.ContainerRegistry;
using Azure.Core;
using Moq;
using Tes.Runner.Authentication;
using Tes.Runner.Docker;
using Tes.Runner.Models;

namespace Tes.Runner.Test.Docker;

[TestClass]
public class ContainerRegistryAuthorizationManagerTests
{
    //private ContainerRegistryAuthorizationManager containerRegistryAuthorizationManager = null!;

    //private Mock<CredentialsManager> mockCredentialsManager = null!;
    //private Mock<TokenCredential> mockCredentials = null!;

    [TestInitialize]
    public void SetUp()
    {
        //var mockResponseManifest = new Mock<Azure.Response<GetManifestResult>>();
        //mockResponseManifest.Setup(c => c.Value)
        //    .Returns((GetManifestResult)null!);

        //var mockContainerRegistryContentClient = new Mock<ContainerRegistryContentClient>();
        //mockContainerRegistryContentClient.Setup(c => c.GetManifestAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
        //    .ReturnsAsync(mockResponseManifest.Object);

        //mockCredentialsManager = new Mock<CredentialsManager>();
        //mockCredentials = new Mock<TokenCredential>();
        //mockCredentialsManager.Setup(c => c.GetTokenCredential(It.IsAny<RuntimeOptions>(), It.IsAny<string>()))
        //    .Returns(mockCredentials.Object);


        //containerRegistryAuthorizationManager = new(mockCredentialsManager.Object, Microsoft.Extensions.Logging.Abstractions.NullLogger<ContainerRegistryAuthorizationManager>.Instance);
    }

    [DataTestMethod]
    [DataRow("test.azurecr.io/azure-cli", true, new[] { "test.azurecr.io", "azure-cli" })]
    [DataRow("mcr.microsoft.com/azure-cli", false, new[] { "mcr.microsoft.com", "azure-cli" })]
    [DataRow("azure-cli", false, new[] { "azure-cli" })]
    public void TryParseAzureContainerRegisterParts_ImageNameProvided_ReturnsExpectedResult(string imageName, bool expectedResult, string[] expectedParts)
    {
        var result = ContainerRegistryAuthorizationManager.TryParseAzureContainerRegisteryParts(imageName, out var imageParts);

        Assert.AreEqual(expectedResult, result);
        CollectionAssert.AreEqual(expectedParts, imageParts);
    }
}
