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
    private ContainerRegistryAuthorizationManager containerRegistryAuthorizationManager = null!;

    private Mock<CredentialsManager> mockCredentialsManager = null!;
    private Mock<TokenCredential> mockCredentials = null!;

    [TestInitialize]
    public void SetUp()
    {

        var mockResponseManifest = new Mock<Azure.Response<GetManifestResult>>();
        mockResponseManifest.Setup(c => c.Value)
            .Returns((GetManifestResult)null!);

        var mockContainerRegistryContentClient = new Mock<ContainerRegistryContentClient>();
        mockContainerRegistryContentClient.Setup(c => c.GetManifestAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(mockResponseManifest.Object);

        mockCredentialsManager = new Mock<CredentialsManager>();
        mockCredentials = new Mock<TokenCredential>();
        mockCredentialsManager.Setup(c => c.GetTokenCredential(It.IsAny<RuntimeOptions>()))
            .Returns(mockCredentials.Object);


        containerRegistryAuthorizationManager = new ContainerRegistryAuthorizationManager(mockCredentialsManager.Object);
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

    [DataTestMethod]
    [DataRow("jsotoimputation.azurecr.io/broad-gotc-prod/imputation-bcf-vcf", "1.0.5-1.10.2-0.1.16-1649948623")]
    public async Task TryGetAuthConfigForAzureContainerRegistryAsync_PublicImage(string imageName, string imageTag)
    {
        var authManager = new ContainerRegistryAuthorizationManager(new CredentialsManager());
        var result = await authManager.TryGetAuthConfigForAzureContainerRegistryAsync(imageName, imageTag, new RuntimeOptions());
        Assert.IsNull(result);
    }
}
