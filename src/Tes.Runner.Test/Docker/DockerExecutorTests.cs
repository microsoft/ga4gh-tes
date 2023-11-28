// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using Docker.DotNet;
using Docker.DotNet.Models;
using Moq;
using Tes.Runner.Authentication;
using Tes.Runner.Docker;
using Tes.Runner.Logs;
using Tes.Runner.Models;

namespace Tes.Runner.Test.Docker;

[TestClass]
public class DockerExecutorTests
{
    private DockerExecutor dockerExecutor = null!;
    private Mock<IDockerClient> mockDockerClient = null!;
    private Mock<IStreamLogReader> mockStreamLogReader = null!;
    private Mock<CredentialsManager> mockCredentialsManager = null!;
    private Mock<TokenCredential> mockCredentials = null!;
    private Mock<IContainerOperations> mockContainerOperations = null!;
    private Mock<IImageOperations> mockImageOperations = null!;
    private readonly AccessToken accessToken = new AccessToken("abcdef123", DateTimeOffset.UtcNow);
    private AuthConfig? captureAuthConfig;

    [TestInitialize]
    public void SetUp()
    {

        mockImageOperations = new Mock<IImageOperations>();
        mockImageOperations.Setup(i => i.CreateImageAsync(It.IsAny<ImagesCreateParameters>(),
                                      It.IsAny<AuthConfig>(), It.IsAny<IProgress<JSONMessage>>(), It.IsAny<CancellationToken>()))
            .Callback<ImagesCreateParameters, AuthConfig, IProgress<JSONMessage>, CancellationToken>((_, a, _, _) =>
                               captureAuthConfig = a);

        mockContainerOperations = new Mock<IContainerOperations>();
        mockContainerOperations.Setup(c => c.CreateContainerAsync(It.IsAny<CreateContainerParameters>(),
                           It.IsAny<CancellationToken>())).ReturnsAsync(new CreateContainerResponse()
                           { ID = "test" });
        mockContainerOperations.Setup(c => c.StartContainerAsync(It.IsAny<string>(), It.IsAny<ContainerStartParameters>(),
                       It.IsAny<CancellationToken>())).ReturnsAsync(true);
        mockContainerOperations.Setup(c => c.WaitContainerAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ContainerWaitResponse() { StatusCode = 0 });

        mockDockerClient = new Mock<IDockerClient>();
        mockDockerClient.Setup(c => c.Images).Returns(mockImageOperations.Object);
        mockDockerClient.Setup(c => c.Containers).Returns(mockContainerOperations.Object);

        mockStreamLogReader = new Mock<IStreamLogReader>();
        mockCredentialsManager = new Mock<CredentialsManager>();
        mockCredentials = new Mock<TokenCredential>();
        mockCredentialsManager.Setup(c => c.GetTokenCredential(It.IsAny<RuntimeOptions>()))
            .Returns(mockCredentials.Object);
        mockCredentials.Setup(c => c.GetTokenAsync(It.IsAny<TokenRequestContext>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(accessToken);

        dockerExecutor = new DockerExecutor(mockDockerClient.Object, mockStreamLogReader.Object, mockCredentialsManager.Object);
    }

    [TestMethod]
    public async Task RunOnContainerAsync_AzureContainerRegistryImage_AuthInfoIsProvided()
    {
        var execOptions =
            new ExecutionOptions(ImageName: "test.azurecr.io/test", Tag: null, CommandsToExecute: new List<string>() { "echo 'hello'" }, VolumeBindings: null, WorkingDir: null, new RuntimeOptions());

        await dockerExecutor.RunOnContainerAsync(execOptions);

        Assert.IsNotNull(captureAuthConfig);
        Assert.AreEqual("test.azurecr.io", captureAuthConfig!.ServerAddress);
        Assert.AreEqual(DockerExecutor.ManagedIdentityUserName, captureAuthConfig.Username);
        Assert.AreEqual(accessToken.Token, captureAuthConfig.Password);
    }

    [TestMethod]
    public async Task RunOnContainerAsync_NotAzureContainerRegistryImage_AuthInfoNull()
    {
        var execOptions =
            new ExecutionOptions(ImageName: "test.docker.io/test", Tag: null, CommandsToExecute: new List<string>() { "echo 'hello'" }, VolumeBindings: null, WorkingDir: null, new RuntimeOptions());

        await dockerExecutor.RunOnContainerAsync(execOptions);

        Assert.IsNull(captureAuthConfig);
    }
}
