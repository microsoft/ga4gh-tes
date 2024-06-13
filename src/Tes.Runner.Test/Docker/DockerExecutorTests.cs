// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text;
using Docker.DotNet;
using Docker.DotNet.Models;
using Moq;
using Tes.Runner.Authentication;
using Tes.Runner.Docker;
using Tes.Runner.Exceptions;
using Tes.Runner.Logs;
using Tes.Runner.Models;

namespace Tes.Runner.Test.Docker
{
    [TestClass, TestCategory("Unit")]
    public class DockerExecutorTests
    {
        private IDockerClient dockerClient = null!;
        private Host.IRunnerHost runnerHost = null!;
        private Mock<IImageOperations> dockerImageMock = null!;
        private Mock<IVolumeOperations> dockerVolumeMock = null!;
        private IStreamLogReader streamLogReader = null!;
        private ContainerRegistryAuthorizationManager containerRegistryAuthorizationManager = null!;

        [TestInitialize]
        public void SetUp()
        {
            streamLogReader = new Mock<IStreamLogReader>().Object;
            dockerImageMock = new();
            dockerVolumeMock = new();
            runnerHost = new TestRunnerHost();
            Mock<IDockerClient> dockerClientMock = new();
            dockerClientMock.Setup(d => d.Images).Returns(dockerImageMock.Object);
            dockerClientMock.Setup(d => d.Volumes).Returns(dockerVolumeMock.Object);
            dockerClient = dockerClientMock.Object;
            var credentialsManager = new Mock<CredentialsManager>();
            credentialsManager.Setup(m => m.GetTokenCredential(It.IsAny<RuntimeOptions>(), It.IsAny<string>()))
                .Throws(new IdentityUnavailableException());
            containerRegistryAuthorizationManager = new(credentialsManager.Object);
        }

        [DataTestMethod]
        [DataRow(System.Net.HttpStatusCode.Forbidden, "")]
        [DataRow(System.Net.HttpStatusCode.Unauthorized, "")]
        [DataRow(System.Net.HttpStatusCode.InternalServerError, "{\"message\":\"Head \\\"https://msftsc022830.azurecr.io/v2/broadinstitute/gatk/manifests/4.5.0.0-squash\\\": unauthorized: authentication required, visit https://aka.ms/acr/authorization for more information.\"}")]
        public async Task RunOnContainerAsync_DockerClientReturnsAuthNeeded_CallsContainerRegistryAuthorizationManager(System.Net.HttpStatusCode statusCode, string responseBody)
        {
            var exception = new DockerApiException(statusCode, responseBody);
            dockerImageMock.Setup(d => d.CreateImageAsync(It.IsAny<ImagesCreateParameters>(), It.IsAny<AuthConfig>(), It.IsAny<IProgress<JSONMessage>>(), It.IsAny<CancellationToken>()))
                .Throws(exception);

            DockerExecutor executor = new(dockerClient, streamLogReader, containerRegistryAuthorizationManager, runnerHost, false);
            Models.RuntimeOptions runtimeOptions = new();
            try
            {
                var result = await executor.RunOnContainerAsync(new("msftsc022830.azurecr.io/broadinstitute/gatk", "4.5.0.0-squash", [""], default, default, runtimeOptions));
            }
            catch (IdentityUnavailableException) { } // Success
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }

            Assert.AreEqual(2, dockerImageMock.Invocations.Count);
        }

        [DataTestMethod]
        [DataRow(System.Net.HttpStatusCode.BadRequest, "", false)]
        [DataRow(System.Net.HttpStatusCode.InternalServerError, "{\"message\":\"Something went wrong: badrequest: something else happended.\"}", true)]
        public async Task RunOnContainerAsync_DockerClientReturnsOtherError_DoesNotCallContainerRegistryAuthorizationManager(System.Net.HttpStatusCode statusCode, string responseBody, bool retries)
        {
            DockerExecutor.dockerPullRetryPolicyOptions.ExponentialBackOffExponent = 1;
            var exception = new DockerApiException(statusCode, responseBody);
            dockerImageMock.Setup(d => d.CreateImageAsync(It.IsAny<ImagesCreateParameters>(), It.IsAny<AuthConfig>(), It.IsAny<IProgress<JSONMessage>>(), It.IsAny<CancellationToken>()))
                .Throws(exception);

            DockerExecutor executor = new(dockerClient, streamLogReader, containerRegistryAuthorizationManager, runnerHost, false);
            Models.RuntimeOptions runtimeOptions = new();
            try
            {
                var result = await executor.RunOnContainerAsync(new("msftsc022830.azurecr.io/broadinstitute/gatk", "4.5.0.0-squash", [""], default, default, runtimeOptions));
                Assert.Fail();
            }
            catch (IdentityUnavailableException)
            {
                Assert.Fail();
            }
            catch (Exception ex)
            {
                Assert.AreSame(exception, ex);
            }

            Assert.AreEqual(2 + (retries ? DockerExecutor.dockerPullRetryPolicyOptions.MaxRetryCount : 0), dockerImageMock.Invocations.Count);
        }

        //[DataTestMethod]
        //[DataRow(System.Net.HttpStatusCode.BadRequest, "")]
        //[DataRow(System.Net.HttpStatusCode.InternalServerError, "{\"message\":\"Something went wrong: badrequest: something else happended.\"}")]
        //public async Task RunOnContainerAsync_DockerClientReturnsSuccess_DoesNotCallContainerRegistryAuthorizationManager(System.Net.HttpStatusCode statusCode, string responseBody)
        //{
        //    dockerImageMock.Setup(d => d.CreateImageAsync(It.IsAny<ImagesCreateParameters>(), It.IsAny<AuthConfig>(), It.IsAny<IProgress<JSONMessage>>(), It.IsAny<CancellationToken>()))
        //        .Returns(Task.CompletedTask);

        //    DockerExecutor executor = new(dockerClient, streamLogReader, containerRegistryAuthorizationManager);
        //    Models.RuntimeOptions runtimeOptions = new();
        //    try
        //    {
        //        var result = await executor.RunOnContainerAsync(new("msftsc022830.azurecr.io/broadinstitute/gatk", "4.5.0.0-squash", [""], default, default, runtimeOptions));
        //    }
        //    catch (IdentityUnavailableException)
        //    {
        //        Assert.Fail();
        //    }
        //    catch (Exception ex)
        //    {
        //        Assert.Fail(ex.Message);
        //    }

        //    Assert.AreEqual(1, dockerImageMock.Invocations.Count);
        //}

        [DataTestMethod]
        [DataRow("")]
        [DataRow("https://msftsc022830.azurecr.io/v2/broadinstitute/gatk/manifests/4.5.0.0-squash")]
        public async Task CleanupVolumesAndImages_CallsDockerClient(string image)
        {
            if (!string.IsNullOrEmpty(image))
            {
                runnerHost.WriteSharedFile(DockerExecutor.LastImageFile, Encoding.UTF8.GetBytes(image));
            }

            dockerImageMock.Setup(d => d.DeleteImageAsync(It.IsAny<string>(), It.IsAny<ImageDeleteParameters>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult<IList<IDictionary<string, string>>>([]));
            dockerVolumeMock.Setup(d => d.PruneAsync(It.IsAny<VolumesPruneParameters>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(new VolumesPruneResponse()));

            DockerExecutor executor = new(dockerClient, streamLogReader, containerRegistryAuthorizationManager, runnerHost, false);
            await executor.NodeCleanupAsync(new(image, default, default, default, default, new()));

            Assert.AreEqual(1, dockerVolumeMock.Invocations.Count);
            Assert.AreEqual(string.IsNullOrEmpty(image) ? 0 : 1, dockerImageMock.Invocations.Count);
        }

        private class TestRunnerHost : Host.RunnerHost
        {
            private readonly FileInfo file;

            public TestRunnerHost()
            {
                file = new(Path.GetTempFileName());
                file.Delete();
            }

            public override FileInfo GetSharedFile(string name)
            {
                return file;
            }

            public override Task NodeCleanupAsync()
            {
                throw new NotSupportedException();
            }
        }
    }
}
