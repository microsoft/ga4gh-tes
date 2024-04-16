// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Tes.Runner.Models;
using TesApi.Web.Runner;

namespace TesApi.Tests.Runner
{
    [TestClass, TestCategory("Unit")]
    public class NodeTaskBuilderTests
    {
        private NodeTaskBuilder nodeTaskBuilder;

        [TestInitialize]
        public void SetUp()
        {
            nodeTaskBuilder = new NodeTaskBuilder();
        }

        [TestMethod]
        public void WithId_ValidId_IdIsSet()
        {
            var id = "123";
            nodeTaskBuilder.WithId(id);
            Assert.AreEqual(id, nodeTaskBuilder.Build().Id);
        }

        [TestMethod]
        public void WithWorkflowId_ValidId_IdIsSet()
        {
            var workflowId = "123";
            nodeTaskBuilder.WithWorkflowId(workflowId);
            Assert.AreEqual(workflowId, nodeTaskBuilder.Build().WorkflowId);
        }

        [TestMethod]
        public void WithContainerWorkingDirectory_WorkingDirIsProvided_WorkingDirIsSet()
        {
            var workingDir = "/home";
            nodeTaskBuilder.WithContainerWorkingDirectory(workingDir);
            Assert.AreEqual(workingDir, nodeTaskBuilder.Build().ContainerWorkDir);
        }

        [TestMethod]
        public void WithInputUsingCombinedTransformationStrategy_WithTerraRuntimeSet_InputUsesTerraCombinedTransformationStrategy()
        {
            nodeTaskBuilder.WithTerraAsRuntimeEnvironment("https://wsm.foo", "https://lz.foo", sasAllowedIpRange: String.Empty);
            nodeTaskBuilder.WithInputUsingCombinedTransformationStrategy("/root/input", "http://foo.bar/input", "/root");

            var input = nodeTaskBuilder.Build().Inputs![0];
            Assert.AreEqual(TransformationStrategy.CombinedTerra, input.TransformationStrategy);
        }

        [TestMethod]
        public void
            WithInputUsingCombinedTransformationStrategy_WithTerraRuntimeNotSet_InputUseCombinedARMTransformationStrategy()
        {
            nodeTaskBuilder.WithInputUsingCombinedTransformationStrategy("/root/input", "http://foo.bar/input", "/root");
            var input = nodeTaskBuilder.Build().Inputs![0];
            Assert.AreEqual(TransformationStrategy.CombinedAzureResourceManager, input.TransformationStrategy);
        }

        [TestMethod]
        public void WithInputUsingCombineTransformationStrategy_ValidInput_AllPropertiesSet()
        {
            nodeTaskBuilder.WithInputUsingCombinedTransformationStrategy("/root/input", "http://foo.bar/input", "/root");
            var input = nodeTaskBuilder.Build().Inputs![0];
            Assert.AreEqual("/root/input", input.Path);
            Assert.AreEqual("http://foo.bar/input", input.SourceUrl);
            Assert.AreEqual("/root", input.MountParentDirectory);
        }

        [TestMethod]
        public void WhenInputContainsUriQuery_ValidInput_AllPropertiesSet()
        {
            nodeTaskBuilder.WithInputUsingCombinedTransformationStrategy("/root/input", "http://foo.bar/input?test", "/root");
            var input = nodeTaskBuilder.Build().Inputs![0];
            Assert.AreEqual("/root/input", input.Path);
            Assert.AreEqual("http://foo.bar/input?test", input.SourceUrl);
            Assert.AreEqual("/root", input.MountParentDirectory);
            Assert.AreEqual(TransformationStrategy.None, input.TransformationStrategy);
        }

        [TestMethod]
        public void WhenInputPathContainsUriQuery_ValidInput_AllPropertiesSet()
        {
            nodeTaskBuilder.WithInputUsingCombinedTransformationStrategy("/root/input?test", "http://foo.bar/input", "/root");
            var input = nodeTaskBuilder.Build().Inputs![0];
            Assert.AreEqual("/root/input", input.Path);
            Assert.AreEqual("http://foo.bar/input", input.SourceUrl);
            Assert.AreEqual("/root", input.MountParentDirectory);
        }

        [TestMethod]
        public void WithOutputUsingCombinedTransformationStrategy_WithTerraRuntimeSet_OutputUsesCombinedTerraTransformationStrategy()
        {
            nodeTaskBuilder.WithTerraAsRuntimeEnvironment("https://wsm.foo", "https://lz.foo", sasAllowedIpRange: String.Empty);
            nodeTaskBuilder.WithOutputUsingCombinedTransformationStrategy("/root/output", "http://foo.bar/output", FileType.File, "/root");
            var output = nodeTaskBuilder.Build().Outputs![0];
            Assert.AreEqual(TransformationStrategy.CombinedTerra, output.TransformationStrategy);
        }

        [TestMethod]
        public void WithOutputUsingCombineTransformationStrategy_ValidOutput_AllPropertiesSet()
        {
            nodeTaskBuilder.WithOutputUsingCombinedTransformationStrategy("/root/output", "http://foo.bar/output", FileType.File, "/root");
            var output = nodeTaskBuilder.Build().Outputs![0];
            Assert.AreEqual("/root/output", output.Path);
            Assert.AreEqual("http://foo.bar/output", output.TargetUrl);
            Assert.AreEqual(FileType.File, output.FileType);
            Assert.AreEqual("/root", output.MountParentDirectory);
        }

        [TestMethod]
        public void
            WithOutputUsingCombinedTransformationStrategy_WithTerraRuntimeNotSet_OutputUsesCombinedARMTransformationStrategy()
        {
            nodeTaskBuilder.WithOutputUsingCombinedTransformationStrategy("/root/output", "http://foo.bar/output", FileType.File, "/root");
            var output = nodeTaskBuilder.Build().Outputs![0];
            Assert.AreEqual(TransformationStrategy.CombinedAzureResourceManager, output.TransformationStrategy);
        }

        [TestMethod]
        public void WithContainerCommands_CommandsProvided_CommandsAreSet()
        {
            var commands = new List<string>() { "echo", "world" };
            nodeTaskBuilder.WithContainerCommands(commands);
            var containerInfo = nodeTaskBuilder.Build().CommandsToExecute;
            CollectionAssert.AreEqual(commands, containerInfo);
        }

        [TestMethod]
        [DataRow("foo", "foo", "latest")]
        [DataRow("foo:bar", "foo", "bar")]
        [DataRow("foo:bar", "foo", "bar")]
        [DataRow("broadinstitute/gatk@sha256:f80d33060cb4872d29b9a248b193d267f838b1a636c5a6120aaa45b08a1f09e9", "broadinstitute/gatk", "sha256:f80d33060cb4872d29b9a248b193d267f838b1a636c5a6120aaa45b08a1f09e9")]
        public void WithContainerImageTest_ImageInfoIsProvided_ImageInfoIsSet(string imageInfo, string expectedImage, string expectedTag)
        {
            nodeTaskBuilder.WithContainerImage(imageInfo);
            var nodeTask = nodeTaskBuilder.Build();
            Assert.AreEqual(expectedImage, nodeTask.ImageName);
            Assert.AreEqual(expectedTag, nodeTask.ImageTag);
        }

        [TestMethod]
        public void Build_NoWithStatementCall_NodeTaskInstanceIsReturned()
        {
            Assert.IsNotNull(nodeTaskBuilder.Build());
        }

        [TestMethod]
        public void WithMetricsFile_Called_MetricInformationIsSet()
        {
            nodeTaskBuilder.WithMetricsFile("metrics.txt");
            var metricsFile = nodeTaskBuilder.Build().MetricsFilename;

            Assert.AreEqual("metrics.txt", metricsFile);
        }

        [DataTestMethod]
        [DataRow(@"/subscriptions/aaaaa450-5f22-4b20-9326-b5852bb89d90/resourcegroups/foo/providers/Microsoft.ManagedIdentity/userAssignedIdentities/bar-identity", @"/subscriptions/aaaaa450-5f22-4b20-9326-b5852bb89d90/resourcegroups/foo/providers/Microsoft.ManagedIdentity/userAssignedIdentities/bar-identity")]
        [DataRow(null, null)]
        [DataRow("", null)]
        public void WithResourceIdManagedIdentity_dResourceIdProvided_ResourceIdIsSet(string resourceId, string expectedResourceId)
        {
            nodeTaskBuilder.WithResourceIdManagedIdentity(resourceId);
            var nodeTask = nodeTaskBuilder.Build();
            Assert.AreEqual(expectedResourceId, nodeTask.RuntimeOptions?.NodeManagedIdentityResourceId);
        }

        [TestMethod]
        public void WithResourceIdManagedIdentity_InvalidResourceIdProvided_ExceptionIsThrown()
        {
            var resourceId = "invalid-resource-id";
            Assert.ThrowsException<ArgumentException>(() => nodeTaskBuilder.WithResourceIdManagedIdentity(resourceId));
        }

        [TestMethod]
        public void WithStorageEventSink_Called_StorageSinkIsSet()
        {
            var url = "https://foo.blob.core.windows.net/cont";
            nodeTaskBuilder.WithStorageEventSink(new(url));
            var nodeTask = nodeTaskBuilder.Build();
            Assert.AreEqual(url, nodeTask.RuntimeOptions.StorageEventSink!.TargetUrl);
            Assert.AreEqual(TransformationStrategy.CombinedAzureResourceManager, nodeTask.RuntimeOptions.StorageEventSink.TransformationStrategy);
        }


        [TestMethod]
        public void WithLogPublisher_Called_LogStorageIsSet()
        {
            var url = "https://foo.blob.core.windows.net/cont/log";
            nodeTaskBuilder.WithLogPublisher(new(url));
            var nodeTask = nodeTaskBuilder.Build();
            Assert.AreEqual(url, nodeTask.RuntimeOptions.StreamingLogPublisher!.TargetUrl);
            Assert.AreEqual(TransformationStrategy.CombinedAzureResourceManager, nodeTask.RuntimeOptions.StreamingLogPublisher.TransformationStrategy);
        }

        [TestMethod]
        public void WithLogPublisher_CalledThenSetUpTerraRuntimeEnv_LogTargetUrlTransformationStrategyIsTerra()
        {
            var url = "https://foo.blob.core.windows.net/cont/log";
            nodeTaskBuilder.WithLogPublisher(new(url))
                .WithTerraAsRuntimeEnvironment("http://wsm.terra.foo", "http://lz.terra.foo", sasAllowedIpRange: null);
            var nodeTask = nodeTaskBuilder.Build();

            Assert.AreEqual(TransformationStrategy.CombinedTerra, nodeTask.RuntimeOptions.StreamingLogPublisher!.TransformationStrategy);
        }

        [TestMethod]
        public void WithDrsHubUrl_CalledThenSetUpTerraRuntimeEnv_DrsApiHostIsKept()
        {
            var drsHubUrl = "https://drshub.foo";
            nodeTaskBuilder
                .WithDrsHubUrl(drsHubUrl)
                .WithTerraAsRuntimeEnvironment("http://wsm.terra.foo", "http://lz.terra.foo", sasAllowedIpRange: null);

            var nodeTask = nodeTaskBuilder.Build();

            Assert.AreEqual(drsHubUrl, nodeTask.RuntimeOptions.Terra!.DrsHubApiHost);
        }

        [DataTestMethod]
        [DataRow("https://drshub.foo", "https://drshub.foo")]
        [DataRow("https://drshub.foo/", "https://drshub.foo")]
        [DataRow("https://drshub.foo/api/v4/drs/resolve", "https://drshub.foo")]
        public void WithDrsHubUrl_DrsHubUrlIsProvided_DrsHubApiHostIsSet(string drsHubUrl, string drsExpectedDrsApiHost)
        {
            var nodeTask = nodeTaskBuilder
                .WithDrsHubUrl(drsHubUrl)
                .Build();
            Assert.AreEqual(drsExpectedDrsApiHost, nodeTask.RuntimeOptions.Terra!.DrsHubApiHost);
        }
    }
}
