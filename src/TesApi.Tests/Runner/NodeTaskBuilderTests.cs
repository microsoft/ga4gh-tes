﻿// Copyright (c) Microsoft Corporation.
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
        [DataRow("foo:bar", "foo", "bar")]
        [DataRow("foo", "foo", "")]
        public void WithContainerImageTest_ImageInfoIsProvided_ImageInfoIsSet(string imageInfo, string expectedImage, string expectedTag)
        {
            nodeTaskBuilder.WithContainerImage(imageInfo);
            var nodeTask = nodeTaskBuilder.Build();
            Assert.AreEqual(expectedImage, nodeTask.ImageName);
            Assert.AreEqual(expectedTag, nodeTask.ImageTag);
        }

        [TestMethod]
        public void WithDockerCleanUpOptions_OptionsProvided_CleanupOptionsAreSet()
        {
            var options = new RuntimeContainerCleanupOptions(
                ExecuteDockerPrune: true,
                ExecuteDockerRmi: true);

            nodeTaskBuilder.WithDockerCleanUpOptions(options);
            var dockerCleanUp = nodeTaskBuilder.Build().RuntimeOptions.DockerCleanUp;
            Assert.AreEqual(options.ExecuteDockerPrune, dockerCleanUp.ExecutePrune);
            Assert.AreEqual(options.ExecuteDockerRmi, dockerCleanUp.ExecuteRmi);
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

        [TestMethod]
        public void WithNodeManagedIdentity_Called_ClientIdIsSet()
        {
            nodeTaskBuilder.WithNodeManagedIdentity("clientId");
            var nodeTask = nodeTaskBuilder.Build();
            Assert.AreEqual("clientId", nodeTask.RuntimeOptions.NodeManagedIdentityClientId);
        }
    }
}
