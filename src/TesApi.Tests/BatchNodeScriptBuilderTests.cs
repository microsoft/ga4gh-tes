// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass, TestCategory("Unit")]
    public class BatchNodeScriptBuilderTests
    {
        private BatchNodeScriptBuilder builder;

        [TestInitialize]
        public void SetUp()
        {
            builder = new BatchNodeScriptBuilder();
        }

        [TestMethod]
        public void CreateWgetDownloadCommand_ValidUrl_ReturnsCommand()
        {
            var url = "https://foo.bar";
            var local = "/local";
            var expectedCommand = $"wget --https-only --no-verbose --timeout=20 --waitretry=1 --tries=9 --retry-connrefused --continue -O {local} '{url}'";

            var result = BatchNodeScriptBuilder.CreateWgetDownloadCommand(url, "/local");

            Assert.AreEqual(expectedCommand, result);
        }

        [TestMethod]
        public void CreateWgetDownloadCommand_InvalidUrl_ThrowsArgumentException()
        {
            var url = "foo.bar";
            var local = "/local";
            Assert.ThrowsException<ArgumentException>(() => BatchNodeScriptBuilder.CreateWgetDownloadCommand(url, local));
        }


        [TestMethod]
        public void WithAlpineWgetInstallation_BuildCalled_ScriptContainsWgetInstallation()
        {
            var expectedLine = @"(grep -q alpine /etc/os-release && apk add bash wget || :) && \";

            var result = builder.WithAlpineWgetInstallation()
                .Build();
            Assert.IsTrue(result.Contains(expectedLine));
        }

        [TestMethod]
        public void WithRunnerFilesDownloadUsingWget_BuildCalled_WgetCallsAreCreated()
        {
            var expectedLine1 = BatchNodeScriptBuilder.CreateWgetDownloadCommand("https://foo.bar1", $"${BatchNodeScriptBuilder.BatchTaskDirEnvVarName}/{BatchNodeScriptBuilder.NodeTaskRunnerFilename}", setExecutable: true);
            var expectedLine2 = BatchNodeScriptBuilder.CreateWgetDownloadCommand("https://foo.bar2", $"${BatchNodeScriptBuilder.BatchTaskDirEnvVarName}/{BatchNodeScriptBuilder.NodeRunnerTaskDefinitionFilename}", setExecutable: false);

            var result = builder
                .WithRunnerFilesDownloadUsingWget("https://foo.bar1", "https://foo.bar2")
                .Build();

            Assert.IsTrue(result.Contains(expectedLine1));
            Assert.IsTrue(result.Contains(expectedLine2));
        }

        [TestMethod]
        public void Build_ScriptEndsWithEchoTaskComplete()
        {
            var result = builder.WithMetrics()
                 .WithExecuteRunner()
                 .Build();

            Assert.IsTrue(result.EndsWith("echo Task complete\n"));
        }
    }
}
