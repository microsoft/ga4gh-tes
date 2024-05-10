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
            Uri url = new("https://foo.bar");
            var local = "/local";
            var expectedCommand = $"wget --no-verbose --https-only --timeout=20 --waitretry=1 --tries=9 --retry-connrefused --continue -O {local} '{url}' && sleep 5s";

            var result = BatchScheduler.CreateWgetDownloadCommand(url, "/local");

            Assert.AreEqual(expectedCommand, result);
        }

        [TestMethod]
        public void CreateWgetDownloadCommand_InvalidUrl_ThrowsArgumentException()
        {
            Uri url = null;
            var local = "/local";
            Assert.ThrowsException<ArgumentNullException>(() => BatchScheduler.CreateWgetDownloadCommand(url, local));
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
            //var expectedLine1 = BatchScheduler.CreateWgetDownloadCommand(new("https://foo.bar1"), $"${BatchNodeScriptBuilder.BatchTaskDirEnvVarName}/{BatchNodeScriptBuilder.NodeTaskRunnerFilename}", setExecutable: true);
            var expectedLine = BatchScheduler.CreateWgetDownloadCommand(new("https://foo.bar"), $"${BatchNodeScriptBuilder.BatchTaskDirEnvVarName}/{BatchNodeScriptBuilder.NodeRunnerTaskDefinitionFilename}", setExecutable: false);

            var result = builder
                .WithRunnerTaskDownloadUsingWget(new("https://foo.bar"))
                .Build();

            Assert.IsTrue(result.Contains(expectedLine));
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
