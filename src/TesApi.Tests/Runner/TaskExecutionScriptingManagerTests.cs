// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using TesApi.Web;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Runner;
using TesApi.Web.Storage;

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.


namespace TesApi.Tests.Runner
{
    [TestClass, TestCategory("Unit")]
    public class TaskExecutionScriptingManagerTests
    {
        private TaskExecutionScriptingManager taskExecutionScriptingManager;
        private TaskToNodeTaskConverter taskToNodeTaskConverter;
        private Mock<IStorageAccessProvider> storageAccessProviderMock;
        private BatchNodeScriptBuilder batchNodeScriptBuilder;
        private const string WgetOptions = "--https-only --timeout=20 --waitretry=1 --tries=9 --retry-connrefused --continue";

        [TestInitialize]
        public void SetUp()
        {
            storageAccessProviderMock = new Mock<IStorageAccessProvider>();
            batchNodeScriptBuilder = new BatchNodeScriptBuilder();
            taskToNodeTaskConverter = new TaskToNodeTaskConverter(Options.Create(new TerraOptions()), storageAccessProviderMock.Object, new NullLogger<TaskToNodeTaskConverter>());
            taskExecutionScriptingManager = new TaskExecutionScriptingManager(storageAccessProviderMock.Object, taskToNodeTaskConverter, batchNodeScriptBuilder, new NullLogger<TaskExecutionScriptingManager>());
        }

        [TestMethod]
        public void ParseBatchRunCommand_ReturnsRunCommandWithScriptDownloadAndExecution()
        {
            var scriptName = "batch_script";
            var scriptUrl = $"https://foo.bar/{scriptName}";
            var nodeTaskUrl = $"https://foo.bar/{scriptName}";
            var scriptAssets = new ScriptingAssetsInfo(scriptUrl, nodeTaskUrl, scriptName);

            var expectedCommand = $"/bin/bash -c \"wget {WgetOptions} -O ${BatchNodeScriptBuilder.BatchTaskDirEnvVarName}/{scriptName} '{scriptUrl}' && chmod +x ${BatchNodeScriptBuilder.BatchTaskDirEnvVarName}/{scriptName} && ${BatchNodeScriptBuilder.BatchTaskDirEnvVarName}/{scriptName}\"";

            var runCommand = taskExecutionScriptingManager.ParseBatchRunCommand(scriptAssets);

            Assert.AreEqual(expectedCommand, runCommand);
        }
    }
}
