// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;
using Tes.Models;
using Tes.Runner.Models;
using TesApi.Web;
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
        private Mock<TaskToNodeTaskConverter> taskToNodeTaskConverterMock;
        private Mock<IStorageAccessProvider> storageAccessProviderMock;
        private Mock<BatchNodeScriptBuilder> batchNodeScriptBuilderMock;
        private const string WgetOptions = "--https-only --no-verbose --timeout=20 --waitretry=1 --tries=9 --retry-connrefused --continue";
        private const string AssetUrl = "http://foo.bar/bar";
        private TesTask tesTask;
        private NodeTask nodeTask;


        [TestInitialize]
        public void SetUp()
        {

            tesTask = GetTestTesTask();
            nodeTask = new NodeTask();
            storageAccessProviderMock = new Mock<IStorageAccessProvider>();
            storageAccessProviderMock.Setup(x =>
                    x.GetInternalTesTaskBlobUrlAsync(It.IsAny<TesTask>(), It.IsAny<string>(),
                        It.IsAny<CancellationToken>(), It.IsAny<bool?>()))
                .ReturnsAsync(AssetUrl);
            storageAccessProviderMock.Setup(x =>
                    x.GetInternalTesBlobUrlAsync(It.IsAny<string>(), It.IsAny<CancellationToken>(),
                        It.IsAny<bool?>(), It.IsAny<bool?>(), It.IsAny<bool?>()))
                .ReturnsAsync(AssetUrl);

            taskToNodeTaskConverterMock = new Mock<TaskToNodeTaskConverter>();
            taskToNodeTaskConverterMock.Setup(x => x.ToNodeTaskAsync(It.IsAny<TesTask>(),
                 It.IsAny<NodeTaskConversionOptions>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(nodeTask);

            batchNodeScriptBuilderMock = new Mock<BatchNodeScriptBuilder>();
            batchNodeScriptBuilderMock.Setup(x => x.Build()).Returns("echo");

            taskExecutionScriptingManager = new TaskExecutionScriptingManager(storageAccessProviderMock.Object, taskToNodeTaskConverterMock.Object, batchNodeScriptBuilderMock.Object, new NullLogger<TaskExecutionScriptingManager>());
        }

        [TestMethod]
        public void ParseBatchRunCommand_ReturnsRunCommandWithScriptDownloadAndExecution()
        {
            var scriptName = "batch_script";
            var scriptUrl = $"https://foo.bar/{scriptName}";
            var nodeTaskUrl = $"https://foo.bar/{scriptName}";
            var scriptAssets = new BatchScriptAssetsInfo(scriptUrl, nodeTaskUrl, scriptName);

            var expectedCommand = $"/bin/bash -c \"wget {WgetOptions} -O ${BatchNodeScriptBuilder.BatchTaskDirEnvVarName}/{scriptName} '{scriptUrl}' && chmod +x ${BatchNodeScriptBuilder.BatchTaskDirEnvVarName}/{scriptName} && ${BatchNodeScriptBuilder.BatchTaskDirEnvVarName}/{scriptName}\"";

            var runCommand = taskExecutionScriptingManager.ParseBatchRunCommand(scriptAssets);

            Assert.AreEqual(expectedCommand, runCommand);
        }

        [TestMethod]
        public async Task PrepareBatchScript_ValidTask_CreatesAndUploadsExpectedAssetsToInternalStorageLocation()
        {
            var options = new NodeTaskConversionOptions();

            var assets = await taskExecutionScriptingManager.PrepareBatchScriptAsync(tesTask,
                options, CancellationToken.None);

            Assert.IsNotNull(assets);

            //creates a node task definition:
            taskToNodeTaskConverterMock.Verify(c => c.ToNodeTaskAsync(It.IsAny<TesTask>(),
                    It.IsAny<NodeTaskConversionOptions>(), It.IsAny<CancellationToken>()),
                Times.Once);

            //creates the script
            batchNodeScriptBuilderMock.Verify(c => c.Build(), Times.Once);

            //it should upload three assets/blobs: server task, node task definition and script
            storageAccessProviderMock.Verify(p => p.UploadBlobAsync(It.IsAny<Uri>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(3));
        }

        private static TesTask GetTestTesTask()
        {
            return JsonConvert.DeserializeObject<TesTask>(File.ReadAllText("testask1.json"));
        }
    }
}
