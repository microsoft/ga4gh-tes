// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
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
        private const string WgetOptions = "--no-verbose --https-only --timeout=20 --waitretry=1 --tries=9 --retry-connrefused --continue";
        private static readonly Uri AssetUrl = new("http://foo.bar/bar");
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
                        It.IsAny<Azure.Storage.Sas.BlobSasPermissions>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(AssetUrl);
            storageAccessProviderMock.Setup(x =>
                    x.GetInternalTesBlobUrlAsync(It.IsAny<string>(),
                        It.IsAny<Azure.Storage.Sas.BlobSasPermissions>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(AssetUrl);

            taskToNodeTaskConverterMock = new Mock<TaskToNodeTaskConverter>();
            taskToNodeTaskConverterMock.Setup(x => x.ToNodeTaskAsync(It.IsAny<TesTask>(),
                 It.IsAny<NodeTaskConversionOptions>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(nodeTask);


            taskExecutionScriptingManager = new TaskExecutionScriptingManager(storageAccessProviderMock.Object, taskToNodeTaskConverterMock.Object, new NullLogger<TaskExecutionScriptingManager>());
        }

        [TestMethod]
        public void ParseBatchRunCommand_ReturnsRunCommandWithScriptDownloadAndExecution()
        {
            var scriptName = "node_task";
            var nodeTaskUrl = $"https://foo.bar/{scriptName}";
            var scriptAssets = new BatchScriptAssetsInfo(new(nodeTaskUrl), new Dictionary<string, string>().AsReadOnly());

            var expectedCommand = $"/usr/bin/env -S \"{BatchScheduler.BatchNodeSharedEnvVar}/{BatchScheduler.NodeTaskRunnerFilename} -i '{nodeTaskUrl}'\"";

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

            //it should upload two assets/blobs: server task and node task definition
            storageAccessProviderMock.Verify(p => p.UploadBlobAsync(It.IsAny<Uri>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        }

        private static TesTask GetTestTesTask()
        {
            return JsonConvert.DeserializeObject<TesTask>(File.ReadAllText("testask1.json"));
        }
    }
}
