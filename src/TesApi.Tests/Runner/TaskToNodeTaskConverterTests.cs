// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;
using Tes.Models;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Options;
using TesApi.Web.Runner;
using TesApi.Web.Storage;

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Tests.Runner
{
    [TestClass, TestCategory("Unit")]
    public class TaskToNodeTaskConverterTests
    {
        private Mock<IStorageAccessProvider> storageAccessProviderMock;
        private TaskToNodeTaskConverter taskToNodeTaskConverter;
        private readonly TesTask tesTask = GetTestTesTask();
        private TerraOptions terraOptions;
        private StorageOptions storageOptions;

        private const string SasToken = "sv=2019-12-12&ss=bfqt&srt=sco&spr=https&st=2023-09-27T17%3A32%3A57Z&se=2023-09-28T17%3A32%3A57Z&sp=rwdlacupx&sig=SIGNATURE";
        const string DefaultStorageAccountName = "default";
        const string InternalBlobUrl = "http://foo.bar/tes-internal";
        const string InternalBlobUrlWithSas = $"{InternalBlobUrl}?{SasToken}";

        private const string ExternalStorageContainer =
            $"https://external{StorageUrlUtils.BlobEndpointHostNameSuffix}/cont";
        private const string ExternalStorageContainerWithSas =
            $"{ExternalStorageContainer}?{SasToken}";

        [TestInitialize]
        public void SetUp()
        {
            terraOptions = new TerraOptions();
            storageOptions = new StorageOptions() { ExternalStorageContainers = ExternalStorageContainerWithSas };
            storageAccessProviderMock = new Mock<IStorageAccessProvider>();
            storageAccessProviderMock.Setup(x =>
                                   x.GetInternalTesTaskBlobUrlAsync(It.IsAny<TesTask>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(InternalBlobUrlWithSas);

            taskToNodeTaskConverter = new TaskToNodeTaskConverter(Options.Create(terraOptions), storageAccessProviderMock.Object, Options.Create(storageOptions), new NullLogger<TaskToNodeTaskConverter>());
        }
        [TestMethod]
        public async Task ToNodeTaskAsync_TesTaskWithContentInputs_ContentInputsAreUploadedAndSetInTaskDefinition()
        {
            var contentInput = tesTask.Inputs.Find(i => !string.IsNullOrWhiteSpace(i.Content));
            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, additionalInputs: null, new RuntimeContainerCleanupOptions(false, false), DefaultStorageAccountName, CancellationToken.None);

            Assert.IsNotNull(nodeTask);

            // Verify that the content input was uploaded and the node task has the correct url
            storageAccessProviderMock.Verify(x => x.UploadBlobAsync(It.IsAny<Uri>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(1));
            // The input must be uploaded to the internal blob url without a sas token.
            Assert.AreEqual(InternalBlobUrl, nodeTask.Inputs!.Find(i => i.Path == $"{TaskToNodeTaskConverter.BatchTaskWorkingDirEnvVar}{contentInput!.Path}")!.SourceUrl);
        }

        [TestMethod]
        public async Task
            ToNodeTaskAsync_TesTaskWithInputsAndOutputs_AllInputsAndOutputsArePrefixedWithBatchTaskWorkingDir()
        {
            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, additionalInputs: null, new RuntimeContainerCleanupOptions(false, false), DefaultStorageAccountName, CancellationToken.None);
            Assert.IsNotNull(nodeTask);
            // Verify that all inputs and outputs are prefixed with the batch task working dir
            foreach (var input in nodeTask.Inputs!)
            {
                Assert.IsTrue(input.Path!.StartsWith(TaskToNodeTaskConverter.BatchTaskWorkingDirEnvVar));
            }

            foreach (var output in nodeTask.Outputs!)
            {
                Assert.IsTrue(output.Path!.StartsWith(TaskToNodeTaskConverter.BatchTaskWorkingDirEnvVar));
            }
        }

        [TestMethod]
        public async Task ToNodeTaskAsync_ExternalStorageInputsProvided_NodeTesTaskContainsUrlsWithSasTokens()
        {

            tesTask.Inputs = new List<TesInput>
            {
                new TesInput()
                {
                    Path = "/input/file1",
                    Url = $"{ExternalStorageContainer}/blob"
                }
            };

            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, additionalInputs: null, new RuntimeContainerCleanupOptions(false, false), DefaultStorageAccountName, CancellationToken.None);

            Assert.IsNotNull(nodeTask);

            var url = new BlobUriBuilder(new Uri(nodeTask.Inputs![0].SourceUrl!));

            Assert.IsNotNull(url);
            Assert.AreEqual($"?{SasToken}", url.ToUri().Query);
            Assert.AreEqual("blob", url.BlobName);
        }

        [TestMethod]
        public async Task ToNodeTaskAsync_AdditionalInputsProvided_NodeTesTaskContainsTheAdditionalInputs()
        {
            var additionalInputs = CreateAdditionalInputs();

            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, additionalInputs, new RuntimeContainerCleanupOptions(false, false), DefaultStorageAccountName, CancellationToken.None);

            Assert.IsNotNull(nodeTask);

            //verify the additional inputs are added to the node task
            var expectedPath = $"{TaskToNodeTaskConverter.BatchTaskWorkingDirEnvVar}{additionalInputs[0].Path}";
            Assert.IsTrue(nodeTask.Inputs!.Any(i => i.Path!.Equals(expectedPath, StringComparison.OrdinalIgnoreCase)));
            expectedPath = $"{TaskToNodeTaskConverter.BatchTaskWorkingDirEnvVar}{additionalInputs[1].Path}";
            Assert.IsTrue(nodeTask.Inputs!.Any(i => i.Path!.Equals(expectedPath, StringComparison.OrdinalIgnoreCase)));
        }


        [TestMethod]
        public async Task ToNodeTaskAsync_TesTaskWithNoInputsAndOutputs_NodeTaskContainsNoInputsAndOutputs()
        {
            var task = GetTestTesTask();
            task.Inputs = null;
            task.Outputs = null;
            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(task, additionalInputs: null, new RuntimeContainerCleanupOptions(false, false), defaultStorageAccountName: default, CancellationToken.None);
            Assert.IsNotNull(nodeTask);
            Assert.IsNull(nodeTask.Inputs);
            Assert.IsNull(nodeTask.Outputs);
        }

        [TestMethod]
        public async Task
            ToNodeTaskAsync_TesTaskWithNoInputsAndOutputsAndAdditionalInputs_NodeTaskContainsOnlyAdditionalInputs()
        {
            var task = GetTestTesTask();
            task.Inputs = null;
            task.Outputs = null;
            var additionalInputs = CreateAdditionalInputs();
            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(task, additionalInputs, new RuntimeContainerCleanupOptions(false, false), defaultStorageAccountName: default, CancellationToken.None);

            Assert.IsNotNull(nodeTask);
            Assert.AreEqual(2, nodeTask.Inputs!.Count);
            Assert.IsNull(nodeTask.Outputs);
        }

        [TestMethod]
        public async Task
            ToNodeTaskAsync_TesTaskWithNoInputsAndOutputsAndNoAdditionalInputs_NodeTaskContainsNoInputsAndOutputs()
        {
            var task = GetTestTesTask();
            task.Inputs = null;
            task.Outputs = null;
            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(task, additionalInputs: null, new RuntimeContainerCleanupOptions(false, false), defaultStorageAccountName: default, CancellationToken.None);
            Assert.IsNotNull(nodeTask);
            Assert.IsNull(nodeTask.Inputs);
            Assert.IsNull(nodeTask.Outputs);
        }

        [TestMethod]
        public async Task ToNodeTaskAsync_AdditionalInputsContainsAnExistingInput_OnlyDistinctAdditionalInputsAreAdded()
        {
            var additionalInputs = CreateAdditionalInputs();
            additionalInputs[0].Path = tesTask.Inputs.First().Path;

            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, additionalInputs, new RuntimeContainerCleanupOptions(false, false), DefaultStorageAccountName, CancellationToken.None);
            Assert.IsNotNull(nodeTask);

            //verify that only one additional inputs is added to the node task
            Assert.AreEqual(tesTask.Inputs.Count + 1, nodeTask.Inputs!.Count);
            var expectedPath = $"{TaskToNodeTaskConverter.BatchTaskWorkingDirEnvVar}{additionalInputs[1].Path}";
            Assert.IsTrue(nodeTask.Inputs!.Any(i => i.Path!.Equals(expectedPath, StringComparison.OrdinalIgnoreCase)));
        }

        [TestMethod]
        public async Task ToNodeTaskAsync_InputUrlIsALocalPath_UrlIsConverted()
        {
            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, additionalInputs: null, new RuntimeContainerCleanupOptions(false, false), DefaultStorageAccountName, CancellationToken.None);

            Assert.IsNotNull(nodeTask);
            //first input in the task has a url with the the format: /storageaccount/container
            var url = new Uri(nodeTask.Inputs![0].SourceUrl!);
            Assert.IsNotNull(url);
        }

        [TestMethod]
        public async Task ToNodeTaskAsync_InputUrlIsACromwellLocalPath_UrlIsConvertedUsesDefaultStorageAccount()
        {
            tesTask.Inputs = new List<TesInput>
            {
                new TesInput()
                {
                    Name = "local input",
                    Path = "/cromwell-executions/file",
                    Url = "/cromwell-executions/file",
                    Type = TesFileType.FILEEnum
                }
            };

            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, additionalInputs: null, new RuntimeContainerCleanupOptions(false, false), DefaultStorageAccountName, CancellationToken.None);

            Assert.IsNotNull(nodeTask);
            var url = new BlobUriBuilder(new Uri(nodeTask.Inputs![0].SourceUrl!));
            Assert.IsNotNull(url);
            Assert.AreEqual(DefaultStorageAccountName, url.AccountName);
            Assert.AreEqual("cromwell-executions", url.BlobContainerName);
        }

        private static TesInput[] CreateAdditionalInputs()
        {
            return new[]
            {
                new TesInput
                {
                    Name = "additionalInput1",
                    Path = "/additionalInput1",
                    Type = TesFileType.FILEEnum,
                    Url = "http://foo.bar/additionalInput1"
                },
                new TesInput
                {
                    Name = "additionalInput2",
                    Path = "/additionalInput2",
                    Type = TesFileType.FILEEnum,
                    Url = "http://foo.bar/additionalInput2"
                }
            };
        }

        private static TesTask GetTestTesTask()
        {
            return JsonConvert.DeserializeObject<TesTask>(File.ReadAllText("testask1.json"));
        }
    }
}
