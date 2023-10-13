﻿// Copyright (c) Microsoft Corporation.
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
        const string ManagedIdentityResourceId = "resourceId";


        const string ExternalStorageAccountName = "external";
        const string ExternalStorageContainer =
            $"https://{ExternalStorageAccountName}{StorageUrlUtils.BlobEndpointHostNameSuffix}/cont";
        const string ExternalStorageContainerWithSas =
            $"{ExternalStorageContainer}?{SasToken}";

        [TestInitialize]
        public void SetUp()
        {
            terraOptions = new TerraOptions();
            storageOptions = new StorageOptions() { ExternalStorageContainers = ExternalStorageContainerWithSas };
            storageAccessProviderMock = new Mock<IStorageAccessProvider>();
            storageAccessProviderMock.Setup(x =>
                                   x.GetInternalTesTaskBlobUrlAsync(It.IsAny<TesTask>(), It.IsAny<string>(), It.IsAny<CancellationToken>(), It.IsAny<bool?>()))
                .ReturnsAsync(InternalBlobUrlWithSas);
            storageAccessProviderMock.Setup(x =>
                    x.GetInternalTesBlobUrlWithoutSasToken(It.IsAny<string>()))
                .Returns(InternalBlobUrl);
            storageAccessProviderMock.Setup(x =>
                    x.GetInternalTesTaskBlobUrlWithoutSasToken(It.IsAny<TesTask>(), It.IsAny<string>()))
                .Returns(InternalBlobUrl);


            taskToNodeTaskConverter = new TaskToNodeTaskConverter(Options.Create(terraOptions), storageAccessProviderMock.Object, Options.Create(storageOptions), new NullLogger<TaskToNodeTaskConverter>());
        }
        [TestMethod]
        public async Task ToNodeTaskAsync_TesTaskWithContentInputs_ContentInputsAreUploadedAndSetInTaskDefinition()
        {
            var contentInput = tesTask.Inputs.Find(i => !string.IsNullOrWhiteSpace(i.Content));
            var options = OptionsWithoutAdditionalInputs();
            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, options, CancellationToken.None);

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
            var options = OptionsWithoutAdditionalInputs();
            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, options, CancellationToken.None);
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

            var options = OptionsWithoutAdditionalInputs();

            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, options, CancellationToken.None);

            Assert.IsNotNull(nodeTask);

            var url = new BlobUriBuilder(new Uri(nodeTask.Inputs![0].SourceUrl!));

            Assert.IsNotNull(url);
            Assert.AreEqual($"?{SasToken}", url.ToUri().Query);
            Assert.AreEqual("blob", url.BlobName);
        }


        [TestMethod]
        public async Task ToNodeTaskAsync_AdditionalInputsProvided_NodeTesTaskContainsTheAdditionalInputs()
        {
            var options = OptionsWithAdditionalInputs();

            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, options, CancellationToken.None);

            Assert.IsNotNull(nodeTask);

            //verify the additional inputs are added to the node task
            var expectedPath = $"{TaskToNodeTaskConverter.BatchTaskWorkingDirEnvVar}{options.AdditionalInputs[0].Path}";
            Assert.IsTrue(nodeTask.Inputs!.Any(i => i.Path!.Equals(expectedPath, StringComparison.OrdinalIgnoreCase)));
            expectedPath = $"{TaskToNodeTaskConverter.BatchTaskWorkingDirEnvVar}{options.AdditionalInputs[1].Path}";
            Assert.IsTrue(nodeTask.Inputs!.Any(i => i.Path!.Equals(expectedPath, StringComparison.OrdinalIgnoreCase)));
        }


        [TestMethod]
        public async Task ToNodeTaskAsync_TesTaskWithNoInputsAndOutputs_NodeTaskContainsNoInputsAndOutputs()
        {
            var task = GetTestTesTask();
            task.Inputs = null;
            task.Outputs = null;
            var options = OptionsWithoutAdditionalInputs();
            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(task, options, CancellationToken.None);
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
            var options = OptionsWithAdditionalInputs();
            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(task, options, CancellationToken.None);

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
            var options = OptionsWithoutAdditionalInputs();
            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(task, options, CancellationToken.None);
            Assert.IsNotNull(nodeTask);
            Assert.IsNull(nodeTask.Inputs);
            Assert.IsNull(nodeTask.Outputs);
        }

        [TestMethod]
        public async Task ToNodeTaskAsync_AdditionalInputsContainsAnExistingInput_OnlyDistinctAdditionalInputsAreAdded()
        {
            var options = OptionsWithAdditionalInputs();
            options.AdditionalInputs[0].Path = tesTask.Inputs.First().Path;

            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, options, CancellationToken.None);
            Assert.IsNotNull(nodeTask);

            //verify that only one additional inputs is added to the node task
            Assert.AreEqual(tesTask.Inputs.Count + 1, nodeTask.Inputs!.Count);
            var expectedPath = $"{TaskToNodeTaskConverter.BatchTaskWorkingDirEnvVar}{options.AdditionalInputs[1].Path}";
            Assert.IsTrue(nodeTask.Inputs!.Any(i => i.Path!.Equals(expectedPath, StringComparison.OrdinalIgnoreCase)));
        }

        [TestMethod]
        public async Task ToNodeTaskAsync_InputUrlIsALocalPath_UrlIsConverted()
        {
            var options = OptionsWithoutAdditionalInputs();
            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, options, CancellationToken.None);

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

            var options = OptionsWithoutAdditionalInputs();

            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, options, CancellationToken.None);

            Assert.IsNotNull(nodeTask);
            var url = new BlobUriBuilder(new Uri(nodeTask.Inputs![0].SourceUrl!));
            Assert.IsNotNull(url);
            Assert.AreEqual(DefaultStorageAccountName, url.AccountName);
            Assert.AreEqual("cromwell-executions", url.BlobContainerName);
        }

        [TestMethod]
        public async Task ToNodeTaskAsync_InputUrlIsAExternalLocalPath_UrlIsConvertedUsesExternalAccountSas()
        {
            tesTask.Inputs = new List<TesInput>
            {
                new TesInput()
                {
                    Name = "local input",
                    Path = $"/{ExternalStorageAccountName}/cont/file",
                    Url = $"/{ExternalStorageAccountName}/cont/file",
                    Type = TesFileType.FILEEnum
                }
            };

            var options = OptionsWithoutAdditionalInputs();

            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, options, CancellationToken.None);

            Assert.IsNotNull(nodeTask);

            var url = new BlobUriBuilder(new Uri(nodeTask.Inputs![0].SourceUrl!));

            Assert.IsNotNull(url);
            Assert.AreEqual(ExternalStorageAccountName, url.AccountName);
            Assert.AreEqual($"?{SasToken}", url.ToUri().Query);
            Assert.AreEqual("file", url.BlobName);
        }

        private static NodeTaskConversionOptions OptionsWithAdditionalInputs()
        {
            var inputs = new[]
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

            return new NodeTaskConversionOptions(AdditionalInputs: inputs,
                DefaultStorageAccountName: DefaultStorageAccountName,
                NodeManagedIdentityResourceId: ManagedIdentityResourceId);
        }
        private static NodeTaskConversionOptions OptionsWithoutAdditionalInputs()
        {
            return new NodeTaskConversionOptions(AdditionalInputs: null,
                DefaultStorageAccountName: DefaultStorageAccountName,
                NodeManagedIdentityResourceId: ManagedIdentityResourceId);
        }

        private static TesTask GetTestTesTask()
        {
            return JsonConvert.DeserializeObject<TesTask>(File.ReadAllText("testask1.json"));
        }
    }
}
