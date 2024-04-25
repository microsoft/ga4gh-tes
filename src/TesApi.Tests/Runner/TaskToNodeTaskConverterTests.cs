// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using CommonUtilities.AzureCloud;
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
        private BatchAccountOptions batchAccountOptions;

        private const string SasToken = "sv=2019-12-12&ss=bfqt&srt=sco&spr=https&st=2023-09-27T17%3A32%3A57Z&se=2023-09-28T17%3A32%3A57Z&sp=rwdlacupx&sig=SIGNATURE";

        const string DefaultStorageAccountName = "default";
        static readonly Uri InternalBlobUrl = new("http://foo.bar/tes-internal");
        static readonly Uri InternalBlobUrlWithSas = new($"{InternalBlobUrl}?{SasToken}");
        const string GlobalManagedIdentity = $@"/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/globalId";
        private const string DrsHubApiHost = "https://drshub.foo.bar";


        const string ExternalStorageAccountName = "external";
        const string ExternalStorageContainer =
            $"https://{ExternalStorageAccountName}{StorageUrlUtils.DefaultBlobEndpointHostNameSuffix}/cont";
        const string ExternalStorageContainerWithSas =
            $"{ExternalStorageContainer}?{SasToken}";
        const string ResourceGroup = "myResourceGroup";
        const string SubscriptionId = "12345678-1234-5678-abcd-1234567890ab";

        [TestInitialize]
        public void SetUp()
        {
            terraOptions = new TerraOptions();
            storageOptions = new StorageOptions() { ExternalStorageContainers = ExternalStorageContainerWithSas };
            batchAccountOptions = new BatchAccountOptions() { SubscriptionId = SubscriptionId, ResourceGroup = ResourceGroup };
            storageAccessProviderMock = new Mock<IStorageAccessProvider>();
            storageAccessProviderMock.Setup(x =>
                    x.GetInternalTesTaskBlobUrlAsync(It.IsAny<TesTask>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(InternalBlobUrlWithSas);
            storageAccessProviderMock.Setup(x =>
                    x.GetInternalTesBlobUrlWithoutSasToken(It.IsAny<string>()))
                .Returns(InternalBlobUrl);
            storageAccessProviderMock.Setup(x =>
                    x.GetInternalTesTaskBlobUrlWithoutSasToken(It.IsAny<TesTask>(), It.IsAny<string>()))
                .Returns(InternalBlobUrl);

            var azureCloudIdentityConfig = AzureCloudConfig.CreateAsync().Result.AzureEnvironmentConfig;
            taskToNodeTaskConverter = new TaskToNodeTaskConverter(Options.Create(terraOptions), storageAccessProviderMock.Object,
                Options.Create(storageOptions), Options.Create(batchAccountOptions), azureCloudIdentityConfig, new NullLogger<TaskToNodeTaskConverter>());
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
            Assert.AreEqual(InternalBlobUrl.AbsoluteUri, nodeTask.Inputs!.Find(i => i.Path == $"{TaskToNodeTaskConverter.BatchTaskWorkingDirEnvVar}{contentInput!.Path}")!.SourceUrl);
        }

        [DataTestMethod]
        [DataRow("myIdentity", $@"/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/myIdentity")]
        [DataRow($@"/subscriptions/{SubscriptionId}/resourcegroups/{ResourceGroup}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/myIdentity", $@"/subscriptions/{SubscriptionId}/resourcegroups/{ResourceGroup}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/myIdentity")]
        [DataRow($@"/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/myIdentity", $@"/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/myIdentity")]
        [DataRow("", GlobalManagedIdentity)]
        [DataRow(null, GlobalManagedIdentity)]
        public void GetNodeManagedIdentityResourceId_ResourceIsProvided_ReturnsExpectedResult(string workflowIdentity, string expectedResourceId)
        {
            tesTask.Resources = new TesResources()
            {
                BackendParameters = new Dictionary<string, string>()
                {
                    {TesResources.SupportedBackendParameters.workflow_execution_identity.ToString(), workflowIdentity}
                }
            };

            var resourceId = taskToNodeTaskConverter.GetNodeManagedIdentityResourceId(tesTask, GlobalManagedIdentity);

            Assert.AreEqual(expectedResourceId, resourceId);
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
        public async Task ToNodeTaskAsync_DuplicateInputsAreRemoved()
        {
            var options = OptionsWithoutAdditionalInputs();

            // Add a duplicate to tesTask.Inputs
            var duplicateInput = tesTask.Inputs.First();
            tesTask.Inputs.Add(duplicateInput);

            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, options, CancellationToken.None);

            Assert.IsNotNull(nodeTask);

            Assert.IsTrue(nodeTask.Inputs!.Count == tesTask.Inputs.Count - 1);
        }


        [TestMethod]
        public async Task ToNodeTaskAsync_ExternalStorageInputsProvided_NodeTesTaskContainsUrlsWithSasTokens()
        {
            tesTask.Inputs = new List<TesInput>
            {
                new()
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
            tesTask.Inputs = null;
            tesTask.Outputs = null;
            var options = OptionsWithoutAdditionalInputs();
            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, options, CancellationToken.None);
            Assert.IsNotNull(nodeTask);
            Assert.IsNull(nodeTask.Inputs);
            Assert.IsNull(nodeTask.Outputs);
        }

        [TestMethod]
        public async Task
            ToNodeTaskAsync_TesTaskWithNoInputsAndOutputsAndAdditionalInputs_NodeTaskContainsOnlyAdditionalInputs()
        {
            tesTask.Inputs = null;
            tesTask.Outputs = null;
            var options = OptionsWithAdditionalInputs();
            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, options, CancellationToken.None);

            Assert.IsNotNull(nodeTask);
            Assert.AreEqual(2, nodeTask.Inputs!.Count);
            Assert.IsNull(nodeTask.Outputs);
        }

        [TestMethod]
        public async Task
            ToNodeTaskAsync_TesTaskWithNoInputsAndOutputsAndNoAdditionalInputs_NodeTaskContainsNoInputsAndOutputs()
        {
            tesTask.Inputs = null;
            tesTask.Outputs = null;
            var options = OptionsWithoutAdditionalInputs();
            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, options, CancellationToken.None);
            Assert.IsNotNull(nodeTask);
            Assert.IsNull(nodeTask.Inputs);
            Assert.IsNull(nodeTask.Outputs);
        }

        [TestMethod]
        public async Task
            ToNodeTaskAsync_TesTaskWithOnlyStreamableInputsAndOutputsAndNoAdditionalInputs_NodeTaskContainsNoInputsAndOutputs()
        {
            tesTask.Inputs = new List<TesInput>
            {
                new()
                {
                    Name = "local input",
                    Streamable = true,
                    Path = "/cromwell-executions/file",
                    Url = "/cromwell-executions/file",
                    Type = TesFileType.FILEEnum
                }
            };

            tesTask.Outputs = null;
            var options = OptionsWithoutAdditionalInputs();
            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, options, CancellationToken.None);
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
                new()
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
                new()
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

        [TestMethod]
        public async Task ToNodeTaskAsync_DrsHubApiHostIsProvided_DrsHubApiHostIsSetInNodeTask()
        {
            var options = OptionsWithoutAdditionalInputs();

            var nodeTask = await taskToNodeTaskConverter.ToNodeTaskAsync(tesTask, options, CancellationToken.None);

            Assert.IsNotNull(nodeTask);
            Assert.AreEqual(DrsHubApiHost, nodeTask.RuntimeOptions.Terra!.DrsHubApiHost);
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
                GlobalManagedIdentity: GlobalManagedIdentity);
        }
        private static NodeTaskConversionOptions OptionsWithoutAdditionalInputs()
        {
            return new NodeTaskConversionOptions(AdditionalInputs: null,
                DefaultStorageAccountName: DefaultStorageAccountName,
                GlobalManagedIdentity: GlobalManagedIdentity,
                DrsHubApiHost: DrsHubApiHost);
        }

        private static TesTask GetTestTesTask()
        {
            return JsonConvert.DeserializeObject<TesTask>(File.ReadAllText("testask1.json"));
        }
    }
}
