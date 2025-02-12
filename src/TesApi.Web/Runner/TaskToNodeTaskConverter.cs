// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using CommonUtilities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tes.Extensions;
using Tes.Models;
using Tes.Runner.Models;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Options;
using TesApi.Web.Storage;
using FileType = Tes.Runner.Models.FileType;

namespace TesApi.Web.Runner
{
    /// <summary>
    /// Handles the creation of a NodeTask from a TesTask. This class also handles the creation of the NodeTask's inputs and outputs. With the following key functionality:
    /// 1 .- Handles content inputs by creating them as blobs in storage and setting the NodeTasks' inputs to the blobs' URLs.
    /// 2 .- Handles local file paths in inputs when Cromwell is using the local filesystem with a blob FUSE driver.
    /// </summary>
    public class TaskToNodeTaskConverter
    {
        /// <summary>
        /// Metrics file name
        /// </summary>
        public const string MetricsFileName = "metrics.txt";
        /// <summary>
        /// Batch task working directory environment variable
        /// </summary>
        public const string BatchTaskWorkingDirEnvVar = "%AZ_BATCH_TASK_WORKING_DIR%";

        private readonly string pathParentDirectory = BatchTaskWorkingDirEnvVar;
        private readonly string containerMountParentDirectory = BatchTaskWorkingDirEnvVar;
        private readonly IStorageAccessProvider storageAccessProvider;

        private readonly TerraOptions terraOptions;
        private readonly ILogger<TaskToNodeTaskConverter> logger;
        private readonly IList<ExternalStorageContainerInfo> externalStorageContainers;
        private readonly IAzureProxy azureProxy;
        private readonly AzureEnvironmentConfig azureCloudIdentityConfig;

        /// <summary>
        /// Constructor of TaskToNodeTaskConverter
        /// </summary>
        /// <param name="terraOptions"></param>
        /// <param name="storageOptions"></param>
        /// <param name="storageAccessProvider"></param>
        /// <param name="azureProxy"></param>
        /// <param name="azureCloudIdentityConfig"></param>
        /// <param name="logger"></param>
        public TaskToNodeTaskConverter(IOptions<TerraOptions> terraOptions, IOptions<StorageOptions> storageOptions, IStorageAccessProvider storageAccessProvider, IAzureProxy azureProxy, AzureEnvironmentConfig azureCloudIdentityConfig, ILogger<TaskToNodeTaskConverter> logger)
        {
            ArgumentNullException.ThrowIfNull(terraOptions);
            ArgumentNullException.ThrowIfNull(storageOptions);
            ArgumentNullException.ThrowIfNull(storageAccessProvider);
            ArgumentNullException.ThrowIfNull(azureProxy);
            ArgumentNullException.ThrowIfNull(azureCloudIdentityConfig);
            ArgumentNullException.ThrowIfNull(logger);

            this.terraOptions = terraOptions.Value;
            this.logger = logger;
            this.storageAccessProvider = storageAccessProvider;
            this.azureProxy = azureProxy;
            this.azureCloudIdentityConfig = azureCloudIdentityConfig;
            externalStorageContainers = StorageUrlUtils.GetExternalStorageContainerInfos(storageOptions.Value);
        }

        /// <summary>
        /// Parameter-less constructor for mocking
        /// </summary>
        protected TaskToNodeTaskConverter() { }

        /// <summary>
        /// Generates <see cref="NodeTaskResolverOptions"/>.
        /// </summary>
        /// <param name="task">The TES task.</param>
        /// <param name="nodeTaskConversionOptions">The node task conversion options.</param>
        /// <returns>Environment required for runner to retrieve blobs from storage.</returns>
        public virtual NodeTaskResolverOptions ToNodeTaskResolverOptions(TesTask task, NodeTaskConversionOptions nodeTaskConversionOptions)
        {
            try
            {
                var builder = new NodeTaskBuilder();
                builder.WithAzureCloudIdentityConfig(azureCloudIdentityConfig)
                    .WithStorageEventSink(storageAccessProvider.GetInternalTesBlobUrlWithoutSasToken(blobPath: string.Empty))
                    .WithResourceIdManagedIdentity(GetNodeManagedIdentityResourceId(nodeTaskConversionOptions.GlobalManagedIdentity, task));

                if (terraOptions is not null && !string.IsNullOrEmpty(terraOptions.WsmApiHost))
                {
                    logger.LogInformation("Setting up Terra as the runtime environment for the runner");
                    builder.WithTerraAsRuntimeEnvironment(terraOptions.WsmApiHost, terraOptions.LandingZoneApiHost,
                        terraOptions.SasAllowedIpRange);
                }

                var runtimeOptions = builder.Build().RuntimeOptions;
                runtimeOptions.StorageEventSink.TargetUrl = default;

                return new()
                {
                    RuntimeOptions = runtimeOptions,
                    TransformationStrategy = runtimeOptions.StorageEventSink.TransformationStrategy,
                };
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to create the node task resolver options.");
                throw;
            }
        }

        /// <summary>
        /// Converts TesTask to a new NodeTask
        /// </summary>
        /// <param name="task">Node task</param>
        /// <param name="nodeTaskConversionOptions"></param>
        /// <param name="cancellationToken"></param>
        public virtual async Task<NodeTask> ToNodeTaskAsync(TesTask task, NodeTaskConversionOptions nodeTaskConversionOptions, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(task);

            try
            {
                var builder = new NodeTaskBuilder();

                builder.WithId(task.Id)
                    .WithAzureCloudIdentityConfig(azureCloudIdentityConfig)
                    .WithMountParentDirectory(containerMountParentDirectory)
                    .WithResourceIdManagedIdentity(GetNodeManagedIdentityResourceId(task, nodeTaskConversionOptions.GlobalManagedIdentity))
                    .WithAcrPullResourceIdManagedIdentity(nodeTaskConversionOptions.AcrPullIdentity)
                    .WithWorkflowId(task.WorkflowId)
                    .WithContainerVolumes(task.Volumes)
                    .WithExecutors(task.Executors)
                    .WithContainerMountParentDirectory(containerMountParentDirectory)
                    .WithStorageEventSink(storageAccessProvider.GetInternalTesBlobUrlWithoutSasToken(blobPath: string.Empty))
                    .WithLogPublisher(storageAccessProvider.GetInternalTesTaskBlobUrlWithoutSasToken(task, blobPath: string.Empty))
                    .WithDrsHubUrl(nodeTaskConversionOptions.DrsHubApiHost)
                    .WithOnUploadSetContentMD5(nodeTaskConversionOptions.SetContentMd5OnUpload)
                    .WithMetricsFile(MetricsFileName);

                switch (nodeTaskConversionOptions.VmFamilyGroup)
                {
                    case BatchScheduler.VmFamilySeries.standardNCFamilies:
                    case BatchScheduler.VmFamilySeries.standardNDFamilies:
                    case BatchScheduler.VmFamilySeries.standardNVFamilies:
                        builder.WithGpuSupport();
                        break;
                }

                if (terraOptions is not null && !string.IsNullOrEmpty(terraOptions.WsmApiHost))
                {
                    logger.LogInformation("Setting up Terra as the runtime environment for the runner");
                    builder.WithTerraAsRuntimeEnvironment(terraOptions.WsmApiHost, terraOptions.LandingZoneApiHost,
                        terraOptions.SasAllowedIpRange);
                }

                await BuildInputsAsync(task, builder, nodeTaskConversionOptions.AdditionalInputs, nodeTaskConversionOptions.DefaultStorageAccountName, cancellationToken);

                BuildOutputs(task, nodeTaskConversionOptions.DefaultStorageAccountName, builder);

                AddTaskOutputs(task, builder);

                return builder.Build();
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to convert the TES task to a Node Task");
                throw;
            }
        }

        private void AddTaskOutputs(TesTask task, NodeTaskBuilder builder)
        {
            foreach (var (path, url) in new List<string>(["stderr.txt", "stdout.txt", MetricsFileName])
                .Select(file => (Path: $"/{file}", Url: storageAccessProvider.GetInternalTesTaskBlobUrlWithoutSasToken(task, file))))
            {
                builder.WithOutputUsingCombinedTransformationStrategy(
                    AppendParentDirectoryIfSet(path, $"%{NodeTaskBuilder.BatchTaskDirEnvVarName}%"),
                    url.AbsoluteUri,
                    fileType: FileType.File);
            }
        }

        private void BuildOutputs(TesTask task, string defaultStorageAccount, NodeTaskBuilder builder)
        {
            if (task.Outputs is not null)
            {
                logger.LogInformation(@"Mapping {TaskOutputsCount} outputs", task.Outputs.Count);

                var outputs = PrepareLocalOutputsForMapping(task, defaultStorageAccount);

                MapOutputs(outputs, pathParentDirectory, containerMountParentDirectory, builder);
            }
        }

        private static List<TesOutput> PrepareLocalOutputsForMapping(TesTask task, string defaultStorageAccount)
        {
            var outputs = new List<TesOutput>();
            if (task.Outputs is null)
            {
                return outputs;
            }

            foreach (var output in task.Outputs)
            {
                var preparedOutput = PrepareLocalOrLocalCromwellFileOutput(output, defaultStorageAccount);

                if (preparedOutput != null)
                {
                    outputs.Add(preparedOutput);
                    continue;
                }

                outputs.Add(output);
            }

            return outputs;
        }

        private static TesOutput PrepareLocalOrLocalCromwellFileOutput(TesOutput output, string defaultStorageAccount)
        {
            if (StorageUrlUtils.IsLocalAbsolutePath(output.Url))
            {
                return new TesOutput()
                {
                    Name = output.Name,
                    Description = output.Description,
                    Path = output.Path,
                    Url = StorageUrlUtils.ConvertLocalPathOrCromwellLocalPathToUrl(output.Url, defaultStorageAccount),
                    Type = output.Type
                };
            }

            return default;
        }

        private async Task BuildInputsAsync(TesTask task, NodeTaskBuilder builder, IList<TesInput> additionalInputs,
            string defaultStorageAccount, CancellationToken cancellationToken)
        {
            if (task.Inputs is not null || additionalInputs is not null)
            {
                logger.LogInformation($"Mapping inputs");

                var inputs = await PrepareInputsForMappingAsync(task, defaultStorageAccount, cancellationToken);

                //add additional inputs if not already set
                var distinctAdditionalInputs = additionalInputs?
                    .Where(additionalInput => !inputs.Any(input =>
                        input.Path != null && input.Path.Equals(additionalInput.Path, StringComparison.OrdinalIgnoreCase)))
                    .ToList();

                if (distinctAdditionalInputs != null)
                {
                    inputs.AddRange(distinctAdditionalInputs);
                }

                await MapInputsAsync(inputs, pathParentDirectory, builder, cancellationToken);
            }
        }

        private async Task<List<TesInput>> PrepareInputsForMappingAsync(TesTask tesTask, string defaultStorageAccountName,
            CancellationToken cancellationToken)
        {
            var inputs = new Dictionary<string, TesInput>();

            if (tesTask.Inputs is null)
            {
                return [];
            }

            foreach (var input in tesTask.Inputs)
            {
                var key = $"{input.Path}{input.Url}";

                logger.LogInformation(@"Preparing input {InputPath}", input.Path);

                if (input.Streamable == true) // Don't download files where localization_optional is set to true in WDL (corresponds to "Streamable" property being true on TesInput)
                {
                    continue;
                }

                if (inputs.ContainsKey(key))
                {
                    logger.LogWarning(@"Input {InputPath} has the same path and URL as another input and will be ignored", input.Path);
                    continue;
                }

                var preparedInput = await PrepareContentInputAsync(tesTask, input, cancellationToken);

                if (preparedInput != null)
                {
                    logger.LogInformation(@"Input {InputPath} is a content input", input.Path);
                    inputs.Add(key, preparedInput);
                    continue;
                }

                await ResolveInputType(input, cancellationToken);

                preparedInput = PrepareLocalFileInput(input, defaultStorageAccountName);

                if (preparedInput != null)
                {
                    logger.LogInformation(@"Input {InputPath} is a local input", input.Path);

                    inputs.Add(key, preparedInput);
                    continue;
                }

                preparedInput = PrepareExternalStorageAccountInput(input);

                if (preparedInput != null)
                {
                    logger.LogInformation(@"Input {InputPath} is an external storage account input", input.Path);

                    inputs.Add(key, preparedInput);
                    continue;
                }

                logger.LogInformation(@"Input {InputPath} is a regular input", input.Path);

                inputs.Add(key, input);
            }

            return [.. inputs.Values];
        }

        private async Task ResolveInputType(TesInput input, CancellationToken cancellationToken)
        {
            if (input.Type != default)
            {
                return; // Already set
            }

            var uri = (await storageAccessProvider.IsPublicHttpUrlAsync(input.Url, cancellationToken))
                ? new(input.Url)
                : await storageAccessProvider.MapLocalPathToSasUrlAsync(input.Url, cancellationToken, default, getContainerSas: true);

            if (uri is null)
            {
                return; // Not azure storage. We'll let other parts of the system handle it.
            }

            try
            {
                input.Type = (await storageAccessProvider.GetBlobUrlsAsync(
                        uri,
                        cancellationToken))
                    .Any()
                    ? TesFileType.DIRECTORY
                    : TesFileType.FILE;
            }
            catch (Azure.RequestFailedException)
            {
                input.Type = TesFileType.FILE; // Likely not azure storage. We only support directory URLs in known blob-style storage containers.
            }
        }

        /// <summary>
        /// This returns the node managed identity resource id from the task if it is set, otherwise it returns the global managed identity.
        /// If the value in the workflow identity is not a full resource id, it is assumed to be the name. In this case, the resource id is constructed from the name.
        /// </summary>
        /// <param name="task"></param>
        /// <param name="globalManagedIdentity"></param>
        /// <returns></returns>
        public string GetNodeManagedIdentityResourceId(TesTask task, string globalManagedIdentity)
        {
            var workflowId =
                task.Resources?.GetBackendParameterValue(TesResources.SupportedBackendParameters
                    .workflow_execution_identity);

            if (string.IsNullOrWhiteSpace(workflowId))
            {
                if (!NodeTaskBuilder.IsValidManagedIdentityResourceId(globalManagedIdentity))
                {
                    throw new TesException("NoManagedIdentityForRunner", "Neither the TES server nor the task provided an Azure User Managed Identity for the task runner. Please check your configuration.");
                }

                return globalManagedIdentity;
            }

            if (NodeTaskBuilder.IsValidManagedIdentityResourceId(workflowId))
            {
                return workflowId;
            }

            return azureProxy.GetManagedIdentityInBatchAccountResourceGroup(workflowId);
        }

        /// <summary>
        /// This returns the global managed identity if it is set, otherwise it returns the node managed identity resource id from the task.
        /// If the value in the workflow identity is not a full resource id, it is assumed to be the name. In this case, the resource id is constructed from the name.
        /// </summary>
        /// <param name="globalManagedIdentity"></param>
        /// <param name="task"></param>
        /// <returns></returns>
        public string GetNodeManagedIdentityResourceId(string globalManagedIdentity, TesTask task)
        {
            if (NodeTaskBuilder.IsValidManagedIdentityResourceId(globalManagedIdentity))
            {
                return globalManagedIdentity;
            }

            var workflowId =
                task.Resources?.GetBackendParameterValue(TesResources.SupportedBackendParameters
                    .workflow_execution_identity);

            if (string.IsNullOrWhiteSpace(workflowId))
            {
                throw new TesException("NoManagedIdentityForRunner", "Neither the TES server nor the task provided an Azure User Managed Identity for the task runner. Please check your configuration.");
            }

            if (NodeTaskBuilder.IsValidManagedIdentityResourceId(workflowId))
            {
                return workflowId;
            }

            return azureProxy.GetManagedIdentityInBatchAccountResourceGroup(workflowId);
        }


        private string GetSasTokenFromExternalStorageAccountIfSet(string storageAccount)
        {
            var configuredExternalStorage = externalStorageContainers.FirstOrDefault(e => e.AccountName.Equals(storageAccount, StringComparison.OrdinalIgnoreCase));
            return configuredExternalStorage is null ? string.Empty : configuredExternalStorage.SasToken;
        }

        /// <summary>
        /// This method will prepare an external storage account if provided as Azure storage URL
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        private TesInput PrepareExternalStorageAccountInput(TesInput input)
        {

            if (!StorageUrlUtils.IsValidAzureStorageAccountUri(input.Url))
            {
                return default;
            }

            var blobUrl = AppendSasTokenIfExternalAccount(input.Url);

            return new TesInput
            {
                Name = input.Name,
                Description = input.Description,
                Path = input.Path,
                Type = input.Type,
                Url = blobUrl,
            };
        }

        private string AppendSasTokenIfExternalAccount(string url)
        {
            var blobUrl = new BlobUriBuilder(new Uri(url));

            var sasToken = GetSasTokenFromExternalStorageAccountIfSet(blobUrl.AccountName);

            blobUrl.Query = StorageUrlUtils.SetOrAddSasTokenToQueryString(blobUrl.Query, sasToken);

            return blobUrl.ToUri().ToString();
        }

        /// <summary>
        /// This method converts a local path /storageaccount/cont/file to the corresponding Azure Storage URL.
        /// If the path is a Cromwell local path, the default storage account name is used to construct the URL.
        /// If the path is a local path, the storage account name is extracted from the path.
        /// If the storage account is a configured external storage account, the SAS token from the configuration is added to the URL.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="defaultStorageAccountName"></param>
        /// <returns></returns>
        private TesInput PrepareLocalFileInput(TesInput input, string defaultStorageAccountName)
        {
            //When Cromwell runs in local mode with a Blob FUSE drive, the URL property may contain an absolute path.
            //The path must be converted to a URL. For Terra this scenario doesn't apply.
            if (StorageUrlUtils.IsLocalAbsolutePath(input.Url))
            {
                var convertedUrl = StorageUrlUtils.ConvertLocalPathOrCromwellLocalPathToUrl(input.Url, defaultStorageAccountName);

                var blobUrl = AppendSasTokenIfExternalAccount(convertedUrl);

                return new TesInput()
                {
                    Name = input.Name,
                    Description = input.Description,
                    Path = input.Path,
                    Url = blobUrl,
                    Type = input.Type
                };
            }

            return default;
        }

        private async Task<TesInput> UploadContentAndCreateTesInputAsync(TesTask tesTask, string inputPath,
            string content,
            CancellationToken cancellationToken)
        {
            var inputFileUrl =
                await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(tesTask, Guid.NewGuid().ToString(),
                    cancellationToken);

            //return the URL without the SAS token, the runner will add it using the transformation strategy
            await storageAccessProvider.UploadBlobAsync(inputFileUrl, content, cancellationToken);

            var inputUrl = StorageUrlUtils.RemoveQueryStringFromUrl(inputFileUrl);

            logger.LogInformation(@"Successfully uploaded content input as a new blob at: {InputUrl}", inputUrl);

            return new TesInput
            {
                Path = inputPath,
                Url = inputUrl,
                Type = TesFileType.FILE,
            };
        }

        private async Task<TesInput> PrepareContentInputAsync(TesTask tesTask, TesInput input,
            CancellationToken cancellationToken)
        {
            if (String.IsNullOrWhiteSpace(input?.Content))
            {
                return default;
            }

            logger.LogInformation(@"The input is content. Uploading its content to the internal storage location. Input path:{InputPath}", input.Path);

            if (input.Type == TesFileType.DIRECTORY)
            {
                throw new ArgumentException("Content inputs cannot be directories.", nameof(input));
            }

            input.Type = TesFileType.FILE;

            return await UploadContentAndCreateTesInputAsync(tesTask, input.Path, input.Content, cancellationToken);
        }

        private static void MapOutputs(List<TesOutput> outputs, string pathParentDirectory, string containerMountParentDirectory,
            NodeTaskBuilder builder)
        {
            outputs?.ForEach(output =>
            {
                builder.WithOutputUsingCombinedTransformationStrategy(
                    AppendParentDirectoryIfSet(output.Path, pathParentDirectory), output.Url, ToNodeTaskFileType(output.Type));
            });
        }

        private async Task MapInputsAsync(List<TesInput> inputs, string pathParentDirectory, NodeTaskBuilder builder, CancellationToken cancellationToken)
        {
            if (inputs is null || inputs.Count == 0)
            {
                return;
            }

            foreach (var input in inputs)
            {
                if (input.Type == TesFileType.FILE)
                {
                    AddInputToBuilder(input.Path, input.Url);
                }
                else
                {
                    // Nextflow directory example
                    // input.Url = /storageaccount/work/tmp/cf/d1be3bf1f9622165d553fed8ddd226/bin
                    // input.Path = /work/tmp/cf/d1be3bf1f9622165d553fed8ddd226/bin
                    var blobDirectoryUrlWithSasToken = await storageAccessProvider.IsPublicHttpUrlAsync(input.Url, cancellationToken)
                        ? new(input.Url)
                        : await storageAccessProvider.MapLocalPathToSasUrlAsync(input.Url, cancellationToken, default, getContainerSas: true);
                    var blobDirectoryUrlWithoutSasToken = blobDirectoryUrlWithSasToken.GetLeftPart(UriPartial.Path);
                    IList<Uri> blobAbsoluteUrls;

                    try
                    {
                        blobAbsoluteUrls = await storageAccessProvider.GetBlobUrlsAsync(blobDirectoryUrlWithSasToken, cancellationToken);
                    }
                    catch (Azure.RequestFailedException)
                    {
                        blobAbsoluteUrls = [new(input.Url)]; // Pass this off to the runner if the input type has not been specified or determined
                    }

                    if (input.Type == default && blobAbsoluteUrls.Count == 0)
                    {
                        AddInputToBuilder(input.Path, input.Url);
                        continue;
                    }

                    foreach (var blobAbsoluteUrl in blobAbsoluteUrls)
                    {
                        var blobSuffix = blobAbsoluteUrl.AbsoluteUri[blobDirectoryUrlWithoutSasToken.TrimEnd('/').Length..].TrimStart('/');
                        var localPath = $"{input.Path.TrimEnd('/')}/{blobSuffix}";

                        AddInputToBuilder(localPath, blobAbsoluteUrl.AbsoluteUri);
                    }
                }
            }

            void AddInputToBuilder(string path, string url)
            {
                builder.WithInputUsingCombinedTransformationStrategy(
                    AppendParentDirectoryIfSet(path, pathParentDirectory), url);
            }
        }

        private static FileType? ToNodeTaskFileType(TesFileType outputType)
        {
            return outputType switch
            {
                TesFileType.FILE => FileType.File,
                TesFileType.DIRECTORY => FileType.Directory,
                _ => FileType.File
            };
        }

        private static string AppendParentDirectoryIfSet(string inputPath, string pathParentDirectory)
        {
            if (!string.IsNullOrWhiteSpace(pathParentDirectory))
            {
                //it is assumed the input path is an absolute path
                return $"{pathParentDirectory}{inputPath}";
            }

            return inputPath;
        }
    }

    /// <summary>
    /// Additional configuration options for the Node runner.
    /// </summary>
    /// <param name="AdditionalInputs"></param>
    /// <param name="DefaultStorageAccountName"></param>
    /// <param name="GlobalManagedIdentity"></param>
    /// <param name="AcrPullIdentity"></param>
    /// <param name="DrsHubApiHost"></param>
    /// <param name="SetContentMd5OnUpload"></param>
    /// <param name="VmFamilyGroup"></param>
    public record NodeTaskConversionOptions(IList<TesInput> AdditionalInputs = default, string DefaultStorageAccountName = default,
        string GlobalManagedIdentity = default, string AcrPullIdentity = default, string DrsHubApiHost = default, bool SetContentMd5OnUpload = false,
            BatchScheduler.VmFamilySeries VmFamilyGroup = default);
}
