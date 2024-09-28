// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using CommonUtilities;
using Tes.Runner.Models;

namespace TesApi.Web.Runner
{
    /// <summary>
    /// Builder of NodeTask
    /// </summary>
    public class NodeTaskBuilder
    {
        /// <summary>
        /// Name of the environment variable that contains the path to the task directory
        /// </summary>
        public const string BatchTaskDirEnvVarName = "AZ_BATCH_TASK_DIR";

        internal const string BatchTaskDirEnvVar = $"${BatchTaskDirEnvVarName}";

        private const string ManagedIdentityResourceIdPattern = @"^/subscriptions/[^/]+/resourcegroups/[^/]+/providers/Microsoft.ManagedIdentity/userAssignedIdentities/[^/]+$";

        private const string DefaultDockerImageTag = "latest";
        private readonly NodeTask nodeTask;
        const string NodeTaskOutputsMetricsFormat = "FileUploadSizeInBytes={Size}";
        const string NodeTaskInputsMetricsFormat = "FileDownloadSizeInBytes={Size}";


        /// <summary>
        /// Creates a new builder
        /// </summary>
        public NodeTaskBuilder()
        {
            nodeTask = new NodeTask();
        }

        /// <summary>
        /// Creates a new builder with an existing NodeTask
        /// </summary>
        /// <param name="nodeTask"></param>
        public NodeTaskBuilder(NodeTask nodeTask)
        {
            this.nodeTask = nodeTask;
        }

        /// <summary>
        /// Sets the Id of the NodeTask
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public NodeTaskBuilder WithId(string id)
        {
            ArgumentException.ThrowIfNullOrEmpty(id, nameof(id));

            nodeTask.Id = id;
            return this;
        }

        /// <summary>
        /// Sets the workflow ID of the NodeTask
        /// </summary>
        /// <param name="workflowId"></param>
        /// <returns></returns>
        public NodeTaskBuilder WithWorkflowId(string workflowId)
        {
            nodeTask.WorkflowId = workflowId ?? Guid.NewGuid().ToString();
            return this;
        }

        /// <summary>
        /// Sets container volumes
        /// </summary>
        /// <param name="volumes"></param>
        /// <returns></returns>
        public NodeTaskBuilder WithContainerVolumes(List<string> volumes)
        {
            nodeTask.ContainerVolumes = volumes;
            return this;
        }

        /// <summary>
        /// Sets the executors
        /// </summary>
        /// <param name="executors"></param>
        /// <returns></returns>
        public NodeTaskBuilder WithExecutors(List<Tes.Models.TesExecutor> executors)
        {
            ArgumentNullException.ThrowIfNull(executors);

            if (executors.Count == 0)
            {
                throw new ArgumentException("The list executors can't be empty.", nameof(executors));
            }

            nodeTask.Executors = [.. executors.Select(ConvertExecutor)];
            return this;
        }

        /// <summary>
        /// Creates an input for the NodeTask using a combined transformation strategy.
        /// If the Terra is set as the runtime environment, the transformation strategy will be CombinedTerra.
        /// Otherwise, the transformation strategy will be CombinedAzureResourceManager.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="sourceUrl"></param>
        /// <returns></returns>
        public NodeTaskBuilder WithInputUsingCombinedTransformationStrategy(string path, string sourceUrl)
        {
            ArgumentException.ThrowIfNullOrEmpty(path, nameof(path));
            TransformationStrategy transformationStrategy = GetCombinedTransformationStrategyFromRuntimeOptions();

            if (path.Contains('?'))
            {
                // Cromwell bug - when the WDL input contains a SAS token, it's being included in the path
                // Remove the SAS token
                path = path[..path.LastIndexOf('?')];
            }

            if (sourceUrl.Contains('?'))
            {
                // When the input is a SAS token, don't transform
                transformationStrategy = TransformationStrategy.None;
            }

            nodeTask.Inputs ??= new List<FileInput>();

            nodeTask.Inputs.Add(
                new FileInput()
                {
                    Path = path,
                    SourceUrl = sourceUrl,
                    TransformationStrategy = transformationStrategy
                }
            );

            return this;
        }

        /// <summary>
        /// Creates an output for the NodeTask using a combined transformation strategy.
        /// If the Terra is set as the runtime environment, the transformation strategy will be CombinedTerra.
        /// Otherwise, the transformation strategy will be CombinedAzureResourceManager.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="targetUrl"></param>
        /// <param name="fileType"></param>
        /// <returns></returns>
        public NodeTaskBuilder WithOutputUsingCombinedTransformationStrategy(string path, string targetUrl,
            FileType? fileType)
        {
            ArgumentException.ThrowIfNullOrEmpty(path, nameof(path));
            ArgumentException.ThrowIfNullOrEmpty(targetUrl, nameof(targetUrl));
            nodeTask.Outputs ??= [];
            nodeTask.Outputs.Add(
                new FileOutput()
                {
                    Path = path,
                    TargetUrl = targetUrl,
                    TransformationStrategy = GetCombinedTransformationStrategyFromRuntimeOptions(),
                    FileType = fileType ?? FileType.File
                }
                );
            return this;
        }

        /// <summary>
        /// Set the host path for the container root
        /// </summary>
        /// <param name="mountParentDirectory"></param>
        /// <returns></returns>
        public NodeTaskBuilder WithMountParentDirectory(string mountParentDirectory)
        {
            nodeTask.MountParentDirectory = mountParentDirectory;
            return this;
        }

        /// <summary>
        /// Parses an Executor from a TesExecutor
        /// </summary>
        /// <param name="executor"></param>
        /// <returns></returns>
        internal static Executor ConvertExecutor(Tes.Models.TesExecutor executor)
        {
            ArgumentNullException.ThrowIfNull(executor);

            if (executor.Command.Count == 0)
            {
                throw new InvalidOperationException("The list commands can't be empty");
            }

            ArgumentException.ThrowIfNullOrWhiteSpace(executor.Image, nameof(executor));

            Executor nodeExecutor = new();

            //check if the image name is a digest
            if (executor.Image.Contains('@'))
            {
                var splitByDigest = executor.Image.Split('@', 2);
                nodeExecutor.ImageName = splitByDigest[0];
                nodeExecutor.ImageTag = splitByDigest[1];
            }
            else
            {
                var splitByTag = executor.Image.Split(':', 2);
                nodeExecutor.ImageName = splitByTag[0];
                nodeExecutor.ImageTag = splitByTag.Length == 2 ? splitByTag[1] : DefaultDockerImageTag;
            }

            nodeExecutor.CommandsToExecute = executor.Command;
            nodeExecutor.ContainerWorkDir = executor.Workdir;
            nodeExecutor.ContainerStdIn = executor.Stdin;
            nodeExecutor.ContainerStdOut = executor.Stdout;
            nodeExecutor.ContainerStdErr = executor.Stderr;
            nodeExecutor.ContainerEnv = executor.Env;
            nodeExecutor.IgnoreError = executor.IgnoreError ?? false;

            return nodeExecutor;
        }

        /// <summary>
        /// Docker container GPU support.
        /// </summary>
        public void WithGpuSupport()
        {
            // https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/docker-specialized.html
            // https://github.com/docker/cli/blob/v24.0.7/opts/gpus_test.go
            nodeTask.ContainerDeviceRequests ??= [];
            nodeTask.ContainerDeviceRequests.Add(new()
            {
                Driver = "nvidia",
                Count = -1,
                Capabilities = [["compute", "utility", "gpu"]],
                Options = []
            });
        }

        /// <summary>
        /// Sets Terra as runtime environment and enables the Terra transformation strategy for URLs in inputs and outputs.
        /// </summary>
        /// <param name="wsmApiHost"></param>
        /// <param name="landingZoneApiHost"></param>
        /// <param name="sasAllowedIpRange"></param>
        /// <returns></returns>
        public NodeTaskBuilder WithTerraAsRuntimeEnvironment(string wsmApiHost, string landingZoneApiHost,
            string sasAllowedIpRange)
        {
            ArgumentException.ThrowIfNullOrEmpty(wsmApiHost, nameof(wsmApiHost));
            ArgumentException.ThrowIfNullOrEmpty(landingZoneApiHost, nameof(landingZoneApiHost));
            nodeTask.RuntimeOptions ??= new RuntimeOptions();

            nodeTask.RuntimeOptions.Terra ??= new TerraRuntimeOptions();

            nodeTask.RuntimeOptions.Terra.WsmApiHost = wsmApiHost;
            nodeTask.RuntimeOptions.Terra.LandingZoneApiHost = landingZoneApiHost;
            nodeTask.RuntimeOptions.Terra.SasAllowedIpRange = sasAllowedIpRange;

            SetCombinedTerraTransformationStrategyForAllTransformations();

            return this;
        }


        /// <summary>
        /// Returns the built NodeTask
        /// </summary>
        /// <returns></returns>
        public NodeTask Build()
        {
            return nodeTask;
        }

        private void SetCombinedTerraTransformationStrategyForAllTransformations()
        {
            if (nodeTask.Inputs != null)
            {
                foreach (var input in nodeTask.Inputs)
                {
                    input.TransformationStrategy = TransformationStrategy.CombinedTerra;
                }
            }

            if (nodeTask.Outputs != null)
            {
                foreach (var output in nodeTask.Outputs)
                {
                    output.TransformationStrategy = TransformationStrategy.CombinedTerra;
                }
            }

            if (nodeTask.RuntimeOptions.StorageEventSink is not null)
            {
                nodeTask.RuntimeOptions.StorageEventSink.TransformationStrategy = TransformationStrategy.CombinedTerra;
            }

            if (nodeTask.RuntimeOptions.StreamingLogPublisher is not null)
            {
                nodeTask.RuntimeOptions.StreamingLogPublisher.TransformationStrategy = TransformationStrategy.CombinedTerra;
            }
        }

        private TransformationStrategy GetCombinedTransformationStrategyFromRuntimeOptions()
        {
            if (nodeTask.RuntimeOptions?.Terra is null)
            {
                return TransformationStrategy.CombinedAzureResourceManager;
            }

            return TransformationStrategy.CombinedTerra;
        }

        /// <summary>
        /// Adds metrics file support for the node task
        /// </summary>
        /// <param name="metricsFileName">Metrics filename</param>
        /// <returns></returns>
        public NodeTaskBuilder WithMetricsFile(string metricsFileName)
        {
            ArgumentException.ThrowIfNullOrEmpty(metricsFileName, nameof(metricsFileName));

            nodeTask.MetricsFilename = metricsFileName;

            nodeTask.OutputsMetricsFormat = NodeTaskOutputsMetricsFormat;

            nodeTask.InputsMetricsFormat = NodeTaskInputsMetricsFormat;

            nodeTask.TimestampMetricsFormats =
            [
                "ExecuteNodeTesTaskStart={Time}",
                "ExecuteNodeTesTaskEnd={Time}",
            ];

            nodeTask.BashScriptMetricsFormats =
            [
                $"echo DiskSizeInKiB=$\"$(df -k --output=size {BatchTaskDirEnvVar} | tail -1)\"",
                $"echo DiskUsedInKiB=$\"$(df -k --output=used {BatchTaskDirEnvVar} | tail -1)\"",
                "echo VmCpuModelName=$\"$(cat /proc/cpuinfo | grep -m1 name | cut -f 2 -d ':' | xargs)\"",
            ];

            return this;
        }

        /// <summary>
        /// Sets managed identity for the node task. If the resource ID is empty or null, the property won't be set.
        /// </summary>
        /// <param name="resourceId">A valid managed identity resource ID</param>
        /// <returns></returns>
        public NodeTaskBuilder WithResourceIdManagedIdentity(string resourceId)
        {
            if (string.IsNullOrEmpty(resourceId))
            {
                return this;
            }

            if (!IsValidManagedIdentityResourceId(resourceId))
            {
                throw new ArgumentException("Invalid resource ID. The ID must be a valid Azure resource ID.", nameof(resourceId));
            }

            nodeTask.RuntimeOptions ??= new RuntimeOptions();
            nodeTask.RuntimeOptions.NodeManagedIdentityResourceId = resourceId;
            return this;
        }

        /// <summary>
        /// Sets the managed identity to be used for ACR pulls for the node task. If the resource ID is empty or null, the property won't be set.
        /// </summary>
        /// <param name="resourceId">A valid managed identity resource ID</param>
        /// <returns></returns>
        public NodeTaskBuilder WithAcrPullResourceIdManagedIdentity(string resourceId)
        {
            if (string.IsNullOrEmpty(resourceId))
            {
                return this;
            }

            if (!IsValidManagedIdentityResourceId(resourceId))
            {
                throw new ArgumentException("Invalid resource ID. The ID must be a valid Azure resource ID.", nameof(resourceId));
            }

            nodeTask.RuntimeOptions ??= new RuntimeOptions();
            nodeTask.RuntimeOptions.AcrPullManagedIdentityResourceId = resourceId;
            return this;
        }

        /// <summary>
        /// (Optional) sets the azure authority host for the node task.  If not set, the default Azure Public cloud is used.
        /// </summary>
        /// <param name="azureCloudIdentityConfig">Azure cloud identity config</param>
        /// <returns></returns>
        public NodeTaskBuilder WithAzureCloudIdentityConfig(AzureEnvironmentConfig azureCloudIdentityConfig)
        {
            if (azureCloudIdentityConfig == null)
            {
                return this;
            }

            nodeTask.RuntimeOptions ??= new RuntimeOptions();
            nodeTask.RuntimeOptions.AzureEnvironmentConfig = azureCloudIdentityConfig;
            return this;
        }

        /// <summary>
        /// Returns true of the value provided is a valid resource id for a managed identity.
        /// </summary>
        /// <param name="resourceId"></param>
        /// <returns></returns>
        public static bool IsValidManagedIdentityResourceId(string resourceId)
        {
            if (string.IsNullOrWhiteSpace(resourceId))
            {
                return false;
            }
            //Ignore the case because constant segments could be lower case, pascal case or camel case.
            // e.g. /resourcegroup/ or /resourceGroup/
            return Regex.IsMatch(resourceId, ManagedIdentityResourceIdPattern, RegexOptions.IgnoreCase);
        }

        /// <summary>
        /// Adds the storage event sink to the node task with its transformation strategy
        /// </summary>
        /// <param name="targetUrl"></param>
        /// <returns></returns>
        public NodeTaskBuilder WithStorageEventSink(Uri targetUrl)
        {
            ArgumentNullException.ThrowIfNull(targetUrl);

            nodeTask.RuntimeOptions ??= new RuntimeOptions();
            nodeTask.RuntimeOptions.StorageEventSink = new StorageTargetLocation()
            {
                TargetUrl = targetUrl.AbsoluteUri,
                TransformationStrategy = GetCombinedTransformationStrategyFromRuntimeOptions()
            };

            return this;
        }

        /// <summary>
        /// Adds the streaming log publisher storage destination to the node task with its transformation strategy
        /// </summary>
        /// <param name="targetUrl"></param>
        /// <returns></returns>
        public NodeTaskBuilder WithLogPublisher(Uri targetUrl)
        {
            ArgumentNullException.ThrowIfNull(targetUrl);

            nodeTask.RuntimeOptions ??= new RuntimeOptions();
            nodeTask.RuntimeOptions.StreamingLogPublisher = new StorageTargetLocation()
            {
                TargetUrl = targetUrl.AbsoluteUri,
                TransformationStrategy = GetCombinedTransformationStrategyFromRuntimeOptions()
            };

            return this;
        }

        /// <summary>
        /// Adds DRS Hub URL to the node task, if the DRS Hub URL is not set, the property won't be set.
        /// </summary>
        /// <param name="drsHubUrl"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public NodeTaskBuilder WithDrsHubUrl(string drsHubUrl)
        {
            if (String.IsNullOrWhiteSpace(drsHubUrl))
            {
                return this;
            }

            var apiHost = GetApiHostFromUrl(drsHubUrl);

            nodeTask.RuntimeOptions ??= new RuntimeOptions();
            nodeTask.RuntimeOptions.Terra ??= new TerraRuntimeOptions();
            nodeTask.RuntimeOptions.Terra.DrsHubApiHost = apiHost;

            return this;
        }

        /// <summary>
        /// Switch to enable setting ContentMD5 on uploads.
        /// </summary>
        /// <param name="enable">Set to <c>true</c> to have the runner calculate and provide the blob content MD5 to the storage account, <c>false</c> otherwise.</param>
        /// <returns></returns>
        public NodeTaskBuilder WithOnUploadSetContentMD5(bool enable)
        {
            nodeTask.RuntimeOptions ??= new RuntimeOptions();
            nodeTask.RuntimeOptions.SetContentMd5OnUpload = enable;

            return this;
        }

        private static string GetApiHostFromUrl(string drsHubUrl)
        {
            var uri = new Uri(drsHubUrl);

            return $"{uri.Scheme}://{uri.Host}";
        }
    }
}
