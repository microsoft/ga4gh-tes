// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Tes.Runner.Models;

namespace TesApi.Web.Runner
{
    /// <summary>
    /// Builder of NodeTask
    /// </summary>
    public class NodeTaskBuilder
    {
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
        /// Sets the container working directory of the NodeTask
        /// </summary>
        /// <param name="workingDirectory"></param>
        /// <returns></returns>
        public NodeTaskBuilder WithContainerWorkingDirectory(string workingDirectory)
        {
            ArgumentException.ThrowIfNullOrEmpty(workingDirectory, nameof(workingDirectory));

            nodeTask.ContainerWorkDir = workingDirectory;
            return this;
        }

        /// <summary>
        /// Creates an input for the NodeTask using a combined transformation strategy.
        /// If the Terra is set as the runtime environment, the transformation strategy will be CombinedTerra.
        /// Otherwise, the transformation strategy will be CombinedAzureResourceManager.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="sourceUrl"></param>
        /// <param name="mountParentDirectory"></param>
        /// <returns></returns>
        public NodeTaskBuilder WithInputUsingCombinedTransformationStrategy(string path, string sourceUrl, string mountParentDirectory)
        {
            ArgumentException.ThrowIfNullOrEmpty(path, nameof(path));

            nodeTask.Inputs ??= new List<FileInput>();

            nodeTask.Inputs.Add(
                new FileInput()
                {
                    MountParentDirectory = mountParentDirectory,
                    Path = path,
                    SourceUrl = sourceUrl,
                    TransformationStrategy = GetCombinedTransformationStrategyFromRuntimeOptions()
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
        /// <param name="mountParentDirectory"></param>
        /// <returns></returns>
        public NodeTaskBuilder WithOutputUsingCombinedTransformationStrategy(string path, string targetUrl,
            FileType? fileType, string mountParentDirectory)
        {
            ArgumentException.ThrowIfNullOrEmpty(path, nameof(path));
            ArgumentException.ThrowIfNullOrEmpty(targetUrl, nameof(targetUrl));
            nodeTask.Outputs ??= new List<FileOutput>();
            nodeTask.Outputs.Add(
                new FileOutput()
                {
                    MountParentDirectory = mountParentDirectory,
                    Path = path,
                    TargetUrl = targetUrl,
                    TransformationStrategy = GetCombinedTransformationStrategyFromRuntimeOptions(),
                    FileType = fileType ?? FileType.File
                }
                );
            return this;
        }

        /// <summary>
        /// Sets the commands to the NodeTask
        /// </summary>
        /// <param name="commands"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public NodeTaskBuilder WithContainerCommands(List<string> commands)
        {
            ArgumentNullException.ThrowIfNull(commands);

            if (commands.Count == 0)
            {
                throw new InvalidOperationException("The list commands can't be empty");
            }

            nodeTask.CommandsToExecute = commands;

            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="image"></param>
        /// <returns></returns>
        public NodeTaskBuilder WithContainerImage(string image)
        {
            ArgumentNullException.ThrowIfNull(image);

            var splitByTag = image.Split(':');

            nodeTask.ImageName = splitByTag[0];
            nodeTask.ImageTag = splitByTag.Length == 2 ? splitByTag[1] : string.Empty;

            return this;
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
            nodeTask.RuntimeOptions.Terra = new TerraRuntimeOptions()
            {
                WsmApiHost = wsmApiHost,
                LandingZoneApiHost = landingZoneApiHost,
                SasAllowedIpRange = sasAllowedIpRange
            };

            SetCombinedTerraTransformationStrategyForInputsAndOutputs();

            return this;
        }

        /// <summary>
        /// Sets docker clean up options for the NodeTask
        /// </summary>
        /// <param name="dockerCleanUpOptions"></param>
        /// <returns></returns>
        public NodeTaskBuilder WithDockerCleanUpOptions(RuntimeContainerCleanupOptions dockerCleanUpOptions)
        {
            ArgumentNullException.ThrowIfNull(dockerCleanUpOptions);

            nodeTask.RuntimeOptions ??= new RuntimeOptions();
            nodeTask.RuntimeOptions.DockerCleanUp = new DockerCleanUpOptions()
            {
                ExecuteRmi = dockerCleanUpOptions.ExecuteDockerRmi,
                ExecutePrune = dockerCleanUpOptions.ExecuteDockerPrune
            };

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

        private void SetCombinedTerraTransformationStrategyForInputsAndOutputs()
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

            return this;
        }
    }
}
