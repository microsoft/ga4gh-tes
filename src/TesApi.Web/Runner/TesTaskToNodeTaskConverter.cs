// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tes.Models;
using Tes.Runner.Models;
using TesApi.Web.Management.Configuration;

namespace TesApi.Web.Runner
{
    /// <summary>
    /// Handles the creation of a NodeTask from a TesTask
    /// </summary>
    public class TesTaskToNodeTaskConverter
    {
        private readonly TerraOptions terraOptions;
        private readonly ILogger<TesTaskToNodeTaskConverter> logger;


        /// <summary>
        /// Constructor of TesTaskToNodeTaskConverter
        /// </summary>
        /// <param name="terraOptions"></param>
        /// <param name="logger"></param>
        public TesTaskToNodeTaskConverter(IOptions<TerraOptions> terraOptions, ILogger<TesTaskToNodeTaskConverter> logger)
        {
            this.terraOptions = terraOptions?.Value;
            this.logger = logger;
        }

        /// <summary>
        /// Converts TesTask to a new NodeTask
        /// </summary>
        /// <param name="task">Node task</param>
        /// <param name="pathParentDirectory">Parent directory in the execution compute node. This value will be appended to the path in all inputs and outputs</param>
        /// <param name="containerMountParentDirectory">Parent directory from which a mount in the container is created.</param>
        /// <param name="metricsFile">Metrics filename</param>
        public NodeTask ToNodeTask(TesTask task, string pathParentDirectory, string containerMountParentDirectory, string metricsFile)
        {

            ArgumentNullException.ThrowIfNull(task);
            ArgumentException.ThrowIfNullOrEmpty(pathParentDirectory, nameof(pathParentDirectory));
            ArgumentException.ThrowIfNullOrEmpty(containerMountParentDirectory, nameof(containerMountParentDirectory));

            try
            {
                var builder = new NodeTaskBuilder();

                //TODO: Revise this assumption (carried over from the current implementation) and consider Single() if in practice only one executor per task is supported.
                var executor = task.Executors.First();

                builder.WithId(task.Id)
                    .WithWorkflowId(task.WorkflowId)
                    .WithContainerCommands(executor.Command.Select(EscapeBashArgument).ToList())
                    .WithContainerImage(executor.Image)
                    .WithMetricsFile(metricsFile);



                if (terraOptions is not null && !string.IsNullOrEmpty(terraOptions.WsmApiHost))
                {
                    logger.LogInformation("Setting up Terra as the runtime environment for the runner");
                    builder.WithTerraAsRuntimeEnvironment(terraOptions.WsmApiHost, terraOptions.LandingZoneApiHost,
                        terraOptions.SasAllowedIpRange);
                }

                if (task.Inputs is not null)
                {
                    logger.LogInformation($"Mapping {task.Inputs.Count} inputs");

                    MapInputs(task.Inputs, pathParentDirectory, containerMountParentDirectory, builder);
                }

                if (task.Outputs is not null)
                {
                    logger.LogInformation($"Mapping {task.Outputs.Count} outputs");

                    MapOutputs(task.Outputs, pathParentDirectory, containerMountParentDirectory, builder);
                }

                return builder.Build();
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to convert the TES task to a Node Task");
                throw;
            }
        }

        /// <summary>
        /// Adds additional inputs a NodeTask if they are not already included in the node task. Inputs are compared using the path property. 
        /// </summary>
        /// <param name="inputs">Additional inputs to add to the node task</param>
        /// <param name="nodeTask">Node task</param>
        /// <param name="pathParentDirectory">Parent directory in the execution compute node. This value will be appended to the path in all inputs</param>
        /// <param name="mountParentDirectory">Parent directory from which a mount in the container is created. If not set, the path won't be mounted in container</param>
        public NodeTask AddAdditionalInputsIfNotSet(List<TesInput> inputs, NodeTask nodeTask, string pathParentDirectory,
            string mountParentDirectory = default)
        {
            ArgumentNullException.ThrowIfNull(inputs);
            ArgumentNullException.ThrowIfNull(nodeTask);
            ArgumentException.ThrowIfNullOrEmpty(pathParentDirectory);

            var distinctInputs = inputs.Where(tesInput => nodeTask.Inputs != null && nodeTask.Inputs.Any(nodeInput => nodeInput.Path != null && nodeInput.Path.Equals(tesInput.Path, StringComparison.OrdinalIgnoreCase)))
                                                    .ToList();

            var builder = new NodeTaskBuilder(nodeTask);

            MapInputs(distinctInputs, pathParentDirectory, mountParentDirectory, builder);

            return builder.Build();
        }

        /// <summary>
        /// Adds additional outputs a NodeTask if they are not already included in the node task. Outputs are compared using the path property. 
        /// </summary>
        /// <param name="outputs">Additional outputs to add to the node task</param>
        /// <param name="nodeTask">Node task</param>
        /// <param name="pathParentDirectory">Parent directory in the execution compute node. This value will be appended to the path in all outputs</param>
        /// <param name="mountParentDirectory">Parent directory from which a mount in the container is created. If not set, the path won't be mounted in container</param>
        public NodeTask AddAdditionalOutputsIfNotSet(List<TesOutput> outputs, NodeTask nodeTask,
            string pathParentDirectory, string mountParentDirectory = default)
        {
            ArgumentNullException.ThrowIfNull(outputs);
            ArgumentNullException.ThrowIfNull(nodeTask);
            ArgumentException.ThrowIfNullOrEmpty(pathParentDirectory);

            var tesOutputs = outputs.Where(tesOutput => nodeTask.Outputs != null && nodeTask.Outputs.Any(nodeOutput => nodeOutput.Path != null && nodeOutput.Path.Equals(tesOutput.Path, StringComparison.OrdinalIgnoreCase)))
                .ToList();

            var builder = new NodeTaskBuilder(nodeTask);

            MapOutputs(tesOutputs, pathParentDirectory, mountParentDirectory, builder);

            return builder.Build();
        }

        private static string EscapeBashArgument(string arg)
        {
            return $"'{arg.Replace(@"'", @"'\''")}'";
        }


        private void MapOutputs(List<TesOutput> outputs, string pathParentDirectory, string containerMountParentDirectory,
            NodeTaskBuilder builder)
        {
            outputs?.ForEach(output =>
            {
                builder.WithOutputUsingCombinedTransformationStrategy(
                    AppendParentDirectoryIfSet(output.Path, pathParentDirectory), output.Url, ToNodeTaskFileType(output.Type),
                    containerMountParentDirectory);
            });
        }

        private void MapInputs(List<TesInput> inputs, string pathParentDirectory, string containerMountParentDirectory,
            NodeTaskBuilder builder)
        {
            inputs?.ForEach(input =>
            {
                builder.WithInputUsingCombinedTransformationStrategy(
                    AppendParentDirectoryIfSet(input.Path, pathParentDirectory), input.Url,
                    containerMountParentDirectory);
            });

        }

        private FileType? ToNodeTaskFileType(TesFileType outputType)
        {
            return outputType switch
            {
                TesFileType.FILEEnum => FileType.File,
                TesFileType.DIRECTORYEnum => FileType.Directory,
                _ => FileType.File
            };
        }

        private string AppendParentDirectoryIfSet(string inputPath, string pathParentDirectory)
        {
            if (!string.IsNullOrWhiteSpace(pathParentDirectory))
            {
                //it is assumed the input path is an absolute path
                return $"{pathParentDirectory}{inputPath}";
            }

            return inputPath;
        }
    }
}
