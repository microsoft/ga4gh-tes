// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tes.Models;
using Tes.Runner.Models;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Storage;

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
        /// <param name="storageAccessProvider"></param>
        /// <param name="cancellationToken"></param>
        public async Task<NodeTask> ToNodeTaskAsync(TesTask task, string pathParentDirectory, string containerMountParentDirectory, string metricsFile, IStorageAccessProvider storageAccessProvider, CancellationToken cancellationToken)
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

                    var inputs = await PrepareInputsForMappingAsync(task, storageAccessProvider, cancellationToken);

                    MapInputs(inputs, pathParentDirectory, containerMountParentDirectory, builder);
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

        private async Task<List<TesInput>> PrepareInputsForMappingAsync(TesTask tesTask,
            IStorageAccessProvider storageAccessProvider, CancellationToken cancellationToken)
        {
            var inputs = new List<TesInput>();
            if (tesTask.Inputs is null)
            {
                return inputs;
            }

            foreach (var input in tesTask.Inputs)
            {
                var preparedInput = await PrepareContentInputAsync(tesTask, input, storageAccessProvider, cancellationToken);

                if (preparedInput != null)
                {
                    inputs.Add(preparedInput);
                    continue;
                }

                preparedInput = await PrepareLocalFileInputAsync(tesTask, input, storageAccessProvider, cancellationToken);

                if (preparedInput != null)
                {
                    inputs.Add(preparedInput);
                    continue;
                }

                inputs.Add(input);
            }
            return inputs;
        }

        private async Task<TesInput?> PrepareLocalFileInputAsync(TesTask tesTask, TesInput input,
            IStorageAccessProvider storageAccessProvider, CancellationToken cancellationToken)
        {
            if (TryGetCromwellTmpFilePath(input.Url, out var localPath))
            {
                logger.LogInformation($"The input is a local file. Uploading its content to the internal storage location. Input URI: {input?.Url}. Local path: {localPath} ");

                var content = await File.ReadAllTextAsync(localPath, cancellationToken);

                return await UploadContentAndCreateTesInputAsync(tesTask, input.Path, content, storageAccessProvider, cancellationToken);
            }

            return default;
        }

        private async Task<TesInput> UploadContentAndCreateTesInputAsync(TesTask tesTask, string inputPath,
            string content,
            IStorageAccessProvider storageAccessProvider, CancellationToken cancellationToken)
        {
            var inputFileUrl =
                await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(tesTask, Guid.NewGuid().ToString(),
                    cancellationToken);

            //return the URL without the SAS token, the runner will add it using the transformation strategy
            await storageAccessProvider.UploadBlobAsync(new Uri(inputFileUrl), content, cancellationToken);

            var inputUrl = RemoveQueryStringFromUrl(inputFileUrl);

            logger.LogInformation($"Successfully content as a new blob at: {inputUrl}");

            return new TesInput
            {
                Path = inputPath,
                Url = inputUrl,
                Type = TesFileType.FILEEnum,
            };
        }

        /// <summary>
        /// Verifies existence and translates local file URLs to absolute paths (e.g. file:///tmp/cwl_temp_dir_8026387118450035757/args.py becomes /tmp/cwl_temp_dir_8026387118450035757/args.py)
        /// Only considering files in /cromwell-tmp because that is the only local directory mapped from Cromwell container
        /// </summary>
        /// <param name="fileUri">File URI</param>
        /// <param name="localPath">Local path</param>
        /// <returns></returns>
        private bool TryGetCromwellTmpFilePath(string fileUri, out string localPath)
        {
            localPath = Uri.TryCreate(fileUri, UriKind.Absolute, out var uri)
                        && uri.IsFile
                        && uri.AbsolutePath.StartsWith("/cromwell-tmp/") ? uri.AbsolutePath : null;

            return localPath is not null;
        }

        private async Task<TesInput?> PrepareContentInputAsync(TesTask tesTask, TesInput input, IStorageAccessProvider storageAccessProvider, CancellationToken cancellationToken)
        {

            if (String.IsNullOrWhiteSpace(input?.Content))
            {
                return default;
            }

            logger.LogInformation($"The input is content. Uploading its content to the internal storage location. Input path:{input?.Path}");

            return await UploadContentAndCreateTesInputAsync(tesTask, input.Path, input.Content, storageAccessProvider, cancellationToken);
        }

        private static string RemoveQueryStringFromUrl(string url)
        {
            var uri = new Uri(url);
            return uri.GetLeftPart(UriPartial.Path);
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

            var distinctInputs = inputs.Where(tesInput => nodeTask.Inputs != null && !nodeTask.Inputs.Any(nodeInput => nodeInput.Path != null && nodeInput.Path.Equals(tesInput.Path, StringComparison.OrdinalIgnoreCase)))
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

            var tesOutputs = outputs.Where(tesOutput => nodeTask.Outputs != null && !nodeTask.Outputs.Any(nodeOutput => nodeOutput.Path != null && nodeOutput.Path.Equals(tesOutput.Path, StringComparison.OrdinalIgnoreCase)))
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
