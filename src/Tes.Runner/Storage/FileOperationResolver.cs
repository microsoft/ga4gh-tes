// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;
using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.Runner.Storage
{
    /// <summary>
    /// Expands the file inputs and outputs and resolves the SAS tokens.
    /// </summary>
    public class FileOperationResolver
    {
        private readonly NodeTask nodeTask = null!;
        private readonly ResolutionPolicyHandler resolutionPolicyHandler = null!;
        private readonly IFileInfoProvider fileInfoProvider = null!;
        private readonly ILogger logger = PipelineLoggerFactory.Create<FileOperationResolver>();

        public FileOperationResolver(NodeTask nodeTask) : this(nodeTask, new ResolutionPolicyHandler(nodeTask.RuntimeOptions), new DefaultFileInfoProvider())
        {
        }

        /// <summary>
        /// Parameter-less constructor for mocking
        /// </summary>
        protected FileOperationResolver() { }

        public FileOperationResolver(NodeTask nodeTask, ResolutionPolicyHandler resolutionPolicyHandler,
            IFileInfoProvider fileInfoProvider)
        {
            ArgumentNullException.ThrowIfNull(nodeTask);
            ArgumentNullException.ThrowIfNull(resolutionPolicyHandler);
            ArgumentNullException.ThrowIfNull(fileInfoProvider);

            this.nodeTask = nodeTask;
            this.resolutionPolicyHandler = resolutionPolicyHandler;
            this.fileInfoProvider = fileInfoProvider;
        }

        public virtual async Task<List<DownloadInfo>?> ResolveInputsAsync()
        {
            var expandedInputs = ExpandInputs();

            return await resolutionPolicyHandler.ApplyResolutionPolicyAsync(expandedInputs);
        }

        public virtual async Task<List<UploadInfo>?> ResolveOutputsAsync()
        {
            var expandedOutputs = ExpandOutputs();

            return await resolutionPolicyHandler.ApplyResolutionPolicyAsync(expandedOutputs);
        }

        private List<FileInput> ExpandInputs()
        {
            var expandedInputs = new List<FileInput>();

            foreach (var input in nodeTask.Inputs ?? Enumerable.Empty<FileInput>())
            {
                expandedInputs.Add(CreateExpandedFileInput(input));
            }

            return expandedInputs;
        }

        private FileInput CreateExpandedFileInput(FileInput input)
        {
            ValidateFileInput(input);

            return new FileInput
            {
                TransformationStrategy = input.TransformationStrategy,
                SourceUrl = input.SourceUrl,
                Path = fileInfoProvider.GetExpandedFileName(input.Path!),
            };
        }

        private void ValidateFileInput(FileInput input)
        {
            try
            {
                ArgumentNullException.ThrowIfNull(input);
                ArgumentException.ThrowIfNullOrEmpty(input.Path, nameof(input.Path));
                ArgumentException.ThrowIfNullOrEmpty(input.SourceUrl, nameof(input.SourceUrl));
                ArgumentNullException.ThrowIfNull(input.TransformationStrategy, nameof(input.TransformationStrategy));
            }
            catch (Exception e)
            {
                logger.LogError(e, "Invalid file input. Please the task definition. All required properties must be set. The required properties are: path, sourceUrl and sasStrategy");
                throw;
            }
        }

        private List<FileOutput> ExpandOutputs()
        {
            var outputs = new List<FileOutput>();

            foreach (var output in nodeTask.Outputs ?? Enumerable.Empty<FileOutput>())
            {
                outputs.AddRange(ExpandOutput(output));
            }

            return outputs;
        }

        private IEnumerable<FileOutput> ExpandOutput(FileOutput output)
        {
            ValidateFileOutput(output);

            IEnumerable<FileOutput> expandedOutputs;

            switch (output.FileType)
            {
                case FileType.Directory:
                    expandedOutputs = ExpandDirectoryOutput(output);
                    break;
                case FileType.File:
                    expandedOutputs = ExpandFileOutput(output);
                    break;
                default:
                    logger.LogWarning($"File type was not set for the output: {output.Path}. Expanding the output as file type.");
                    expandedOutputs = ExpandFileOutput(output);
                    break;
            }

            foreach (var fileOutput in expandedOutputs)
            {
                yield return fileOutput;
            }
        }

        private void ValidateFileOutput(FileOutput output)
        {
            try
            {
                ArgumentNullException.ThrowIfNull(output);
                ArgumentException.ThrowIfNullOrEmpty(output.Path, nameof(output.Path));
                ArgumentException.ThrowIfNullOrEmpty(output.TargetUrl, nameof(output.TargetUrl));
                ArgumentNullException.ThrowIfNull(output.TransformationStrategy, nameof(output.TransformationStrategy));
                ArgumentNullException.ThrowIfNull(output.FileType, nameof(output.FileType));
            }
            catch (Exception e)
            {
                logger.LogError(e, "Invalid file output. Please the task definition. All required properties must be set. The required properties are: path, targetUrl, sasStrategy and type");
                throw;
            }
        }

        private IEnumerable<FileOutput> ExpandDirectoryOutput(FileOutput output)
        {
            foreach (var file in fileInfoProvider.GetAllFilesInDirectory(output.Path!))
            {
                yield return CreateExpandedFileOutputWithCombinedTargetUrl(output, absoluteFilePath: file.AbsolutePath, relativePathToSearchPath: file.RelativePathToSearchPath);
            }
        }

        private IEnumerable<FileOutput> ExpandFileOutput(FileOutput output)
        {
            var expandedPath = fileInfoProvider.GetExpandedFileName(output.Path!);

            if (fileInfoProvider.FileExists(expandedPath))
            {
                //treat the output as a single file and use the target URL as is
                logger.LogInformation($"Adding file: {expandedPath} to the output list with a target URL as is");

                yield return CreateExpandedFileOutputUsingTargetUrl(output,
                    absoluteFilePath: expandedPath);

                yield break;
            }

            //at this point, the output is not a single file, so it must be a search pattern
            //break the given path into root and relative path, where the relative path is the search pattern
            var rootPathPair = fileInfoProvider.GetRootPathPair(expandedPath);

            foreach (var file in fileInfoProvider.GetFilesBySearchPattern(rootPathPair.Root, rootPathPair.RelativePath))
            {
                logger.LogInformation($"Adding file: {file.RelativePathToSearchPath} with absolute path: {file.AbsolutePath} to the output list with a combined target URL");

                yield return CreateExpandedFileOutputWithCombinedTargetUrl(output, absoluteFilePath: file.AbsolutePath, relativePathToSearchPath: file.RelativePathToSearchPath);
            }
        }

        private FileOutput CreateExpandedFileOutputWithCombinedTargetUrl(FileOutput output, string absoluteFilePath, string relativePathToSearchPath)
        {
            return new FileOutput()
            {
                Path = absoluteFilePath,
                TargetUrl = ToCombinedTargetUrl(output.TargetUrl!, prefixToRemoveFromPath: string.Empty, relativePathToSearchPath),
                TransformationStrategy = output.TransformationStrategy,
                MountParentDirectory = string.IsNullOrWhiteSpace(output.MountParentDirectory) ? null : fileInfoProvider.GetExpandedFileName(output.MountParentDirectory),
                FileType = FileType.File,
            };
        }

        private FileOutput CreateExpandedFileOutputUsingTargetUrl(FileOutput output, string absoluteFilePath)
        {
            return new FileOutput()
            {
                Path = absoluteFilePath,
                TargetUrl = output.TargetUrl,
                TransformationStrategy = output.TransformationStrategy,
                MountParentDirectory = string.IsNullOrWhiteSpace(output.MountParentDirectory) ? null : fileInfoProvider.GetExpandedFileName(output.MountParentDirectory),
                FileType = FileType.File,
            };
        }

        private static string ToCombinedTargetUrl(string targetUrl, string prefixToRemoveFromPath, string path)
        {
            var builder = new UriBuilder(targetUrl);

            builder.Path = $"{builder.Path}/{RemovePrefixFromPath(path, prefixToRemoveFromPath)}";

            return builder.Uri.ToString();
        }

        private static string RemovePrefixFromPath(string path, string? prefix)
        {
            var expandedPath = Environment.ExpandEnvironmentVariables(path);

            if (string.IsNullOrEmpty(prefix))
            {
                return expandedPath.TrimStart('/');
            }

            var expandedPrefix = Environment.ExpandEnvironmentVariables(prefix);

            if (expandedPath.StartsWith(expandedPrefix))
            {
                return expandedPath.Substring(expandedPrefix.Length).TrimStart('/');
            }

            return expandedPath.TrimStart('/');
        }
    }
}
