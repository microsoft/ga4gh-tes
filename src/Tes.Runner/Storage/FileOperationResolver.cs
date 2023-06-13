// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;
using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.Runner.Storage
{
    public class FileOperationResolver
    {
        private readonly NodeTask nodeTask;
        private readonly ResolutionPolicyHandler resolutionPolicyHandler;
        private readonly IFileInfoProvider fileInfoProvider;
        private readonly ILogger logger = PipelineLoggerFactory.Create<FileOperationResolver>();

        public FileOperationResolver(NodeTask nodeTask) : this(nodeTask, new ResolutionPolicyHandler(), new DefaultFileInfoProvider())
        {
        }

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

        public async Task<List<DownloadInfo>?> ResolveInputsAsync()
        {
            var expandedInputs = ExpandInputs();

            return await resolutionPolicyHandler.ApplyResolutionPolicyAsync(expandedInputs);
        }

        public async Task<List<UploadInfo>?> ResolveOutputsAsync()
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
                SasStrategy = input.SasStrategy,
                SourceUrl = input.SourceUrl,
                Path = fileInfoProvider.GetFileName(input.Path!),
            };
        }

        private void ValidateFileInput(FileInput input)
        {
            try
            {
                ArgumentNullException.ThrowIfNull(input);
                ArgumentException.ThrowIfNullOrEmpty(input.Path, nameof(input.Path));
                ArgumentException.ThrowIfNullOrEmpty(input.SourceUrl, nameof(input.SourceUrl));
                ArgumentNullException.ThrowIfNull(input.SasStrategy, nameof(input.SasStrategy));
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
                    throw new ArgumentOutOfRangeException(nameof(output.FileType), output.FileType, "File output type");
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
                ArgumentNullException.ThrowIfNull(output.SasStrategy, nameof(output.SasStrategy));
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
            foreach (var file in fileInfoProvider.GetFilesInDirectory(output.Path!))
            {
                yield return CreateExpandedFileOutputWithCombinedTargetUrl(output, file);
            }
        }

        private IEnumerable<FileOutput> ExpandFileOutput(FileOutput output)
        {
            //consider the output as a single file if the path prefix is not specified
            if (string.IsNullOrEmpty(output.PathPrefix))
            {
                //outputs are optional, so if the file does not exist, we just skip it
                if (fileInfoProvider.FileExists(output.Path!))
                {
                    yield return CreateExpandedFileOutput(output);
                }

                yield break;
            }

            var path = RemovePrefixFromPath(output.Path!, output.PathPrefix!);

            foreach (var file in fileInfoProvider.GetFilesInAllDirectories(output.PathPrefix!, path))
            {
                logger.LogInformation($"Adding file {file} to the output list");
                yield return CreateExpandedFileOutputWithCombinedTargetUrl(output, file);
            }
        }

        private static FileOutput CreateExpandedFileOutput(FileOutput output)
        {
            return new FileOutput()
            {
                Path = Environment.ExpandEnvironmentVariables(output.Path!),
                PathPrefix = output.PathPrefix,
                TargetUrl = output.TargetUrl,
                SasStrategy = output.SasStrategy,
                FileType = FileType.File,
            };
        }
        private FileOutput CreateExpandedFileOutputWithCombinedTargetUrl(FileOutput output, string path)
        {
            return new FileOutput()
            {
                Path = path,
                PathPrefix = output.PathPrefix,
                TargetUrl = ToCombinedTargetUrl(output, path),
                SasStrategy = output.SasStrategy,
                FileType = FileType.File,
            };
        }

        private string ToCombinedTargetUrl(FileOutput output, string path)
        {
            var builder = new UriBuilder(output.TargetUrl!);

            builder.Path = $"{builder.Path}/{RemovePrefixFromPath(path, output.PathPrefix)}";

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
