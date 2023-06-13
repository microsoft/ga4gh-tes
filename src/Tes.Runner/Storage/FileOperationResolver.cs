// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.Runner.Storage
{
    public class FileOperationResolver
    {
        private readonly NodeTask nodeTask;
        private readonly ResolutionPolicyHandler resolutionPolicyHandler;
        private readonly IFileInfoProvider fileInfoProvider;

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
                expandedInputs.Add(new FileInput
                {
                    SasStrategy = input.SasStrategy,
                    SourceUrl = input.SourceUrl,
                    FullFileName = fileInfoProvider.GetFileName(input.FullFileName!),
                });
            }

            return expandedInputs;
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

        private IEnumerable<FileOutput> ExpandDirectoryOutput(FileOutput output)
        {
            foreach (var file in fileInfoProvider.GetFilesInDirectory(output.FullFileName!))
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
                if (fileInfoProvider.FileExists(output.FullFileName!))
                {
                    yield return CreateExpandedFileOutput(output);
                }

                yield break;
            }

            var path = RemovePrefixFromPath(output.FullFileName!, output.PathPrefix!);

            foreach (var file in fileInfoProvider.GetFilesInAllDirectories(output.PathPrefix!, path))
            {
                yield return CreateExpandedFileOutputWithCombinedTargetUrl(output, file);
            }
        }

        private static FileOutput CreateExpandedFileOutput(FileOutput output)
        {
            return new FileOutput()
            {
                FullFileName = Environment.ExpandEnvironmentVariables(output.FullFileName!),
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
                FullFileName = path,
                PathPrefix = output.PathPrefix,
                TargetUrl = ToCombinedTargetUrl(output, path),
                SasStrategy = output.SasStrategy,
                FileType = FileType.File,
            };
        }

        private string ToCombinedTargetUrl(FileOutput output, string path)
        {
            var builder = new UriBuilder(output.TargetUrl!);

            builder.Path = $"{builder.Path}{RemovePrefixFromPath(path, output.PathPrefix)}";

            return builder.Uri.ToString();
        }

        private static string RemovePrefixFromPath(string path, string? prefix)
        {
            var expandedPath = Environment.ExpandEnvironmentVariables(path);

            if (string.IsNullOrEmpty(prefix))
            {
                return expandedPath;
            }

            var expandedPrefix = Environment.ExpandEnvironmentVariables(prefix);

            if (expandedPath.StartsWith(expandedPrefix))
            {
                return expandedPath.Substring(expandedPrefix.Length);
            }

            return expandedPath;
        }
    }
}
