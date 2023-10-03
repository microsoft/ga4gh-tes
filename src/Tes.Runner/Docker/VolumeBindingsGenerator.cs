// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;
using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.Runner.Docker
{
    public class VolumeBindingsGenerator
    {
        private readonly ILogger<VolumeBindingsGenerator> logger = PipelineLoggerFactory.Create<VolumeBindingsGenerator>();
        private readonly IFileInfoProvider fileInfoProvider;

        public VolumeBindingsGenerator() : this(new DefaultFileInfoProvider())
        {
        }

        protected VolumeBindingsGenerator(IFileInfoProvider fileInfoProvider)
        {
            ArgumentNullException.ThrowIfNull(fileInfoProvider);

            this.fileInfoProvider = fileInfoProvider;
        }

        public List<string> GenerateVolumeBindings(List<FileInput>? inputs, List<FileOutput>? outputs)
        {
            var volumeBindings = new HashSet<string>();

            if (inputs != null)
            {
                foreach (var input in inputs)
                {
                    AddVolumeBindingIfRequired(volumeBindings, input.MountParentDirectory, input.Path!);
                }
            }

            if (outputs != null)
            {
                foreach (var output in outputs)
                {
                    AddVolumeBindingIfRequired(volumeBindings, output.MountParentDirectory, output.Path!);
                }
            }

            return volumeBindings.ToList();
        }

        private void AddVolumeBindingIfRequired(HashSet<string> volumeBindings, string? mountParentDirectory, string path)
        {
            var mountPath = ToVolumeBinding(mountParentDirectory, path);

            if (!string.IsNullOrEmpty(mountPath))
            {
                volumeBindings.Add(mountPath);
            }
        }

        private string? ToVolumeBinding(string? mountParentDirectory, string path)
        {
            if (string.IsNullOrEmpty(mountParentDirectory))
            {
                logger.LogDebug(
                    $"The file {path} does not have a mount parent directory defined in the task definition. No volume binding will be created for this file in the container.");
                return default;
            }

            var expandedMountParentDirectory = fileInfoProvider.GetExpandedFileName(mountParentDirectory);
            var expandedPath = fileInfoProvider.GetExpandedFileName(path);

            if (!expandedPath.StartsWith(expandedMountParentDirectory))
            {
                logger.LogWarning(
                    $"The expanded path value {expandedPath} does not contain the specified mount parent directory: {expandedMountParentDirectory}. No volume binding will be created for this file in the container.");
                return default;
            }

            var targetDir = $"{expandedPath.Substring(expandedMountParentDirectory.Length).Split('/', StringSplitOptions.RemoveEmptyEntries)[0].TrimStart('/')}";

            var volBinding = $"{expandedMountParentDirectory.TrimEnd('/')}/{targetDir}:/{targetDir}";

            logger.LogInformation($"Volume binding for {expandedPath} is {volBinding}");

            return volBinding;
        }
    }
}
