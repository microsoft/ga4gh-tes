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
                logger.LogWarning(
                    $"The file {path} does not have a mount parent directory defined in the task definition. No volume binding will be created for this file in the container.");
                return default;
            }

            var expandedMountParentDirectory = fileInfoProvider.GetExpandedFileName(mountParentDirectory);

            if (!path.StartsWith(expandedMountParentDirectory))
            {
                logger.LogWarning(
                    $"The path value {path} does not contain the specified mount parent directory: {expandedMountParentDirectory}. No volume binding will be created for this file in the container.");
                return default;
            }

            var targetDir = $"{path.Substring(expandedMountParentDirectory.Length - 1).Split('/')[0].TrimStart('/')}";

            var volBinding = $"{expandedMountParentDirectory}/{targetDir}:/{targetDir}";

            logger.LogDebug($"Volume binding for {path} is {volBinding}");

            return volBinding;
        }
    }
}
