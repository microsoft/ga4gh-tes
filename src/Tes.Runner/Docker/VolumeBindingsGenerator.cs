// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;
using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.Runner.Docker
{
    public class VolumeBindingsGenerator
    {
        private readonly ILogger logger;
        private readonly IFileInfoProvider fileInfoProvider;

        public VolumeBindingsGenerator(IFileInfoProvider fileInfoProvider, ILogger<VolumeBindingsGenerator> logger)
        {
            ArgumentNullException.ThrowIfNull(fileInfoProvider);

            this.fileInfoProvider = fileInfoProvider;
            this.logger = logger;
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

            return [.. volumeBindings];
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
                    "The file {FilePath} does not have a mount parent directory defined in the task definition. No volume binding will be created for this file in the container.", path);
                return default;
            }

            var expandedMountParentDirectory = fileInfoProvider.GetExpandedFileName(mountParentDirectory);
            var expandedPath = fileInfoProvider.GetExpandedFileName(path);

            if (!expandedPath.StartsWith(expandedMountParentDirectory))
            {
                logger.LogWarning(
                    "The expanded path value {FilePath} does not contain the specified mount parent directory: {MountParentDirectory}. No volume binding will be created for this file in the container.", expandedPath, expandedMountParentDirectory);
                return default;
            }

            var targetDir = $"{expandedPath[expandedMountParentDirectory.Length..].Split('/', StringSplitOptions.RemoveEmptyEntries)[0].TrimStart('/')}";

            var volBinding = $"{expandedMountParentDirectory.TrimEnd('/')}/{targetDir}:/{targetDir}";

            logger.LogDebug("Volume binding for {FilePath} is {Binding}", expandedPath, volBinding);

            return volBinding;
        }
    }
}
