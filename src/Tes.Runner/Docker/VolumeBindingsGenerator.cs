﻿// Copyright (c) Microsoft Corporation.
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
        private readonly string mountParentDirectory;

        public VolumeBindingsGenerator(string mountParentDirectory) : this(mountParentDirectory, new DefaultFileInfoProvider())
        {
        }

        protected VolumeBindingsGenerator(string mountParentDirectory, IFileInfoProvider fileInfoProvider)
        {
            ArgumentNullException.ThrowIfNull(fileInfoProvider);

            this.fileInfoProvider = fileInfoProvider;
            this.mountParentDirectory = string.IsNullOrWhiteSpace(mountParentDirectory) ? null! : fileInfoProvider.GetExpandedFileName(mountParentDirectory);
        }

        public List<string> GenerateVolumeBindings(List<FileInput>? inputs, List<FileOutput>? outputs)
        {
            var volumeBindings = new HashSet<string>();

            if (inputs != null)
            {
                foreach (var input in inputs)
                {
                    AddVolumeBindingIfRequired(volumeBindings, input.Path!);
                }
            }

            if (outputs != null)
            {
                foreach (var output in outputs)
                {
                    AddVolumeBindingIfRequired(volumeBindings, output.Path!);
                }
            }

            return volumeBindings.ToList();
        }

        private void AddVolumeBindingIfRequired(HashSet<string> volumeBindings, string path)
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
                return default;
            }

            var expandedPath = fileInfoProvider.GetExpandedFileName(path);

            if (!expandedPath.StartsWith(mountParentDirectory))
            {
                logger.LogDebug(
                    @"The expanded path value {ExpandedPath} does not contain the specified mount parent directory: {MountParentDirectory}. No volume binding will be created for this file in the container.",
                    expandedPath, mountParentDirectory);
                return default;
            }

            var targetDir = $"{expandedPath.Substring(mountParentDirectory.Length).Split('/', StringSplitOptions.RemoveEmptyEntries)[0].TrimStart('/')}";

            var volBinding = $"{mountParentDirectory.TrimEnd('/')}/{targetDir}:/{targetDir}";

            logger.LogDebug(@"Volume binding for {ExpandedPath} is {VolBinding}", expandedPath, volBinding);

            return volBinding;
        }
    }
}
