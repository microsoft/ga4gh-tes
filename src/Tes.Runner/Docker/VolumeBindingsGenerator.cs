// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Docker
{
    public class VolumeBindingsGenerator
    {
        private readonly string mountParentDirectory;

        public VolumeBindingsGenerator(string mountParentDirectory)
        {
            ArgumentException.ThrowIfNullOrEmpty(mountParentDirectory, nameof(mountParentDirectory));

            this.mountParentDirectory = mountParentDirectory;
        }
    }
}
