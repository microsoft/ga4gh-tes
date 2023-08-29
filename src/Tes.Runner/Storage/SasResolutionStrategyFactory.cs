﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Identity;
using Azure.Storage.Blobs;
using Tes.Runner.Models;

namespace Tes.Runner.Storage
{
    public static class SasResolutionStrategyFactory
    {
        public static ISasResolutionStrategy CreateSasResolutionStrategy(SasResolutionStrategy sasResolutionStrategy, RuntimeOptions runtimeOptions)
        {
            switch (sasResolutionStrategy)
            {
                case SasResolutionStrategy.None:
                    return new PassThroughSasResolutionStrategy();
                case SasResolutionStrategy.SchemeConverter:
                    return new CloudProviderSchemeConverter();
                case SasResolutionStrategy.AzureResourceManager:
                    return new ArmSasResolutionStrategy(u => new BlobServiceClient(u, new DefaultAzureCredential()));
                case SasResolutionStrategy.TerraWsm:
                    return new TerraSasResolutionStrategy(runtimeOptions.Terra!);
            }

            throw new NotImplementedException();
        }
    }
}
