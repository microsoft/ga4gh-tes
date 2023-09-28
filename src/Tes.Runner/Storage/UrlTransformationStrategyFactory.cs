﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Tes.Runner.Models;

namespace Tes.Runner.Storage
{
    public static class UrlTransformationStrategyFactory
    {
        public static IUrlTransformationStrategy CreateStrategy(TransformationStrategy transformationStrategy, RuntimeOptions runtimeOptions)
        {
            switch (transformationStrategy)
            {
                case TransformationStrategy.None:
                    return new PassThroughUrlTransformationStrategy();
                case TransformationStrategy.SchemeConverter:
                    return new CloudProviderSchemeConverter();
                case TransformationStrategy.AzureResourceManager:
                    return new ArmUrlTransformationStrategy(u => new BlobServiceClient(u, GeTokenCredential(runtimeOptions)));
                case TransformationStrategy.TerraWsm:
                    return new TerraUrlTransformationStrategy(runtimeOptions.Terra!, GeTokenCredential(runtimeOptions));
                case TransformationStrategy.CombinedTerra:
                    return new CombinedTransformationStrategy(new List<IUrlTransformationStrategy>
                    {
                        new CloudProviderSchemeConverter(),
                        new TerraUrlTransformationStrategy(runtimeOptions.Terra!, GeTokenCredential(runtimeOptions)),
                    });
                case TransformationStrategy.CombinedAzureResourceManager:
                    return new CombinedTransformationStrategy(new List<IUrlTransformationStrategy>
                    {
                        new CloudProviderSchemeConverter(),
                        new ArmUrlTransformationStrategy(u => new BlobServiceClient(u, GeTokenCredential(runtimeOptions)))
                    });
            }

            throw new NotImplementedException();
        }

        public static TokenCredential GeTokenCredential(RuntimeOptions runtimeOptions)
        {
            if (!string.IsNullOrWhiteSpace(runtimeOptions.NodeManagedIdentityClientId))
            {
                return new ManagedIdentityCredential(clientId: runtimeOptions.NodeManagedIdentityClientId);
            }

            return new DefaultAzureCredential();
        }
    }
}
