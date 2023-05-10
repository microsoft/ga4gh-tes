// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Models;

namespace Tes.Runner.Storage
{
    public static class SasResolutionStrategyFactory
    {
        public static ISasResolutionStrategy CreateSasResolutionStrategy(SasResolutionStrategy sasResolutionStrategy)
        {
            switch (sasResolutionStrategy)
            {
                case SasResolutionStrategy.None:
                    return new PassThroughSasResolutionStrategy();
                case SasResolutionStrategy.SchemeConverter:
                    return new CloudProviderSchemeConverter();
            }

            throw new NotImplementedException();
        }
    }
}
