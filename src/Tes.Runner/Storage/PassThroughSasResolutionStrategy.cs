// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Storage
{
    public class PassThroughSasResolutionStrategy : ISasResolutionStrategy
    {
        public Task<Uri> CreateSasTokenWithStrategyAsync(string sourceUrl)
        {
            ArgumentException.ThrowIfNullOrEmpty(sourceUrl);

            return Task.FromResult(new Uri(sourceUrl));
        }
    }
}
