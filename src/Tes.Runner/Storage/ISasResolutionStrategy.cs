// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Storage
{
    public interface ISasResolutionStrategy
    {
        Task<Uri> CreateSasTokenWithStrategyAsync(string sourceUrl);
    }
}
