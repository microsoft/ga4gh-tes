// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace BuildPushAcr
{
    public interface IArchive
    {
        public IAsyncEnumerable<System.Formats.Tar.TarEntry> Get(CancellationToken cancellationToken, string? root = default);
        public ValueTask<Version> GetTagAsync(CancellationToken cancellationToken);
    }
}
