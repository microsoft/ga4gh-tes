// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace BuildPushAcr
{
    public interface IArchive
    {
        public IAsyncEnumerable<System.Formats.Tar.TarEntry> Get(CancellationToken cancellationToken, string? root = default);
        public ValueTask<(Version Version, string? Prerelease)> GetTagAsync(CancellationToken cancellationToken, bool allowAnyPrerelease = false);

        static (Version Version, string? Prerelease)? ParseTag(string? tagName)
        {
            Version? version;

            if (string.IsNullOrEmpty(tagName))
            {
                return default;
            }

            if (tagName.Contains('-'))
            {
                var name = tagName[..tagName.IndexOf('-')];
                return Version.TryParse(name, out version) ? (version, tagName[(tagName.IndexOf('-') + 1)..]) : default;
            }

            return Version.TryParse(tagName, out version) ? (version, null) : default;
        }
    }
}
