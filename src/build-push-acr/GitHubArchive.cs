// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.IO.Compression;
using System.Net;
using CommonUtilities;
using Microsoft.Kiota.Abstractions.Authentication;

namespace BuildPushAcr
{
    public sealed class GitHubArchive(GitHub.GitHubClient client, string owner, string repo, string @ref, IEnumerable<string>? submodulePaths = default) : IArchive, IDisposable, IAsyncDisposable
    {
        private readonly GitHub.GitHubClient client = client ?? throw new ArgumentNullException(nameof(client));
        private readonly string owner = owner ?? throw new ArgumentNullException(nameof(owner));
        private readonly string repo = repo ?? throw new ArgumentNullException(nameof(repo));
        private readonly string @ref = @ref ?? throw new ArgumentNullException(nameof(@ref));
        private readonly IEnumerable<string>? submodulePaths = submodulePaths;

        private readonly HashSet<System.IO.MemoryMappedFiles.MemoryMappedFile> mappedFiles = [];
        private readonly HashSet<GitHubArchive> accessedModules = [];
        private readonly Dictionary<string, (string Owner, string Repo, string Ref)> submodules = [];

        private System.Formats.Tar.TarReader? reader;
        private CancellationToken processEntryToken;
        private string? srcRoot;
        private string? root;

        public static IAccessTokenProvider? GetAccessTokenProvider()
        {
            var pat = Environment.GetEnvironmentVariable("GITHUB_TOKEN");

            if (string.IsNullOrWhiteSpace(pat))
            {
                return default;
            }

            return new AccessTokenProvider(pat);
        }

        private class AccessTokenProvider(string pat) : IAccessTokenProvider
        {
            private readonly string pat = pat;

            AllowedHostsValidator IAccessTokenProvider.AllowedHostsValidator { get; } = new(["api.github.com"]);

            Task<string> IAccessTokenProvider.GetAuthorizationTokenAsync(Uri uri, Dictionary<string, object>? additionalAuthenticationContext, CancellationToken cancellationToken)
                => Task.FromResult(pat);
        }

        async ValueTask<Version> IArchive.GetTagAsync(CancellationToken cancellationToken)
        {
            List<GitHub.Models.Tag> tags = [];

            {
                List<GitHub.Models.Tag>? results;
                var page = 0;

                do
                {
                    results = await client.Repos[owner][repo].Tags.GetAsync(request => { request.QueryParameters.Page = ++page; request.QueryParameters.PerPage = 100; }, cancellationToken: cancellationToken);
                    tags.AddRange(results ?? []);
                }
                while ((results?.Count ?? 0) == 100);
            }

            // Check if the ref is a tag or a commit with a tag
            var result = tags
                .Where(tag => @ref.Equals(tag.Name) || (tag.Commit?.Sha?.StartsWith(@ref) ?? false))
                .Select(tag => Version.TryParse(tag.Name, out var version) ? version : default)
                .Where(version => version is not null)
                .Max();

            if (result is not null)
            {
                return result;
            }

            string sha;

            // Check if the ref is a branch name
            try
            {
                sha = (await client.Repos[owner][repo].Branches[@ref].GetAsync(cancellationToken: cancellationToken))!.Commit?.Sha ?? throw new InvalidOperationException("Tag not found.");
            }
            catch (GitHub.Models.BasicError ex) when (ex.ResponseStatusCode == (int)HttpStatusCode.NotFound)
            {
                sha = ((await client.Repos[owner][repo].Commits[@ref].GetAsync(cancellationToken: cancellationToken))?.Sha) ?? throw new InvalidOperationException("Tag not found.");
            }

            // Look for parents until a tag is found.
            var channel = System.Threading.Channels.Channel.CreateUnbounded<string>(new() { AllowSynchronousContinuations = true, SingleReader = true, SingleWriter = false });

            try
            {
                await channel.Writer.WriteAsync(sha, cancellationToken);

                List<Version> candidates = [];

                await foreach (var commit in channel.Reader.ReadAllAsync(cancellationToken))
                {
                    if (commit is null)
                    {
                        channel.Writer.Complete();
                        throw new InvalidOperationException("Tag not found.");
                    }

                    try
                    {
                        candidates.Add(tags
                            .Where(tag => tag.Commit?.Sha?.Equals(commit) ?? false)
                            .Select(tag => Version.TryParse(tag.Name, out var version) ? version : default)
                            .Where(version => version is not null)
                            .Max() ?? throw new InvalidOperationException());
                    }
                    catch (InvalidOperationException)
                    {
                        await ((await client.Repos[owner][repo].Commits[commit].GetAsync(cancellationToken: cancellationToken))?.Parents ?? [])
                            .Select(parent => parent.Sha)
                            .Where(sha => sha is not null)
                            .Cast<string>()
                            .ForEachAsync(channel.Writer.WriteAsync, cancellationToken);
                    }

                    if (!channel.Reader.TryPeek(out _))
                    {
                        channel.Writer.Complete();
                    }
                }

                return candidates.Max() ?? throw new InvalidOperationException("Tag not found.");
            }
            catch (GitHub.Models.BasicError ex) when (ex.ResponseStatusCode == (int)HttpStatusCode.NotFound)
            {
                throw new InvalidOperationException("Tag not found.");
            }
        }

        async IAsyncEnumerable<System.Formats.Tar.TarEntry> IArchive.Get([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken, string? root)
        {
            if (reader is not null)
            {
                throw new InvalidOperationException("Already opened");
            }

            processEntryToken = cancellationToken;
            this.root = root ?? string.Empty;

            if (this.root.Length > 0 && !this.root.EndsWith('/'))
            {
                this.root += '/';
            }

            await (submodulePaths ?? []).ToAsyncEnumerable().ForEachAwaitWithCancellationAsync(async (path, token) =>
            {
                try
                {
                    var content = await client.Repos[owner][repo].Contents[path].GetAsync(request => request.QueryParameters.Ref = @ref, cancellationToken: token);

                    if (content?.ContentSubmodule is null)
                    {
                        throw new InvalidOperationException(path + " is not a submodule");
                    }

                    Uri uri = new(content.ContentSubmodule.SubmoduleGitUrl!);
                    var parts = uri.GetComponents(UriComponents.Path, UriFormat.Unescaped).Split('/', 2);
                    parts[1] = parts[1][..^4];
                    submodules.Add(path + "/", (parts[0], parts[1], content.ContentSubmodule.Sha!));
                }
                catch (GitHub.Models.BasicError ex) when (ex.ResponseStatusCode == (int)HttpStatusCode.NotFound)
                {
                    Console.WriteLine($"Submodule reference '{path}' not found");
                }
            }, cancellationToken);

            reader = new(new GZipStream((await client.Repos[owner][repo].Tarball[@ref].GetAsync(cancellationToken: cancellationToken))!, CompressionMode.Decompress));

            await foreach (var entry in reader
                .GetEntriesAsync(copyData: true, cancellationToken: cancellationToken)
                .SelectMany(ProcessEntry))
            {
                if (entry.DataStream is not null)
                {
                    var file = entry.DataStream.ToMappedFile();
                    mappedFiles.Add(file);
                    entry.DataStream = file.CreateViewStream(0, entry.Length, System.IO.MemoryMappedFiles.MemoryMappedFileAccess.Read);
                }

                yield return entry;
            }
        }

        private IAsyncEnumerable<System.Formats.Tar.TarEntry> ProcessEntry(System.Formats.Tar.TarEntry entry)
        {
            if (entry.EntryType == System.Formats.Tar.TarEntryType.Directory && srcRoot is null)
            {
                srcRoot = entry.Name;
            }

            if (entry.Name.StartsWith(srcRoot ?? "/"))
            {
                entry.Name = root + entry.Name[srcRoot!.Length..];
            }

            switch (entry.EntryType)
            {
                case System.Formats.Tar.TarEntryType.Directory:
                case System.Formats.Tar.TarEntryType.DirectoryList:
                    if (submodules?.TryGetValue(entry.Name[root!.Length..], out var submoduleData) ?? false)
                    {
                        GitHubArchive submodule = new(client, submoduleData.Owner, submoduleData.Repo, submoduleData.Ref);
                        accessedModules.Add(submodule);
                        return ((IArchive)submodule).Get(cancellationToken: processEntryToken, entry.Name);
                    }
                    break;

                case System.Formats.Tar.TarEntryType.GlobalExtendedAttributes:
                    if (((System.Formats.Tar.PaxGlobalExtendedAttributesTarEntry)entry).GlobalExtendedAttributes.TryGetValue("comment", out var commit))
                    {
                        Console.WriteLine($"'{owner}/{repo}' Commit: {commit}");
                    }
                    break;

                case System.Formats.Tar.TarEntryType.RenamedOrSymlinked:
                    return AsyncEnumerable.Empty<System.Formats.Tar.TarEntry>();

                case System.Formats.Tar.TarEntryType.MultiVolume:
                case System.Formats.Tar.TarEntryType.SparseFile:
                case System.Formats.Tar.TarEntryType.TapeVolume:
                    throw new NotSupportedException(entry.EntryType.ToString());
            }

            return AsyncEnumerable.Empty<System.Formats.Tar.TarEntry>().Append(entry);
        }

        void IDisposable.Dispose()
        {
            reader?.Dispose();
            accessedModules.ForEach(submodule => ((IDisposable)submodule).Dispose());
            mappedFiles.ForEach(file => file.Dispose());
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await (reader?.DisposeAsync() ?? ValueTask.CompletedTask);
            await accessedModules.ToAsyncEnumerable().ForEachAsync(async submodule => await ((IAsyncDisposable)submodule).DisposeAsync(), CancellationToken.None);
            mappedFiles.ForEach(file => file.Dispose());
        }
    }
}
