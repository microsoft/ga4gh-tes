// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Formats.Tar;
using System.IO.Compression;
using System.Text.RegularExpressions;
using Azure.Containers.ContainerRegistry;
using Azure.Core;
using Azure.ResourceManager;
using Azure.ResourceManager.ContainerRegistry;
using Azure.ResourceManager.ContainerRegistry.Models;
using Azure.Storage.Blobs;

namespace BuildPushAcr
{
    public enum LogType
    {
        None,
        NonInteractive,
        Interactive,
        CapturedOnError
    }

    public enum BuildType
    {
        Tes,
        CoA
    }

    public partial class AcrBuild(BuildType buildType, Version tag, ResourceIdentifier acrId, TokenCredential credential, ContainerRegistryAudience audience)
    {
        private readonly BuildType buildType = buildType;
        private readonly Version tag = tag ?? throw new ArgumentNullException(nameof(tag));
        private readonly ResourceIdentifier acrId = acrId ?? throw new ArgumentNullException(nameof(acrId));
        private readonly TokenCredential credential = credential ?? throw new ArgumentNullException(nameof(credential));
        private readonly ContainerRegistryAudience audience = audience;
        private readonly Dictionary<string, Regex>? replacements = new()
        {
            ["CommonAssemblyInfo.props"] = VersionRegex()
        };

        private ContainerRegistryResource? acr;
        List<TarEntry>? entries = default;
        private Version? build;

        private const string Root = "root";

        public Version Tag => build ?? new();

        public async ValueTask LoadAsync(IArchive archive, ArmEnvironment environment, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(buildType);
            ArgumentNullException.ThrowIfNull(archive);

            Console.WriteLine("Determining build revision");
            acr = await new ArmClient(
                    credential,
                    null,
                    new()
                    {
                        Environment = environment,
                        RetryPolicy = new Azure.Core.Pipeline.RetryPolicy(3, DelayStrategy.CreateExponentialDelayStrategy(TimeSpan.FromSeconds(5)))
                    })
                .GetContainerRegistryResource(acrId)
                .GetAsync(cancellationToken);

            var repository = new ContainerRegistryClient(
                new UriBuilder(
                        Uri.UriSchemeHttps,
                        acr.Data.LoginServer).Uri,
                        credential,
                        new()
                        {
                            Audience = audience,
                            RetryPolicy = new Azure.Core.Pipeline.RetryPolicy(3, DelayStrategy.CreateExponentialDelayStrategy(TimeSpan.FromSeconds(2)))
                        })
                    .GetRepository(buildType switch
                    {
                        BuildType.Tes => "ga4gh/tes",
                        BuildType.CoA => "cromwellonazure/triggerservice",
                        _ => throw new System.Diagnostics.UnreachableException()
                    });

            Version? maxTag;

            // Figure out version from ACR using tag
            try
            {
                maxTag = await repository.GetAllManifestPropertiesAsync(cancellationToken: cancellationToken)
                    .SelectMany(props => props.Tags.ToAsyncEnumerable())
                    .Where(tag => tag.StartsWith(this.tag.ToString(3)) && Version.TryParse(tag, out _))
                    .Select(tag => new Version(tag))
                    .MaxAsync(cancellationToken);
            }
            catch (Azure.RequestFailedException ex) when (ex.Status == 404)
            {
                maxTag = default;
            }

            build = new(tag.ToString(3) + "." + (NextValue(maxTag?.Revision) ?? "0"));

            Console.WriteLine("Preparing source code");
            entries = await archive.Get(cancellationToken, Root)
                .Where(entry => !(entry.EntryType == TarEntryType.RegularFile && "nuget.config".Equals(Path.GetFileName(entry.Name), StringComparison.OrdinalIgnoreCase)))
                .Select(ProcessEntry)
                .ToListAsync(cancellationToken);

            static string? NextValue(int? i) => i is null ? default : (i + 1).ToString();
        }

        public async ValueTask<(bool Success, string? Log)> BuildAsync(LogType logType, CancellationToken cancellationToken)
        {
            if (build is null || entries is null || acr is null)
            {
                throw new InvalidOperationException("LoadAsync must be called first");
            }

            FileInfo? tesTar = default;
            FileInfo? coaTar = default;
            var captured = string.Empty;

            try
            {
                if (logType != LogType.CapturedOnError)
                {
                    Console.WriteLine("Staging source code");
                }

                switch (buildType)
                {
                    case BuildType.Tes:
                        tesTar = await TrimTarAsync(entries, Root + "/src/", cancellationToken);
                        break;
                    case BuildType.CoA:
                        tesTar = await TrimTarAsync(entries, Root + "/src/ga4gh-tes/src/", cancellationToken);
                        coaTar = await TrimTarAsync(entries, Root + "/src/", cancellationToken);
                        break;
                    default:
                        throw new System.Diagnostics.UnreachableException();
                }


                entries.ForEach(entry => entry.DataStream?.Dispose());

                if (logType != LogType.CapturedOnError)
                {
                    Console.WriteLine();
                }

                (var succeeded, captured) = await BuildAsync(logType, acr, tesTar, "Dockerfile-Tes", [$"ga4gh/tes:{build}", $"cromwellonazure/tes:{build}"], cancellationToken);

                if (succeeded && coaTar is not null)
                {
                    if (logType != LogType.CapturedOnError)
                    {
                        Console.WriteLine();
                    }

                    (succeeded, var moreLog) = await BuildAsync(logType, acr, coaTar, "Dockerfile-TriggerService", [$"cromwellonazure/triggerservice:{build}"], cancellationToken);
                    captured += Environment.NewLine + moreLog;
                }

                if (logType != LogType.CapturedOnError)
                {
                    Console.WriteLine();
                }

                return (succeeded, logType != LogType.CapturedOnError ? null : captured);
            }
            finally
            {
                tesTar?.Delete();
                coaTar?.Delete();
            }
        }

        private static async ValueTask<FileInfo> TrimTarAsync(List<TarEntry> entries, string prefix, CancellationToken cancellationToken)
        {
            FileInfo tarFile = new(Path.GetTempFileName());

            {
                using TarWriter writer = new(new GZipStream(tarFile.Create(), CompressionMode.Compress));
                await entries.ToAsyncEnumerable()
                    .Where(entry => (entry.Name.Length > prefix.Length && entry.Name.StartsWith(prefix)) || TarEntryType.GlobalExtendedAttributes.Equals(entry.EntryType))
                    .ForEachAwaitWithCancellationAsync((entry, token) =>
                    {
                        entry = entry.Clone();
                        entry.Name = entry.Name[prefix.Length..];
                        return writer.WriteEntryAsync(entry, token);
                    }, cancellationToken);
            }

            return tarFile;
        }

        private static HttpClient GetLogClient(string url)
        {
            HttpClient client = new() { BaseAddress = new(url, UriKind.Absolute) };
            client.DefaultRequestHeaders.Accept.Add(new("text/plain"));
            client.DefaultRequestHeaders.Accept.Add(new("application/json"));
            return client;
        }

        private static async ValueTask<(bool Success, string Log)> BuildAsync(LogType logType, ContainerRegistryResource acr, FileInfo tarFile, string dockerFilePath, IEnumerable<string> imageNames, CancellationToken cancellationToken)
        {
            using StringWriter writer = new();
            Action<string> ConsoleWrite = new(message =>
            {
                if (logType == LogType.CapturedOnError)
                {
                    writer.Write(message);
                }
                else
                {
                    Console.Write(message);
                }
            });

            Action<string> ConsoleWriteLine = new(message => ConsoleWrite(message + Environment.NewLine));

            var sourceUpload = await acr.GetBuildSourceUploadUrlAsync(cancellationToken);
            {
                using var stream = tarFile.OpenRead();
                await new BlobClient(sourceUpload.Value.UploadUri, new()
                {
                    RetryPolicy = new Azure.Core.Pipeline.RetryPolicy(3, DelayStrategy.CreateExponentialDelayStrategy(TimeSpan.FromSeconds(2)))
                })
                    .UploadAsync(BinaryData.FromStream(stream), cancellationToken);
            }

            ConsoleWriteLine($"Building {dockerFilePath} in '{sourceUpload.Value.RelativePath}'");
            ConsoleWriteLine(string.Empty);
            ContainerRegistryDockerBuildContent content = new(dockerFilePath, new(ContainerRegistryOS.Linux)) { SourceLocation = sourceUpload.Value.RelativePath };

            foreach (var name in imageNames.SelectMany(name => (string[])[name, name[..name.LastIndexOf('.')]]))
            {
                content.ImageNames.Add(name);
            }

            var taskEndFound = false;
            var task = (await acr.ScheduleRunAsync(Azure.WaitUntil.Completed, content, cancellationToken)).Value;

            if (logType != LogType.Interactive)
            {
                while ((await task.GetAsync(cancellationToken)).Value.Data.FinishOn is null)
                {
                    await Task.Delay(1000, cancellationToken);
                }
            }

            if (logType == LogType.NonInteractive || (logType == LogType.CapturedOnError && !await BuildSucceeded()))
            {
                if (logType == LogType.CapturedOnError)
                {
                    ConsoleWriteLine((await task.GetAsync(cancellationToken)).Value.Data.RunErrorMessage);
                }

                ConsoleWriteLine(await (await GetLogClient((await task.GetLogSasUrlAsync(cancellationToken)).Value.LogLink)
                    .GetAsync(string.Empty, cancellationToken))
                    .Content.ReadAsStringAsync(cancellationToken));
            }

            if (logType == LogType.Interactive)
            {
                await ShowLogAsync((await task.GetLogSasUrlAsync(cancellationToken)).Value.LogLink, IsRunning, cancellationToken);
            }

            return (await BuildSucceeded(), writer.ToString());

            async ValueTask<bool> BuildSucceeded()
            {
                ContainerRegistryRunStatus?[] successful = [ContainerRegistryRunStatus.Succeeded];
                ContainerRegistryRunStatus?[] failure = [ContainerRegistryRunStatus.Failed, ContainerRegistryRunStatus.Canceled, ContainerRegistryRunStatus.Error, ContainerRegistryRunStatus.Timeout];

                while (true)
                {
                    var status = (await task.GetAsync(cancellationToken)).Value.Data.Status;

                    if (successful.Contains(status))
                    {
                        return true;
                    }
                    else if (failure.Contains(status))
                    {
                        return false;
                    }

                    await Task.Delay(1000, cancellationToken);
                }
            }

            async ValueTask<bool> IsRunning(CancellationToken token)
            {
                if ((await task.GetAsync(token)).Value.Data.FinishOn is null)
                {
                    return true;
                }

                if (taskEndFound)
                {
                    return false;
                }

                taskEndFound = true;
                return true;
            }

            static async ValueTask ShowLogAsync(string url, Func<CancellationToken, ValueTask<bool>> isRunning, CancellationToken cancellationToken)
            {
                var left = Console.CursorLeft;
                var top = Console.CursorTop;
                using var client = GetLogClient(url);
                DateTimeOffset? lastModified = DateTimeOffset.MinValue;
                bool rangeBytesunit = default;
                long length = 0;

                do
                {
                    await Task.Delay(1000, cancellationToken);
                    using HttpRequestMessage request = new(HttpMethod.Get, (Uri?)null);

                    if (lastModified > DateTimeOffset.MinValue)
                    {
                        request.Headers.IfModifiedSince = lastModified;
                    }

                    if (rangeBytesunit)
                    {
                        request.Headers.Range = new(length, null);
                    }

                    using var response = await client.SendAsync(request, cancellationToken);

                    if (response.StatusCode == System.Net.HttpStatusCode.NotModified)
                    {
                        continue;
                    }

                    try
                    {
                        response.EnsureSuccessStatusCode();
                    }
                    catch (HttpRequestException ex)
                    {
                        Console.WriteLine(response.RequestMessage?.RequestUri?.ToString() ?? "<none>");
                        Console.WriteLine($"HttpRequestException({ex.HResult:x}): {ex.HttpRequestError.ToString() ?? "<none>"} StatusCode: ({ex.StatusCode}) InnerException: {ex.InnerException?.Message ?? "<none>"}");
                        Console.WriteLine(await response.Content.ReadAsStringAsync(cancellationToken));
                        throw;
                    }

                    lastModified = response.Content.Headers.LastModified;

                    if (!rangeBytesunit && response.Headers.AcceptRanges is not null)
                    {
                        foreach (var item in response.Headers.AcceptRanges)
                        {
                            rangeBytesunit |= "bytes".Equals(item, StringComparison.OrdinalIgnoreCase);
                        }
                    }

                    var log = await response.Content.ReadAsStringAsync(cancellationToken);

                    if (response.Content.Headers.ContentRange is null)
                    {
                        for (var line = Console.CursorTop; line >= top; --line)
                        {
                            Console.SetCursorPosition(left, line);
                            Console.Write(new string(' ', Console.BufferWidth - (line == top ? left : 0)));
                        }

                        Console.SetCursorPosition(left, top);

                        length = response.Content.Headers.ContentLength ?? 0;
                    }
                    else
                    {
                        length += response.Content.Headers.ContentLength ?? 0;
                    }

                    Console.Write(log);
                }
                while (await isRunning(cancellationToken));
            }
        }

        private TarEntry ProcessEntry(TarEntry entry)
        {
            switch (entry.EntryType)
            {
                case TarEntryType.V7RegularFile:
                case TarEntryType.RegularFile:
                case TarEntryType.ContiguousFile:
                    if (replacements?.TryGetValue(Path.GetFileName(entry.Name), out var replacement) ?? false)
                    {
                        MemoryStream? stream = default;

                        try
                        {
                            if (replacement.GetGroupNames().Contains("version", StringComparer.Ordinal))
                            {
                                string? content;

                                {
                                    using StreamReader reader = new(entry.DataStream!, leaveOpen: true);
                                    content = reader.ReadToEnd();
                                }

                                var match = replacement.Match(content);

                                if (match.Success)
                                {
                                    var group = match.Groups["version"];
                                    content = content[..group.Index] + build!.ToString() + content[(group.Index + group.Length)..];
                                    stream = new(content.Length);

                                    {
                                        using StreamWriter writer = new(stream, leaveOpen: true);
                                        writer.Write(content);
                                        writer.Flush();
                                    }

                                    stream.Position = 0;
                                    entry.DataStream = stream;
                                    stream = default;
                                }
                            }
                        }
                        finally
                        {
                            stream?.Dispose();

                            try
                            {
                                entry.DataStream!.Position = 0;
                            }
                            catch (NotSupportedException)
                            { }
                        }
                    }
                    break;
            }

            return entry;
        }

        [GeneratedRegex(@"^.*<AssemblyVersion>(?<version>\d+\.\d+\.\d+\.\d+)<\/AssemblyVersion>.*$", RegexOptions.Compiled | RegexOptions.Singleline)]
        private static partial Regex VersionRegex();
    }
}
