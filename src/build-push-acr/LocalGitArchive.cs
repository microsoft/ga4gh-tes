// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
using System.Formats.Tar;
using System.Text;

namespace BuildPushAcr
{
    internal sealed class LocalGitArchive : IArchive
    {
        private const UnixFileMode FileMode =
            UnixFileMode.UserRead |
            UnixFileMode.UserWrite |
            UnixFileMode.GroupRead |
            UnixFileMode.GroupWrite |
            UnixFileMode.OtherRead |
            UnixFileMode.OtherWrite;

        private readonly DirectoryInfo srcDir;

        public LocalGitArchive(DirectoryInfo solutionDir)
        {
            ArgumentNullException.ThrowIfNull(solutionDir);

            this.srcDir = solutionDir;
        }

        async ValueTask<Version> IArchive.GetTagAsync(CancellationToken cancellationToken)
        {
            ProcessStartInfo startInfo = new()
            {
                FileName = Environment.GetEnvironmentVariable(Environment.OSVersion.Platform is PlatformID.Win32NT ? "ComSpec" : "SHELL"),
                WorkingDirectory = srcDir.FullName,
                RedirectStandardOutput = true,
                StandardOutputEncoding = Encoding.UTF8,
                UseShellExecute = false
            };

            startInfo.ArgumentList.Add(Environment.OSVersion.Platform is PlatformID.Win32NT ? "/C" : "-c");
            startInfo.ArgumentList.Add("git");
            startInfo.ArgumentList.Add("tag");
            startInfo.ArgumentList.Add("-l");

            using var git = Process.Start(startInfo) ?? throw new InvalidOperationException("Failed to start git");
            await git.WaitForExitAsync(cancellationToken);

            if (git.ExitCode != 0)
            {
                throw new InvalidOperationException($"Failed to run git ({git.ExitCode})");
            }

            return git.StandardOutput
                .ReadToEnd()
                .ReplaceLineEndings(Environment.NewLine)
                .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(line => Version.TryParse(line, out var version) ? version : default)
                .Where(version => version is not null)
                .Max() ?? throw new InvalidOperationException("No version-like tags found");
        }

        IAsyncEnumerable<TarEntry> IArchive.Get(CancellationToken _1, string? root)
        {
            string[] foldersToIgnore = ["bin", "obj", "TestResults"];
            string[] rootFoldersToIgnore = [".git", ".github", ".vs"];
            var prefix = string.IsNullOrWhiteSpace(root) ? string.Empty : root + '/';
            var sourceRootLength = srcDir.FullName.Length + 1;

            return srcDir
                .EnumerateFileSystemInfos("*", SearchOption.AllDirectories)
                .Where(entry => !rootFoldersToIgnore.Contains(Path.GetRelativePath(srcDir.FullName, entry.FullName).Split((char[]?)['/', '\\'], 2)[0]))
                .OrderBy(entry => entry.FullName, StringComparer.Ordinal)
                .Where(entry => !(NormalizePath(entry.FullName)[sourceRootLength..].Split('/', StringSplitOptions.RemoveEmptyEntries).Any(name => foldersToIgnore.Contains(name))))
                .Select(Convert)
                .Where(entry => entry is not null)
                .Cast<PaxTarEntry>()
                .Select(Configure)
                .ToAsyncEnumerable();

            PaxTarEntry? Convert(FileSystemInfo info)
                => info switch
                {
                    DirectoryInfo dir => new PaxTarEntry(TarEntryType.Directory, prefix + NormalizePath(dir.FullName[sourceRootLength..])),
                    FileInfo file => CreateFileEntry(file, prefix, sourceRootLength),
                    _ => throw new NotSupportedException()
                };

            static PaxTarEntry? CreateFileEntry(FileInfo file, string prefix, int sourceRootLength)
            {
                try
                {
                    return new(TarEntryType.RegularFile, prefix + NormalizePath(file.FullName[sourceRootLength..])) { DataStream = file.OpenRead() };
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    Console.Error.WriteLine($"Skipping file '{file.FullName}': ({ex.GetType().FullName}): {ex.Message}");
                    return null;
                }
            }

            static string NormalizePath(string path)
                => Environment.OSVersion.Platform is PlatformID.Win32NT ? path.Replace('\\', '/') : path;

            PaxTarEntry Configure(PaxTarEntry entry)
            {
                entry.Mode = FileMode;
                entry.Uid = 0;
                entry.Gid = 0;
                entry.UserName = "root";
                entry.GroupName = "root";
                return entry;
            }
        }
    }
}
