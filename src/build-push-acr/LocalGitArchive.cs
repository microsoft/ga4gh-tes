// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
using System.Formats.Tar;
using System.Text;

namespace BuildPushAcr
{
    public sealed class LocalGitArchive(DirectoryInfo solutionDir) : IArchive
    {
        private readonly DirectoryInfo srcDir = solutionDir ?? throw new ArgumentNullException(nameof(solutionDir));

        /// <summary>
        /// Creates a source code archive from the local filesystem
        /// </summary>
        /// <param name="solution">The directory containing the VisualStudio solution file.</param>
        /// <returns>The archive.</returns>
        public static IArchive Create(DirectoryInfo solution)
        {
            ArgumentNullException.ThrowIfNull(solution);

            Console.WriteLine("Scanning source code");
            return new LocalGitArchive(solution);
        }

        private const UnixFileMode FileMode =
            UnixFileMode.UserRead |
            UnixFileMode.UserWrite |
            UnixFileMode.GroupRead |
            UnixFileMode.GroupWrite |
            UnixFileMode.OtherRead |
            UnixFileMode.OtherWrite;

        /// <inheritdoc/>
        async ValueTask<(Version Version, string? Prerelease)> IArchive.GetTagAsync(CancellationToken cancellationToken)
        {
            var gitBinary = FindExecutable.FindExecutable.FullPath("git");

            if (string.IsNullOrWhiteSpace(gitBinary))
            {
                throw new InvalidOperationException("Failed to locate the `git` tool.");
            }

            ProcessStartInfo startInfo = new()
            {
                FileName = gitBinary,
                WorkingDirectory = srcDir.FullName,
                RedirectStandardOutput = true,
                StandardOutputEncoding = Encoding.UTF8,
                UseShellExecute = false
            };

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
                .Select(IArchive.ParseTag)
                .Where(version => version is not null)
                .MaxBy(version => version!.Value.Version) ?? throw new InvalidOperationException("No version-like tags found");
        }

        /// <inheritdoc/>
        IAsyncEnumerable<TarEntry> IArchive.Get(CancellationToken _1, string? root)
        {
            string[] subFoldersToIgnore = ["bin", "obj", "TestResults"];
            string[] rootFoldersToIgnore = [".git", ".github", ".vs"];
            var prefix = string.IsNullOrWhiteSpace(root) ? string.Empty : root + '/';
            var sourceRootLength = srcDir.FullName.Length + 1;

            return srcDir
                .EnumerateFileSystemInfos("*", SearchOption.AllDirectories)
                .Where(entry => !rootFoldersToIgnore.Contains(Path.GetRelativePath(srcDir.FullName, entry.FullName).Split((char[]?)['/', '\\'], 2)[0]))
                .OrderBy(entry => entry.FullName, StringComparer.Ordinal)
                .Where(entry => !(NormalizePath(entry.FullName)[sourceRootLength..].Split('/', StringSplitOptions.RemoveEmptyEntries).Any(name => subFoldersToIgnore.Contains(name))))
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
                    _ => throw new UnreachableException(),
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
