// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer;

/// <summary>
/// Default file info provider implementation that uses the standard .NET APIs.
/// Glob support is limited to the standard .NET APIs.
/// </summary>
public class DefaultFileInfoProvider : IFileInfoProvider
{
    private readonly ILogger logger = PipelineLoggerFactory.Create<DefaultFileInfoProvider>();

    public long GetFileSize(string fileName)
    {
        logger.LogTrace("Getting file size for file: {Path}", fileName);

        return GetFileInfoOrThrowIfFileDoesNotExist(fileName).Length;
    }

    public string GetExpandedFileName(string fileName)
    {
        logger.LogTrace("Expanding file name: {Path}", fileName);

        var expandedValue = Environment.ExpandEnvironmentVariables(fileName);

        logger.LogTrace("Expanded file name: {ExpandedPath}", expandedValue);

        return expandedValue;
    }

    public bool FileExists(string fileName)
    {
        logger.LogTrace("Checking if file exists: {Path}", fileName);

        var fileInfo = new FileInfo(Environment.ExpandEnvironmentVariables(fileName));

        return fileInfo.Exists;
    }


    public List<FileResult> GetFilesBySearchPattern(string searchPath, string searchPattern)
    {
        logger.LogDebug("Searching for files in the search path: {Path} with search pattern: {SearchPattern}", searchPath, searchPattern);

        return Directory.GetFiles(Environment.ExpandEnvironmentVariables(searchPath), Environment.ExpandEnvironmentVariables(searchPattern), SearchOption.AllDirectories)
            .Select(f => new FileResult(f, ToRelativePathToSearchPath(searchPath, searchPattern, f), searchPath))
            .ToList();
    }

    private string ToRelativePathToSearchPath(string searchPath, string searchPattern, string absolutePath)
    {
        var delimiter = "/";

        if (searchPath.Equals("/", StringComparison.OrdinalIgnoreCase))
        {
            delimiter = string.Empty;
        }
        var prefixToRemove = Path.GetDirectoryName($"{searchPath}{delimiter}{searchPattern.TrimStart('/')}");

        if (!string.IsNullOrWhiteSpace(prefixToRemove) && absolutePath.StartsWith(prefixToRemove))
        {
            logger.LogDebug("Removing prefix: {Prefix} from absolute path: {Path}", prefixToRemove, absolutePath);

            return absolutePath[(prefixToRemove.Length + 1)..];
        }

        return absolutePath;
    }

    public List<FileResult> GetAllFilesInDirectory(string path)
    {
        var expandedPath = Environment.ExpandEnvironmentVariables(path);

        logger.LogDebug("Getting all files in directory: {Path}", expandedPath);

        if (!Directory.Exists(expandedPath))
        {
            logger.LogWarning("The directory provided does not exist: {Path}. The output will be ignored.", expandedPath);

            return [];
        }

        return Directory.GetFiles(expandedPath, "*", SearchOption.AllDirectories)
            .Select(f => new FileResult(f, ToRelativePathToSearchPath(expandedPath, searchPattern: string.Empty, f), expandedPath))
            .ToList();
    }

    public RootPathPair GetRootPathPair(string path)
    {
        ArgumentNullException.ThrowIfNull(path);

        var root = Path.GetPathRoot(path);

        if (string.IsNullOrEmpty(root))
        {
            throw new ArgumentException($"Path {path} does not have a root");
        }

        var relativePath = path[root.Length..];

        return new RootPathPair(root, relativePath);
    }

    private static FileInfo GetFileInfoOrThrowIfFileDoesNotExist(string fileName)
    {
        var expandedFilename = Environment.ExpandEnvironmentVariables(fileName);

        var fileInfo = new FileInfo(expandedFilename);

        if (!fileInfo.Exists)
        {
            throw new FileNotFoundException($"File {fileName} does not exist. Expanded value: {expandedFilename}");
        }

        return fileInfo;
    }
}
