﻿// Copyright (c) Microsoft Corporation.
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
        logger.LogInformation($"Getting file size for file: {fileName}");

        return GetFileInfoOrThrowIfFileDoesNotExist(fileName).Length;
    }

    public string GetExpandedFileName(string fileName)
    {
        logger.LogInformation($"Expanding file name: {fileName}");

        return Environment.ExpandEnvironmentVariables(fileName);
    }

    public bool FileExists(string fileName)
    {
        logger.LogInformation($"Checking if file exists: {fileName}");

        var fileInfo = new FileInfo(Environment.ExpandEnvironmentVariables(fileName));

        return fileInfo.Exists;
    }


    public List<FileResult> GetFilesBySearchPattern(string searchPath, string searchPattern)
    {
        logger.LogInformation($"Searching for files in the search path: {searchPath} with search pattern: {searchPattern}");

        return Directory.GetFiles(Environment.ExpandEnvironmentVariables(searchPath), Environment.ExpandEnvironmentVariables(searchPattern), SearchOption.AllDirectories)
            .Select(f => new FileResult(f, ToRelativePathToSearchPath(searchPath, f), searchPath))
            .ToList();
    }

    private string ToRelativePathToSearchPath(string searchPath, string absolutePath)
    {

        if (absolutePath.StartsWith(searchPath))
        {
            return absolutePath.Substring(searchPath.Length + 1);
        }

        return absolutePath;
    }

    public List<FileResult> GetAllFilesInDirectory(string path)
    {
        logger.LogInformation($"Getting all files in directory: {path}");

        if (!Directory.Exists(path))
        {
            logger.LogWarning($"The directory provided does not exists: {path}. The output will be ignored.");

            return new List<FileResult>();
        }

        return Directory.GetFiles(Environment.ExpandEnvironmentVariables(path), "*", SearchOption.AllDirectories)
            .Select(f => new FileResult(f, ToRelativePathToSearchPath(path, f), path))
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

        var relativePath = path.Substring(root.Length);

        return new RootPathPair(root, relativePath);
    }

    private FileInfo GetFileInfoOrThrowIfFileDoesNotExist(string fileName)
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
