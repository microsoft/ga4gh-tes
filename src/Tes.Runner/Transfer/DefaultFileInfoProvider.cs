// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer;

public class DefaultFileInfoProvider : IFileInfoProvider
{
    private readonly ILogger logger = PipelineLoggerFactory.Create<DefaultFileInfoProvider>();

    public long GetFileSize(string fileName)
    {
        logger.LogInformation($"Getting file size for file: {fileName}");

        return new FileInfo(Environment.ExpandEnvironmentVariables(fileName)).Length;
    }

    public string GetFileName(string fileName)
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

    public string[] GetFilesInAllDirectories(string path, string searchPattern)
    {
        logger.LogInformation($"Searching for files in path: {path} with search pattern: {searchPattern}");

        return Directory.GetFiles(Environment.ExpandEnvironmentVariables(path), Environment.ExpandEnvironmentVariables(searchPattern), SearchOption.AllDirectories);
    }

    public string[] GetFilesInDirectory(string path)
    {
        logger.LogDebug($"Searching for files in path: {path}");

        return Directory.GetFiles(Environment.ExpandEnvironmentVariables(path));
    }
}
