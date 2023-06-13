// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer;

public class DefaultFileInfoProvider : IFileInfoProvider
{
    public long GetFileSize(string fileName)
    {
        return new FileInfo(Environment.ExpandEnvironmentVariables(fileName)).Length;
    }

    public string GetFileName(string fileName)
    {
        return Environment.ExpandEnvironmentVariables(fileName);
    }

    public bool FileExists(string fileName)
    {
        var fileInfo = new FileInfo(Environment.ExpandEnvironmentVariables(fileName));

        return fileInfo.Exists;
    }

    public string[] GetFilesInAllDirectories(string path, string searchPattern)
    {
        return Directory.GetFiles(Environment.ExpandEnvironmentVariables(path), Environment.ExpandEnvironmentVariables(searchPattern), SearchOption.AllDirectories);
    }

    public string[] GetFilesInDirectory(string path)
    {
        return Directory.GetFiles(Environment.ExpandEnvironmentVariables(path));
    }
}
