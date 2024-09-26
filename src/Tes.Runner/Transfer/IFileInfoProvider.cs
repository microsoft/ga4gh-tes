// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer;

// A simple abstraction for file searching and stats.
// The main purpose of this is to facilitate testing and provide a light abstraction on top of the file system
// and enable future extensibility to support full glob POSIX semantics.

public interface IFileInfoProvider
{
    long GetFileSize(string fileName);

    string GetExpandedFileName(string fileName);

    bool FileExists(string fileName);

    List<FileResult> GetFilesBySearchPattern(string searchPath, string searchPattern);

    List<FileResult> GetAllFilesInDirectory(string path);

    RootPathPair GetRootPathPair(string path);
}

public record RootPathPair(string Root, string RelativePath);

public record FileResult(string AbsolutePath, string RelativePathToSearchPath, string SearchPath);
