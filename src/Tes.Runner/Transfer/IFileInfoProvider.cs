// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer;

// A simple abstraction to the file stats information.
// The main purpose of this is to facilitate testing and provide a light abstraction on top of the file system
// and enable future extensibility to support full glob POSIX semantics.

public interface IFileInfoProvider
{
    long GetFileSize(string fileName);

    string GetExpandedFileName(string fileName);

    bool FileExists(string fileName);

    string[] GetFilesBySearchPattern(string path, string searchPattern);

    string[] GetAllFilesInDirectory(string path);
}
