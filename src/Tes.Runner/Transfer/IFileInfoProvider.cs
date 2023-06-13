// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer;

// A simple abstraction to the file stats information.
// The main purpose of this is to facilitate testing of scenarios such as when the file size of very
// large files are required and is not feasible to create temp test files e.g. a 500GiB file. 

public interface IFileInfoProvider
{
    long GetFileSize(string fileName);

    string GetFileName(string fileName);

    bool FileExists(string fileName);

    string[] GetFilesInAllDirectories(string path, string searchPattern);

    string[] GetFilesInDirectory(string path);
}
