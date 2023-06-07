// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer;

public class DefaultFileInfoProvider : IFileInfoProvider
{
    public long GetFileSize(string fileName)
    {
        return new FileInfo(Environment.ExpandEnvironmentVariables(fileName)).Length;
    }
}
