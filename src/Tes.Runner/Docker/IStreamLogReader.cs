// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Docker.DotNet;

namespace Tes.Runner.Docker;

public interface IStreamLogReader
{
    void StartReadingFromLogStream(MultiplexedStream multiplexedStream);
    Task WaitUntilAsync(TimeSpan timeout);
}
