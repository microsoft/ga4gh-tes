// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Docker.DotNet;

namespace Tes.Runner.Logs;

public interface IStreamLogReader
{
    void StartReadingFromLogStreams(MultiplexedStream multiplexedStream, Stream? stdIn = default, Stream? stdOut = default, Stream? stdErr = default);
    void StartReadingFromLogStreams(StreamReader stdOut, StreamReader stdErr);
    Task WaitUntilAsync(TimeSpan timeout);
}
