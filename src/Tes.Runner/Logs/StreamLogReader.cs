// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text;
using Docker.DotNet;
using Microsoft.Extensions.Logging;
using Tes.Runner.Transfer;

namespace Tes.Runner.Logs
{
    public abstract class StreamLogReader : IStreamLogReader
    {
        const int KiB = 1024;
        private readonly ILogger logger = PipelineLoggerFactory.Create<StreamLogReader>();
        protected Task? Reader;

        public abstract Task AppendStandardOutputAsync(string data);
        public abstract Task AppendStandardErrAsync(string data);

        public abstract void OnComplete(Exception? err);

        public void StartReadingFromLogStreams(MultiplexedStream multiplexedStream, Stream? stdIn, Stream? stdOut, Stream? stdErr)
        {
            if (Reader is not null)
            {
                throw new InvalidOperationException("Reader was already started");
            }

            var multiplexedReader = ReadOutputToEndAsync(multiplexedStream, stdOut, stdErr);
            var stdInWriter = stdIn is null ? Task.CompletedTask : WriteInputStream(multiplexedStream, stdIn);

            Reader = Task.WhenAll(multiplexedReader, stdInWriter);
        }

        public void StartReadingFromLogStreams(StreamReader stdOut, StreamReader stdErr)
        {
            if (Reader is not null)
            {
                throw new InvalidOperationException("Reader was already started");
            }

            var stdOutReader = ReadOutputToEndAsync(stdOut, StreamSource.StandardOut);
            var stdErrReader = ReadOutputToEndAsync(stdErr, StreamSource.StandardErr);

            Reader = Task.WhenAll(stdErrReader, stdOutReader);
        }

        public async Task WaitUntilAsync(TimeSpan timeout)
        {
            try
            {
                if (Reader is null)
                {
                    throw new InvalidOperationException("Stream reading has not been started");
                }

                await Reader.WaitAsync(timeout);
                OnComplete(default);
            }
            catch (Exception? e)
            {
                OnComplete(e);
                throw;
            }
        }

        private async Task ReadOutputToEndAsync(MultiplexedStream multiplexedStream, Stream? stdOut, Stream? stdErr)
        {
            try
            {
                var buffer = new byte[16 * KiB]; //16K at the time
                using (multiplexedStream)
                {
                    for (var result = await multiplexedStream.ReadOutputAsync(buffer, 0, buffer.Length, CancellationToken.None);
                        !result.EOF;
                        result = await multiplexedStream.ReadOutputAsync(buffer, 0, buffer.Length, CancellationToken.None))
                    {
                        var data = Encoding.UTF8.GetString(buffer, 0, result.Count);

                        switch (result.Target)
                        {
                            case MultiplexedStream.TargetStream.StandardOut:
                                await AppendStandardOutputAsync(data);

                                if (stdOut is not null)
                                {
                                    await stdOut.WriteAsync(buffer, 0, result.Count);
                                }
                                break;

                            case MultiplexedStream.TargetStream.StandardError:
                                await AppendStandardErrAsync(data);

                                if (stdErr is not null)
                                {
                                    await stdErr.WriteAsync(buffer, 0, result.Count);
                                }
                                break;
                        }
                    }
                }
            }

            catch (Exception e)
            {
                logger.LogError(e, "Failed read form the multiplexed stream");
            }
        }

        private async Task ReadOutputToEndAsync(StreamReader streamSource, StreamSource source)
        {
            try
            {
                var buffer = new Memory<char>(new char[16 * KiB]); //16K at the time
                using (streamSource)
                {
                    while (!streamSource.EndOfStream)
                    {
                        var result = await streamSource.ReadAsync(buffer, CancellationToken.None);

                        switch (source)
                        {
                            case StreamSource.StandardOut:
                                await AppendStandardOutputAsync(buffer[0..result].ToString());
                                break;

                            case StreamSource.StandardErr:
                                await AppendStandardErrAsync(buffer[0..result].ToString());
                                break;

                            default:
                                throw new ArgumentOutOfRangeException(nameof(source), source, null);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to read and process stream");
            }
        }

        private static async Task WriteInputStream(MultiplexedStream multiplexedStream, Stream stream)
        {
            await multiplexedStream.CopyFromAsync(stream, CancellationToken.None);
            multiplexedStream.CloseWrite();
        }
    }

    internal enum StreamSource
    {
        StandardOut,
        StandardErr,
    }
}
