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

        public void StartReadingFromLogStreams(MultiplexedStream multiplexedStream)
        {
            if (Reader is not null)
            {
                throw new InvalidOperationException("Reader is already started");
            }

            Reader = Task.Run(async () => await ReadOutputToEndAsync(multiplexedStream));
        }

        public void StartReadingFromLogStreams(StreamReader stdOut, StreamReader stdErr)
        {
            if (Reader is not null)
            {
                throw new InvalidOperationException("Reader is already started");
            }

            var stdOutReader = Task.Run(async () => await ReadOutputToEndAsync(stdOut, StreamSource.StandardOut));
            var stdErrReader = Task.Run(async () => await ReadOutputToEndAsync(stdErr, StreamSource.StandardErr));

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
                await Task.WhenAll(Reader).WaitAsync(timeout);

                OnComplete(default);
            }
            catch (Exception? e)
            {
                OnComplete(e);
                throw;
            }
        }

        private async Task ReadOutputToEndAsync(MultiplexedStream multiplexedStream)
        {
            try
            {
                var buffer = new byte[16 * KiB]; //8K at the time
                using (multiplexedStream)
                {
                    var result = await multiplexedStream.ReadOutputAsync(buffer, 0, buffer.Length, CancellationToken.None);

                    while (!result.EOF)
                    {
                        var data = Encoding.UTF8.GetString(buffer, 0, result.Count);
                        if (result.Target == MultiplexedStream.TargetStream.StandardOut)
                        {
                            await AppendStandardOutputAsync(data);
                        }
                        else if (result.Target == MultiplexedStream.TargetStream.StandardError)
                        {
                            await AppendStandardErrAsync(data);
                        }

                        result = await multiplexedStream.ReadOutputAsync(buffer, 0, buffer.Length, CancellationToken.None);
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
                logger.LogError(e, "Failed read and process stream");
            }
        }
    }

    internal enum StreamSource
    {
        StandardOut,
        StandardErr,
    }
}
