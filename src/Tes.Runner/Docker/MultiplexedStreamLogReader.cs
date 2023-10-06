// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text;
using Docker.DotNet;

namespace Tes.Runner.Docker
{
    public abstract class MultiplexedStreamLogReader : IStreamLogReader
    {
        const int KiB = 1024;

        protected Task? Reader;

        public abstract Task AppendStandardOutputAsync(string data);
        public abstract Task AppendStandardErrAsync(string data);

        public abstract void OnComplete(Exception? err);

        public void StartReadingFromLogStream(MultiplexedStream multiplexedStream)
        {
            Reader = Task.Run(async () => await ReadOutputToEndAsync(multiplexedStream));
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

            var buffer = new byte[8 * KiB]; //8K at the time
            MultiplexedStream.ReadResult result;
            do
            {
                result = await multiplexedStream.ReadOutputAsync(buffer, 0, buffer.Length, CancellationToken.None);
                var data = Encoding.UTF8.GetString(buffer, 0, result.Count);
                if (result.Target == MultiplexedStream.TargetStream.StandardOut)
                {
                    await AppendStandardOutputAsync(data);
                }
                else if (result.Target == MultiplexedStream.TargetStream.StandardError)
                {
                    await AppendStandardErrAsync(data);
                }
            }
            while (!result.EOF);
        }
    }
}
