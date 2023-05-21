using System.Security.Cryptography;
using System.Threading.Channels;

namespace Tes.Runner.Transfer
{
    public class Md5Processor
    {
        private readonly MD5 md5Provider;
        private int partCounter = 1;
        private readonly Channel<PipelineBuffer> deferredChannel;
        private readonly SemaphoreSlim semaphore;
        private Task? deferredProcessor;

        public Md5Processor(MD5 md5Provider, SemaphoreSlim semaphore)
        {
            this.md5Provider = md5Provider;
            this.semaphore = semaphore;

            deferredChannel = Channel.CreateUnbounded<PipelineBuffer>();
        }

        public async Task TryProcessPartAsync(PipelineBuffer buffer, Channel<byte[]> memoryBufferChannel)
        {
            await semaphore.WaitAsync();

            try
            {
                if (TryApplyTransform(buffer))
                {
                    await memoryBufferChannel.Writer.WriteAsync(buffer.Data);
                    return;
                }

                deferredProcessor ??= StartDeferredChannelProcessorAsync();

                await deferredChannel.Writer.WriteAsync(buffer);
            }
            finally
            {
                semaphore.Release();
            }
        }

        private bool TryApplyTransform(PipelineBuffer buffer)
        {
            if (partCounter == buffer.NumberOfParts)
            {
                md5Provider.TransformFinalBlock(buffer.Data, 0, buffer.Length);
                deferredChannel.Writer.Complete();
                partCounter++;
                return true;
            }
            if (partCounter == buffer.Ordinal)
            {
                md5Provider.TransformBlock(buffer.Data, 0, buffer.Length, null, 0);
                partCounter++;
                return true;
            }

            return false;
        }

        private async Task StartDeferredChannelProcessorAsync()
        {
            PipelineBuffer buffer;
            while (await deferredChannel.Reader.WaitToReadAsync())
            {
                while (deferredChannel.Reader.TryRead(out buffer!))
                {
                    if (!TryApplyTransform(buffer))
                    {
                        await deferredChannel.Writer.WriteAsync(buffer);
                    }
                }
            }
        }

        public async Task<string> GetHashAsync()
        {

            await semaphore.WaitAsync();

            try
            {
                if (deferredProcessor != null)
                {
                    await deferredProcessor;
                }

                return BitConverter.ToString(md5Provider.Hash!).Replace("-", "").ToLowerInvariant();
            }
            finally
            {
                semaphore.Release();
            }

        }
    }
}
