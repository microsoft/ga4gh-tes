// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Security.Cryptography;
using System.Threading.Channels;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test;

public class RunnerTestUtils
{
    public static async Task<string> CreateTempFileAsync()
    {
        var file = $"{Guid.NewGuid()}.tmp";
        await using var fs = File.Create(file);
        fs.Close();

        return file;
    }

    public static string CalculateMd5(string file)
    {
        using var md5 = MD5.Create();
        using var stream = File.OpenRead(file);
        var hash = md5.ComputeHash(stream);
        return Convert.ToBase64String(hash);
    }
    public static void DeleteFileIfExists(string file)
    {
        if (File.Exists(file))
        {
            File.Delete(file);
        }
    }

    public static async Task<List<T>> ReadAllPipelineBuffersAsync<T>(IAsyncEnumerable<T> source)
    {
        var pipelineBuffers = new List<T>();
        await foreach (var item in source)
        {
            pipelineBuffers.Add(item);
        }
        return pipelineBuffers;
    }

    static Random random = new Random();

    public static async Task<string> CreateTempFileWithContentAsync(int numberOfMiB, int extraBytes = 0)
    {
        var file = Guid.NewGuid().ToString();
        await using var fs = File.Create($"{file}.tmp", Units.MiB);

        var data = new byte[BlobSizeUtils.MiB];
        random.NextBytes(data);

        for (var blocks = 0; blocks < numberOfMiB; blocks++)
        {
            await fs.WriteAsync(data, 0, BlobSizeUtils.MiB);
        }

        if (extraBytes > 0)
        {
            var extraData = new byte[extraBytes];
            random.NextBytes(extraData);
            await fs.WriteAsync(extraData, 0, extraBytes);
        }

        fs.Close();

        return fs.Name;
    }

    public static async Task AddPipelineBuffersAndCompleteChannelAsync(Channel<PipelineBuffer> pipelineBuffers,
        int numberOfParts, Uri blobUrl, int blockSize, long fileSize, string fileName)
    {
        for (int partOrdinal = 0; partOrdinal < numberOfParts; partOrdinal++)
        {
            var buffer = new PipelineBuffer()
            {
                BlobUrl = blobUrl,
                Offset = (long)partOrdinal * blockSize,
                Length = blockSize,
                FileName = fileName,
                Ordinal = partOrdinal,
                NumberOfParts = numberOfParts,
                FileSize = fileSize,
            };

            if (partOrdinal == numberOfParts - 1)
            {
                buffer.Length = (int)(fileSize - buffer.Offset);
            }

            await pipelineBuffers.Writer.WriteAsync(buffer);
        }

        pipelineBuffers.Writer.Complete();
    }

    public static async Task AddProcessedBufferAsync(Channel<ProcessedBuffer> processedBuffer, string fileName, int numberOfParts, long fileSize)
    {
        for (int i = 0; i < numberOfParts; i++)
        {
            var processedPart = new ProcessedBuffer(fileName, null, fileSize, i, numberOfParts, Channel.CreateUnbounded<FileStream>(), null, 0);

            await processedBuffer.Writer.WriteAsync(processedPart);
        }
    }
}
