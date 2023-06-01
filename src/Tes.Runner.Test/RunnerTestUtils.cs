// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Security.Cryptography;
using System.Text;
using System.Threading.Channels;
using Tes.Runner.Transfer;
namespace Tes.Runner.Test;

public class RunnerTestUtils
{
    static readonly Random Random = new();

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


    public static string AddRandomDataAndReturnMd5(byte[] data)
    {
        Random.NextBytes(data);
        using var md5 = MD5.Create();
        var hash = md5.ComputeHash(data);
        return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
    }

    public static string GetRootHashFromSortedHashList(List<String> hashList)
    {
        var hashListContent = string.Join("", hashList);
        using var md5 = MD5.Create();
        var hash = md5.ComputeHash(Encoding.UTF8.GetBytes(hashListContent));

        return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
    }

    public static async Task<string> CreateTempFileWithContentAsync(int numberOfMiB, int extraBytes = 0)
    {
        var file = Guid.NewGuid().ToString();
        await using var fs = File.Create($"{file}.tmp", BlobSizeUtils.MiB);

        var data = new byte[BlobSizeUtils.MiB];
        Random.NextBytes(data);

        for (var blocks = 0; blocks < numberOfMiB; blocks++)
        {
            await fs.WriteAsync(data, 0, BlobSizeUtils.MiB);
        }

        if (extraBytes > 0)
        {
            var extraData = new byte[extraBytes];
            Random.NextBytes(extraData);
            await fs.WriteAsync(extraData, 0, extraBytes);
        }

        fs.Close();

        return fs.Name;
    }

    public static async Task AddPipelineBuffersAndCompleteChannelAsync(Channel<PipelineBuffer> pipelineBuffers,
        int numberOfParts, Uri blobUrl, int blockSize, long fileSize, string fileName)
    {
        var buffers = CreatePipelineBuffers(numberOfParts, blobUrl, blockSize, fileSize, fileName);

        foreach (var buffer in buffers)
        {
            await pipelineBuffers.Writer.WriteAsync(buffer);
        }

        pipelineBuffers.Writer.Complete();

    }

    public static List<PipelineBuffer> CreatePipelineBuffers(int numberOfParts, Uri blobUrl, int blockSize,
        long fileSize, string fileName)
    {
        var pipelineBuffers = new List<PipelineBuffer>();
        for (var partOrdinal = 0; partOrdinal < numberOfParts; partOrdinal++)
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
                Data = new byte[blockSize]
            };
            if (partOrdinal == numberOfParts - 1)
            {
                buffer.Length = (int)(fileSize - buffer.Offset);
            }
            pipelineBuffers.Add(buffer);
        }
        return pipelineBuffers;
    }

    public static async Task AddProcessedBufferAsync(Channel<ProcessedBuffer> processedBuffer, string fileName, int numberOfParts, long fileSize)
    {
        for (int i = 0; i < numberOfParts; i++)
        {
            var processedPart = new ProcessedBuffer(fileName, null, fileSize, i, numberOfParts, Channel.CreateUnbounded<FileStream>(), null, 0, null);

            await processedBuffer.Writer.WriteAsync(processedPart);
        }
    }
}
