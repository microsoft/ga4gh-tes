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

    public static DirectoryInfo CreateTempFilesInDirectory(string dirStructure, string filePrefix)
    {
        var parentDirName = Guid.NewGuid().ToString();
        var root = Directory.CreateDirectory($"{parentDirName}/{dirStructure}");

        while (root.Parent != null)
        {
            var fileName = $"{root.FullName}/{filePrefix}{Guid.NewGuid()}.tmp";

            using var fs = File.Create(fileName);

            fs.Close();

            if (root.Name.Equals(parentDirName, StringComparison.InvariantCultureIgnoreCase))
            {
                return root;
            }

            root = root.Parent;
        }

        throw new Exception("Could not find root directory");
    }

    public static string CalculateMd5(string file)
    {
        using var md5 = MD5.Create();
        using var stream = File.OpenRead(file);
        var hash = md5.ComputeHash(stream);
        return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
    }

    public static void DeleteFileIfExists(string file)
    {
        if (File.Exists(file))
        {
            try
            {
                File.Delete(file);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }

    public static PipelineBuffer CreateBufferWithRandomData(int blockSizeInBytes)
    {
        var data = new byte[blockSizeInBytes];
        Random.NextBytes(data);
        return new PipelineBuffer() { Data = data, Length = data.Length };
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

    public static string CalculateMd5Hash(byte[] data)
    {
        var hash = MD5.HashData(data);
        return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
    }

    public static string GetRootHashFromSortedHashList(List<string> hashList)
    {
        var hashListContent = string.Join("", hashList);
        var hash = MD5.HashData(Encoding.UTF8.GetBytes(hashListContent));

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
            await fs.WriteAsync(data.AsMemory(0, BlobSizeUtils.MiB));
        }

        if (extraBytes > 0)
        {
            var extraData = new byte[extraBytes];
            Random.NextBytes(extraData);
            await fs.WriteAsync(extraData.AsMemory(0, extraBytes));
        }

        fs.Close();

        return fs.Name;
    }

    public static async Task<int> PreparePipelineChannelAsync(int blockSizeBytes, long fileSize, string fileName, string blobUrl, Channel<PipelineBuffer> pipelineChannel)
    {
        var numberOfParts = BlobSizeUtils.GetNumberOfParts(fileSize, blockSizeBytes);
        await RunnerTestUtils.AddPipelineBuffersAndCompleteChannelAsync(pipelineChannel, numberOfParts,
            new Uri(blobUrl), blockSizeBytes, fileSize, fileName);
        return numberOfParts;
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
        for (var i = 0; i < numberOfParts; i++)
        {
            var processedPart = new ProcessedBuffer(fileName, null, fileSize, i, numberOfParts, Channel.CreateUnbounded<FileStream>(), null, 0, null);

            await processedBuffer.Writer.WriteAsync(processedPart);
        }
    }

    public static string GenerateRandomTestAzureStorageKey()
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        var length = 64;
        var random = new Random();
        var result = new StringBuilder(length);

        for (var i = 0; i < length; i++)
        {
            result.Append(chars[random.Next(chars.Length)]);
        }

        return result.ToString();
    }

    public const int MemBuffersCapacity = 20;
    public const int PipelineBufferCapacity = 20;
}
