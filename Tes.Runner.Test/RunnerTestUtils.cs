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

    public static void DeleteFileIfExists(string file)
    {
        if (File.Exists(file))
        {
            File.Delete(file);
        }
    }

    public static async Task<List<PipelineBuffer>> ReadAllPipelineBuffersAsync(IAsyncEnumerable<PipelineBuffer> source)
    {
        var pipelineBuffers = new List<PipelineBuffer>();
        await foreach (var item in source)
        {
            pipelineBuffers.Add(item);
        }
        return pipelineBuffers;
    }

    static Random random = new Random();

    public static async Task<string> CreateTempFileWithContentAsync(int numberOfMiB)
    {
        var file = Guid.NewGuid().ToString();
        await using var fs = File.Create($"{file}.tmp", Units.MiB);

        var data = new byte[numberOfMiB];
        random.NextBytes(data);

        for (var blocks = 0; blocks < numberOfMiB; blocks++)
        {
            await fs.WriteAsync(data, 0, Units.MiB);
        }

        fs.Close();

        return file;
    }
}
