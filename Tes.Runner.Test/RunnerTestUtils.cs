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
}
