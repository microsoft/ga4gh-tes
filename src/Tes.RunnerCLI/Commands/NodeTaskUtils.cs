// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands;

public static class NodeTaskUtils
{
    private static readonly ILogger Logger = PipelineLoggerFactory.Create(nameof(NodeTaskUtils));

    public static async Task<NodeTask> DeserializeNodeTaskAsync(string tesNodeTaskFilePath)
    {
        try
        {
            var nodeTaskText = await File.ReadAllTextAsync(tesNodeTaskFilePath);

            var nodeTask = JsonSerializer.Deserialize(nodeTaskText, NodeTaskContext.Default.NodeTask) ?? throw new InvalidOperationException("The JSON data provided is invalid.");

            AddDefaultValuesIfMissing(nodeTask);

            return nodeTask;
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Failed to deserialize task JSON file.");
            throw;
        }
    }

    private static void AddDefaultValuesIfMissing(NodeTask nodeTask)
    {
        nodeTask.RuntimeOptions ??= new RuntimeOptions();
    }
}

[JsonSourceGenerationOptions(PropertyNameCaseInsensitive = true)]
[JsonSerializable(typeof(NodeTask))]
public partial class NodeTaskContext : JsonSerializerContext
{ }
