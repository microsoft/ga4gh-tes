// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json;
using Microsoft.Extensions.Logging;
using Tes.Runner.Models;
using Tes.Runner.Storage;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands;

public static class NodeTaskUtils
{
    private static readonly ILogger Logger = PipelineLoggerFactory.Create(nameof(NodeTaskUtils));

    public static T DeserializeJson<T>(string json)
    {
        return JsonSerializer.Deserialize<T>(json, jsonSerializerOptions) ?? throw new System.Diagnostics.UnreachableException("Failure to deserialize JSON.");
    }

    public static async Task<NodeTask> DeserializeNodeTaskAsync(string tesNodeTaskFilePath)
    {
        try
        {
            var nodeTaskText = await File.ReadAllTextAsync(tesNodeTaskFilePath);

            var nodeTask = DeserializeJson<NodeTask>(nodeTaskText);

            AddDefaultValuesIfMissing(nodeTask);

            return nodeTask;
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Failed to deserialize task JSON file.");
            throw;
        }
    }

    public static async Task SerializeNodeTaskAsync(this NodeTask nodeTask, string tesNodeTaskFilePath)
    {
        try
        {
            var nodeTaskText = JsonSerializer.Serialize(nodeTask, jsonSerializerOptions) ?? throw new InvalidOperationException("The JSON data provided is invalid.");

            await File.WriteAllTextAsync(tesNodeTaskFilePath, nodeTaskText);
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Failed to serialize task JSON file.");
            throw;
        }
    }

    public static async Task<NodeTask> ResolveNodeTaskAsync(FileInfo? file, Uri? uri, NodeTaskResolverOptions? options)
    {
        file?.Refresh();

        if (file?.Exists ?? false)
        {
            return await DeserializeNodeTaskAsync(file.FullName);
        }

        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(options);

        try
        {
            ResolutionPolicyHandler resolutionPolicy = new(options.RuntimeOptions!);
            List<FileInput> sources = [new() { TransformationStrategy = options.TransformationStrategy, SourceUrl = uri.AbsoluteUri, Path = "_" }];
            var blobUri = (await resolutionPolicy.ApplyResolutionPolicyAsync(sources) ?? []).FirstOrDefault()?.SourceUrl ?? throw new Exception("The JSON data blob could not be resolved.");

            BlobApiHttpUtils downloader = new();
            var response = await downloader.ExecuteHttpRequestAsync(() => new HttpRequestMessage(HttpMethod.Get, blobUri));
            var nodeTaskText = await response.Content.ReadAsStringAsync();

            var nodeTask = DeserializeJson<NodeTask>(nodeTaskText) ?? throw new Exception("Failed to deserialize task JSON file.");

            AddDefaultValuesIfMissing(nodeTask);

            return nodeTask;
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Failed to download or deserialize task JSON file.");
            throw;
        }
    }

    private static readonly JsonSerializerOptions jsonSerializerOptions = new() { PropertyNameCaseInsensitive = true, DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingDefault };

    private static void AddDefaultValuesIfMissing(NodeTask nodeTask)
    {
        nodeTask.RuntimeOptions ??= new RuntimeOptions();
    }
}
