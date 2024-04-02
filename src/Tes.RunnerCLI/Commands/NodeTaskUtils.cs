// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json;
using Microsoft.Extensions.Logging;
using Tes.Runner.Models;
using Tes.Runner.Storage;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands;

public class NodeTaskUtils(Func<BlobApiHttpUtils>? blobApiHttpUtilsFactory = default)
{
    internal static NodeTaskUtils Instance => SingletonFactory.Value;
    private static readonly Lazy<NodeTaskUtils> SingletonFactory = new(() => new());

    private readonly ILogger Logger = PipelineLoggerFactory.Create(nameof(NodeTaskUtils));
    private readonly Lazy<BlobApiHttpUtils> blobApiHttpUtils = new(blobApiHttpUtilsFactory ?? (() => new()));
    private static readonly JsonSerializerOptions jsonSerializerOptions = new() { DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingDefault };

    public static T DeserializeJson<T>(string json)
    {
        return JsonSerializer.Deserialize<T>(json, jsonSerializerOptions) ?? throw new System.Diagnostics.UnreachableException("Failure to deserialize JSON.");
    }

    public async Task<NodeTask> DeserializeNodeTaskAsync(string tesNodeTaskFilePath)
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

    public async Task SerializeNodeTaskAsync(NodeTask nodeTask, string tesNodeTaskFilePath)
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

    public async Task<NodeTask> ResolveNodeTaskAsync(FileInfo? file, Uri? uri, Lazy<NodeTaskResolverOptions> options)
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
            ResolutionPolicyHandler resolutionPolicy = new(options.Value.RuntimeOptions ?? throw new InvalidOperationException($"Environment variable '{nameof(NodeTaskResolverOptions)}' is missing the '{nameof(NodeTaskResolverOptions.RuntimeOptions)}' property."));
            List<FileInput> sources = [new() { TransformationStrategy = options.Value.TransformationStrategy, SourceUrl = uri.AbsoluteUri, Path = "_" }];  // Path is not used, but it is required
            var blobUri = (await resolutionPolicy.ApplyResolutionPolicyAsync(sources) ?? []).FirstOrDefault()?.SourceUrl ?? throw new Exception("The JSON data blob could not be resolved.");

            var response = await blobApiHttpUtils.Value.ExecuteHttpRequestAsync(() => new HttpRequestMessage(HttpMethod.Get, blobUri));
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

    private static void AddDefaultValuesIfMissing(NodeTask nodeTask)
    {
        nodeTask.RuntimeOptions ??= new RuntimeOptions();
    }
}
