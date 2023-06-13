// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json.Serialization;

namespace Tes.Runner.Models
{
    public class NodeTask
    {
        public string? ImageTag { get; set; }
        public string? ImageName { get; set; }
        public List<string>? CommandsToExecute { get; set; }
        public List<FileInput>? Inputs { get; set; }
        public List<FileOutput>? Outputs { get; set; }
        public string? MetricsFilename { get; set; }
        public string? InputsMetricsFormat { get; set; }
        public string? OutputsMetricsFormat { get; set; }
    }

    public class FileOutput
    {
        public string? FullFileName { get; set; }
        public string? TargetUrl { get; set; }
        public SasResolutionStrategy? SasStrategy { get; set; }
        public FileType? FileType { get; set; }
        public string? PathPrefix { get; set;}
    }

    public class FileInput
    {
        public string? FullFileName { get; set; }
        public string? SourceUrl { get; set; }
        public SasResolutionStrategy? SasStrategy { get; set; }
    }

    [JsonConverter(typeof(JsonStringEnumConverter))]
    public enum SasResolutionStrategy
    {
        None,
        StorageAccountNameAndKey,
        TerraWsm,
        SchemeConverter,
    }

    [JsonConverter(typeof(JsonStringEnumConverter))]
    public enum FileType
    {
        File,
        Directory,
    }
}
