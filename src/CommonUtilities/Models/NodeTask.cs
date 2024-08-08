﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json.Serialization;
using CommonUtilities;

namespace Tes.Runner.Models
{
    public class NodeTask
    {
        public string? Id { get; set; }
        public string? WorkflowId { get; set; }
        public string? ImageTag { get; set; }
        public string? ImageName { get; set; }
        public string? ContainerWorkDir { get; set; }
        public List<string>? CommandsToExecute { get; set; }
        public List<FileInput>? Inputs { get; set; }
        public List<FileOutput>? Outputs { get; set; }
        public string? MetricsFilename { get; set; }
        public string? InputsMetricsFormat { get; set; }
        public string? OutputsMetricsFormat { get; set; }
        public List<string>? TimestampMetricsFormats { get; set; }
        public List<string>? BashScriptMetricsFormats { get; set; }
        public RuntimeOptions RuntimeOptions { get; set; } = null!;
    }

    public class FileOutput
    {
        public string? Path { get; set; }
        public string? MountParentDirectory { get; set; }
        public string? TargetUrl { get; set; }
        public TransformationStrategy? TransformationStrategy { get; set; }
        public FileType? FileType { get; set; }
    }

    public class FileInput
    {
        public string? Path { get; set; }
        public string? MountParentDirectory { get; set; }
        public string? SourceUrl { get; set; }
        public TransformationStrategy? TransformationStrategy { get; set; }
    }

    public class RuntimeOptions
    {
        public TerraRuntimeOptions? Terra { get; set; }

        public string? NodeManagedIdentityResourceId { get; set; }
        public string? AcrPullManagedIdentityResourceId { get; set; }

        public StorageTargetLocation? StorageEventSink { get; set; }

        public StorageTargetLocation? StreamingLogPublisher { get; set; }

        public AzureEnvironmentConfig? AzureEnvironmentConfig { get; set; }

        public bool? SetContentMd5OnUpload { get; set; }
    }

    public class StorageTargetLocation
    {
        public string TargetUrl { get; set; } = null!;
        public TransformationStrategy TransformationStrategy { get; set; }
    }

    public class TerraRuntimeOptions
    {
        public string? WsmApiHost { get; set; }
        public string? LandingZoneApiHost { get; set; }
        public string? SasAllowedIpRange { get; set; }
        public string? DrsHubApiHost { get; set; }
    }

    public class NodeTaskResolverOptions
    {
        public RuntimeOptions? RuntimeOptions { get; set; }
        public TransformationStrategy TransformationStrategy { get; set; }
    }

    [JsonConverter(typeof(JsonStringEnumConverter<TransformationStrategy>))]
    public enum TransformationStrategy
    {
        None,
        AzureResourceManager,
        TerraWsm,
        SchemeConverter,
        CombinedTerra,
        CombinedAzureResourceManager,
    }

    [JsonConverter(typeof(JsonStringEnumConverter<FileType>))]
    public enum FileType
    {
        File,
        Directory,
    }
}
