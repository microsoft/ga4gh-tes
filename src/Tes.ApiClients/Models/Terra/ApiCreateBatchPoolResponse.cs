// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json.Serialization;

namespace TesApi.Web.Management.Models.Terra
{
#pragma warning disable CS1591
    public class ApiCreateBatchPoolResponse
    {
        [JsonPropertyName("resourceId")]
        public Guid ResourceId { get; set; }

        [JsonPropertyName("azureBatchPool")]
        public ApiAzureBatchPoolResource AzureBatchPool { get; set; }
    }

    public class ApiAzureBatchPoolResource
    {
        [JsonPropertyName("metadata")]
        public ApiResourceMetadata Metadata { get; set; }

        [JsonPropertyName("attributes")]
        public ApiAzureBatchPoolAttributes Attributes { get; set; }
    }

    public class ApiAzureBatchPoolAttributes
    {
        [JsonPropertyName("id")]
        public string Id { get; set; }

        [JsonPropertyName("vmSize")]
        public string VmSize { get; set; }

        [JsonPropertyName("displayName")]
        public string DisplayName { get; set; }

        [JsonPropertyName("deploymentConfiguration")]
        public ApiDeploymentConfiguration DeploymentConfiguration { get; set; }

        [JsonPropertyName("userAssignedIdentities")]
        public ApiUserAssignedIdentity[] UserAssignedIdentities { get; set; }

        [JsonPropertyName("scaleSettings")]
        public ApiScaleSettings ScaleSettings { get; set; }

        [JsonPropertyName("startTask")]
        public ApiStartTask StartTask { get; set; }

        [JsonPropertyName("applicationPackages")]
        public ApiApplicationPackage[] ApplicationPackages { get; set; }

        [JsonPropertyName("networkConfiguration")]
        public ApiNetworkConfiguration NetworkConfiguration { get; set; }
    }


    public class ApiResourceMetadata
    {
        [JsonPropertyName("workspaceId")]
        public Guid WorkspaceId { get; set; }

        [JsonPropertyName("resourceId")]
        public Guid ResourceId { get; set; }

        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("description")]
        public string Description { get; set; }

        [JsonPropertyName("resourceType")]
        public string ResourceType { get; set; }

        [JsonPropertyName("stewardshipType")]
        public string StewardshipType { get; set; }

        [JsonPropertyName("cloudPlatform")]
        public string CloudPlatform { get; set; }

        [JsonPropertyName("cloningInstructions")]
        public string CloningInstructions { get; set; }

        [JsonPropertyName("controlledResourceMetadata")]
        public ApiControlledResourceMetadata ControlledResourceMetadata { get; set; }

        [JsonPropertyName("resourceLineage")]
        public ApiResourceLineage[] ResourceLineage { get; set; }

        [JsonPropertyName("properties")]
        public ApiProperty[] Properties { get; set; }

        [JsonPropertyName("createdBy")]
        public string CreatedBy { get; set; }

        [JsonPropertyName("createdDate")]
        public DateTimeOffset CreatedDate { get; set; }

        [JsonPropertyName("lastUpdatedBy")]
        public string LastUpdatedBy { get; set; }

        [JsonPropertyName("lastUpdatedDate")]
        public DateTimeOffset LastUpdatedDate { get; set; }
    }

    public class ApiControlledResourceMetadata
    {
        [JsonPropertyName("accessScope")]
        public string AccessScope { get; set; }

        [JsonPropertyName("managedBy")]
        public string ManagedBy { get; set; }

        [JsonPropertyName("privateResourceUser")]
        public ApiPrivateResourceUser PrivateResourceUser { get; set; }

        [JsonPropertyName("privateResourceState")]
        public string PrivateResourceState { get; set; }

        [JsonPropertyName("region")]
        public string Region { get; set; }
    }

    public class ApiResourceLineage
    {
        [JsonPropertyName("sourceWorkspaceId")]
        public Guid SourceWorkspaceId { get; set; }

        [JsonPropertyName("sourceResourceId")]
        public Guid SourceResourceId { get; set; }
    }

    [JsonSerializable(typeof(ApiCreateBatchPoolResponse))]
    public partial class ApiCreateBatchPoolResponseContext : JsonSerializerContext
    { }

#pragma warning restore CS1591
}
