// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Runtime.Serialization;
using System.Text.Json.Serialization;

namespace TesApi.Web.Management.Models.Terra
{
    /// <summary>
    /// Wsm Api ApiCreateBatchPoolRequest 
    /// </summary>
    public class ApiCreateBatchPoolRequest
    {
#pragma warning disable CS1591
        [JsonPropertyName("common")]
        public ApiCommon Common { get; set; }


        [JsonPropertyName("azureBatchPool")]
        public ApiAzureBatchPool AzureBatchPool { get; set; }
    }

    public class ApiAzureBatchPool
    {
        [JsonPropertyName("id")]
        public string Id { get; set; }

        [JsonPropertyName("displayName")]
        public string DisplayName { get; set; }

        [JsonPropertyName("vmSize")]
        public string VmSize { get; set; }

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

        [JsonPropertyName("metadata")]
        public ApiBatchPoolMetadataItem[] Metadata { get; set; }
    }

    public class ApiBatchPoolMetadataItem
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }
        [JsonPropertyName("value")]
        public string Value { get; set; }
    }

    public class ApiApplicationPackage
    {
        [JsonPropertyName("id")]
        public string Id { get; set; }

        [JsonPropertyName("version")]
        public string Version { get; set; }
    }

    public class ApiDeploymentConfiguration
    {
        [JsonPropertyName("virtualMachineConfiguration")]
        public ApiVirtualMachineConfiguration VirtualMachineConfiguration { get; set; }

        [JsonPropertyName("cloudServiceConfiguration")]
        public ApiCloudServiceConfiguration CloudServiceConfiguration { get; set; }
    }

    public class ApiCloudServiceConfiguration
    {
        [JsonPropertyName("osFamily")]
        public string OsFamily { get; set; }

        [JsonPropertyName("osVersion")]
        public string OsVersion { get; set; }
    }

    public class ApiVirtualMachineConfiguration
    {
        [JsonPropertyName("imageReference")]
        public ApiImageReference ImageReference { get; set; }

        [JsonPropertyName("nodeAgentSkuId")]
        public string NodeAgentSkuId { get; set; }
    }

    public class ApiImageReference
    {
        [JsonPropertyName("publisher")]
        public string Publisher { get; set; }

        [JsonPropertyName("offer")]
        public string Offer { get; set; }

        [JsonPropertyName("sku")]
        public string Sku { get; set; }

        [JsonPropertyName("version")]
        public string Version { get; set; }

        [JsonPropertyName("id")]
        public string Id { get; set; }
    }

    public class ApiNetworkConfiguration
    {
        [JsonPropertyName("subnetId")]
        public string SubnetId { get; set; }

        [JsonPropertyName("dynamicVNetAssignmentScope")]
        public ApiDynamicVNetAssignmentScope DynamicVNetAssignmentScope { get; set; }

        [JsonPropertyName("endpointConfiguration")]
        public ApiEndpointConfiguration EndpointConfiguration { get; set; }

        [JsonPropertyName("publicIpAddressConfiguration")]
        public ApiPublicIpAddressConfiguration PublicIpAddressConfiguration { get; set; }
    }

    [JsonConverter(typeof(JsonStringEnumConverter))]
    public enum ApiDynamicVNetAssignmentScope
    {
        [EnumMember(Value = "none")] none,
        [EnumMember(Value = "job")] job
    }


    public class ApiEndpointConfiguration

    {
        [JsonPropertyName("inboundNatPools")]
        public ApiInboundNatPool[] InboundNatPools { get; set; }
    }

    public class ApiInboundNatPool
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("protocol")]
        public ApiInboundEndpointProtocol Protocol { get; set; }

        [JsonPropertyName("backendPort")]
        public long BackendPort { get; set; }

        [JsonPropertyName("frontendPortRangeStart")]
        public long FrontendPortRangeStart { get; set; }

        [JsonPropertyName("frontendPortRangeEnd")]
        public long FrontendPortRangeEnd { get; set; }

        [JsonPropertyName("networkSecurityGroupRules")]
        public ApiNetworkSecurityGroupRule[] NetworkSecurityGroupRules { get; set; }
    }

    public class ApiNetworkSecurityGroupRule
    {
        [JsonPropertyName("priority")]
        public long Priority { get; set; }

        [JsonPropertyName("access")]
        public ApiDynamicVNetAssignmentScope Access { get; set; }

        [JsonPropertyName("sourceAddressPrefix")]
        public string SourceAddressPrefix { get; set; }

        [JsonPropertyName("sourcePortRanges")]
        public string[] SourcePortRanges { get; set; }
    }

    public class ApiPublicIpAddressConfiguration
    {
        [JsonPropertyName("provision")]
        public ApiIpProvisionType Provision { get; set; }

        [JsonPropertyName("ipAddressIds")]
        public string[] IpAddressIds { get; set; }
    }

    [JsonConverter(typeof(JsonStringEnumConverter<ApiIpProvisionType>))]
    public enum ApiIpProvisionType
    {
        [EnumMember(Value = "BatchManaged")]
        BatchManaged,
        [EnumMember(Value = "UserManaged")]
        UserManaged,
        [EnumMember(Value = "NoPublicIPAddresses")]
        NoPublicIPAddresses,
    }

    public class ApiScaleSettings
    {
        [JsonPropertyName("fixedScale")]
        public ApiFixedScale FixedScale { get; set; }

        [JsonPropertyName("autoScale")]
        public ApiAutoScale AutoScale { get; set; }
    }

    public class ApiAutoScale
    {
        [JsonPropertyName("formula")]
        public string Formula { get; set; }

        [JsonPropertyName("evaluationInterval")]
        public long? EvaluationInterval { get; set; }
    }

    public class ApiFixedScale
    {
        [JsonPropertyName("resizeTimeout")]
        public TimeSpan? ResizeTimeout { get; set; }

        [JsonPropertyName("targetDedicatedNodes")]
        public long TargetDedicatedNodes { get; set; }

        [JsonPropertyName("targetLowPriorityNodes")]
        public long TargetLowPriorityNodes { get; set; }

        [JsonPropertyName("nodeDeallocationOption")]
        public ApiAzureBatchPoolComputeNodeDeallocationOption NodeDeallocationOption { get; set; }
    }

    public class ApiStartTask
    {
        [JsonPropertyName("commandLine")]
        public string CommandLine { get; set; }

        [JsonPropertyName("resourceFiles")]
        public ApiResourceFile[] ResourceFiles { get; set; }

        [JsonPropertyName("environmentSettings")]
        public ApiEnvironmentSetting[] EnvironmentSettings { get; set; }

        [JsonPropertyName("userIdentity")]
        public ApiUserIdentity UserIdentity { get; set; }

        [JsonPropertyName("maxTaskRetryCount")]
        public long MaxTaskRetryCount { get; set; }

        [JsonPropertyName("waitForSuccess")]
        public bool WaitForSuccess { get; set; }

        [JsonPropertyName("containerSettings")]
        public ApiContainerSettings ContainerSettings { get; set; }
    }

    public class ApiContainerSettings
    {
        [JsonPropertyName("containerRunOptions")]
        public string ContainerRunOptions { get; set; }

        [JsonPropertyName("imageName")]
        public string ImageName { get; set; }

        [JsonPropertyName("registry")]
        public ApiContainerRegistry Registry { get; set; }

        [JsonPropertyName("workingDirectory")]
        public ApiAzureBatchPoolContainerWorkingDirectory WorkingDirectory { get; set; }
    }

    public class ApiContainerRegistry
    {
        [JsonPropertyName("userName")]
        public string UserName { get; set; }

        [JsonPropertyName("password")]
        public string Password { get; set; }

        [JsonPropertyName("registryServer")]
        public string RegistryServer { get; set; }

        [JsonPropertyName("identityReference")]
        public ApiIdentityReference IdentityReference { get; set; }
    }

    public class ApiIdentityReference
    {
        [JsonPropertyName("resourceId")]
        public string ResourceId { get; set; }
    }

    public class ApiEnvironmentSetting
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("value")]
        public string Value { get; set; }
    }

    public class ApiResourceFile
    {
        [JsonPropertyName("autoStorageContainerName")]
        public string AutoStorageContainerName { get; set; }

        [JsonPropertyName("storageContainerUrl")]
        public string StorageContainerUrl { get; set; }

        [JsonPropertyName("httpUrl")]
        public string HttpUrl { get; set; }

        [JsonPropertyName("blobPrefix")]
        public string BlobPrefix { get; set; }

        [JsonPropertyName("filePath")]
        public string FilePath { get; set; }

        [JsonPropertyName("fileMode")]
        public string FileMode { get; set; }

        [JsonPropertyName("identityReference")]
        public ApiIdentityReference IdentityReference { get; set; }
    }

    public class ApiUserIdentity
    {
        [JsonPropertyName("userName")]
        public string UserName { get; set; }

        [JsonPropertyName("autoUser")]
        public ApiAutoUserSpecification AutoUser { get; set; }
    }

    public class ApiAutoUserSpecification
    {
        [JsonPropertyName("scope")]
        public ApiAzureBatchPoolAutoUserScope Scope { get; set; }

        [JsonPropertyName("elevationLevel")]
        public ApiAzureBatchPoolElevationLevel ElevationLevel { get; set; }
    }

    public class ApiUserAssignedIdentity
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("resourceGroupName")]
        public string ResourceGroupName { get; set; }

        [JsonPropertyName("clientId")]
        public string ClientId { get; set; }
    }

    public class ApiCommon
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("description")]
        public string Description { get; set; }

        [JsonPropertyName("cloningInstructions")]
        public string CloningInstructions { get; set; }

        [JsonPropertyName("accessScope")]
        public string AccessScope { get; set; }

        [JsonPropertyName("managedBy")]
        public string ManagedBy { get; set; }

        [JsonPropertyName("privateResourceUser")]
        public ApiPrivateResourceUser PrivateResourceUser { get; set; }

        [JsonPropertyName("resourceId")]
        public Guid ResourceId { get; set; }

        [JsonPropertyName("properties")]
        public ApiProperty[] Properties { get; set; }
    }

    public class ApiPrivateResourceUser
    {
        [JsonPropertyName("username")]
        public string Username { get; set; }

        [JsonPropertyName("privateResourceIamRole")]
        public string PrivateResourceIamRole { get; set; }
    }

    public class ApiProperty
    {
        [JsonPropertyName("key")]
        public string Key { get; set; }

        [JsonPropertyName("value")]
        public string Value { get; set; }
    }

    [JsonConverter(typeof(JsonStringEnumConverter<ApiAzureBatchPoolAutoUserScope>))]
    public enum ApiAzureBatchPoolAutoUserScope
    {
        [EnumMember(Value = "Task")] Task,
        [EnumMember(Value = "Pool")] Pool
    }

    [JsonConverter(typeof(JsonStringEnumConverter<ApiAzureBatchPoolElevationLevel>))]
    public enum ApiAzureBatchPoolElevationLevel
    {
        [EnumMember(Value = "NonAdmin")] NonAdmin,
        [EnumMember(Value = "Admin")] Admin
    }

    [JsonConverter(typeof(JsonStringEnumConverter<ApiAzureBatchPoolContainerWorkingDirectory>))]
    public enum ApiAzureBatchPoolContainerWorkingDirectory
    {
        [EnumMember(Value = "TaskWorkingDirectory")] TaskWorkingDirectory,
        [EnumMember(Value = "ContainerImageDefault")] ContainerImageDefault
    }

    [JsonConverter(typeof(JsonStringEnumConverter<ApiAzureBatchPoolComputeNodeDeallocationOption>))]
    public enum ApiAzureBatchPoolComputeNodeDeallocationOption
    {
        [EnumMember(Value = "Requeue")] Requeue,
        [EnumMember(Value = "Terminate")] Terminate,
        [EnumMember(Value = "TaskCompletion")] TaskCompletion,
        [EnumMember(Value = "RetainedData")] RetainedData
    }

    [JsonConverter(typeof(JsonStringEnumConverter<ApiInboundEndpointProtocol>))]
    public enum ApiInboundEndpointProtocol
    {
        [EnumMember(Value = "TCP")] TCP,
        [EnumMember(Value = "UDP")] UDP
    }

    [JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonSerializable(typeof(ApiCreateBatchPoolRequest))]
    public partial class ApiCreateBatchPoolRequestContext : JsonSerializerContext
    { }

#pragma warning restore CS1591
}
