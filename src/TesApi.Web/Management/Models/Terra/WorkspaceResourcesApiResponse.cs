// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using System;

namespace TesApi.Web.Management.Models.Terra
{
    public class WorkspaceResourcesApiResponse
    {
    }

    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse);
    public class ApplicationPackage
    {
        public string id { get; set; }
        public string version { get; set; }
    }

    public class AutoScale
    {
        public string formula { get; set; }
        public int evaluationInterval { get; set; }
    }

    public class AutoUser
    {
        public string scope { get; set; }
        public string elevationLevel { get; set; }
    }

    public class AwsS3StorageFolder
    {
        public string bucketName { get; set; }
        public string prefix { get; set; }
    }

    public class AwsSageMakerNotebook
    {
        public string instanceName { get; set; }
        public string instanceType { get; set; }
    }

    public class AzureBatchPool
    {
        public string id { get; set; }
        public string vmSize { get; set; }
        public string displayName { get; set; }
        public DeploymentConfiguration deploymentConfiguration { get; set; }
        public List<UserAssignedIdentity> userAssignedIdentities { get; set; }
        public ScaleSettings scaleSettings { get; set; }
        public StartTask startTask { get; set; }
        public List<ApplicationPackage> applicationPackages { get; set; }
        public NetworkConfiguration networkConfiguration { get; set; }
    }

    public class AzureDatabase
    {
        public string databaseName { get; set; }
        public string databaseOwner { get; set; }
    }

    public class AzureDisk
    {
        public string diskName { get; set; }
        public string region { get; set; }
    }

    public class AzureManagedIdentity
    {
        public string managedIdentityName { get; set; }
    }

    public class AzureStorageContainer
    {
        public string storageContainerName { get; set; }
    }

    public class AzureVm
    {
        public string vmName { get; set; }
        public string region { get; set; }
        public string vmSize { get; set; }
        public string vmImage { get; set; }
        public string diskId { get; set; }
    }

    public class CloudServiceConfiguration
    {
        public string osFamily { get; set; }
        public string osVersion { get; set; }
    }

    public class ContainerSettings
    {
        public string containerRunOptions { get; set; }
        public string imageName { get; set; }
        public Registry registry { get; set; }
        public string workingDirectory { get; set; }
    }

    public class ControlledResourceMetadata
    {
        public string accessScope { get; set; }
        public string managedBy { get; set; }
        public PrivateResourceUser privateResourceUser { get; set; }
        public string privateResourceState { get; set; }
        public string region { get; set; }
    }

    public class DeploymentConfiguration
    {
        public VirtualMachineConfiguration virtualMachineConfiguration { get; set; }
        public CloudServiceConfiguration cloudServiceConfiguration { get; set; }
    }

    public class EndpointConfiguration
    {
        public List<InboundNatPool> inboundNatPools { get; set; }
    }

    public class EnvironmentSetting
    {
        public string name { get; set; }
        public string value { get; set; }
    }

    public class ErrorReport
    {
        public string message { get; set; }
        public int statusCode { get; set; }
        public List<string> causes { get; set; }
    }

    public class FixedScale
    {
        public int resizeTimeout { get; set; }
        public int targetDedicatedNodes { get; set; }
        public int targetLowPriorityNodes { get; set; }
        public string nodeDeallocationOption { get; set; }
    }

    public class FlexibleResource
    {
        public string typeNamespace { get; set; }
        public string type { get; set; }
        public string data { get; set; }
    }

    public class GcpAiNotebookInstance
    {
        public string projectId { get; set; }
        public string location { get; set; }
        public string instanceId { get; set; }
    }

    public class GcpBqDataset
    {
        public string projectId { get; set; }
        public string datasetId { get; set; }
    }

    public class GcpBqDataTable
    {
        public string projectId { get; set; }
        public string datasetId { get; set; }
        public string dataTableId { get; set; }
    }

    public class GcpDataRepoSnapshot
    {
        public string instanceName { get; set; }
        public string snapshot { get; set; }
    }

    public class GcpGcsBucket
    {
        public string bucketName { get; set; }
    }

    public class GcpGcsObject
    {
        public string bucketName { get; set; }
        public string fileName { get; set; }
    }

    public class GitRepo
    {
        public string gitRepoUrl { get; set; }
    }

    public class IdentityReference
    {
        public string resourceId { get; set; }
    }

    public class ImageReference
    {
        public string publisher { get; set; }
        public string offer { get; set; }
        public string sku { get; set; }
        public string version { get; set; }
        public string id { get; set; }
    }

    public class InboundNatPool
    {
        public string name { get; set; }
        public string protocol { get; set; }
        public int backendPort { get; set; }
        public int frontendPortRangeStart { get; set; }
        public int frontendPortRangeEnd { get; set; }
        public List<NetworkSecurityGroupRule> networkSecurityGroupRules { get; set; }
    }

    public class Metadata
    {
        public string workspaceId { get; set; }
        public string resourceId { get; set; }
        public string name { get; set; }
        public string description { get; set; }
        public string resourceType { get; set; }
        public string stewardshipType { get; set; }
        public string cloudPlatform { get; set; }
        public string cloningInstructions { get; set; }
        public ControlledResourceMetadata controlledResourceMetadata { get; set; }
        public List<ResourceLineage> resourceLineage { get; set; }
        public List<Property> properties { get; set; }
        public string createdBy { get; set; }
        public DateTime createdDate { get; set; }
        public string lastUpdatedBy { get; set; }
        public DateTime lastUpdatedDate { get; set; }
        public string state { get; set; }
        public ErrorReport errorReport { get; set; }
        public string jobId { get; set; }
    }

    public class NetworkConfiguration
    {
        public string subnetId { get; set; }
        public string dynamicVNetAssignmentScope { get; set; }
        public EndpointConfiguration endpointConfiguration { get; set; }
        public PublicIpAddressConfiguration publicIpAddressConfiguration { get; set; }
    }

    public class NetworkSecurityGroupRule
    {
        public int priority { get; set; }
        public string access { get; set; }
        public string sourceAddressPrefix { get; set; }
        public List<string> sourcePortRanges { get; set; }
    }

    public class PrivateResourceUser
    {
        public string userName { get; set; }
        public string privateResourceIamRole { get; set; }
    }

    public class Property
    {
        public string key { get; set; }
        public string value { get; set; }
    }

    public class PublicIpAddressConfiguration
    {
        public string provision { get; set; }
        public List<string> ipAddressIds { get; set; }
    }

    public class Registry
    {
        public string userName { get; set; }
        public string password { get; set; }
        public string registryServer { get; set; }
        public IdentityReference identityReference { get; set; }
    }

    public class Resource
    {
        public Metadata metadata { get; set; }
        public ResourceAttributes resourceAttributes { get; set; }
    }

    public class ResourceAttributes
    {
        public GcpBqDataset gcpBqDataset { get; set; }
        public GcpBqDataTable gcpBqDataTable { get; set; }
        public GcpDataRepoSnapshot gcpDataRepoSnapshot { get; set; }
        public GcpGcsBucket gcpGcsBucket { get; set; }
        public GcpGcsObject gcpGcsObject { get; set; }
        public GcpAiNotebookInstance gcpAiNotebookInstance { get; set; }
        public AzureManagedIdentity azureManagedIdentity { get; set; }
        public AzureDatabase azureDatabase { get; set; }
        public AzureDisk azureDisk { get; set; }
        public AzureStorageContainer azureStorageContainer { get; set; }
        public AzureVm azureVm { get; set; }
        public AzureBatchPool azureBatchPool { get; set; }
        public AwsS3StorageFolder awsS3StorageFolder { get; set; }
        public AwsSageMakerNotebook awsSageMakerNotebook { get; set; }
        public GitRepo gitRepo { get; set; }
        public TerraWorkspace terraWorkspace { get; set; }
        public FlexibleResource flexibleResource { get; set; }
    }

    public class ResourceFile
    {
        public string autoStorageContainerName { get; set; }
        public string storageContainerUrl { get; set; }
        public string httpUrl { get; set; }
        public string blobPrefix { get; set; }
        public string filePath { get; set; }
        public string fileMode { get; set; }
        public IdentityReference identityReference { get; set; }
    }

    public class ResourceLineage
    {
        public string sourceWorkspaceId { get; set; }
        public string sourceResourceId { get; set; }
    }

    public class Root
    {
        public List<Resource> resources { get; set; }
    }

    public class ScaleSettings
    {
        public FixedScale fixedScale { get; set; }
        public AutoScale autoScale { get; set; }
    }

    public class StartTask
    {
        public string commandLine { get; set; }
        public List<ResourceFile> resourceFiles { get; set; }
        public List<EnvironmentSetting> environmentSettings { get; set; }
        public UserIdentity userIdentity { get; set; }
        public int maxTaskRetryCount { get; set; }
        public bool waitForSuccess { get; set; }
        public ContainerSettings containerSettings { get; set; }
    }

    public class TerraWorkspace
    {
        public string referencedWorkspaceId { get; set; }
    }

    public class UserAssignedIdentity
    {
        public string name { get; set; }
        public string clientId { get; set; }
        public string resourceGroupName { get; set; }
    }

    public class UserIdentity
    {
        public string userName { get; set; }
        public AutoUser autoUser { get; set; }
    }

    public class VirtualMachineConfiguration
    {
        public ImageReference imageReference { get; set; }
        public string nodeAgentSkuId { get; set; }
    }


}
