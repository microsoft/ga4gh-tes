// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json;
using TesApi.Web.Management.Models.Terra;

namespace Tes.ApiClients.Tests;

public class TerraApiStubData
{
    public const string LandingZoneApiHost = "https://landingzone.host";
    public const string WsmApiHost = "https://wsm.host";
    public const string SamApiHost = "https://sam.host";
    public const string ResourceGroup = "mrg-terra-dev-previ-20191228";
    public const string WorkspaceAccountName = "lzaccount1";
    public const string SasToken = "SASTOKENSTUB=";
    private const string WorkspaceIdValue = "41aa9346-670f-4206-8b6f-6b921a564bdd";

    public const string WorkspaceStorageContainerName = $"sc-{WorkspaceIdValue}";
    public const string WsmGetSasResponseStorageUrl = $"https://{WorkspaceAccountName}.blob.core.windows.net/{WorkspaceStorageContainerName}";

    public const string TerraPetName = "pet-2674060218359759651b0";

    public Guid TenantId { get; } = Guid.NewGuid();
    public Guid LandingZoneId { get; } = Guid.NewGuid();
    public Guid SubscriptionId { get; } = Guid.NewGuid();
    public Guid BillingProfileId { get; } = Guid.NewGuid();
    public Guid ContainerResourceId { get; } = Guid.NewGuid();
    public Guid WorkspaceId { get; } = Guid.Parse(WorkspaceIdValue);

    public string BatchAccountName => "lzee170c71b6cf678cfca744";
    public string Region => "westus3";
    public string BatchAccountId =>
        $"/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.Batch/batchAccounts/{BatchAccountName}";

    public string ManagedIdentityObjectId =>
        $"/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{TerraPetName}";
    public string PoolId => "poolId";

    public Guid GetWorkspaceIdFromContainerName(string containerName)
    {
        return Guid.Parse(containerName.Replace("sc-", ""));
    }

    public LandingZoneResourcesApiResponse GetResourceApiResponse()
    {
        return JsonSerializer.Deserialize<LandingZoneResourcesApiResponse>(GetResourceApiResponseInJson())!;
    }

    public QuotaApiResponse GetResourceQuotaApiResponse()
    {
        return JsonSerializer.Deserialize<QuotaApiResponse>(GetResourceQuotaApiResponseInJson())!;
    }

    public WsmSasTokenApiResponse GetWsmSasTokenApiResponse(string blobName = null!)
    {
        return JsonSerializer.Deserialize<WsmSasTokenApiResponse>(GetWsmSasTokenApiResponseInJson(blobName))!;
    }

    public string GetWsmSasTokenApiResponseInJson(string blobName = null!)
    {
        var blobToAppend = blobName;
        if (!string.IsNullOrEmpty(blobName))
        {
            blobToAppend = $"/{blobName.TrimStart('/')}";
        }

        return $$"""
        {
            "token": "{{SasToken}}",
            "url": "{{WsmGetSasResponseStorageUrl}}{{blobToAppend}}?sv={{SasToken}}"
        }
        """;
    }

    public string GetResourceApiResponseInJson()
    {
        return $@"{{
  ""id"": ""{LandingZoneId}"",
  ""resources"": [
    {{
      ""purpose"": ""POSTGRESQL_SUBNET"",
      ""deployedResources"": [
        {{
          ""resourceType"": ""DeployedSubnet"",
          ""resourceName"": ""POSTGRESQL_SUBNET"",
          ""resourceParentId"": ""/subscriptions/{SubscriptionId}/resourceGroups/ResourceGroup/providers/Microsoft.Network/virtualNetworks/lz7f4d78e9882101e6834b0ff98c9e128c79414b85b16bdd3c2bf64b3004a2ab"",
          ""region"": ""{Region}""
        }}
      ]
    }},
    {{
      ""purpose"": ""SHARED_RESOURCE"",
      ""deployedResources"": [
        {{
          ""resourceId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.Batch/batchAccounts/{BatchAccountName}"",
          ""resourceType"": ""Microsoft.Batch/batchAccounts"",
          ""region"": ""{Region}""
        }},
        {{
          ""resourceId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.ContainerService/managedClusters/lz73fc42a6df6b9c9173d1642"",
          ""resourceType"": ""Microsoft.ContainerService/managedClusters"",
          ""region"": ""{Region}""
        }},
        {{
          ""resourceId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.DBforPostgreSQL/servers/lz7634015e1ec0acec24fef4a84c9dcf86f62af29ede09e1a2d3e2c3a415d3a"",
          ""resourceType"": ""Microsoft.DBforPostgreSQL/servers"",
          ""region"": ""{Region}""
        }},
        {{
          ""resourceId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.Insights/components/lz931ac245d357d3741ea6c1643489023a33b1d3c97eb0827d9b7f6a928dff4d52"",
          ""resourceType"": ""Microsoft.Insights/components"",
          ""region"": ""{Region}""
        }},
        {{
          ""resourceId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.Insights/dataCollectionRules/lz8ccc2ee36f14b77a0f2a6a971992e757b63095613df7a9fd22607e2d3074ab"",
          ""resourceType"": ""Microsoft.Insights/dataCollectionRules"",
          ""region"": ""{Region}""
        }},
        {{
          ""resourceId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.OperationalInsights/workspaces/lz8f97a76a9c49a3cfa40ab860e40d4d1fe57520fb4ebd0edb4e204d9c5b0d9"",
          ""resourceType"": ""Microsoft.OperationalInsights/workspaces"",
          ""region"": ""{Region}""
        }},
        {{
          ""resourceId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.Relay/namespaces/lzcf39cbb5964910f3da058ef02595b6862f560718e0e8c16d"",
          ""resourceType"": ""Microsoft.Relay/namespaces"",
          ""region"": ""{Region}""
        }},
        {{
          ""resourceId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.Storage/storageAccounts/lz05d7f0bc9f7d3634aca839"",
          ""resourceType"": ""Microsoft.Storage/storageAccounts"",
          ""region"": ""{Region}""
        }}
      ]
    }},
    {{
      ""purpose"": ""AKS_NODE_POOL_SUBNET"",
      ""deployedResources"": [
        {{
          ""resourceType"": ""DeployedSubnet"",
          ""resourceName"": ""AKS_SUBNET"",
          ""resourceParentId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.Network/virtualNetworks/lz7f4d78e9882101e6834b0ff98c9e128c79414b85b16bdd3c2bf64b3004a2ab"",
          ""region"": ""{Region}""
        }}
      ]
    }},
    {{
      ""purpose"": ""WORKSPACE_COMPUTE_SUBNET"",
      ""deployedResources"": [
        {{
          ""resourceType"": ""DeployedSubnet"",
          ""resourceName"": ""COMPUTE_SUBNET"",
          ""resourceParentId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.Network/virtualNetworks/lz7f4d78e9882101e6834b0ff98c9e128c79414b85b16bdd3c2bf64b3004a2ab"",
          ""region"": ""{Region}""
        }}
      ]
    }},
    {{
      ""purpose"": ""WORKSPACE_BATCH_SUBNET"",
      ""deployedResources"": [
        {{
          ""resourceType"": ""DeployedSubnet"",
          ""resourceName"": ""BATCH_SUBNET"",
          ""resourceParentId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.Network/virtualNetworks/lz7f4d78e9882101e6834b0ff98c9e128c79414b85b16bdd3c2bf64b3004a2ab"",
          ""region"": ""{Region}""
        }}
      ]
    }}
  ]
}}";
    }

    public string GetContainerResourcesApiResponseInJson()
    {
        return $@"{{
  ""resources"": [
    {{
      ""metadata"": {{
        ""workspaceId"": ""{WorkspaceId}"",
        ""resourceId"": ""{ContainerResourceId}"",
        ""name"": ""{WorkspaceStorageContainerName}"",
        ""resourceType"": ""AZURE_STORAGE_CONTAINER"",
        ""stewardshipType"": ""CONTROLLED"",
        ""cloudPlatform"": ""AZURE"",
        ""cloningInstructions"": ""COPY_NOTHING"",
        ""controlledResourceMetadata"": {{
          ""accessScope"": ""SHARED_ACCESS"",
          ""managedBy"": ""USER"",
          ""privateResourceUser"": {{}},
          ""privateResourceState"": ""NOT_APPLICABLE"",
          ""region"": ""southcentralus""
        }},
        ""resourceLineage"": [],
        ""properties"": [],
        ""createdBy"": ""user@foo.com"",
        ""createdDate"": ""2023-02-09T01:48:46.040052Z"",
        ""lastUpdatedBy"": ""user@foo.com"",
        ""lastUpdatedDate"": ""2023-02-09T01:48:48.345442Z"",
        ""state"": ""READY""
      }},
      ""resourceAttributes"": {{
        ""azureStorageContainer"": {{
          ""storageContainerName"": ""{WorkspaceStorageContainerName}""
        }}
      }}
    }}
  ]
}}";
    }

    public string GetResourceQuotaApiResponseInJson()
    {
        return $@"{{
  ""landingZoneId"": ""{LandingZoneId}"",
  ""azureResourceId"": ""{BatchAccountId}"",
  ""resourceType"": ""Microsoft.Batch/batchAccounts"",
  ""quotaValues"": {{
    ""poolQuota"": 100,
    ""dedicatedCoreQuotaPerVMFamily"": {{
      ""standardLSv2Family"": 0,
      ""standardHBv3Family"": 0,
      ""Standard NDASv4_A100 Family"": 0,
      ""standardESv3Family"": 350,
      ""standardFSv2Family"": 175,
      ""standardEDSv5Family"": 0,
      ""standardHBSFamily"": 0,
      ""standardHFamily"": 0,
      ""standardHPromoFamily"": 0,
      ""Standard NCASv3_T4 Family"": 0,
      ""standardNCADSA100v4Family"": 0,
      ""standardEv3Family"": 350,
      ""standardHBrsv2Family"": 0,
      ""standardDFamily"": 0,
      ""standardDSFamily"": 0,
      ""basicAFamily"": 0,
      ""standardNCFamily"": 0,
      ""standardDDv5Family"": 0,
      ""standardNVPromoFamily"": 0,
      ""standardNDSFamily"": 0,
      ""standardHCSFamily"": 88,
      ""standardEDSv4Family"": 350,
      ""standardNVSv3Family"": 0,
      ""standardDSv2Family"": 350,
      ""standardFSFamily"": 0,
      ""standardDDSv5Family"": 0,
      ""standardNVFamily"": 0,
      ""standardMSFamily"": 0,
      ""standardDCSv2Family"": 0,
      ""StandardNVADSA10v5Family"": 0,
      ""standardLSFamily"": 0,
      ""standardDAv4Family"": 0,
      ""standardNCSv3Family"": 0,
      ""standardNPSFamily"": 0,
      ""standardDDSv4Family"": 350,
      ""standardDSv3Family"": 350,
      ""standardEDv4Family"": 350,
      ""standardXEIDSv4Family"": 0,
      ""standardGFamily"": 0,
      ""standardDASv4Family"": 0,
      ""standardAv2Family"": 350,
      ""standardDv3Family"": 350,
      ""standardA8_A11Family"": 0,
      ""standardEIv3Family"": 0,
      ""standardFXMDVSFamily"": 0,
      ""standardNCSv2Family"": 0,
      ""standardEAv4Family"": 0,
      ""standardA0_A7Family"": 0,
      ""standardDv2Family"": 350,
      ""standardEASv4Family"": 0,
      ""standardMSv2Family"": 0,
      ""standardEDv5Family"": 0,
      ""standardDADSv5Family"": 0,
      ""standardNVSv4Family"": 0,
      ""standardNCPromoFamily"": 0,
      ""standardGSFamily"": 0,
      ""standardEADSv5Family"": 0,
      ""standardFFamily"": 0,
      ""standardDDv4Family"": 350
    }},
    ""dedicatedCoreQuotaPerVMFamilyEnforced"": true,
    ""activeJobAndJobScheduleQuota"": 300,
    ""dedicatedCoreQuota"": 350,
    ""lowPriorityCoreQuota"": 100
  }}
}}";
    }

    public string GetSamActionManagedIdentityApiResponseInJson()
    {
        return $@"{{
  ""id"": {{
    ""resourceId"": {{
      ""resourceTypeName"": ""private_azure_container_registry"",
      ""resourceId"": ""{BillingProfileId}""
    }},
    ""action"": ""pull_image"",
    ""billingProfileId"": ""{BillingProfileId}""
  }},
  ""objectId"": ""{ManagedIdentityObjectId}"",
  ""displayName"": ""my nice action identity"",
  ""managedResourceGroupCoordinates"": {{
    ""tenantId"": ""{TenantId}"",
    ""subscriptionId"": ""{SubscriptionId}"",
    ""managedResourceGroupName"": ""{ResourceGroup}""
  }}       
}}";
    }

    public ApiCreateBatchPoolRequest GetApiCreateBatchPoolRequest()
    {
        return new ApiCreateBatchPoolRequest()
        {
            Common = new ApiCommon(),
            AzureBatchPool = new ApiAzureBatchPool()
            {
                UserAssignedIdentities = new[]
                {
                    new ApiUserAssignedIdentity()
                    {
                        Name = "identityName",
                        ResourceGroupName = ResourceGroup
                    }
                }
            }
        };
    }

    public ApiCreateBatchPoolResponse GetApiCreateBatchPoolResponse()
    {
        return new ApiCreateBatchPoolResponse()
        {
            AzureBatchPool = new ApiAzureBatchPoolResource()
            {
                Attributes = new ApiAzureBatchPoolAttributes()
                {
                    Id = PoolId
                }
            },
            ResourceId = new Guid()
        };
    }

    public WsmListContainerResourcesResponse GetWsmContainerResourcesApiResponse()
    {
        return new WsmListContainerResourcesResponse()
        {
            Resources = new List<Resource>()
            {
               new Resource()
               {
                   Metadata = new Metadata()
                   {
                       ResourceId = ContainerResourceId.ToString(),
                       Name = WorkspaceStorageContainerName
                   },
                   ResourceAttributes = new ResourceAttributes()
                   {
                       AzureStorageContainer = new AzureStorageContainer()
                       {
                           StorageContainerName = WorkspaceStorageContainerName
                       }
                   }
                }
            }
        };
    }
}
