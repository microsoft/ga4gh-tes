using System;
using System.Text.Json;
using TesApi.Web.Management.Models.Terra;

namespace TesApi.Tests;

public class TerraApiStubData
{
    public string ApiHost => "https://landingzone.host";
    public string ResourceGroup => "mrg-terra-dev-previ-20191228";
    public Guid LandingZoneId { get; } = Guid.NewGuid();
    public Guid SubscriptionId { get; } = Guid.NewGuid();

    public string BatchAccountId =>
        $"/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.Batch/batchAccounts/lzee170c71b6cf678cfca744";

    public LandingZoneResourcesApiResponse GetResourceApiResponse()
    {
        return JsonSerializer.Deserialize<LandingZoneResourcesApiResponse>(GetResourceApiResponseInJson());
    }
    public QuotaApiResponse GetResourceQuotaApiResponse()
    {
        return JsonSerializer.Deserialize<QuotaApiResponse>(GetResourceQuotaApiResponseInJson());
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
          ""region"": ""westus3""
        }}
      ]
    }},
    {{
      ""purpose"": ""SHARED_RESOURCE"",
      ""deployedResources"": [
        {{
          ""resourceId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.Batch/batchAccounts/lzee170c71b6cf678cfca744"",
          ""resourceType"": ""Microsoft.Batch/batchAccounts"",
          ""region"": ""westus3""
        }},
        {{
          ""resourceId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.ContainerService/managedClusters/lz73fc42a6df6b9c9173d1642"",
          ""resourceType"": ""Microsoft.ContainerService/managedClusters"",
          ""region"": ""westus3""
        }},
        {{
          ""resourceId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.DBforPostgreSQL/servers/lz7634015e1ec0acec24fef4a84c9dcf86f62af29ede09e1a2d3e2c3a415d3a"",
          ""resourceType"": ""Microsoft.DBforPostgreSQL/servers"",
          ""region"": ""westus3""
        }},
        {{
          ""resourceId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.Insights/components/lz931ac245d357d3741ea6c1643489023a33b1d3c97eb0827d9b7f6a928dff4d52"",
          ""resourceType"": ""Microsoft.Insights/components"",
          ""region"": ""westus3""
        }},
        {{
          ""resourceId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.Insights/dataCollectionRules/lz8ccc2ee36f14b77a0f2a6a971992e757b63095613df7a9fd22607e2d3074ab"",
          ""resourceType"": ""Microsoft.Insights/dataCollectionRules"",
          ""region"": ""westus3""
        }},
        {{
          ""resourceId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.OperationalInsights/workspaces/lz8f97a76a9c49a3cfa40ab860e40d4d1fe57520fb4ebd0edb4e204d9c5b0d9"",
          ""resourceType"": ""Microsoft.OperationalInsights/workspaces"",
          ""region"": ""westus3""
        }},
        {{
          ""resourceId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.Relay/namespaces/lzcf39cbb5964910f3da058ef02595b6862f560718e0e8c16d"",
          ""resourceType"": ""Microsoft.Relay/namespaces"",
          ""region"": ""westus3""
        }},
        {{
          ""resourceId"": ""/subscriptions/{SubscriptionId}/resourceGroups/{ResourceGroup}/providers/Microsoft.Storage/storageAccounts/lz05d7f0bc9f7d3634aca839"",
          ""resourceType"": ""Microsoft.Storage/storageAccounts"",
          ""region"": ""westus3""
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
          ""region"": ""westus3""
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
          ""region"": ""westus3""
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
          ""region"": ""westus3""
        }}
      ]
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
}
