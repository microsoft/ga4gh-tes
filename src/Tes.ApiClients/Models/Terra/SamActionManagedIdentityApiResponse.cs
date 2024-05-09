// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json.Serialization;

namespace Tes.ApiClients.Models.Terra
{
    public class SamActionManagedIdentityApiResponse
    {
        [JsonPropertyName("id")]
        public ActionManagedIdentityId actionManagedIdentityId { get; set; }

        [JsonPropertyName("displayName")]
        public string DisplayName { get; set; }

        [JsonPropertyName("managedResourceGroupCoordinates")]
        public ManagedResourceGroupCoordinates managedResourceGroupCoordinates { get; set; }

        [JsonPropertyName("objectId")]
        public string ObjectId { get; set; }

    }

    public class ActionManagedIdentityId
    {
        [JsonPropertyName("resourceId")]
        public FullyQualifiedResourceId ResourceId { get; set; }

        [JsonPropertyName("action")]
        public string Action { get; set; }

        [JsonPropertyName("billingProfileId")]
        public Guid BillingProfileId { get; set; }
    }

    public class FullyQualifiedResourceId
    {
        [JsonPropertyName("resourceTypeName")]
        public string ResourceTypeName { get; set; }

        [JsonPropertyName("resourceId")]
        public string ResourceId { get; set; }
    }

    public class ManagedResourceGroupCoordinates
    {
        [JsonPropertyName("tenantId")]
        public Guid TenantId { get; set; }

        [JsonPropertyName("subscriptionId")]
        public Guid SubscriptionId { get; set; }

        [JsonPropertyName("managedResourceGroupName")]
        public string ManagedResourceGroupName { get; set; }
    }

    [JsonSerializable(typeof(SamActionManagedIdentityApiResponse))]
    public partial class SamActionManagedIdentityApiResponseContext : JsonSerializerContext
    { }

}
