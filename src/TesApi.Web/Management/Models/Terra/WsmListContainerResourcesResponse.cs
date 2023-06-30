// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace TesApi.Web.Management.Models.Terra
{
    /// <summary>
    /// Azure storage container WSM resource
    /// </summary>
    public class AzureStorageContainer
    {
        /// <summary>
        /// Storage container name
        /// </summary>
        [JsonPropertyName("storageContainerName")]
        public string StorageContainerName { get; set; }
    }

    /// <summary>
    /// Controlled resource metadata
    /// </summary>
    public class ControlledResourceMetadata
    {
        /// <summary>
        /// Access scope
        /// </summary>
        [JsonPropertyName("accessScope")]
        public string AccessScope { get; set; }

        /// <summary>
        /// Managed by
        /// </summary>
        [JsonPropertyName("managedBy")]
        public string ManagedBy { get; set; }

        /// <summary>
        /// Private resource user
        /// </summary>
        [JsonPropertyName("privateResourceUser")]
        public PrivateResourceUser PrivateResourceUser { get; set; }

        /// <summary>
        /// Private resource state
        /// </summary>
        [JsonPropertyName("privateResourceState")]
        public string PrivateResourceState { get; set; }

        /// <summary>
        /// Resource region
        /// </summary>
        [JsonPropertyName("region")]
        public string Region { get; set; }
    }

    /// <summary>
    /// WSM resource metadata
    /// </summary>
    public class Metadata
    {
        /// <summary>
        /// WSM Workspace ID
        /// </summary>
        [JsonPropertyName("workspaceId")]
        public string WorkspaceId { get; set; }

        /// <summary>
        /// Resource ID
        /// </summary>
        [JsonPropertyName("resourceId")]
        public string ResourceId { get; set; }

        /// <summary>
        /// Resource name
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; set; }

        /// <summary>
        /// Resource type
        /// </summary>
        [JsonPropertyName("resourceType")]
        public string ResourceType { get; set; }

        /// <summary>
        /// Stewardship type
        /// </summary>
        [JsonPropertyName("stewardshipType")]
        public string StewardshipType { get; set; }

        /// <summary>
        /// Cloud platform
        /// </summary>
        [JsonPropertyName("cloudPlatform")]
        public string CloudPlatform { get; set; }

        /// <summary>
        /// 
        /// </summary>
        [JsonPropertyName("cloningInstructions")]
        public string CloningInstructions { get; set; }

        /// <summary>
        /// Controlled resource metadata
        /// </summary>
        [JsonPropertyName("controlledResourceMetadata")]
        public ControlledResourceMetadata ControlledResourceMetadata { get; set; }

        /// <summary>
        /// Resource linage
        /// </summary>
        [JsonPropertyName("resourceLineage")]
        public List<object> ResourceLineage { get; set; }

        /// <summary>
        /// Additional properties
        /// </summary>
        [JsonPropertyName("properties")]
        public List<object> Properties { get; set; }

        /// <summary>
        /// Created by
        /// </summary>
        [JsonPropertyName("createdBy")]
        public string CreatedBy { get; set; }

        /// <summary>
        /// Creation date
        /// </summary>
        [JsonPropertyName("createdDate")]
        public DateTime CreatedDate { get; set; }

        /// <summary>
        /// Last updated by
        /// </summary>
        [JsonPropertyName("lastUpdatedBy")]
        public string LastUpdatedBy { get; set; }

        /// <summary>
        /// Last updated date
        /// </summary>
        [JsonPropertyName("lastUpdatedDate")]
        public DateTime LastUpdatedDate { get; set; }

        /// <summary>
        /// Resource state
        /// </summary>
        [JsonPropertyName("state")]
        public string State { get; set; }
    }

    /// <summary>
    /// Private resource user
    /// </summary>
    public class PrivateResourceUser
    {
        /// <summary>
        /// Iam Role
        /// </summary>
        [JsonPropertyName("privateResourceIamRole")]
        public object PrivateResourceIamRole { get; set; }

        /// <summary>
        /// email of the workspace user to grant access
        /// </summary>
        [JsonPropertyName("userName")]
        public string UserName { get; set; }
    }

    /// <summary>
    /// WSM resource
    /// </summary>
    public class Resource
    {
        /// <summary>
        /// Metadata
        /// </summary>
        [JsonPropertyName("metadata")]
        public Metadata Metadata { get; set; }
        /// <summary>
        /// Resource attributes
        /// </summary>
        [JsonPropertyName("resourceAttributes")]
        public ResourceAttributes ResourceAttributes { get; set; }
    }

    /// <summary>
    /// WSM resource attributes
    /// </summary>
    public class ResourceAttributes
    {
        /// <summary>
        /// Azure storage container
        /// </summary>
        [JsonPropertyName("azureStorageContainer")]
        public AzureStorageContainer AzureStorageContainer { get; set; }
    }

    /// <summary>
    /// Response to get storage container resources from a workspace
    /// </summary>
    public class WsmListContainerResourcesResponse
    {
        /// <summary>
        /// List of resources in the workspace
        /// </summary>
        [JsonPropertyName("resources")]
        public List<Resource> Resources { get; set; }
    }
}
