// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using AutoMapper;
using Azure.Core;
using Azure.ResourceManager.Batch;
using Azure.ResourceManager.Batch.Models;
using TesApi.Web.Management.Models.Terra;
using TesApi.Web.Runner;

namespace TesApi.Web.Management.Batch
{
    /// <summary>
    /// AutoMapper mapping profile for Batch Pool to Wsm Create Batch API
    /// </summary>
    public class MappingProfilePoolToWsmRequest : Profile
    {
        /// <summary>
        /// Constructor of MappingProfilePoolToWsmRequest
        /// </summary>
        public MappingProfilePoolToWsmRequest()
        {
            CreateMap<BatchDeploymentConfiguration, ApiDeploymentConfiguration>()
                .ForMember(dst => dst.VirtualMachineConfiguration, opt => opt.MapFrom(src => src.VmConfiguration))
                .ForMember(dst => dst.CloudServiceConfiguration, opt => opt.MapFrom(src => src.CloudServiceConfiguration));
            CreateMap<BatchVmConfiguration, ApiVirtualMachineConfiguration>();
            CreateMap<BatchCloudServiceConfiguration, ApiCloudServiceConfiguration>();
            CreateMap<BatchAccountPoolScaleSettings, ApiScaleSettings>();
            CreateMap<BatchAccountAutoScaleSettings, ApiAutoScale>()
                .ForMember(dst => dst.EvaluationInterval, opt => opt.MapFrom(src => Convert.ToInt64(src.EvaluationInterval.Value.TotalMinutes)));
            CreateMap<BatchAccountPoolStartTask, ApiStartTask>();
            CreateMap<BatchResourceFile, ApiResourceFile>()
                .ForMember(dst => dst.AutoStorageContainerName, opt => opt.MapFrom(src => src.AutoBlobContainerName))
                .ForMember(dst => dst.HttpUrl, opt => opt.MapFrom(src => src.HttpUri))
                .ForMember(dst => dst.IdentityReference, opt => opt.MapFrom(src => src.IdentityResourceId))
                .ForMember(dst => dst.StorageContainerUrl, opt => opt.MapFrom(src => src.BlobContainerUri));
            CreateMap<ResourceIdentifier, ApiIdentityReference>()
                .ForMember(dst => dst.ResourceId, opt => opt.MapFrom(src => src.ToString()));
            CreateMap<BatchEnvironmentSetting, ApiEnvironmentSetting>();
            CreateMap<BatchUserIdentity, ApiUserIdentity>();
            CreateMap<BatchAutoUserSpecification, ApiAutoUserSpecification>();
            CreateMap<BatchTaskContainerSettings, ApiContainerSettings>();
            CreateMap<BatchApplicationPackageReference, ApiApplicationPackage>();
            CreateMap<BatchVmContainerRegistry, ApiContainerRegistry>()
                .ForMember(dst => dst.IdentityReference, opt => opt.MapFrom(src => src.IdentityResourceId));
            CreateMap<BatchNetworkConfiguration, ApiNetworkConfiguration>()
                .ForMember(dst => dst.EndpointConfiguration, opt => opt.MapFrom(src => src.EndpointInboundNatPools));
            CreateMap<IList<BatchInboundNatPool>, ApiEndpointConfiguration>()
                .ForMember(dst => dst.InboundNatPools, opt => opt.MapFrom(src => src))
                .ReverseMap();
            CreateMap<ApiInboundNatPool[], IList<BatchInboundNatPool>>()
                .ForMember("Item", opt => opt.Ignore());
            CreateMap<BatchInboundNatPool, ApiInboundNatPool>();
            CreateMap<BatchNetworkSecurityGroupRule, ApiNetworkSecurityGroupRule>();
            CreateMap<BatchImageReference, ApiImageReference>();
            CreateMap<BatchAccountFixedScaleSettings, ApiFixedScale>();
            CreateMap<BatchPublicIPAddressConfiguration, ApiPublicIpAddressConfiguration>();
            CreateMap<BatchAccountPoolMetadataItem, ApiBatchPoolMetadataItem>();
            CreateMap<BatchAccountPoolData, ApiAzureBatchPool>()
                .ForMember(dst => dst.Id, opt => opt.MapFrom(src => src.Name))
                .ForMember(dst => dst.UserAssignedIdentities, opt => opt.MapFrom(src => src.Identity.UserAssignedIdentities.Select(kvp => new ApiUserAssignedIdentity() { Name = TryGetManagedIdentityNameFromResourceId(kvp.Key) })));
        }

        /// <summary>
        /// Gets name of the managed identity if the resource ID is a valid Azure MI resource ID.
        /// If the resource ID is not a valid Resource ID, returns the value provided.
        /// </summary>
        /// <param name="resourceId"></param>
        /// <returns></returns>
        private static string TryGetManagedIdentityNameFromResourceId(string resourceId)
        {
            if (NodeTaskBuilder.IsValidManagedIdentityResourceId(resourceId))
            {
                var parts = resourceId.Split('/');

                if (parts.Length < 2)
                {
                    return string.Empty;
                }

                return parts[^1];
            }

            return resourceId;
        }
    }
}
