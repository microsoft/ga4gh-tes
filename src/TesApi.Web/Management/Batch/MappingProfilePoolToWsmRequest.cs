// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
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
    /// Automapper mapping profile for Batch Pool to Wsm Create Batch API
    /// </summary>
    public class MappingProfilePoolToWsmRequest : Profile
    {
        /// <summary>
        /// Constructor of MappingProfilePoolToWsmRequest
        /// </summary>
        public MappingProfilePoolToWsmRequest()
        {
            CreateMap<BatchDeploymentConfiguration, ApiDeploymentConfiguration>()
                .ForMember(dest => dest.VirtualMachineConfiguration, opt => opt.MapFrom(src => src.VmConfiguration))
                .ForMember(dest => dest.CloudServiceConfiguration, opt => opt.MapFrom(src => src.CloudServiceConfiguration));
            CreateMap<BatchVmConfiguration, ApiVirtualMachineConfiguration>();
            CreateMap<BatchCloudServiceConfiguration, ApiCloudServiceConfiguration>();
            CreateMap<BatchAccountPoolScaleSettings, ApiScaleSettings>();
            CreateMap<BatchAccountAutoScaleSettings, ApiAutoScale>()
                .ForMember(dest => dest.EvaluationInterval, opt => opt.MapFrom(src => Convert.ToInt64(src.EvaluationInterval.Value.TotalMinutes)));
            CreateMap<BatchAccountPoolStartTask, ApiStartTask>();
            CreateMap<BatchResourceFile, ApiResourceFile>()
                .ForMember(dest => dest.AutoStorageContainerName, opt => opt.MapFrom(src => src.AutoBlobContainerName))
                .ForMember(dest => dest.HttpUrl, opt => opt.MapFrom(src => src.HttpUri))
                .ForMember(dest => dest.IdentityReference, opt => opt.MapFrom(src => src.IdentityResourceId))
                .ForMember(dest => dest.StorageContainerUrl, opt => opt.MapFrom(src => src.BlobContainerUri));
            CreateMap<ResourceIdentifier, ApiIdentityReference>()
                .ForMember(dest => dest.ResourceId, opt => opt.MapFrom(src => src.ToString()));
            CreateMap<BatchEnvironmentSetting, ApiEnvironmentSetting>();
            CreateMap<BatchUserIdentity, ApiUserIdentity>();
            CreateMap<BatchAutoUserSpecification, ApiAutoUserSpecification>();
            CreateMap<BatchTaskContainerSettings, ApiContainerSettings>();
            CreateMap<BatchApplicationPackageReference, ApiApplicationPackage>();
            CreateMap<BatchVmContainerRegistry, ApiContainerRegistry>()
                .ForMember(dest => dest.IdentityReference, opt => opt.MapFrom(src => src.IdentityResourceId));
            CreateMap<BatchNetworkConfiguration, ApiNetworkConfiguration>()
                .ForPath(dest => dest.EndpointConfiguration.InboundNatPools, opt => opt.MapFrom(src => src.EndpointInboundNatPools));
            CreateMap<BatchInboundNatPool, ApiInboundNatPool>();
            CreateMap<BatchNetworkSecurityGroupRule, ApiNetworkSecurityGroupRule>();
            CreateMap<BatchImageReference, ApiImageReference>();
            CreateMap<BatchAccountFixedScaleSettings, ApiFixedScale>();
            CreateMap<BatchPublicIPAddressConfiguration, ApiPublicIpAddressConfiguration>();
            CreateMap<BatchAccountPoolMetadataItem, ApiBatchPoolMetadataItem>();
            //CreateMap<UserAssignedIdentity, ApiUserAssignedIdentity>()
            //    .ForMember(dest => dest.ClientId, opt => opt.Ignore())
            //    .ForMember(dest => dest.ResourceGroupName, opt => opt.Ignore())
            //    .ForMember(dest => dest.Name, opt => opt.Ignore());
            CreateMap<BatchAccountPoolData, ApiAzureBatchPool>()
                .ForMember(dest => dest.Id, opt => opt.MapFrom(src => src.Name))
                .ForMember(dest => dest.UserAssignedIdentities, opt => opt.MapFrom(src => src.Identity.UserAssignedIdentities.Select(kvp => new ApiUserAssignedIdentity() { Name = TryGetManagedIdentityNameFromResourceId(kvp.Key) })));
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
