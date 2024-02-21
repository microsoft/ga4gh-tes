// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using AutoMapper;
using Azure.ResourceManager.Batch;
using Azure.ResourceManager.Batch.Models;
using Microsoft.Azure.Batch;
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
            CreateMap<BatchDeploymentConfiguration, ApiDeploymentConfiguration>();
            CreateMap<BatchVmConfiguration, ApiVirtualMachineConfiguration>();
            CreateMap<BatchCloudServiceConfiguration, ApiCloudServiceConfiguration>();
            CreateMap<BatchAccountPoolScaleSettings, ApiScaleSettings>();
            CreateMap<BatchAccountAutoScaleSettings, ApiAutoScale>()
                .ForMember(dest => dest.EvaluationInterval, opt => opt.MapFrom(src => Convert.ToInt64(src.EvaluationInterval.Value.TotalMinutes)));
            CreateMap<BatchAccountPoolStartTask, ApiStartTask>();
            CreateMap<BatchResourceFile, ApiResourceFile>();
            CreateMap<ComputeNodeIdentityReference, ApiIdentityReference>();
            CreateMap<BatchEnvironmentSetting, ApiEnvironmentSetting>();
            CreateMap<BatchUserIdentity, ApiUserIdentity>();
            CreateMap<BatchAutoUserSpecification, ApiAutoUserSpecification>();
            CreateMap<BatchTaskContainerSettings, ApiContainerSettings>();
            CreateMap<BatchApplicationPackageReference, ApiApplicationPackage>();
            CreateMap<BatchVmContainerRegistry, ApiContainerRegistry>();
            CreateMap<BatchNetworkConfiguration, ApiNetworkConfiguration>();
            CreateMap<PoolEndpointConfiguration, ApiEndpointConfiguration>();
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
