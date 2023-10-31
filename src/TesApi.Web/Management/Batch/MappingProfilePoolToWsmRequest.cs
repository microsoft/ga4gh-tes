// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using AutoMapper;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Azure.Management.ContainerRegistry.Fluent.Models;
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
            CreateMap<DeploymentConfiguration, ApiDeploymentConfiguration>();
            CreateMap<VirtualMachineConfiguration, ApiVirtualMachineConfiguration>();
            CreateMap<CloudServiceConfiguration, ApiCloudServiceConfiguration>();
            CreateMap<ScaleSettings, ApiScaleSettings>();
            CreateMap<AutoScaleSettings, ApiAutoScale>()
                .ForMember(dest => dest.EvaluationInterval, opt => opt.MapFrom(src => Convert.ToInt64(src.EvaluationInterval.Value.TotalMinutes)));
            CreateMap<StartTask, ApiStartTask>();
            CreateMap<ResourceFile, ApiResourceFile>();
            CreateMap<ComputeNodeIdentityReference, ApiIdentityReference>();
            CreateMap<EnvironmentSetting, ApiEnvironmentSetting>();
            CreateMap<UserIdentity, ApiUserIdentity>();
            CreateMap<AutoUserSpecification, ApiAutoUserSpecification>();
            CreateMap<TaskContainerSettings, ApiContainerSettings>();
            CreateMap<ApplicationPackageReference, ApiApplicationPackage>();
            CreateMap<ContainerRegistry, ApiContainerRegistry>();
            CreateMap<NetworkConfiguration, ApiNetworkConfiguration>();
            CreateMap<PoolEndpointConfiguration, ApiEndpointConfiguration>();
            CreateMap<InboundNatPool, ApiInboundNatPool>();
            CreateMap<NetworkSecurityGroupRule, ApiNetworkSecurityGroupRule>();
            CreateMap<ImageReference, ApiImageReference>();
            CreateMap<FixedScaleSettings, ApiFixedScale>();
            CreateMap<PublicIPAddressConfiguration, ApiPublicIpAddressConfiguration>();
            CreateMap<MetadataItem, ApiBatchPoolMetadataItem>();
            //TODO: This mapping to be updated once the WSM API changes to support the correct values 
            CreateMap<UserAssignedIdentities, ApiUserAssignedIdentity>()
                .ForMember(dest => dest.ClientId, opt => opt.MapFrom(src => src.ClientId))
                .ForMember(dest => dest.ResourceGroupName, opt => opt.Ignore())
                .ForMember(dest => dest.Name, opt => opt.Ignore());
            CreateMap<Pool, ApiAzureBatchPool>()
               .ForMember(dest => dest.UserAssignedIdentities, opt => opt.MapFrom(src => src.Identity.UserAssignedIdentities.Select(kvp => new ApiUserAssignedIdentity() { Name = TryGetManagedIdentityNameFromResourceId(kvp.Key), ClientId = kvp.Value.ClientId })));
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
