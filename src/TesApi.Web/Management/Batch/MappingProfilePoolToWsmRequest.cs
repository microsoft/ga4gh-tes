// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using AutoMapper;
using Microsoft.Azure.Management.Batch.Models;
using TesApi.Web.Management.Models.Terra;

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
                .ForMember(dest => dest.EvaluationInterval, opt => opt.MapFrom(src => Convert.ToInt64(src.EvaluationInterval.Value.TotalSeconds)));
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
               .ForMember(dest => dest.UserAssignedIdentities, opt => opt.MapFrom(src => src.Identity.UserAssignedIdentities.Select(kvp => new ApiUserAssignedIdentity() { Name = kvp.Key, ClientId = kvp.Value.ClientId })));
        }
    }
}
