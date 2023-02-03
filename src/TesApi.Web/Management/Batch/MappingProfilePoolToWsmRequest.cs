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
            CreateMap<AutoScaleSettings, ApiAutoScale>();
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
            //TODO: This mapping to be updated once the WSM API changes to support the correct values 
            CreateMap<UserAssignedIdentities, ApiUserAssignedIdentity>()
                .ForMember(dest => dest.Name, opt => opt.MapFrom(src => src.PrincipalId))
                .ForMember(dest => dest.ResourceGroupName, opt => opt.Ignore());
            CreateMap<Pool, ApiAzureBatchPool>()
               .ForMember(dest => dest.UserAssignedIdentities, opt => opt.Ignore());
        }
    }
}
