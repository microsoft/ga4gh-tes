// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.DependencyInjection;

namespace TesApi.Web.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddOpenIdConnectAuthentication(this IServiceCollection services)
        {
            var serviceProvider = services.BuildServiceProvider();
            var authenticationConfigurationService = serviceProvider.GetRequiredService<AuthenticationConfigurationService>();
            var authenticationBuilder = services.AddAuthentication();
            authenticationConfigurationService.ConfigureJwtBearer(authenticationBuilder);

            return services;
        }
    }
}
