// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using TesApi.Web.Options;

namespace TesApi.Web.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddOpenIdConnectAuthentication(this IServiceCollection services, out bool isAuthConfigured)
        {
            isAuthConfigured = false;
            var serviceProvider = services.BuildServiceProvider();
            var authenticationOptions = serviceProvider.GetService<IOptions<AuthenticationOptions>>();

            if (authenticationOptions?.Value?.Providers?.Any() == true)
            {
                var authenticationConfigurationService = serviceProvider.GetRequiredService<AuthenticationConfigurationService>();
                var authenticationBuilder = services.AddAuthentication();
                authenticationConfigurationService.ConfigureJwtBearer(authenticationBuilder);
                isAuthConfigured = true;
            }

            return services;
        }
    }
}
