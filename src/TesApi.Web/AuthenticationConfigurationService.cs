// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace TesApi.Web
{
    public class AuthenticationConfigurationService
    {
        private readonly TesApi.Web.Options.AuthenticationOptions _authenticationOptions;

        public AuthenticationConfigurationService(IOptions<TesApi.Web.Options.AuthenticationOptions> authenticationOptions)
        {
            _authenticationOptions = authenticationOptions.Value;
        }

        public void ConfigureJwtBearer(AuthenticationBuilder builder)
        {
            foreach (var provider in _authenticationOptions.Providers)
            {
                builder.AddJwtBearer(provider.Name, options =>
                {
                    options.Authority = provider.Authority;
                    options.Audience = provider.Audience;
                });
            }
        }
    }
}
