﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Linq;
using Microsoft.AspNetCore.Mvc.Authorization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using TesApi.Web.Options;

namespace TesApi.Web.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection ConfigureAuthenticationAndControllers(this IServiceCollection services, IConfiguration configuration)
        {
            bool isAuthConfigured = false;

            var authenticationOptions = new AuthenticationOptions();
            configuration.GetSection(AuthenticationOptions.SectionName).Bind(authenticationOptions);

            if (authenticationOptions?.Providers?.Any() == true)
            {
                var authBuilder = services.AddAuthentication();

                foreach (var provider in authenticationOptions.Providers)
                {
                    authBuilder.AddJwtBearer(provider.Name, options =>
                    {
                        options.Authority = provider.Authority;
                        options.Audience = provider.Audience;
                    });
                }

                isAuthConfigured = true;
            }

            services.AddControllers(options =>
            {
                options.Filters.Add<Controllers.OperationCancelledExceptionFilter>();

                if (isAuthConfigured)
                {
                    options.Filters.Add(new AuthorizeFilter());
                }
            })
            .AddNewtonsoftJson(opts =>
            {
                opts.SerializerSettings.ContractResolver = new CamelCasePropertyNamesContractResolver();
                opts.SerializerSettings.Converters.Add(new StringEnumConverter(new CamelCaseNamingStrategy()));
            });

            return services;
        }
    }
}
