// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Linq;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.AspNetCore.Mvc.Authorization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using TesApi.Web.Options;

namespace TesApi.Web.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection ConfigureAuthenticationAndControllers(this IServiceCollection services, IConfiguration configuration)
        {
            var authenticationOptions = new AuthenticationOptions();
            configuration.GetSection(AuthenticationOptions.SectionName).Bind(authenticationOptions);
            var isAuthConfigured = authenticationOptions?.Providers?.Any() == true;

            if (isAuthConfigured)
            {
                var authBuilder = services.AddAuthentication(options =>
                {
                    options.DefaultScheme = JwtBearerDefaults.AuthenticationScheme;
                    options.DefaultChallengeScheme = OpenIdConnectDefaults.AuthenticationScheme;
                });

                foreach (var provider in authenticationOptions.Providers)
                {
                    authBuilder
                        .AddJwtBearer(provider.Name, options =>
                        {
                            options.Authority = provider.Authority;
                            options.Audience = provider.ClientId; // ClientId and Audience are synonymous
                        })
                        .AddOpenIdConnect(options =>
                        {
                            options.Authority = provider.Authority; // $"https://login.microsoftonline.com/{provider.TenantId}/v2.0";
                            options.MetadataAddress = $"{options.Authority.TrimStart('/')}/.well-known/openid-configuration";
                            options.ClientId = provider.ClientId; // Same as audience
                            options.ResponseType = OpenIdConnectResponseType.Code;
                            options.SaveTokens = false; // TODO in the future can use this to make authorized calls to Azure Storage
                            options.TokenValidationParameters = new TokenValidationParameters
                            {
                                ValidateIssuer = true,
                                ValidIssuer = provider.Authority,
                                ValidateAudience = true,
                                ValidAudience = provider.ClientId
                            };
                        });
                }

                services.AddAuthorization();
            }

            services.AddControllers(options =>
            {
                options.Filters.Add<Controllers.OperationCancelledExceptionFilter>();

                if (isAuthConfigured)
                {
                    // Adds authorization to all controllers and operations
                    // TODO might want to expose ServiceInfo with the Authority and MetadataAddress and Audience
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
