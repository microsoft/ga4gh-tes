// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Containers.ContainerRegistry;
using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.Runner.Authentication
{
    public class CredentialsManager
    {
        private readonly ILogger logger = PipelineLoggerFactory.Create<CredentialsManager>();

        private readonly RetryPolicy retryPolicy;
        private const int MaxRetryCount = 5;
        private const int ExponentialBackOffExponent = 2;

        public CredentialsManager()
        {
            retryPolicy = Policy
                    .Handle<Exception>()
                    .WaitAndRetry(MaxRetryCount,
                    SleepDurationHandler);
        }

        private TimeSpan SleepDurationHandler(int attempt)
        {
            logger.LogInformation($"Attempt {attempt} to get token credential");
            var duration = TimeSpan.FromSeconds(Math.Pow(ExponentialBackOffExponent, attempt));
            logger.LogInformation($"Waiting {duration} before retrying");
            return duration;
        }

        public virtual TokenCredential GetTokenCredential(RuntimeOptions runtimeOptions)
        {
            return retryPolicy.Execute(() => GetTokenCredentialImpl(runtimeOptions));
        }

        private TokenCredential GetTokenCredentialImpl(RuntimeOptions runtimeOptions)
        {
            try
            {
                if (!string.IsNullOrWhiteSpace(runtimeOptions.NodeManagedIdentityResourceId))
                {
                    logger.LogInformation($"Token credentials with Managed Identity and resource ID: {runtimeOptions.NodeManagedIdentityResourceId}");

                    return new ManagedIdentityCredential(new ResourceIdentifier(runtimeOptions.NodeManagedIdentityResourceId));
                }

                logger.LogInformation("Token credentials with DefaultAzureCredential");

                return new DefaultAzureCredential();
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to get token credential");
                throw;
            }
        }

        public virtual ContainerRegistryContentClient GetContainerRegistryContentClient(Uri endpoint, string imageName, RuntimeOptions runtimeOptions, Action<string> action)
        {
            // Use a pipeline policy to get access to the ACR access token we will need to pass to Docker.
            var clientOptions = new ContainerRegistryClientOptions();
            clientOptions.AddPolicy(new AcquireDockerAuthPipelinePolicy(action), HttpPipelinePosition.PerCall);
            return new(endpoint, imageName, GetTokenCredential(runtimeOptions), clientOptions);
        }

        private sealed class AcquireDockerAuthPipelinePolicy : Azure.Core.Pipeline.HttpPipelinePolicy
        {
            private const string Prefix = "Bearer ";
            private readonly Action<string> action;

            public AcquireDockerAuthPipelinePolicy(Action<string> action)
            {
                ArgumentNullException.ThrowIfNull(action);
                this.action = action;
            }

            private void Process(HttpMessage message)
            {
                /*
                 * This method is called after calling the rest of the pipeline in order to inspect the final state of the request used to obtain the response.
                 *
                 * There are three tokens at play: the AAD token (the only one we can easily "get"), the ACR refresh token (which can be used to log docker into the registry), and the ACR access token (which can be used to authorize all other calls).
                 *
                 * This method will be called at least three times:
                 *   1. when deriving the ACR refresh token from the AAD token (which can be identified by the end of the request's uri's path and can be extracted from the response's content)
                 *   2. when obtaining the ACR access token from the ACR refresh token (which can be identified by the end of the request's uri's path and can be extracted from the response's content)
                 *   3. when obtaining the image manifest, when the token is presented as an Authorization Bearer token in the request's headers.
                 *
                 * We are using the third option, because it is the cleanest (the Authorization header is not included when the pipeline checks for anonymous access nor is it used for obtaining either of the ACR tokens).
                 */

                if (message.HasResponse)
                {
                    if (message.Request.Headers.TryGetValue(HttpHeader.Names.Authorization, out var header) && header.StartsWith(Prefix))
                    {
                        action(header[Prefix.Length..]);
                    }
                }
            }

            public override void Process(HttpMessage message, ReadOnlyMemory<Azure.Core.Pipeline.HttpPipelinePolicy> pipeline)
            {
                ProcessNext(message, pipeline);
                Process(message);
            }

            public override async ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<Azure.Core.Pipeline.HttpPipelinePolicy> pipeline)
            {
                await ProcessNextAsync(message, pipeline);
                Process(message);
            }
        }
    }
}
