// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Containers.ContainerRegistry;
using Azure.Core;
using Docker.DotNet.Models;
using Microsoft.Extensions.Logging;
using Tes.Runner.Authentication;
using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.Runner.Docker
{
    public class ContainerRegistryAuthorizationManager
    {
        const string AzureContainerRegistryHostSuffix = ".azurecr.io";
        public const string NullGuid = "00000000-0000-0000-0000-000000000000";

        private readonly ILogger logger = PipelineLoggerFactory.Create<ContainerRegistryAuthorizationManager>();
        private readonly CredentialsManager tokenCredentialsManager;

        public ContainerRegistryAuthorizationManager(CredentialsManager tokenCredentialsManager)
        {
            ArgumentNullException.ThrowIfNull(tokenCredentialsManager);

            this.tokenCredentialsManager = tokenCredentialsManager;
        }

        public async Task<AuthConfig?> TryGetAuthConfigForAzureContainerRegistryAsync(string imageName, string? imageTag, RuntimeOptions runtimeOptions)
        {
            if (!TryParseAzureContainerRegisterParts(imageName, out var imageParts))
            {
                logger.LogInformation($"The image is not in ACR. Registry: {imageName}");
                return null;
            }

            var registry = imageParts.First();
            var registryAddress = $"https://{registry}";
            var repositoryName = imageParts.Last(); // the image name is considered the repository name in ACR
            var acrAccessToken = string.Empty;

            // Get the manifest to the image to be pulled. If authentication is needed, this will derive it from the managed identity using the pull-image scope.
            var client = CreateContainerRegistryContentClientWithAcquireAuthTokenPolicy(new Uri(registryAddress), repositoryName, runtimeOptions, token => acrAccessToken = token);
            await client.GetManifestAsync(imageTag);

            if (string.IsNullOrWhiteSpace(acrAccessToken))
            {
                logger.LogInformation($"The ACR instance is public. No authorization is required. Registry: {registryAddress}");
                return null; // image is available anonymously
            }

            logger.LogInformation($"The ACR instance is private. An access token was successfully obtained. Registry: {registryAddress}");

            return new AuthConfig
            {
                Username = NullGuid,
                Password = acrAccessToken,
                ServerAddress = registryAddress
            };
        }

        public bool TryParseAzureContainerRegisterParts(string imageName, out string[] imageParts)
        {
            imageParts = Array.Empty<string>();

            if (string.IsNullOrWhiteSpace(imageName))
            {
                return false;
            }

            imageParts = imageName.Split('/', 2);
            var registry = imageParts.FirstOrDefault();

            if (registry is null)
            {
                return false;
            }

            if (registry.EndsWith(AzureContainerRegistryHostSuffix, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            return false;
        }


        public ContainerRegistryContentClient CreateContainerRegistryContentClientWithAcquireAuthTokenPolicy(Uri endpoint, string repositoryName, RuntimeOptions runtimeOptions, Action<string> onCapture)
        {
            // Use a pipeline policy to get access to the ACR access token we will need to pass to Docker.
            var clientOptions = new ContainerRegistryClientOptions();
            clientOptions.AddPolicy(new AcquireDockerAuthTokenPipelinePolicy(onCapture), HttpPipelinePosition.PerCall);
            return new ContainerRegistryContentClient(endpoint, repositoryName, tokenCredentialsManager.GetTokenCredential(runtimeOptions), clientOptions);
        }

        private sealed class AcquireDockerAuthTokenPipelinePolicy : Azure.Core.Pipeline.HttpPipelinePolicy
        {
            private const string Prefix = "Bearer ";
            private readonly Action<string> action;

            public AcquireDockerAuthTokenPipelinePolicy(Action<string> action)
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
