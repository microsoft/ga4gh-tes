// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Containers.ContainerRegistry;
using Azure.Core;
using CommonUtilities;
using CommonUtilities.AzureCloud;
using Docker.DotNet.Models;
using Microsoft.Azure.Management.Monitor.Fluent.Models;
using Microsoft.Extensions.Logging;
using Tes.ApiClients;
using Tes.Runner.Authentication;
using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.Runner.Docker
{
    public class ContainerRegistryAuthorizationManager
    {
        const string AzureContainerRegistryHostSuffix = ".azurecr.io";
        public static readonly string NullGuid = Guid.Empty.ToString("D");

        private readonly ILogger logger = PipelineLoggerFactory.Create<ContainerRegistryAuthorizationManager>();
        private readonly CredentialsManager tokenCredentialsManager;
        private readonly TerraSamApiClient? terraSamApiClient;

        public ContainerRegistryAuthorizationManager(CredentialsManager tokenCredentialsManager)
        {
            ArgumentNullException.ThrowIfNull(tokenCredentialsManager);

            this.tokenCredentialsManager = tokenCredentialsManager;
        }

        public async Task<AuthConfig?> TryGetAuthConfigForAzureContainerRegistryAsync(string imageName, string? imageTag, RuntimeOptions runtimeOptions)
        {
            if (!TryParseAzureContainerRegisteryParts(imageName, out var imageParts))
            {
                return null;
            }

            var registry = imageParts.First();
            var registryAddress = $"https://{registry}";
            var repositoryName = imageParts.Last(); // the image name is considered the repository name in ACR
            var acrAccessToken = string.Empty;

            // Get the manifest to the image to be pulled. If authentication is needed, this will derive it from the managed identity using the pull-image scope.
            var client = await CreateContainerRegistryContentClientWithAcquireAuthTokenPolicyAsync(new Uri(registryAddress), repositoryName, runtimeOptions, token => acrAccessToken = token);
            _ = await client.GetManifestAsync(imageTag ?? string.Empty);

            if (string.IsNullOrWhiteSpace(acrAccessToken))
            {
                logger.LogInformation(@"The ACR instance is public. No authorization is required. Registry: {RegistryEndpoint}", registryAddress);
                return null; // image is available anonymously
            }

            logger.LogInformation(@"The ACR instance is private. An access token was successfully obtained. Registry: {RegistryEndpoint}", registryAddress);

            return new AuthConfig
            {
                Username = NullGuid,
                Password = acrAccessToken,
                ServerAddress = registryAddress
            };
        }

        public static bool TryParseAzureContainerRegisteryParts(string imageName, out string[] imageParts)
        {
            imageParts = Array.Empty<string>();

            if (string.IsNullOrWhiteSpace(imageName))
            {
                return false;
            }

            imageParts = imageName.Split('/', 2);
            var registry = imageParts.FirstOrDefault();

            return registry?.EndsWith(AzureContainerRegistryHostSuffix, StringComparison.OrdinalIgnoreCase) ?? false;
        }


        public async Task<ContainerRegistryContentClient> CreateContainerRegistryContentClientWithAcquireAuthTokenPolicyAsync(Uri endpoint, string repositoryName, RuntimeOptions runtimeOptions, Action<string> onCapture)
        {
            // TODO we are replacing node identity ACR auth with action identity - should do both instead?

            string? terraACRIdentity = await FetchTerraACRActionIdentityAsync(runtimeOptions);


            // Use a pipeline policy to get access to the ACR access token we will need to pass to Docker.
            var clientOptions = new ContainerRegistryClientOptions();
            clientOptions.AddPolicy(new AcquireDockerAuthTokenPipelinePolicy(onCapture), HttpPipelinePosition.PerCall);
            return new ContainerRegistryContentClient(endpoint, repositoryName, tokenCredentialsManager.GetTokenCredential(runtimeOptions, null, terraACRIdentity), clientOptions);
        }

        private async Task<string?> FetchTerraACRActionIdentityAsync(RuntimeOptions runtimeOptions)
        {
            if (runtimeOptions.Terra is null || string.IsNullOrWhiteSpace(runtimeOptions.Terra.BillingProfileId) || string.IsNullOrWhiteSpace(runtimeOptions.Terra.SamApiHost))
            {
                return null;
            }

            // TODO test with invalid guid for billing profile id
            var billingProfileId = Guid.Parse(runtimeOptions.Terra.BillingProfileId);
            var samClient = TerraSamApiClient.CreateTerraSamApiClient(runtimeOptions.Terra.SamApiHost, tokenCredentialsManager.GetTokenCredential(runtimeOptions), runtimeOptions.AzureEnvironmentConfig);
            var response = await samClient.GetActionManagedIdentityForACRPullAsync(billingProfileId, CancellationToken.None);
            logger.LogInformation(@"Successfully fetched ACR action identity from Sam: {ObjectId}", response.ObjectId);
            return response.ObjectId;
        }

        private sealed class AcquireDockerAuthTokenPipelinePolicy : Azure.Core.Pipeline.HttpPipelinePolicy
        {
            private const string Prefix = "Bearer ";
            private readonly Action<string> OnCapture;

            public AcquireDockerAuthTokenPipelinePolicy(Action<string> onCapture)
            {
                ArgumentNullException.ThrowIfNull(onCapture);
                this.OnCapture = onCapture;
            }

            private void CaptureToken(HttpMessage message)
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
                        OnCapture(header[Prefix.Length..]);
                    }
                }
            }

            public override void Process(HttpMessage message, ReadOnlyMemory<Azure.Core.Pipeline.HttpPipelinePolicy> pipeline)
            {
                ProcessNext(message, pipeline);
                CaptureToken(message);
            }

            public override async ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<Azure.Core.Pipeline.HttpPipelinePolicy> pipeline)
            {
                await ProcessNextAsync(message, pipeline);
                CaptureToken(message);
            }
        }
    }
}
