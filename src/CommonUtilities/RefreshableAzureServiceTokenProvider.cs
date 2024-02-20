// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net.Http.Headers;
using Azure.Identity;
using Microsoft.Rest;

namespace CommonUtilities
{
    /// <summary>
    /// ITokenProvider implementation based on AzureServiceTokenProvider from Microsoft.Azure.Services.AppAuthentication package.
    /// </summary>
    public class RefreshableAzureServiceTokenProvider : ITokenProvider
    {
        private readonly string resource;
        private readonly string? tenantId;
        private readonly AzureCliCredential azureCredential;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="resource">Resource to request tokens for</param>
        /// <param name="tenantId">AAD tenant ID containing the resource</param>
        /// <param name="azureAdInstance">AAD instance to request tokens from</param>
        public RefreshableAzureServiceTokenProvider(string resource, string? tenantId = null)
        {
            ArgumentException.ThrowIfNullOrEmpty(resource);

            this.resource = resource;
            this.tenantId = tenantId;

            this.azureCredential = new AzureCliCredential();
        }

        /// <summary>
        /// Gets the authentication header with token.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Authentication header with token</returns>
        public async Task<AuthenticationHeaderValue> GetAuthenticationHeaderAsync(CancellationToken cancellationToken)
        {
            // AzureServiceTokenProvider caches tokens internally and refreshes them before expiry.
            // This method usually gets called on every request to set the authentication header. This ensures that we cache tokens, and also that we always get a valid one.
            var token = (await azureCredential.GetTokenAsync(new Azure.Core.TokenRequestContext(new string[] { this.resource }, null, null, this.tenantId), cancellationToken)).Token;
            return new("Bearer", token);
        }
    }
}
