// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;

namespace Tes.ApiClients.Tests.TerraIntegration
{
    internal class TestEnvTokenCredential : TokenCredential
    {
        public const string TerraTokenEnvVariableName = "TERRA_AUTH_TOKEN";

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            var token = GetTokenFromEnvVariable();

            return new ValueTask<AccessToken>(new AccessToken(token, DateTimeOffset.MaxValue));
        }

        private static string GetTokenFromEnvVariable()
        {
            var token = Environment.GetEnvironmentVariable(TerraTokenEnvVariableName);

            if (string.IsNullOrEmpty(token))
            {
                throw new InvalidOperationException($"Environment variable {TerraTokenEnvVariableName} is not set.");
            }

            return token;
        }

        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            var token = GetTokenFromEnvVariable();

            return new AccessToken(token, DateTimeOffset.MaxValue);
        }
    }
}
