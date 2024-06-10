// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tes.ApiClients;
using TesApi.Web.Management.Configuration;

namespace TesApi.Web
{
    /// <summary>
    /// An ActionIdentityProvider implementation for use in Terra. Obtains action identities from Sam.
    /// </summary>
    public class TerraActionIdentityProvider : IActionIdentityProvider
    {
        private readonly Guid terraBillingProfileId;
        private readonly TerraSamApiClient terraSamApiClient;
        private readonly ILogger Logger;

        /// <summary>
        /// An ActionIdentityProvider implementation for use in Terra. Obtains action identities from Sam.
        /// </summary>
        /// <param name="terraSamApiClient"><see cref="TerraSamApiClient"/></param>
        /// <param name="terraOptions"><see cref="TerraOptions"/></param>
        /// <param name="Logger"><see cref="ILogger"/></param>
        public TerraActionIdentityProvider(TerraSamApiClient terraSamApiClient, TerraOptions terraOptions, ILogger<TerraActionIdentityProvider> Logger)
        {
            ArgumentNullException.ThrowIfNull(terraOptions.BillingProfileId);
            this.terraBillingProfileId = Guid.Parse(terraOptions.BillingProfileId);
            this.terraSamApiClient = terraSamApiClient;
        }


        /// <summary>
        /// Retrieves the action identity to use for pulling ACR images, if one exists
        /// </summary>
        /// <param name="id">Id to use to look up the identity, by convention the billing profile id</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>The resource id of the action identity, if one exists. Otherwise, null.</returns>
        public async Task<string?> GetAcrPullActionIdentity(CancellationToken cancellationToken)
        {
            try
            {
                var response = await terraSamApiClient.GetActionManagedIdentityForACRPullAsync(terraBillingProfileId, CancellationToken.None);
                if (response is null)
                {
                    // Corresponds to no identity existing in Sam, or the user not having access to it.
                    Logger.LogInformation(@"Found no ACR Pull action identity in Sam for {id}", terraBillingProfileId);
                    return null;
                }
                else
                {
                    Logger.LogInformation(@"Successfully fetched ACR action identity from Sam: {ObjectId}", response.ObjectId);
                    return response.ObjectId;
                }
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Failed when trying to obtain an ACR Pull action identity from Sam");
                return null;
            }
        }

    }
}
