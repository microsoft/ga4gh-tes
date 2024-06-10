// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace TesApi.Web
{
    /// <summary>
    /// Provides methods for interacting with "action identities," managed identities that are associated with specific actions (ex. pulling Docker images)
    /// rather than individual users. At the time of writing this is a Terra-only concern, and the Terra control plane service Sam owns these identities.
    /// </summary>
    public interface IActionIdentityProvider
    {
        /// <summary>
        /// Retrieves the action identity to use for pulling ACR images, if one exists
        /// </summary>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>The resource id of the action identity, if one exists</returns>
        public Task<string?> GetAcrPullActionIdentity(CancellationToken cancellationToken);

    }
}
