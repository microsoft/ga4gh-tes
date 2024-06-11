// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace TesApi.Web
{
    /// <summary>
    /// A default no-op implementation for use outside Terra
    /// </summary>
    public class DefaultActionIdentityProvider : IActionIdentityProvider
    {
        /// <summary>
        /// Returns null, to provide the default behavior
        /// </summary>
        public Task<string?> GetAcrPullActionIdentity(CancellationToken cancellationToken)
        {
            return Task.FromResult<string?>(null);
        }

    }
}
