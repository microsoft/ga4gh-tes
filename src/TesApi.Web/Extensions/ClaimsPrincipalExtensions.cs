// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Extensions
{
    using System;
    using System.Security.Claims;
    using Tes.Models;

    public static class ClaimsPrincipalExtensions
    {
        public static TesUser GetTesUser(this ClaimsPrincipal user)
        {
            var subClaim = user?.FindFirst(ClaimTypes.NameIdentifier);
            var oidClaim = user?.FindFirst("oid"); // TODO set "oid" value from AuthenticationOptions Provider field

            if (string.IsNullOrWhiteSpace(subClaim?.Value))
            {
                // TODO return 401 (although this should never happen)
                throw new UnauthorizedAccessException("User is not authenticated.");
            }

            return new TesUser
            {
                Id = subClaim.Value,
                ExternalId = oidClaim?.Value
            };
        }
    }
}
