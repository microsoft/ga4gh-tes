// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Extensions
{
    using System;
    using System.Security.Claims;

    public static class ClaimsPrincipalExtensions
    {
        public static string GetUserId(this ClaimsPrincipal user)
        {
            var subClaim = user?.FindFirst(ClaimTypes.NameIdentifier);

            if (string.IsNullOrWhiteSpace(subClaim?.Value))
            {
                // TODO return 401 (although this should never happen)
                throw new UnauthorizedAccessException("User is not authenticated.");
            }

            return subClaim.Value;
        }
    }
}
