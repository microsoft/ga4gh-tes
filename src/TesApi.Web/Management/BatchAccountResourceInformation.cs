// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Contains Batch Account resource properties.
    /// </summary>
    /// <param name="Name">Name of the batch account.</param>
    /// <param name="ResourceGroupName">Name of the resource group.</param>
    /// <param name="SubscriptionId">Subscription id</param>
    /// <param name="Region">Region</param>
    /// <param name="BaseUrl">Base URL</param>
    public record BatchAccountResourceInformation(string Name, string ResourceGroupName, string SubscriptionId, string Region, string BaseUrl)
    {
        private const int SubscriptionIdSegment = 2;
        private const int ResourceGroupNameSegment = 4;
        private const int ResourceNameSegment = 8;

        /// <summary>
        /// Creates a new instance of BatchAccountResourceInformation from a batch resource id. 
        /// </summary>
        /// <param name="resourceId">Resource Id</param>
        /// <param name="region">Region</param>
        /// <param name="baseUrl">Base URL</param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public static BatchAccountResourceInformation FromBatchResourceId(string resourceId, string region, string baseUrl)
        {
            ArgumentException.ThrowIfNullOrEmpty(resourceId);
            ArgumentException.ThrowIfNullOrEmpty(region);
            ArgumentException.ThrowIfNullOrEmpty(baseUrl);

            if (!resourceId.StartsWith('/'))
            {
                resourceId = "/" + resourceId;
            }

            var segments = resourceId.Split('/');

            if (segments.Length < 9)
            {
                throw new ArgumentException($"The resource id provided is invalid. Resource id:{resourceId}", nameof(resourceId));
            }

            return new BatchAccountResourceInformation(segments[ResourceNameSegment],
                segments[ResourceGroupNameSegment], segments[SubscriptionIdSegment],
                region, baseUrl);
        }
    }
}
