// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Models
{
    public class TesUser
    {
        /// <summary>
        /// "subject" in JWT
        /// </summary>
        public string Id { get; set; }
        /// <summary>
        /// "oid" in Azure
        /// </summary>
        public string ExternalId { get; set; }
    }
}
