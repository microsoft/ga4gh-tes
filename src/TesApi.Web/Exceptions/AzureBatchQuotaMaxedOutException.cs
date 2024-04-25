// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace TesApi.Web
{
    internal class AzureBatchQuotaMaxedOutException : Exception
    {
        public AzureBatchQuotaMaxedOutException()
        {
        }

        public AzureBatchQuotaMaxedOutException(string message) : base(message)
        {
        }

        public AzureBatchQuotaMaxedOutException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
