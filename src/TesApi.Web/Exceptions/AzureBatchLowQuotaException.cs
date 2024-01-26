// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace TesApi.Web
{
    internal class AzureBatchLowQuotaException : Exception
    {
        public AzureBatchLowQuotaException()
        {
        }

        public AzureBatchLowQuotaException(string message) : base(message)
        {
        }

        public AzureBatchLowQuotaException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
