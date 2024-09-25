// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Net;

using Azure;
using Microsoft.Azure.Batch.Common;

namespace TesApi.Web
{
    internal class AzureBatchPoolCreationException : Exception
    {
        public static bool IsJobQuotaException(string code)
            => BatchErrorCodeStrings.ActiveJobAndScheduleQuotaReached.Equals(code, StringComparison.OrdinalIgnoreCase);

        public static bool IsPoolQuotaException(string code)
            => code switch
            {
                var x when "AutoPoolCreationFailedWithQuotaReached".Equals(x, StringComparison.OrdinalIgnoreCase) => true,
                var x when BatchErrorCodeStrings.PoolQuotaReached.Equals(x, StringComparison.OrdinalIgnoreCase) => true,
                _ => false,
            };

        public static bool IsTimeoutException(string code)
            => code switch
            {
                var x when BatchErrorCodeStrings.OperationTimedOut.Equals(x, StringComparison.OrdinalIgnoreCase) => true,
                var x when BatchErrorCodeStrings.ServerBusy.Equals(x, StringComparison.OrdinalIgnoreCase) => true,
                _ => false,
            };

        public AzureBatchPoolCreationException()
        { }

        public AzureBatchPoolCreationException(string message)
            : base(message)
        { }

        public AzureBatchPoolCreationException(string message, Exception innerException)
            : base(message, innerException)
        {
            (bool isJobQuota, bool isPoolQuota, bool isTimeout) state = default;

            switch (innerException)
            {
                case Microsoft.Rest.Azure.CloudException cloudException:
                    state = GetState(cloudException.Body.Code);
                    break;

                case BatchException batchException:
                    state = GetState((batchException.InnerException as Microsoft.Azure.Batch.Protocol.Models.BatchErrorException)?.Body.Code);
                    break;
            }

            IsJobQuota = state.isJobQuota;
            IsPoolQuota = state.isPoolQuota;
            IsTimeout = state.isTimeout;

            static (bool isJobQuota, bool isPoolQuota, bool isTimeout) GetState(string code)
                => (IsJobQuotaException(code), IsPoolQuotaException(code), IsTimeoutException(code));
        }

        public AzureBatchPoolCreationException(string message, bool isTimeout, Exception innerException)
            : this(message, innerException)
            => IsTimeout = isTimeout;

        public bool IsJobQuota { get; }

        public bool IsPoolQuota { get; }

        public bool IsTimeout { get; }
    }
}
