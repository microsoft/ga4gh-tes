// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using CommonUtilities.AzureCloud;

namespace CommonUtilities
{
    /// <summary>
    /// A utility to help with tests that require expensive objects to create
    /// </summary>
    public static class ExpensiveObjectTestUtility
    {
        public static AzureCloudConfig AzureCloudConfig = AzureCloudConfig.FromKnownCloudNameAsync().Result;
    }
}
