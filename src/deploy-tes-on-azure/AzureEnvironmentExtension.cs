using System;
using Microsoft.Azure.Management.ResourceManager.Fluent;

namespace deploytesonazure
{
    public static class AzureEnvironmentExtension
    {
        public static string GetBlobEndPointSuffix(this AzureEnvironment env)
        {
            return ".blob." + env.StorageEndpointSuffix;
        }

        public static bool IsAvailableEnvironmentName(string envName)
        {
            foreach (var azEnv in AzureEnvironment.KnownEnvironments)
            {
                if (0 == string.Compare(azEnv.Name, envName, true))
                {
                    return true;
                }
            }
            return false;
        }
    }
}

