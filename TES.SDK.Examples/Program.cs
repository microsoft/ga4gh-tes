// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Configuration;
using Tes.SDK;

namespace TES.SDK.Examples
{
    internal class Program
    {
        // How to configure this example
        // https://learn.microsoft.com/en-us/aspnet/core/security/app-secrets?view=aspnetcore-8.0&tabs=windows
        // 1.  Create a new User Secrets file with the following properties:
        //     TesCredentialsPath = path to TesCredentials.json (created during deployment)
        //     StorageAccountName = Name of your storage account in your TES deployment (to save the output file)
        private static async Task Main()
        {
            var builder = new ConfigurationBuilder()
                .AddUserSecrets<Program>();

            IConfiguration configuration = builder.Build();

            string? tesCredentialsPath = configuration["TesCredentialsPath"];
            string? storageAccountName = configuration["StorageAccountName"];

            if (string.IsNullOrEmpty(tesCredentialsPath) || string.IsNullOrEmpty(storageAccountName))
            {
                
                Console.WriteLine("Please set the TesCredentialsPath and StorageAccountName in your User Secrets.");
                return;
            }

            var tesCredentials = TesCredentials.Deserialize(File.Open(tesCredentialsPath, FileMode.Open));
            var tesExamples = new TesExamples(tesCredentials, storageAccountName);

            await tesExamples.RunPrimeSieveAsync();
        }
    }
}
