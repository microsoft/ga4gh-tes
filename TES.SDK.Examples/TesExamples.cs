// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Identity;
using Azure.Storage.Blobs;
using Tes.Models;
using Tes.SDK;

namespace TES.SDK.Examples
{
    public class TesExamples
    {
        private readonly TesCredentials _tesCredentials;
        private readonly string _storageAccountName;

        public TesExamples(TesCredentials tesCredentials, string storageAccountName)
        {
            _tesCredentials = tesCredentials;
            _storageAccountName = storageAccountName;
        }

        public async Task RunPrimeSieveAsync()
        {
            TesTask task = new();
            task.Resources.Preemptible = true;
            task.Resources.CpuCores = 1;
            task.Resources.RamGb = 1;
            //testTesTask.Resources.DiskGb = 100;
            const long maxPrimes = 1_000_000;
            string outputFileName = $"primes-2-{maxPrimes}.txt";
            string command = $"apt-get update && apt-get install -y primesieve && primesieve {maxPrimes} -p > /tmp/{outputFileName}";

            task.Executors.Add(new()
            {
                Image = "ubuntu:22.04",
                Command = [ 
                    "/bin/sh",
                    "-c",
                    $"chmod 1777 /tmp && {command}"
                ]
            });

            task.Outputs.Add(new()
            {
                Path = $"/tmp/{outputFileName}",
                Url = $"https://{_storageAccountName}.blob.core.windows.net/outputs/{outputFileName}"
            });
            
            using ITesClient tesClient = new TesClient(_tesCredentials);
            var completedTask = await tesClient.CreateAndWaitTilDoneAsync(task);
            Console.WriteLine($"TES Task State: {completedTask.State}");

            if (completedTask.State == TesState.COMPLETE)
            {
                var client = new BlobClient(new Uri(task.Outputs.First().Url), new DefaultAzureCredential());
                var downloadResponse = await client.DownloadContentAsync(CancellationToken.None);
                var output = downloadResponse.Value.Content.ToString();
                Console.WriteLine(output);
            }
            else
            {
                Console.WriteLine($"Failure reason: {completedTask.FailureReason}");
                var logs = await tesClient.DownloadLogsAsync(completedTask, _storageAccountName, CancellationToken.None);

                foreach (var key in logs.Keys)
                {
                    Console.WriteLine($"Log type: {key}");
                    Console.WriteLine(logs[key]);
                }
            }
        }
    }
}
