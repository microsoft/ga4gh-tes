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
            task.Resources.DiskGb = 1;

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
                var outputPath = Path.Join(Path.GetTempPath(), outputFileName);
                var client = new BlobClient(new Uri(task.Outputs.First().Url), new AzureCliCredential());
                await client.DownloadToAsync(outputPath);
                Console.WriteLine($"Output file downloaded to: {outputPath}");
            }
            else
            {
                Console.WriteLine($"Failure reason: {completedTask.FailureReason}");
                var paths = await DownloadTaskFilesAsync(completedTask, _storageAccountName, CancellationToken.None);

                foreach (var path in paths)
                {
                    Console.WriteLine($"Task file downloaded to: {path}");
                }
            }
        }

        public async Task<List<string>> DownloadTaskFilesAsync(TesTask tesTask, string storageAccountName, CancellationToken cancellationToken)
        {
            var containerUri = new Uri($"https://{storageAccountName}.blob.core.windows.net/tes-internal");
            var client = new BlobContainerClient(containerUri, new AzureCliCredential());
            var blobs = client.GetBlobs(prefix: $"tasks/{tesTask.Id}").ToList();
            var paths = new List<string>();

            foreach (var blob in blobs)
            {
                var path = Path.Join(Path.GetTempPath(), blob.Name);
                var blobClient = client.GetBlobClient(blob.Name);
                await blobClient.DownloadToAsync(path, cancellationToken);
                paths.Add(path);
            }

            return paths;
        }
    }
}
