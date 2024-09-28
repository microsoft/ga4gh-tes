// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Identity;
using Azure.Storage.Blobs;
using Tes.Models;
using Tes.SDK;
using static Tes.SDK.TesClient;

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
                var client = new BlobClient(new Uri(task.Outputs.First().Url), new AzureCliCredential());
                var downloadResponse = await client.DownloadContentAsync(CancellationToken.None);
                var output = downloadResponse.Value.Content.ToString();
                Console.WriteLine(output);
            }
            else
            {
                Console.WriteLine($"Failure reason: {completedTask.FailureReason}");
                var logs = await DownloadLogsAsync(completedTask, _storageAccountName, CancellationToken.None);

                foreach (var key in logs.Keys)
                {
                    Console.WriteLine($"Log type: {key}");
                    Console.WriteLine(logs[key]);
                }
            }
        }
        public enum TesLogType { NotSet = 0, ExecStdOut, ExecStdErr, DownloadStdOut, DownloadStdErr };
        public async Task<Dictionary<TesLogType, string>> DownloadLogsAsync(TesTask tesTask, string storageAccountName, CancellationToken cancellationToken)
        {
            Dictionary<TesLogType, string> logs = new Dictionary<TesLogType, string>();
            var containerUri = new Uri($"https://{storageAccountName}.blob.core.windows.net/tes-internal");
            var client = new BlobContainerClient(containerUri, new AzureCliCredential());
            var blobs = client.GetBlobs(prefix: $"tasks/{tesTask.Id}").ToList();

            foreach (var blob in blobs)
            {
                TesLogType logType = blob.Name switch
                {
                    var name when name.StartsWith("exec_stdout") => TesLogType.ExecStdOut,
                    var name when name.StartsWith("exec_stderr") => TesLogType.ExecStdErr,
                    var name when name.StartsWith("download_stdout") => TesLogType.DownloadStdOut,
                    var name when name.StartsWith("download_stderr") => TesLogType.DownloadStdErr,
                    _ => TesLogType.NotSet
                };

                if (logType != TesLogType.NotSet)
                {
                    var blobClient = client.GetBlobClient(blob.Name);
                    var text = (await blobClient.DownloadContentAsync()).Value.Content.ToString();
                    logs.Add(logType, text);
                }
            }

            return logs;
        }
    }
}
