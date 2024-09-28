// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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

            task.Executors.Add(new()
            {
                Image = "ubuntu:22.04",
                Command = [ 
                    "/bin/sh",
                    "-c",
                    "chmod 1777 /tmp && apt-get update && apt-get install -y primesieve && primesieve 1e6 > /tmp/output.txt"
                ]
            });

            task.Outputs.Add(new()
            {
                Path = "/tmp/output.txt",
                Url = $"https://{_storageAccountName}.blob.core.windows.net/outputs/output.txt"
            });
            
            using ITesClient tesClient = new TesClient(_tesCredentials);
            var completedTask = await tesClient.CreateAndWaitTilDoneAsync(task);
            Console.WriteLine($"TES Task State: {completedTask.State}");

            if (completedTask.State != TesState.COMPLETE)
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
