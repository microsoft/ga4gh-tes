// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
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

        public async Task RunPrimeSieveAsync(int taskCount = 1)
        {
            const long rangePerTask = 1_000_000; // Each machine will cover a range of 1 million numbers
            var tasks = new List<TesTask>();

            for (int i = 0; i < taskCount; i++)
            {
                long rangeStart = i * rangePerTask;       // Start of the range for this machine
                long rangeEnd = (i + 1) * rangePerTask;   // End of the range for this machine
                string outputFileName = $"primes-{rangeStart}-{rangeEnd}.txt";
                string command = $"apt-get update && apt-get install -y primesieve && primesieve {rangeStart} {rangeEnd} -p > /tmp/{outputFileName}";

                TesTask task = new();
                task.Resources.Preemptible = true;
                task.Resources.CpuCores = 1;
                task.Resources.RamGb = 1;
                task.Resources.DiskGb = 1;

                task.Executors.Add(new()
                {
                    Image = "ubuntu:22.04",
                    Command = new List<string>
                    {
                        "/bin/sh",
                        "-c",
                        $"chmod 1777 /tmp && {command}"
                    }
                });

                task.Outputs.Add(new()
                {
                    Path = $"/tmp/{outputFileName}",
                    Url = $"https://{_storageAccountName}.blob.core.windows.net/outputs/{outputFileName}"
                });

                tasks.Add(task);
            }

            await RunTasks(tasks);
        }

        public async Task RunBwaMemAsync()
        {
            const string outputFileName = "H06HDADXX130110.1.ATCACGAT.20k.bam";

            TesTask task = new()
            {
                Resources = new TesResources
                {
                    CpuCores = 16,
                    RamGb = 32
                },
                Executors = new List<TesExecutor>
                {
                    new TesExecutor
                    {
                        Image = "quay.io/biocontainers/bwa:0.7.18--he4a0461_1",
                        Command = new List<string> 
                        {
                            "/bin/sh", 
                            "-c",
                            $"bwa index /data/Homo_sapiens_assembly38.fasta && bwa mem -t 16 /data/Homo_sapiens_assembly38.fasta /data/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq /data/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq > /data/{outputFileName}"
                        },
                        Workdir = "/data"
                    }
                },
                Inputs = new List<TesInput>
                {
                    new TesInput
                    {
                        Name = "H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq",
                        Path = "/data/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq",
                        Url = "https://datasettestinputs.blob.core.windows.net/dataset/seq-format-conversion/NA12878_20k/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq"
                    },
                    new TesInput
                    {
                        Name = "H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq",
                        Path = "/data/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq",
                        Url = "https://datasettestinputs.blob.core.windows.net/dataset/seq-format-conversion/NA12878_20k/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq"
                    },
                    new TesInput
                    {
                        Name = "Homo_sapiens_assembly38.fasta",
                        Path = "/data/Homo_sapiens_assembly38.fasta",
                        Url = "https://datasettestinputs.blob.core.windows.net/dataset/references/hg38/v0/Homo_sapiens_assembly38.fasta"
                    }
                },
                Outputs = new List<TesOutput>
                {
                    new TesOutput
                    {
                        Name = outputFileName,
                        Path = $"/data/{outputFileName}",
                        Url = $"https://{_storageAccountName}.blob.core.windows.net/outputs/{outputFileName}"
                    }
                }
            };

            await RunTasks(new List<TesTask> { task });
        }

        private async Task RunTasks(List<TesTask> tasks)
        {
            var sw = Stopwatch.StartNew();
            using ITesClient tesClient = new TesClient(_tesCredentials);
            Console.WriteLine($"Submitting {tasks.Count} task(s) and waiting til done...");
            var completedTasks = await tesClient.CreateAndWaitTilDoneAsync(tasks);

            foreach (var completedTask in completedTasks)
            {
                if (completedTask.State == TesState.COMPLETE)
                {
                    string outputFileName = completedTask.Outputs.First().Path.Split('/').Last();
                    var outputPath = Path.Join(Path.GetTempPath(), outputFileName);
                    var client = new BlobClient(new Uri(completedTask.Outputs.First().Url), new AzureCliCredential());
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

            Console.WriteLine($"All tasks completed in {sw.Elapsed}");
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
