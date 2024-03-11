using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Tes.Models;
using Tes.SDK;

namespace TesApi.Tests
{
    [TestClass]
    public class IntegrationTests
    {
        [TestCategory("Integration")]
        [TestMethod]
        public async Task RunScaleTestAsync(int count = 1000)
        {
            var tesClient = GetTesClientFromAzureDevopsPipelineFileSystem();

            if (tesClient == null)
            {
                return;
            }

            var testTaskIds = new List<string>();
            var testTask = CreateTestTask();
            var sw = Stopwatch.StartNew();

            for (int i = 0; i < count; i++)
            {
                var taskId = await tesClient.CreateTaskAsync(testTask);
                testTaskIds.Add(taskId);
            }

            Console.WriteLine($"Posted {count} TES tasks in {sw.Elapsed.TotalSeconds:n3}s");
            sw.Restart();

            while (await tesClient.ListTasksAsync().AnyAsync(t => t.IsActiveState()))
            {
                // Tasks are still running
                await Task.Delay(TimeSpan.FromSeconds(20));
            }

            // All tasks are complete
            Console.WriteLine($"{count} TES tasks completed in {sw.Elapsed.TotalSeconds:n3}s");
        }

        private static TesTask CreateTestTask()
        {
            var task = new TesTask();
            task.Resources.Preemptible = true;
            task.Executors.Add(new TesExecutor
            {
                Image = "ubuntu",
                Command = new() { "/bin/sh", "-c", "cat /proc/sys/kernel/random/uuid" },
            });

            return task;
        }

        private static TesClient GetTesClientFromAzureDevopsPipelineFileSystem()
        {
            // This is set in the Azure Devops pipeline, which writes the file to the .csproj directory
            // The current working directory is this: /mnt/vss/_work/r1/a/CoaArtifacts/AllSource/TesApi.Tests/bin/Debug/net8.0/
            // And the file is available here: /mnt/vss/_work/r1/a/CoaArtifacts/AllSource/TesApi.Tests/TesCredentials.json
            const string storageAccountNamePath = "../../../TesCredentials.json";

            if (!File.Exists(storageAccountNamePath))
            {
                Console.WriteLine($"Path not found - exiting integration test: {storageAccountNamePath}");
                return null;
            }

            Console.WriteLine($"Found path: {storageAccountNamePath}");
            var stream = File.OpenRead(storageAccountNamePath);
            var (hostname, username, password) = System.Text.Json.JsonSerializer.Deserialize<TesCredentials>(stream,
                                new System.Text.Json.JsonSerializerOptions() { IncludeFields = true, PropertyNameCaseInsensitive = true });


            var baseUrl = $"https://{hostname}";
            return new TesClient(baseUrl, username, password);
        }
    }
}
