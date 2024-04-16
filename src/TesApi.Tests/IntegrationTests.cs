// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
        private static readonly System.Text.Json.JsonSerializerOptions credentialsJsonOptions = new()
        {
            IncludeFields = true,
            PropertyNameCaseInsensitive = true
        };

        [TestCategory("Integration")]
        [TestMethod]
        public async Task RunFullScaleTestAsync()
        {
            // 500 to 503 when throttled
            // max egress: 120 Gbps (15 GB/s)
            // max request rate: 20K rps
            // max per blob: 500 rps
            // TCP keep alive vs. request rate
            // target for single block blob (up to ingress/egress limits.  so 120 Gbps)
            // 1.  Create 100 x 10 GiB files (1 TiB)
            // 


            var count = 1000;
            var testTask = CreateFullTestTask();
            using var tesClient = GetTesClientFromAzureDevopsPipelineFileSystem();

            if (tesClient is null)
            {
                return;
            }


            await InternalRunScaleTestAsync(count, testTask, tesClient);
        }

        [TestCategory("Integration")]
        [TestMethod]
        public async Task RunBasicScaleTestAsync()
        {
            var count = 1000;
            var testTask = CreateBasicTestTask();
            using var tesClient = GetTesClientFromAzureDevopsPipelineFileSystem();

            if (tesClient is null)
            {
                return;
            }

            await InternalRunScaleTestAsync(count, testTask, tesClient);
        }

        private static async Task InternalRunScaleTestAsync(int count, TesTask testTask, ITesClient tesClient)
        {
            var testTaskIds = new System.Collections.Concurrent.ConcurrentBag<string>();

            var sw = Stopwatch.StartNew();

            await Parallel.ForEachAsync(
                AsyncEnumerable.Repeat(new object(), count).Select(_ => tesClient.CreateTaskAsync(testTask)),
                new ParallelOptions() { MaxDegreeOfParallelism = 16384 },
                async (idAsTask, _) => testTaskIds.Add(await idAsTask));


            Console.WriteLine($"Posted {testTaskIds.Count} TES tasks in {sw.Elapsed.TotalSeconds:n3}s");
            Assert.AreEqual(count, testTaskIds.Count);
            Assert.AreEqual(count, await tesClient.ListTasksAsync().CountAsync(t => testTaskIds.Contains(t.Id)));
            sw.Restart();

            var taskIdsHashset = new HashSet<string>(testTaskIds);

            while (await tesClient.ListTasksAsync()
                .Where(t => taskIdsHashset.Contains(t.Id))
                .AnyAsync(t => t.IsActiveState()))
            {
                // Tasks are still running
                await Task.Delay(TimeSpan.FromSeconds(20));
            }

            // All tasks are complete
            Console.WriteLine($"{testTaskIds.Count} TES tasks completed in {sw.Elapsed.TotalSeconds:n3}s");

            var completedTasks = await tesClient.ListTasksAsync().Where(t => testTaskIds.Contains(t.Id)).GroupBy(t => t.State).ToDictionaryAwaitAsync(g => ValueTask.FromResult(g.Key), g => g.ToListAsync());

            foreach (var tasksWithState in completedTasks)
            {
                Console.WriteLine($"State: {tasksWithState.Key}: Count: {tasksWithState.Value.Count}");
                if (!new[] { TesState.COMPLETEEnum, TesState.CANCELEDEnum }.Contains(tasksWithState.Key))
                {
                    var tasksByCode = tasksWithState.Value.Select(ParseOutLogs).GroupBy(t => t.Code).ToDictionary(g => g.Key, g => g.Select(t => t.Logs));
                    foreach (var tasksWithCode in tasksByCode)
                    {
                        Console.WriteLine($"{IndentStep(1)}Code: {tasksWithCode.Key}");
                        Console.WriteLine(string.Join(Environment.NewLine, tasksWithCode.Value.Select(t => string.Join(Environment.NewLine, t.Select(l => $"{IndentStep(2)}{l}{Environment.NewLine}").ToList()))) + $"{IndentStep(2)}----");
                    }
                }
            }

            static (string Code, System.Collections.Generic.List<string> Logs) ParseOutLogs(TesTask task)
            {
                var logs = task.Logs?.LastOrDefault()?.SystemLogs;

                return logs switch
                {
                    null => ("No logs!", []),
                    _ => (logs.First(), logs.Skip(1).ToList()),
                };
            }

            static string IndentStep(int step) => new(Enumerable.Repeat(' ', step * 4).ToArray());
        }

        private static TesTask CreateBasicTestTask()
        {
            var task = new TesTask();
            task.Resources.Preemptible = true;
            task.Executors.Add(new TesExecutor
            {
                Image = "ubuntu",
                Command = ["/bin/sh", "-c", "cat /proc/sys/kernel/random/uuid"],
            });

            return task;
        }

        private static TesTask CreateFullTestTask()
        {
            var task = new TesTask();
            task.Resources.Preemptible = true;
            task.Executors.Add(new TesExecutor
            {
                Image = "ubuntu",
                Command = ["/bin/sh", "-c", "cat /proc/sys/kernel/random/uuid"],
            });

            return task;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "We are explicitly using the contract specified in the ITesClient interface.")]
        private static ITesClient GetTesClientFromAzureDevopsPipelineFileSystem()
        {
            // This is set in the Azure Devops pipeline, which writes the file to the .csproj directory
            // The current working directory is this: /mnt/vss/_work/r1/a/CoaArtifacts/AllSource/TesApi.Tests/bin/Debug/net8.0/
            // And the file is available here: /mnt/vss/_work/r1/a/CoaArtifacts/AllSource/TesApi.Tests/TesCredentials.json
            const string storageAccountNamePath = "../../../TesCredentials.json";
            FileInfo path = new(storageAccountNamePath);

            if (!path.Exists)
            {
                Console.WriteLine($"Path not found - exiting integration test: {path}");
                return null;
            }

            Console.WriteLine($"Found path: {path}");
            using var stream = path.OpenRead();
            var (hostname, username, password) = System.Text.Json.JsonSerializer.Deserialize<TesCredentials>(stream, credentialsJsonOptions);

            return new TesClient(new($"https://{hostname}"), username, password);
        }
    }
}
