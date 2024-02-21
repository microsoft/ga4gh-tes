// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CommonUtilities;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Management.PostgreSQL;
using Microsoft.Azure.Management.PostgreSQL.FlexibleServers;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Extensions.Options;
using Microsoft.Rest;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Tes.Models;
using Tes.Utilities;
using TesApi.Controllers;
using FlexibleServer = Microsoft.Azure.Management.PostgreSQL.FlexibleServers;

namespace Tes.Repository.Tests
{
    /// <summary>
    /// These tests will automatically create an "Azure Database for PostgreSQL - Flexible Server",
    /// and then delete it when done.
    /// 
    /// To run these tests,
    /// 1.  From a command prompt, log in with "az login" then start Visual Studio
    /// 2.  Set "subscriptionId" to your subscriptionId
    /// 3.  Remove the "[Ignore]" attribute from this class
    /// </summary>
    [Ignore]
    [TestClass]
    [TestCategory("Integration")]
    public class TesTaskPostgreSqlRepositoryIntegrationTests
    {
        private static TesTaskPostgreSqlRepository repository;
        private static readonly string subscriptionId = "";
        private static readonly string regionName = "southcentralus";
        private static readonly string resourceGroupName = $"tes-test-{Guid.NewGuid().ToString()[..8]}";
        private static readonly string postgreSqlServerName = $"tes{Guid.NewGuid().ToString()[..8]}";
        private static readonly string postgreSqlDatabaseName = "tes_db";
        private static readonly string adminLogin = $"tes{Guid.NewGuid().ToString()[..8]}";
        private static readonly string adminPw = PasswordGenerator.GeneratePassword();

        [ClassInitialize]
        public static async Task ClassInitializeAsync(TestContext context)
        {
            Console.WriteLine("Creating Azure Resource Group and PostgreSql Server...");
            await PostgreSqlTestUtility.CreateTestDbAsync(
                subscriptionId, regionName, resourceGroupName, postgreSqlServerName, postgreSqlDatabaseName, adminLogin, adminPw);

            var connectionString = ConnectionStringUtility
                .GetPostgresConnectionString(Options.Create(new PostgreSqlOptions
                {
                    ServerName = postgreSqlServerName,
                    DatabaseName = postgreSqlDatabaseName,
                    DatabaseUserLogin = adminLogin,
                    DatabaseUserPassword = adminPw
                }));

            repository = new TesTaskPostgreSqlRepository(() => new TesDbContext(
                TesTaskPostgreSqlRepository.NpgsqlDataSourceBuilder(connectionString),
                TesTaskPostgreSqlRepository.NpgsqlDbContextOptionsBuilder));
            Console.WriteLine("Creation complete.");
        }

        [ClassCleanup]
        public static async Task ClassCleanupAsync()
        {
            Console.WriteLine("Deleting Azure Resource Group...");
            await PostgreSqlTestUtility.DeleteResourceGroupAsync(subscriptionId, resourceGroupName);
            Console.WriteLine("Done");
        }

        [TestMethod]
        public async Task TryGetItemAsyncTest()
        {
            var id = Guid.NewGuid().ToString();
            var createdItem = await repository.CreateItemAsync(new TesTask
            {
                Id = id,
                Description = Guid.NewGuid().ToString(),
                CreationTime = DateTime.UtcNow,
                Inputs = [new TesInput { Url = "https://test" }]
            }, CancellationToken.None);
            Assert.IsNotNull(createdItem);

            Models.TesTask updatedAndRetrievedItem = null;

            var isFound = await repository.TryGetItemAsync(id, CancellationToken.None, tesTask => updatedAndRetrievedItem = tesTask);

            Assert.IsNotNull(updatedAndRetrievedItem);
            Assert.IsTrue(isFound);
        }

        [TestMethod]
        public async Task GetItemsAsyncTest()
        {
            var items = (await repository.GetItemsAsync(c => c.Id != null, CancellationToken.None)).ToList();

            foreach (var item in items)
            {
                Assert.IsTrue(!string.IsNullOrWhiteSpace(item.Id));
            }

            Assert.IsTrue(items.Any());
            Console.WriteLine(items.Count);
            Assert.AreEqual(items.Count, items.Select(t => t.Id).Distinct().Count());
        }

        [TestMethod]
        public async Task ListTasksWithPagingRetrievesAllTesTasks()
        {
            try
            {
                const bool createItems = true;
                const int itemCount = 10000;

                if (createItems)
                {
                    var rng = new Random(Guid.NewGuid().GetHashCode());
                    var states = Enum.GetValues(typeof(Models.TesState)).Cast<Models.TesState>().ToArray();

                    var items = new List<Models.TesTask>();

                    for (var i = 0; i < itemCount; i++)
                    {
                        items.Add(new Models.TesTask
                        {
                            Id = Guid.NewGuid().ToString(),
                            Description = Guid.NewGuid().ToString(),
                            CreationTime = DateTime.UtcNow,
                            State = states[rng.Next(states.Length)]
                        });
                    }

                    Assert.IsTrue(items.Select(i => i.Id).Distinct().Count() == itemCount);

                    await repository.CreateItemsAsync(items, System.Threading.CancellationToken.None);
                }

                var controller = new TaskServiceApiController(repository, null, null);
                string pageToken = null;
                var tesTaskIds = new HashSet<string>();

                while (true)
                {
                    var result = await controller.ListTasks(null, 2047, pageToken, "FULL", default);
                    JsonResult jr = (JsonResult)result;
                    var content = (TesListTasksResponse)jr.Value;
                    pageToken = content.NextPageToken;

                    foreach (var tesTask in content.Tasks)
                    {
                        if (tesTaskIds.Contains(tesTask.Id))
                        {
                            int count = tesTaskIds.Count;
                            Debugger.Break();
                            Assert.Fail("Duplicate task id");
                        }

                        tesTaskIds.Add(tesTask.Id);
                    }

                    Console.WriteLine($"Found {tesTaskIds.Count}");

                    if (string.IsNullOrWhiteSpace(pageToken))
                    {
                        break;
                    }
                }

                Assert.IsTrue(tesTaskIds.Count == itemCount);
                Console.WriteLine("Done");
            }
            catch (Exception)
            {
                Debugger.Break();
                throw;
            }
        }

        [TestMethod]
        public async Task Create1mAndQuery1mAsync()
        {
            const bool createItems = true;
            const int itemCount = 1_000_000;

            var sw = Stopwatch.StartNew();

            if (createItems)
            {
                var rng = new Random(Guid.NewGuid().GetHashCode());
                var states = Enum.GetValues(typeof(Models.TesState)).Cast<Models.TesState>().ToArray();

                var items = new List<Models.TesTask>();

                for (var i = 0; i < itemCount; i++)
                {
                    items.Add(new Models.TesTask
                    {
                        Id = Guid.NewGuid().ToString(),
                        Description = Guid.NewGuid().ToString(),
                        CreationTime = DateTime.UtcNow,
                        State = states[rng.Next(states.Length)]
                    });
                }

                Assert.IsTrue(items.Select(i => i.Id).Distinct().Count() == itemCount);

                await repository.CreateItemsAsync(items, CancellationToken.None);
                Console.WriteLine($"Total seconds to insert {items.Count} items: {sw.Elapsed.TotalSeconds:n2}s");
                sw.Restart();
            }

            sw.Restart();
            var runningTasks = (await repository.GetItemsAsync(c => c.State == Models.TesState.RUNNINGEnum, CancellationToken.None)).ToList();

            // Ensure performance is decent
            Assert.IsTrue(sw.Elapsed.TotalSeconds < 20);
            Console.WriteLine($"Retrieved {runningTasks.Count} in {sw.Elapsed.TotalSeconds:n1}s");
            sw.Restart();
            var allOtherTasks = (await repository.GetItemsAsync(c => c.State != Models.TesState.RUNNINGEnum, CancellationToken.None)).ToList();
            Console.WriteLine($"Retrieved {allOtherTasks.Count} in {sw.Elapsed.TotalSeconds:n1}s");
            Console.WriteLine($"Total running tasks: {runningTasks.Count}");
            Console.WriteLine($"Total other tasks: {allOtherTasks.Count}");
            var distinctRunningTasksIds = runningTasks.Select(i => i.Id).Distinct().Count();
            var distinctOtherTaskIds = allOtherTasks.Select(i => i.Id).Distinct().Count();
            Console.WriteLine($"uniqueRunningTasksIds: {distinctRunningTasksIds}");
            Console.WriteLine($"distinctOtherTaskIds: {distinctOtherTaskIds}");

            Assert.IsTrue(distinctRunningTasksIds + distinctOtherTaskIds == itemCount);

            Assert.IsTrue(runningTasks.Count > 0);
            Assert.IsTrue(allOtherTasks.Count > 0);
            Assert.IsTrue(runningTasks.Count != allOtherTasks.Count);
            Assert.IsTrue(runningTasks.All(c => c.State == Models.TesState.RUNNINGEnum));
            Assert.IsTrue(allOtherTasks.All(c => c.State != Models.TesState.RUNNINGEnum));
        }

        [TestMethod]
        public async Task GetItemsContinuationAsyncTest()
        {
            const int pageSize = 256;

            var (continuation, items) = await repository.GetItemsAsync(c => c.Id != null, pageSize, null, CancellationToken.None);
            var itemsList = items.ToList();
            Assert.IsTrue(itemsList.Count <= pageSize);

            while (!string.IsNullOrWhiteSpace(continuation))
            {
                (continuation, items) = await repository.GetItemsAsync(c => c.Id != null, pageSize, continuation, CancellationToken.None);
                itemsList.AddRange(items);
            }

            foreach (var item in itemsList)
            {
                Assert.IsTrue(!string.IsNullOrWhiteSpace(item.Id));
            }

            Assert.IsTrue(itemsList.Any());
            Console.WriteLine(itemsList.Count);
            Assert.AreEqual(itemsList.Count, itemsList.Select(t => t.Id).Distinct().Count());
        }

        [TestMethod]
        public async Task CreateItemAsyncTest()
        {
            var itemId = Guid.NewGuid().ToString();

            var task = await repository.CreateItemAsync(new TesTask
            {
                Id = itemId,
                Description = Guid.NewGuid().ToString(),
                CreationTime = DateTime.UtcNow,
                Inputs = [new TesInput { Url = "https://test" }]
            }, CancellationToken.None);

            Assert.IsNotNull(task);
        }

        [TestMethod]
        public async Task UpdateItemAsyncTest()
        {
            var description = $"created at {DateTime.UtcNow}";
            var id = Guid.NewGuid().ToString();

            var createdItem = await repository.CreateItemAsync(new TesTask
            {
                Id = id,
                Description = Guid.NewGuid().ToString(),
                CreationTime = DateTime.UtcNow,
                Inputs = [new TesInput { Url = "https://test" }]
            }, CancellationToken.None);

            Assert.IsTrue(createdItem.State != Models.TesState.COMPLETEEnum);

            createdItem.Description = description;
            createdItem.State = Models.TesState.COMPLETEEnum;

            await repository.UpdateItemAsync(createdItem, CancellationToken.None);

            Models.TesTask updatedAndRetrievedItem = null;

            var isFound = await repository.TryGetItemAsync(id, CancellationToken.None, tesTask => updatedAndRetrievedItem = tesTask);

            Assert.IsTrue(isFound);
            Assert.IsTrue(updatedAndRetrievedItem.State == Models.TesState.COMPLETEEnum);
            Assert.IsTrue(updatedAndRetrievedItem.Description == description);
        }

        [TestMethod]
        public async Task DeleteItemAsyncTest()
        {
            var id = Guid.NewGuid().ToString();

            var createdItem = await repository.CreateItemAsync(new TesTask
            {
                Id = id,
                Description = Guid.NewGuid().ToString(),
                CreationTime = DateTime.UtcNow,
                Inputs = [new TesInput { Url = "https://test" }]
            }, CancellationToken.None);
            Assert.IsNotNull(createdItem);
            await repository.DeleteItemAsync(id, CancellationToken.None);

            Models.TesTask updatedAndRetrievedItem = null;

            var isFound = await repository.TryGetItemAsync(id, CancellationToken.None, tesTask => updatedAndRetrievedItem = tesTask);
            Assert.IsNull(updatedAndRetrievedItem);
            Assert.IsFalse(isFound);
        }
    }


    public static class PostgreSqlTestUtility
    {
        public static async Task CreateTestDbAsync(
            string subscriptionId,
            string regionName,
            string resourceGroupName,
            string postgreSqlServerName,
            string postgreSqlDatabaseName,
            string adminLogin,
            string adminPw)
        {
            const string postgreSqlVersion = "14";

            var tokenCredentials = new TokenCredentials(new RefreshableAzureServiceTokenProvider("https://management.azure.com//.default"));
            var azureCredentials = new AzureCredentials(tokenCredentials, null, null, AzureEnvironment.AzureGlobalCloud);
            var postgresManagementClient = new FlexibleServer.PostgreSQLManagementClient(azureCredentials) { SubscriptionId = subscriptionId, LongRunningOperationRetryTimeout = 1200 };
            var azureClient = GetAzureClient(azureCredentials);
            var azureSubscriptionClient = azureClient.WithSubscription(subscriptionId);

            var rgs = (await azureSubscriptionClient.ResourceGroups.ListAsync()).ToList();

            if (rgs.Any(r => r.Name.Equals(resourceGroupName, StringComparison.OrdinalIgnoreCase)))
            {
                return;
            }

            await azureSubscriptionClient
                .ResourceGroups
                .Define(resourceGroupName)
                .WithRegion(regionName)
                .CreateAsync();

            await postgresManagementClient.Servers.CreateAsync(
                resourceGroupName,
                postgreSqlServerName,
                        new(
                           location: regionName,
                           version: postgreSqlVersion,
                           sku: new("Standard_B2s", "Burstable"),
                           storage: new(128),
                           administratorLogin: adminLogin,
                           administratorLoginPassword: adminPw,
                           //network: new(publicNetworkAccess: "Enabled"),
                           highAvailability: new("Disabled")
                        ));

            await postgresManagementClient.Databases.CreateAsync(resourceGroupName, postgreSqlServerName, postgreSqlDatabaseName, new());

            var startIp = "0.0.0.0";
            var endIp = "255.255.255.255";

            // Many networks have non-deterministic client IP addresses
            //using var client = new HttpClient();
            //var ip = (await client.GetStringAsync("https://checkip.amazonaws.com")).Trim();
            //startIp = ip;
            //endIp = ip;

            await postgresManagementClient.FirewallRules.CreateOrUpdateAsync(
                resourceGroupName,
                postgreSqlServerName,
                "AllowTestMachine",
                new FlexibleServer.Models.FirewallRule { StartIpAddress = startIp, EndIpAddress = endIp });
        }

        public static async Task DeleteResourceGroupAsync(string subscriptionId, string resourceGroupName)
        {
            var tokenCredentials = new TokenCredentials(new RefreshableAzureServiceTokenProvider("https://management.azure.com//.default"));
            var azureCredentials = new AzureCredentials(tokenCredentials, null, null, AzureEnvironment.AzureGlobalCloud);
            var azureClient = GetAzureClient(azureCredentials);
            var azureSubscriptionClient = azureClient.WithSubscription(subscriptionId);
            await azureSubscriptionClient.ResourceGroups.DeleteByNameAsync(resourceGroupName, CancellationToken.None);
        }

        private static Microsoft.Azure.Management.Fluent.Azure.IAuthenticated GetAzureClient(AzureCredentials azureCredentials)
            => Microsoft.Azure.Management.Fluent.Azure
                .Configure()
                .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                .Authenticate(azureCredentials);
    }
}
