// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Core;
using Azure.ResourceManager;
using Azure.ResourceManager.PostgreSql.FlexibleServers;
using Azure.ResourceManager.PostgreSql.FlexibleServers.Models;
using CommonUtilities;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Tes.Models;
using Tes.Utilities;
using TesApi.Controllers;

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
        private static IRepository<TesTask> repository;
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

            var connectionString = ConnectionStringUtility.GetPostgresConnectionString(
                Options.Create(new PostgreSqlOptions
                {
                    ServerName = postgreSqlServerName,
                    DatabaseName = postgreSqlDatabaseName,
                    DatabaseUserLogin = adminLogin,
                    DatabaseUserPassword = adminPw
                }));

            var dataSource = TesTaskPostgreSqlRepository.NpgsqlDataSourceFunc(connectionString);
            repository = new TesTaskPostgreSqlRepository(() => new TesDbContext(dataSource, TesTaskPostgreSqlRepository.NpgsqlDbContextOptionsBuilder));
            Console.WriteLine("Creation complete.");
        }

        [ClassCleanup]
        public static async Task ClassCleanupAsync()
        {
            Console.WriteLine("Deleting Azure Resource Group...");
            await PostgreSqlTestUtility.DeleteResourceGroupAsync(subscriptionId, resourceGroupName);
            repository?.Dispose();
            Console.WriteLine("Done");
        }

        [TestMethod]
        public async Task TryGetItemAsyncTest()
        {
            var id = Guid.NewGuid().ToString();
            var createdItem = await repository.CreateItemAsync(new()
            {
                Id = id,
                Description = Guid.NewGuid().ToString(),
                CreationTime = DateTime.UtcNow,
                Inputs = [new TesInput { Url = "https://test" }]
            }, CancellationToken.None);
            Assert.IsNotNull(createdItem);

            TesTask updatedAndRetrievedItem = null;

            var isFound = await repository.TryGetItemAsync(id, CancellationToken.None, tesTask => updatedAndRetrievedItem = tesTask);

            Assert.IsNotNull(updatedAndRetrievedItem);
            Assert.IsTrue(isFound);
        }

        [TestMethod]
        public async Task GetItemsAsyncTest()
        {
            var items = (await repository.GetItemsAsync(c => c.Id != null, CancellationToken.None)).ToList();
            var tesTaskIds = new HashSet<string>();

            CheckAndAddItems(items, tesTaskIds);
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
                    var states = Enum.GetValues(typeof(TesState)).Cast<TesState>().ToArray();

                    var items = new List<TesTask>();

                    for (var i = 0; i < itemCount; i++)
                    {
                        items.Add(new()
                        {
                            Id = Guid.NewGuid().ToString(),
                            Description = Guid.NewGuid().ToString(),
                            CreationTime = DateTime.UtcNow,
                            State = states[rng.Next(states.Length)]
                        });
                    }

                    Assert.IsTrue(items.Select(i => i.Id).Distinct().Count() == itemCount);

                    await Parallel.ForEachAsync(items, CancellationToken.None, async (item, token) => await repository.CreateItemAsync(item, token));
                }

                var controller = new TaskServiceApiController(repository, null, null, null, null);
                string pageToken = null;
                var tesTaskIds = new HashSet<string>();

                do
                {
                    var result = await controller.ListTasksAsync(null, null, null, null, 2047, pageToken, "FULL", default);
                    var jr = (JsonResult)result;
                    var content = (TesListTasksResponse)jr.Value;
                    pageToken = content.NextPageToken;

                    CheckAndAddItems(content.Tasks, tesTaskIds);
                    Console.WriteLine($"Found {tesTaskIds.Count}");
                }
                while (!string.IsNullOrWhiteSpace(pageToken));

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
            await Create1mItemsAsync();
        }

        private static async Task Create1mItemsAsync()
        {
            const bool createItems = true;
            const int itemCount = 1_000_000;

            var sw = Stopwatch.StartNew();

            if (createItems)
            {
                var rng = new Random(Guid.NewGuid().GetHashCode());
                var states = Enum.GetValues(typeof(TesState)).Cast<TesState>().ToArray();

                var items = new List<TesTask>();

                for (var i = 0; i < itemCount; i++)
                {
                    items.Add(new()
                    {
                        Id = Guid.NewGuid().ToString(),
                        Description = Guid.NewGuid().ToString(),
                        CreationTime = DateTime.UtcNow,
                        State = states[rng.Next(states.Length)]
                    });
                }

                Assert.IsTrue(items.Select(i => i.Id).Distinct().Count() == itemCount);

                await (repository as TesTaskPostgreSqlRepository).CreateItemsAsync(items, CancellationToken.None);
                Console.WriteLine($"Total seconds to insert {items.Count} items: {sw.Elapsed.TotalSeconds:n2}s");
                sw.Restart();
            }

            sw.Restart();
            var runningTasks = (await repository.GetItemsAsync(c => c.State == TesState.RUNNING, CancellationToken.None)).ToList();

            // Ensure performance is decent
            Assert.IsTrue(sw.Elapsed.TotalSeconds < 20);
            Console.WriteLine($"Retrieved {runningTasks.Count} in {sw.Elapsed.TotalSeconds:n1}s");
            sw.Restart();
            var allOtherTasks = (await repository.GetItemsAsync(c => c.State != TesState.RUNNING, CancellationToken.None)).ToList();
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
            Assert.IsTrue(runningTasks.All(c => c.State == TesState.RUNNING));
            Assert.IsTrue(allOtherTasks.All(c => c.State != TesState.RUNNING));
        }

        [TestMethod]
        public async Task OverloadedDbIsHandled()
        {
            await Create1mItemsAsync();
            var tasks = new List<Task>();
            long overloadedDbExceptionCount = 0;
            long exceptionCount = 0;

            for (var i = 0; i < 200; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        var allOtherTasks = await repository.GetItemsAsync(c => c.State != TesState.RUNNING, CancellationToken.None);
                    }
                    catch (DatabaseOverloadedException)
                    {
                        Interlocked.Increment(ref overloadedDbExceptionCount);
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref exceptionCount);
                        var exType = ex.GetType();
                        var inexType = ex.InnerException?.GetType();
                        var n1 = exType.Name;
                        var n2 = inexType?.Name;
                        Debugger.Break();
                    }
                }));
            }

            await Task.WhenAll(tasks);
            Console.WriteLine(overloadedDbExceptionCount);
            Assert.IsTrue(overloadedDbExceptionCount > 0);
            Assert.IsTrue(exceptionCount == 0);
        }

        [TestMethod]
        public async Task GetItemsTagsAsyncTest()
        {
            foreach (var (filter, tags, match) in GetTestData())
            {
                var createdItem = await repository.CreateItemAsync(new()
                {
                    Id = Guid.NewGuid().ToString(),
                    Description = Guid.NewGuid().ToString(),
                    CreationTime = DateTime.UtcNow,
                    State = TesState.UNKNOWN,
                    Tags = tags
                }, CancellationToken.None);

                var (raw, ef) = TaskServiceApiController.GenerateSearchPredicates(repository, null, null, filter);
                var items = (await repository.GetItemsAsync(null, 2048, CancellationToken.None, raw, ef)).Items.ToList();

                if (match)
                {
                    Assert.AreEqual(1, items.Count);
                    Assert.IsTrue(items.Select(t => t.Id).Contains(createdItem.Id));
                }
                else
                {
                    Assert.IsFalse(items.Select(t => t.Id).Contains(createdItem.Id));
                    Assert.AreEqual(0, items.Count);
                }

                await repository.DeleteItemAsync(createdItem.Id, CancellationToken.None);
            }

            static List<(Dictionary<string, string> Filter, Dictionary<string, string> Tags, bool Match)> GetTestData() =>
            [
                (
                    new(StringComparer.Ordinal) { { "foo", "bar" } },
                    new(StringComparer.Ordinal) { { "foo", "bar" } },
                    true
                ),
                (
                    new(StringComparer.Ordinal) { { "foo", "bar" } },
                    new(StringComparer.Ordinal) { { "foo", "bat" } },
                    false
                ),
                (
                    new(StringComparer.Ordinal) { { "foo", string.Empty } },
                    new(StringComparer.Ordinal) { { "foo", string.Empty } },
                    true
                ),
                (
                    new(StringComparer.Ordinal) { { "foo", "bar" }, { "baz", "bat" } },
                    new(StringComparer.Ordinal) { { "foo", "bar" }, { "baz", "bat" } },
                    true
                ),
                (
                    new(StringComparer.Ordinal) { { "foo", "bar" } },
                    new(StringComparer.Ordinal) { { "foo", "bar" }, { "baz", "bat" } },
                    true
                ),
                (
                    new(StringComparer.Ordinal) { { "foo", "bar" }, { "baz", "bat" } },
                    new(StringComparer.Ordinal) { { "foo", "bar" } },
                    false
                ),
                (
                    new(StringComparer.Ordinal) { { "foo", string.Empty } },
                    new(StringComparer.Ordinal) { { "foo", "bar" } },
                    true
                ),
                (
                    new(StringComparer.Ordinal) { { "foo", string.Empty } },
                    new(StringComparer.Ordinal) { },
                    false
                ),
            ];
        }

        [TestMethod]
        public async Task GetItemsContinuationAsyncTest()
        {
            const int pageSize = 256;
            List<TesTask> itemsList = [];
            HashSet<string> tesTaskIds = [];

            var (continuation, items) = await repository.GetItemsAsync(null, pageSize, CancellationToken.None);
            CheckAndAddItems(items, tesTaskIds, itemsList);
            Assert.IsTrue(itemsList.Count <= pageSize);

            while (!string.IsNullOrWhiteSpace(continuation))
            {
                (continuation, items) = await repository.GetItemsAsync(continuation, pageSize, CancellationToken.None);
                CheckAndAddItems(items, tesTaskIds, itemsList);
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

            var task = await repository.CreateItemAsync(new()
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

            var createdItem = await repository.CreateItemAsync(new()
            {
                Id = id,
                Description = Guid.NewGuid().ToString(),
                CreationTime = DateTime.UtcNow,
                Inputs = [new TesInput { Url = "https://test" }]
            }, CancellationToken.None);

            Assert.IsTrue(createdItem.State != TesState.COMPLETE);

            createdItem.Description = description;
            createdItem.State = TesState.COMPLETE;

            await repository.UpdateItemAsync(createdItem, CancellationToken.None);

            TesTask updatedAndRetrievedItem = null;

            var isFound = await repository.TryGetItemAsync(id, CancellationToken.None, tesTask => updatedAndRetrievedItem = tesTask);

            Assert.IsTrue(isFound);
            Assert.IsTrue(updatedAndRetrievedItem.State == TesState.COMPLETE);
            Assert.IsTrue(updatedAndRetrievedItem.Description == description);
        }

        [TestMethod]
        public async Task DeleteItemAsyncTest()
        {
            var id = Guid.NewGuid().ToString();

            var createdItem = await repository.CreateItemAsync(new()
            {
                Id = id,
                Description = Guid.NewGuid().ToString(),
                CreationTime = DateTime.UtcNow,
                Inputs = [new TesInput { Url = "https://test" }]
            }, CancellationToken.None);
            Assert.IsNotNull(createdItem);
            await repository.DeleteItemAsync(id, CancellationToken.None);

            TesTask updatedAndRetrievedItem = null;

            var isFound = await repository.TryGetItemAsync(id, CancellationToken.None, tesTask => updatedAndRetrievedItem = tesTask);
            Assert.IsNull(updatedAndRetrievedItem);
            Assert.IsFalse(isFound);
        }


        private static void CheckAndAddItems(IEnumerable<TesTask> items, HashSet<string> tesTaskIds, List<TesTask> itemsList = default)
        {
            var countExisting = tesTaskIds.Count;

            foreach (var item in items)
            {
                if (tesTaskIds.Contains(item.Id))
                {
                    var count = tesTaskIds.Count;
                    Console.WriteLine($"Duplicate @{count} (in current page @{count - countExisting}): {item.Id}");
                    Debugger.Break();
                    Assert.Fail("Duplicate task id");
                    continue;
                }

                tesTaskIds.Add(item.Id);
                itemsList?.Add(item);
            }
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

            var azureSubscriptionClient = GetArmClient(subscriptionId).GetDefaultSubscription();

            if (await azureSubscriptionClient.GetResourceGroups().GetAllAsync()
                .AnyAsync(r => r.Id.Name.Equals(resourceGroupName, StringComparison.OrdinalIgnoreCase)))
            {
                return;
            }

            var rg = (await azureSubscriptionClient.GetResourceGroups()
                .CreateOrUpdateAsync(WaitUntil.Completed, resourceGroupName, new(new(regionName)))).Value;

            var server = (await rg.GetPostgreSqlFlexibleServers().CreateOrUpdateAsync(WaitUntil.Completed, postgreSqlServerName, new(new(regionName))
            {
                Version = new(postgreSqlVersion),
                Sku = new("Standard_B2s", PostgreSqlFlexibleServerSkuTier.Burstable),
                StorageSizeInGB = 128,
                AdministratorLogin = adminLogin,
                AdministratorLoginPassword = adminPw,
                //Network = new() { },
                HighAvailability = new() { Mode = PostgreSqlFlexibleServerHighAvailabilityMode.Disabled }
            })).Value;

            var database = (await server.GetPostgreSqlFlexibleServerDatabases().CreateOrUpdateAsync(WaitUntil.Completed, postgreSqlDatabaseName, new())).Value;

            //var postgresManagementClient = new FlexibleServer.PostgreSQLManagementClient(azureCredentials) { SubscriptionId = subscriptionId, LongRunningOperationRetryTimeout = 1200 };

            var startIp = "0.0.0.0";
            var endIp = "255.255.255.255";

            //Many networks have non-deterministic client IP addresses
            {
                using var client = new System.Net.Http.HttpClient();
                var ip = (await client.GetStringAsync("https://checkip.amazonaws.com")).Trim();
                startIp = ip;
                endIp = ip;
            }

            Assert.IsFalse((await server.GetPostgreSqlFlexibleServerFirewallRules()
                .CreateOrUpdateAsync(Azure.WaitUntil.Completed,
                    "AllowTestMachine",
                    new(System.Net.IPAddress.Parse(startIp), System.Net.IPAddress.Parse(endIp))))
                .GetRawResponse().IsError);
        }

        public static async Task DeleteResourceGroupAsync(string subscriptionId, string resourceGroupName)
        {
            var azureSubscriptionClient = GetArmClient(subscriptionId).GetDefaultSubscription();
            Assert.IsFalse((await azureSubscriptionClient.GetResourceGroups().Get(resourceGroupName, CancellationToken.None).Value
                .DeleteAsync(WaitUntil.Completed, cancellationToken: CancellationToken.None))
                .GetRawResponse().IsError);
        }

        private static ArmClient GetArmClient(string subscriptionId)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(subscriptionId);

            Azure.Identity.DefaultAzureCredentialOptions credentialOptions = new()
            {
                AuthorityHost = Azure.Identity.AzureAuthorityHosts.AzurePublicCloud,
                ExcludeManagedIdentityCredential = true,
                ExcludeWorkloadIdentityCredential = true,
            };

            TokenCredential credentials = new Azure.Identity.DefaultAzureCredential(credentialOptions);

            ArmClientOptions clientOptions = new()
            {
                Environment = ArmEnvironment.AzurePublicCloud,
            };
            clientOptions.Diagnostics.IsLoggingEnabled = true;
            clientOptions.Retry.Mode = RetryMode.Exponential;
            clientOptions.Retry.Delay = TimeSpan.FromSeconds(1);
            clientOptions.Retry.MaxDelay = TimeSpan.FromSeconds(30);
            clientOptions.Retry.MaxRetries = 10;
            return new ArmClient(credentials, subscriptionId, clientOptions);
        }
    }
}
