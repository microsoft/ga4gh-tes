// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CommonUtilities;
using Microsoft.Azure.Management.PostgreSQL;
using Microsoft.Azure.Management.PostgreSQL.FlexibleServers;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi.Models;
using Microsoft.Rest;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Npgsql;
using Tes.Models;
using Tes.Utilities;
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
    public class TesTaskPostgreSqlRepositoryTests
    {
        private static TesTaskPostgreSqlRepository repository;
        private static readonly string subscriptionId = "";
        private static readonly string regionName = "southcentralus";
        private static readonly string resourceGroupName = $"tes-test-{Guid.NewGuid().ToString().Substring(0, 8)}";
        private static readonly string postgreSqlServerName = $"tes{Guid.NewGuid().ToString().Substring(0, 8)}";
        private static readonly string postgreSqlDatabaseName = "tes_db";
        private static readonly string adminLogin = $"tes{Guid.NewGuid().ToString().Substring(0, 8)}";
        private static readonly string adminPw = PasswordGenerator.GeneratePassword();

        [ClassInitialize]
        public async static Task ClassInitializeAsync(TestContext context)
        {
            Console.WriteLine("Creating Azure Resource Group and PostgreSql Server...");
            await PostgreSqlTestUtility.CreateTestDbAsync(
                subscriptionId, regionName, resourceGroupName, postgreSqlServerName, postgreSqlDatabaseName, adminLogin, adminPw);

            var options = new PostgreSqlOptions
            {
                ServerName = postgreSqlServerName,
                DatabaseName = postgreSqlDatabaseName,
                DatabaseUserLogin = adminLogin,
                DatabaseUserPassword = adminPw
            };

            var optionsMock = new Mock<IOptions<PostgreSqlOptions>>();
            optionsMock.Setup(x => x.Value).Returns(options);
            var connectionString = new ConnectionStringUtility().GetPostgresConnectionString(optionsMock.Object);
            repository = new TesTaskPostgreSqlRepository(() => new TesDbContext(connectionString));
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
            var createdItem = await repository.CreateItemAsync(new Models.TesTask
            {
                Id = id,
                Description = Guid.NewGuid().ToString(),
                CreationTime = DateTime.UtcNow,
                Inputs = new List<Models.TesInput> { new Models.TesInput { Url = "https://test" } }
            });
            Assert.IsNotNull(createdItem);

            Models.TesTask updatedAndRetrievedItem = null;

            var isFound = await repository.TryGetItemAsync(id, tesTask => updatedAndRetrievedItem = tesTask);

            Assert.IsNotNull(updatedAndRetrievedItem);
            Assert.IsTrue(isFound);
        }

        [TestMethod]
        public async Task GetItemsAsyncTest()
        {
            var items = await repository.GetItemsAsync(c => c.Id != null);

            foreach (var item in items)
            {
                Assert.IsTrue(!string.IsNullOrWhiteSpace(item.Id));
            }

            Assert.IsTrue(items.Count() > 0);
        }

        [TestMethod]
        public async Task Create1mAndQuery1mAsync()
        {
            const bool createItems = true;

            if (createItems)
            {
                await Create1mItemsAsync();
            }

            var sw = Stopwatch.StartNew();
            var runningTasks = await repository.GetItemsAsync(c => c.State == Models.TesState.RUNNINGEnum);

            // Ensure performance is decent.  In manual testing on fast internet, this takes less than 5s typically
            Assert.IsTrue(sw.Elapsed.TotalSeconds < 10);
            Console.WriteLine($"Retrieved {runningTasks.Count()} in {sw.Elapsed.TotalSeconds:n1}s");
            sw.Restart();
            var allOtherTasks = await repository.GetItemsAsync(c => c.State != Models.TesState.RUNNINGEnum);
            Console.WriteLine($"Retrieved {allOtherTasks.Count()} in {sw.Elapsed.TotalSeconds:n1}s");
            Console.WriteLine($"Total running tasks: {runningTasks.Count()}");
            Console.WriteLine($"Total other tasks: {allOtherTasks.Count()}");

            Assert.IsTrue(runningTasks.Count() > 0);
            Assert.IsTrue(allOtherTasks.Count() > 0);
            Assert.IsTrue(runningTasks.Count() != allOtherTasks.Count());
            Assert.IsTrue(runningTasks.All(c => c.State == Models.TesState.RUNNINGEnum));
            Assert.IsTrue(allOtherTasks.All(c => c.State != Models.TesState.RUNNINGEnum));
        }

        private static async Task Create1mItemsAsync()
        {
            var sw = Stopwatch.StartNew();
            var rng = new Random(Guid.NewGuid().GetHashCode());
            var states = Enum.GetValues(typeof(Models.TesState));

            var items = new List<Models.TesTask>();

            for (int i = 0; i < 1_000_000; i++)
            {
                var randomState = (Models.TesState)states.GetValue(rng.Next(states.Length));
                items.Add(new Models.TesTask
                {
                    Id = Guid.NewGuid().ToString(),
                    Description = Guid.NewGuid().ToString(),
                    CreationTime = DateTime.UtcNow,
                    State = randomState
                });
            }

            await repository.CreateItemsAsync(items);
            Console.WriteLine($"Total seconds to insert {items.Count} items: {sw.Elapsed.TotalSeconds:n2}s");
            sw.Restart();
        }

        [TestMethod]
        public async Task OverloadedDbIsHandled()
        {
            await Create1mItemsAsync();
            var tasks = new List<Task>();
            long overloadedDbExceptionCount = 0;
            long exceptionCount = 0;

            for (int i = 0; i < 1000; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        var allOtherTasks = await repository.GetItemsAsync(c => c.State != Models.TesState.RUNNINGEnum);
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
                        var n2 = inexType.Name;
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
        public async Task CreateItemAsyncTest()
        {
            var itemId = Guid.NewGuid().ToString();

            var task = await repository.CreateItemAsync(new Models.TesTask
            {
                Id = itemId,
                Description = Guid.NewGuid().ToString(),
                CreationTime = DateTime.UtcNow,
                Inputs = new List<Models.TesInput> { new Models.TesInput { Url = "https://test" } }
            });

            Assert.IsNotNull(task);
        }

        [TestMethod]
        public async Task UpdateItemAsyncTest()
        {
            string description = $"created at {DateTime.UtcNow}";
            var id = Guid.NewGuid().ToString();

            var createdItem = await repository.CreateItemAsync(new Models.TesTask
            {
                Id = id,
                Description = Guid.NewGuid().ToString(),
                CreationTime = DateTime.UtcNow,
                Inputs = new List<Models.TesInput> { new Models.TesInput { Url = "https://test" } }
            });

            Assert.IsTrue(createdItem.State != Models.TesState.COMPLETEEnum);

            createdItem.Description = description;
            createdItem.State = Models.TesState.COMPLETEEnum;

            await repository.UpdateItemAsync(createdItem);

            Models.TesTask updatedAndRetrievedItem = null;

            var isFound = await repository.TryGetItemAsync(id, tesTask => updatedAndRetrievedItem = tesTask);

            Assert.IsTrue(isFound);
            Assert.IsTrue(updatedAndRetrievedItem.State == Models.TesState.COMPLETEEnum);
            Assert.IsTrue(updatedAndRetrievedItem.Description == description);
        }

        [TestMethod]
        public async Task DeleteItemAsyncTest()
        {
            var id = Guid.NewGuid().ToString();

            var createdItem = await repository.CreateItemAsync(new Models.TesTask
            {
                Id = id,
                Description = Guid.NewGuid().ToString(),
                CreationTime = DateTime.UtcNow,
                Inputs = new List<Models.TesInput> { new Models.TesInput { Url = "https://test" } }
            });
            Assert.IsNotNull(createdItem);
            await repository.DeleteItemAsync(id);

            Models.TesTask updatedAndRetrievedItem = null;

            var isFound = await repository.TryGetItemAsync(id, tesTask => updatedAndRetrievedItem = tesTask);
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

            var tokenCredentials = new TokenCredentials(new RefreshableAzureServiceTokenProvider("https://management.azure.com/"));
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
            var tokenCredentials = new TokenCredentials(new RefreshableAzureServiceTokenProvider("https://management.azure.com/"));
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
