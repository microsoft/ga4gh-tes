using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Management.AppService.Fluent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Tes.Utilities;

namespace Tes.Repository.Tests
{
    /// <summary>
    /// To run these tests,
    /// 1.  Create a new Azure Database for PostgreSQL flexible server
    /// 2.  Create a new database called "tes_db"
    /// 3.  Add your client IP address to the Network -> Firewall allowed IP addresses section
    /// 4.  Set the server name to "myazureserver" etc, set the "user" to the admin username and password
    /// 5.  Remove the "[Ignore]" attribute from this class
    /// 6.  Run the tests
    /// </summary>
    [TestClass]
    public class TesTaskPostgreSqlRepositoryTests
    {
        private readonly TesTaskPostgreSqlRepository repository;

        public TesTaskPostgreSqlRepositoryTests()
        {
            var connectionString = new ConnectionStringUtility().GetPostgresConnectionString(
                postgreSqlServerName: "",
                postgreSqlTesDatabaseName: "tes_db",
                postgreSqlTesDatabasePort: "5432",
                postgreSqlTesUserLogin: "",
                postgreSqlTesUserPassword: "");

            repository = new TesTaskPostgreSqlRepository(() => new TesDbContext(connectionString));
        }

        [TestMethod]
        public async Task TryGetItemAsyncTest()
        {
            var id = Guid.NewGuid().ToString();
            var createdItem = await CreateItemAsyncTest(id);
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

            var sw = Stopwatch.StartNew();

            if (createItems)
            {
                var rng = new Random(Guid.NewGuid().GetHashCode());
                var states = Enum.GetValues(typeof(Models.TesState));

                var items = new List<Models.TesTask>();

                for (int i = 0; i < 1_000_000; i++)
                {
                    var randomState = (Models.TesState)states.GetValue(rng.Next(states.Length));
                    items.Add(new Models.TesTask
                    {
                        Id = Guid.NewGuid().ToString(),
                        CreationTime = DateTime.UtcNow,
                        State = randomState
                    });
                }

                await repository.CreateItemsAsync(items);
                Console.WriteLine($"Total seconds to insert {items.Count} items: {sw.Elapsed.TotalSeconds:n2}s");
                sw.Restart();
            }

            var queriedItems = await repository.GetItemsAsync(c => c.State == Models.TesState.RUNNINGEnum);
            Console.WriteLine($"Total seconds to query 1m items: {sw.Elapsed.TotalSeconds:n2}s");
            Assert.IsTrue(queriedItems.Count() > 0);

            // In manual testing, this takes about 5s the first time
            Assert.IsTrue(sw.Elapsed.TotalSeconds < 10);
        }

        [TestMethod]
        public async Task<Models.TesTask> CreateItemAsyncTest(string id = null)
        {
            var itemId = Guid.NewGuid().ToString();

            if (!string.IsNullOrWhiteSpace(id))
            {
                itemId = id;
            }

            var task = await repository.CreateItemAsync(new Models.TesTask { 
                Id = itemId,
                Description= string.Empty,
                CreationTime = DateTime.UtcNow,
                Inputs = new List<Models.TesInput> { new Models.TesInput { Url = "https://test" } }
            });

            Assert.IsNotNull(task);
            return task;
        }

        [TestMethod]
        public async Task UpdateItemAsyncTest()
        {
            string description = $"created at {DateTime.UtcNow}";
            var id = Guid.NewGuid().ToString();
            var createdItem = await CreateItemAsyncTest(id);

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
            var createdItem = await CreateItemAsyncTest(id);
            Assert.IsNotNull(createdItem);
            await repository.DeleteItemAsync(id);

            Models.TesTask updatedAndRetrievedItem = null;

            var isFound = await repository.TryGetItemAsync(id, tesTask => updatedAndRetrievedItem = tesTask);
            Assert.IsNull(updatedAndRetrievedItem);
            Assert.IsFalse(isFound);
        }
    }
}
