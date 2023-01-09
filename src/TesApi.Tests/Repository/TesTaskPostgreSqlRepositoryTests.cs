using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
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

    [Ignore]
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
                        Description = Guid.NewGuid().ToString(),
                        CreationTime = DateTime.UtcNow,
                        State = randomState
                    });
                }

                await repository.CreateItemsAsync(items);
                Console.WriteLine($"Total seconds to insert {items.Count} items: {sw.Elapsed.TotalSeconds:n2}s");
                sw.Restart();
            }

            sw.Restart();
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
                Description = Guid.NewGuid().ToString(),
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
