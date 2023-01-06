using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Tes.Utilities;

namespace Tes.Repository.Tests
{
    [TestClass]
    public class TesTaskPostgreSqlRepositoryTests
    {
        private readonly TesTaskPostgreSqlRepository repo;
        public TesTaskPostgreSqlRepositoryTests()
        {
            // TODO MOQ TesDbContext and optionally use real DB
            var connectionString = "";
            repo = new TesTaskPostgreSqlRepository(() => new TesDbContext(connectionString));
        }

        [TestMethod]
        public async Task TryGetItemAsyncTest()
        {
            var id = Guid.NewGuid().ToString();
            var createdItem = await CreateItemAsyncTest(id);
            Assert.IsNotNull(createdItem);

            Models.TesTask updatedAndRetrievedItem = null;

            var isFound = await repo.TryGetItemAsync(id, tesTask => updatedAndRetrievedItem = tesTask);

            Assert.IsNotNull(updatedAndRetrievedItem);
            Assert.IsTrue(isFound);
        }

        [TestMethod]
        public async Task GetItemsAsyncTest()
        {
            var items = await repo.GetItemsAsync(c => c.Id != null);

            foreach (var item in items)
            {
                Assert.IsTrue(!string.IsNullOrWhiteSpace(item.Id));
            }

            Assert.IsTrue(items.Count() > 0);
        }

        [Ignore]
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

                await repo.CreateItemsAsync(items);
                Console.WriteLine($"Total seconds to insert {items.Count} items: {sw.Elapsed.TotalSeconds:n2}s");
                sw.Restart();
            }

            var queriedItems = await repo.GetItemsAsync(c => c.State == Models.TesState.RUNNINGEnum);
            Console.WriteLine($"Total seconds to query 1m items: {sw.Elapsed.TotalSeconds:n2}s");
            Assert.IsTrue(queriedItems.Count() > 0);
            Assert.IsTrue(sw.Elapsed.TotalSeconds < 5);
        }

        [TestMethod]
        public async Task<Models.TesTask> CreateItemAsyncTest(string id = null)
        {
            var itemId = Guid.NewGuid().ToString();

            if (!string.IsNullOrWhiteSpace(id))
            {
                itemId = id;
            }

            var task = await repo.CreateItemAsync(new Models.TesTask { 
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
            await repo.UpdateItemAsync(createdItem);

            Models.TesTask updatedAndRetrievedItem = null;

            var isFound = await repo.TryGetItemAsync(id, tesTask => updatedAndRetrievedItem = tesTask);

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
            await repo.DeleteItemAsync(id);

            Models.TesTask updatedAndRetrievedItem = null;

            var isFound = await repo.TryGetItemAsync(id, tesTask => updatedAndRetrievedItem = tesTask);
            Assert.IsNull(updatedAndRetrievedItem);
            Assert.IsFalse(isFound);
        }
    }
}
