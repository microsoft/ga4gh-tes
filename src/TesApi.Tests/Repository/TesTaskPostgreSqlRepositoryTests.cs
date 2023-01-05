using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Tes.Repository.Tests
{
    [TestClass()]
    public class TesTaskPostgreSqlRepositoryTests
    {
        private const string connectionString = "";
        private readonly TesTaskPostgreSqlRepository repo = new TesTaskPostgreSqlRepository(() => new TesDbContext(connectionString));

        [Ignore]
        [TestMethod()]
        public void TesTaskPostgreSqlRepositoryTest()
        {
            throw new NotImplementedException();
        }

        [Ignore]
        [TestMethod()]
        public void TryGetItemAsyncTest()
        {
            throw new NotImplementedException();
        }

        [Ignore]
        [TestMethod()]
        public void GetItemsAsyncTest()
        {
            throw new NotImplementedException();
        }

        [Ignore]
        [TestMethod()]
        public async Task CreateItemAsyncTest()
        {
            var task = await repo.CreateItemAsync(new Models.TesTask { 
                Id = Guid.NewGuid().ToString(),
                Description= string.Empty,
                CreationTime= DateTime.Now,
                Inputs = new List<Models.TesInput> { new Models.TesInput { Url = "https://test" } }
            });

            Assert.IsNotNull(task);
        }

        [Ignore]
        [TestMethod()]
        public void UpdateItemAsyncTest()
        {
            throw new NotImplementedException();
        }

        [Ignore]
        [TestMethod()]
        public void DeleteItemAsyncTest()
        {
            throw new NotImplementedException();
        }
    }
}
