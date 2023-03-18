// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Tes.Models;
using Tes.Repository;

namespace TesApi.Tests
{
    [TestClass]
    public class ConcurrentDictionaryCacheTests
    {
        private ConcurrentDictionaryCache<TesTask> cache = new ConcurrentDictionaryCache<TesTask>();

        [TestMethod]
        public void CacheNeverUsesMoreThanMaxSizeTest()
        {
            for (var i = 0; i < cache.MaxSize * 1.1; i++)
            {
                var task = new TesTask
                {
                    Id = Guid.NewGuid().ToString(),
                    Description = Guid.NewGuid().ToString(),
                    CreationTime = DateTime.UtcNow,
                    Inputs = new List<TesInput> { new TesInput { Url = "https://test" } }
                };

                cache.TryAdd(task.Id, task);

                Assert.IsTrue(cache.Count() <= cache.MaxSize);
            }

            Assert.IsTrue(cache.Count() == cache.MaxSize);
        }
    }
}
