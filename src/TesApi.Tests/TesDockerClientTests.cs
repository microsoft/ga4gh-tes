// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TesApi.Web;

namespace TesApi.Tests
{
    [Ignore]
    [TestClass]
    public class TesDockerClientTests
    {
        private readonly IMemoryCache appCache;
        private readonly ITesDockerClient dockerClient;

        public TesDockerClientTests()
        {
            this.appCache = new MemoryCache(new MemoryCacheOptions());
            this.dockerClient = new TesDockerClient(appCache);
        }

        /// <summary>
        /// Currently fails because Docker authentication is required
        /// </summary>
        /// <returns></returns>
        [Ignore]
        [TestMethod]
        public async Task TestIfImagesArePublicAsync()
        {
            var imageTruthTableValues = new List<(string, bool)> {
                ("mcr.microsoft.com/ga4gh/tes", true),
                ("ubuntu", true),
                ("docker", true),
                ("klwwereljwelkrw", false),
                ("mcr.microsoft.com/lksdlwerew/sdfkjwerwer", false),
                ("sdjfksdklflksd.microsoft.com/klajsdfsd", false),
                (Guid.NewGuid().ToString(), false),
            };

            foreach (var imageTruthTableValue in imageTruthTableValues)
            {
                var isImagePublic = await dockerClient.IsImagePublicAsync(imageTruthTableValue.Item1);
                Assert.AreEqual(isImagePublic, imageTruthTableValue.Item2);
                var cacheKey = $"{nameof(TesDockerClient)}-{imageTruthTableValue.Item1}";
                Assert.IsTrue(appCache.TryGetValue(cacheKey, out bool isImagePublicCacheValue));
                Assert.AreEqual(isImagePublic, isImagePublicCacheValue);
            }
        }

    }
}
