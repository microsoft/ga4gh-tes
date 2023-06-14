// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;

namespace TesApi.Web
{
    /// <summary>
    /// A client to interact with Docker Registry
    /// </summary>
    public interface ITesDockerClient
    {
        /// <summary>
        /// Checks if an image is public
        /// </summary>
        /// <param name="image"></param>
        /// <returns></returns>
        Task<bool> IsImagePublicAsync(string image);
    }

    /// inheritdocs
    public class TesDockerClient : ITesDockerClient
    {
        private const string defaultDockerRegistryHost = "index.docker.io";
        private readonly HttpClient httpClient = new();
        private readonly IMemoryCache cache;

        /// <summary>
        /// Requires the cache
        /// </summary>
        /// <param name="cache"></param>
        public TesDockerClient(IMemoryCache cache)
        {
            this.cache = cache;
        }

        /// <summary>
        /// TODO needs to add support to auth with Docker Hub
        /// </summary>
        /// <param name="image"></param>
        /// <returns></returns>
        public async Task<bool> IsImagePublicAsync(string image)
        {
            try
            {
                var cacheKey = $"{nameof(TesDockerClient)}-{image}";

                if (cache?.TryGetValue(cacheKey, out bool isImagePublic) == true)
                {
                    return isImagePublic;
                }

                var slashIndex = image.IndexOf('/');
                var dockerRegistryHost = defaultDockerRegistryHost;
                var imagePart = $"{image}";

                if (slashIndex > 0)
                {
                    dockerRegistryHost = image.Substring(0, slashIndex);
                    imagePart = image.Substring(slashIndex + 1);
                }

                var url = $"https://{dockerRegistryHost}/v2/{imagePart}/tags/list";
                var registryResponse = await httpClient.GetAsync(url);
                var content = await registryResponse.Content.ReadAsStringAsync();

                var isImagePublicResult = false;

                // If the name is garbage, the API still returns 200 OK but with HTML instead of JSON
                // Check if the response starts with JSON
                if (registryResponse.StatusCode == System.Net.HttpStatusCode.OK && content.StartsWith("{"))
                {
                    isImagePublicResult = true;
                }

                cache?.Set(cacheKey, isImagePublicResult);
                return isImagePublicResult;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}
