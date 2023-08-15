﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Moq;
using Tes.ApiClients.Options;

namespace Tes.ApiClients.Tests;

[TestClass, TestCategory("Unit")]
public class CacheAndRetryHandlerTest
{
    private IMemoryCache appCache = null!;
    private CacheAndRetryHandler cacheAndRetryHandler = null!;
    private Mock<object> mockInstanceToRetry = null!;
    private const int MaxRetryCount = 3;

    [TestInitialize]
    public void SetUp()
    {
        var mockOptions = new Mock<IOptions<RetryPolicyOptions>>();
        appCache = new MemoryCache(new MemoryCacheOptions());
        mockInstanceToRetry = new Mock<object>();
        mockOptions.SetupGet(x => x.Value).Returns(new RetryPolicyOptions() { ExponentialBackOffExponent = 1, MaxRetryCount = MaxRetryCount });
        cacheAndRetryHandler = new(appCache, mockOptions.Object);
    }

    [TestCleanup]
    public void Cleanup()
    {
        appCache.Dispose();
    }

    [TestMethod]
    public async Task ExecuteWithRetryAsync_RetriesMaxTimes()
    {
        mockInstanceToRetry.Setup(o => o.ToString()).Throws<Exception>();

        await Assert.ThrowsExceptionAsync<Exception>(() => cacheAndRetryHandler.ExecuteWithRetryAsync(() => Task.Run(() => mockInstanceToRetry.Object.ToString())));
        mockInstanceToRetry.Verify(o => o.ToString(), Times.Exactly(MaxRetryCount + 1)); // 3 retries (MaxRetryCount), plus original call
    }

    [TestMethod]
    public async Task ExecuteWithRetryAsync_ReturnsValueAndOneExecutionOnSuccess()
    {
        mockInstanceToRetry.Setup(o => o.ToString()).Returns("foo");

        var value = await cacheAndRetryHandler.ExecuteWithRetryAsync(() => Task.Run(() => mockInstanceToRetry.Object.ToString()));
        mockInstanceToRetry.Verify(o => o.ToString(), Times.Once);
        Assert.AreEqual("foo", value);
    }

    [TestMethod]
    public async Task ExecuteWithRetryAndCachingAsync_ValueIsCachedOnSuccessMethodCalledOnce()
    {
        var cacheKey = Guid.NewGuid().ToString();
        mockInstanceToRetry.Setup(o => o.ToString()).Returns("foo");

        var first = await cacheAndRetryHandler.ExecuteWithRetryAndCachingAsync(cacheKey, () => Task.Run(() => mockInstanceToRetry.Object.ToString()));
        var second = await cacheAndRetryHandler.ExecuteWithRetryAndCachingAsync(cacheKey, () => Task.Run(() => mockInstanceToRetry.Object.ToString()));

        mockInstanceToRetry.Verify(o => o.ToString(), Times.Once);
        Assert.AreEqual("foo", first);
        Assert.AreEqual("foo", second);
        Assert.AreEqual("foo", appCache.Get<string>(cacheKey));
    }

    [TestMethod]
    public async Task ExecuteWithRetryAndCachingAsync_ValueIsNotCachedOnFailureAndThrows()
    {
        var cacheKey = Guid.NewGuid().ToString();
        mockInstanceToRetry.Setup(o => o.ToString()).Throws<Exception>();

        await Assert.ThrowsExceptionAsync<Exception>(() => cacheAndRetryHandler.ExecuteWithRetryAndCachingAsync(cacheKey, () => Task.Run(() => mockInstanceToRetry.Object.ToString())));

        Assert.IsFalse(appCache.TryGetValue(cacheKey, out string _));
    }

    [TestMethod]
    [DataRow(HttpStatusCode.OK, 1)]
    [DataRow(HttpStatusCode.Created, 1)]
    [DataRow(HttpStatusCode.Forbidden, 1)]
    [DataRow(HttpStatusCode.BadRequest, 1)]
    [DataRow(HttpStatusCode.TooManyRequests, MaxRetryCount + 1)]
    [DataRow(HttpStatusCode.ServiceUnavailable, MaxRetryCount + 1)]
    [DataRow(HttpStatusCode.InternalServerError, MaxRetryCount + 1)]
    public async Task ExecuteHttpRequestWithRetryAsync_RetriesOnlyOnExpectedFailureCodes(HttpStatusCode statusCode, int numberOfTimes)
    {
        var mockFactory = new Mock<ITestHttpResponseMessageFactory>();
        mockFactory.Setup(f => f.CreateResponseAsync()).Returns(CreateResponseAsync(statusCode));

        var response =
            await cacheAndRetryHandler.ExecuteHttpRequestWithRetryAsync(() =>
                mockFactory.Object.CreateResponseAsync());

        mockFactory.Verify(f => f.CreateResponseAsync(), Times.Exactly(numberOfTimes));
        Assert.AreEqual(response.StatusCode, statusCode);
    }

    [TestMethod]
    [DataRow(HttpStatusCode.OK)]
    [DataRow(HttpStatusCode.Created)]
    [DataRow(HttpStatusCode.Accepted)]
    [DataRow(HttpStatusCode.PartialContent)]
    [DataRow(HttpStatusCode.NoContent)]
    public async Task ExecuteHttpRequestWithRetryAndCachingAsync_CallOnceCachesOnSuccess(HttpStatusCode statusCode)
    {
        var cacheKey = Guid.NewGuid().ToString();
        var mockFactory = new Mock<ITestHttpResponseMessageFactory>();
        mockFactory.Setup(f => f.CreateResponseAsync()).Returns(CreateResponseAsync(statusCode));

        var first =
            await cacheAndRetryHandler.ExecuteHttpRequestWithRetryAndCachingAsync(cacheKey, () =>
                mockFactory.Object.CreateResponseAsync());

        var second =
            await cacheAndRetryHandler.ExecuteHttpRequestWithRetryAndCachingAsync(cacheKey, () =>
                mockFactory.Object.CreateResponseAsync());

        mockFactory.Verify(f => f.CreateResponseAsync(), Times.Once);
        Assert.AreEqual(first.StatusCode, statusCode);
        Assert.AreEqual(second.StatusCode, statusCode);
        Assert.IsTrue(appCache.TryGetValue(cacheKey, out HttpResponseMessage? cachedResponse));
        Assert.AreEqual(first.StatusCode, cachedResponse!.StatusCode);
    }

    [TestMethod]
    [DataRow(HttpStatusCode.Forbidden, 1)] //bad codes but not retriable
    [DataRow(HttpStatusCode.BadRequest, 1)]
    [DataRow(HttpStatusCode.NotFound, 1)]
    [DataRow(HttpStatusCode.Conflict, 1)]
    [DataRow(HttpStatusCode.BadGateway, MaxRetryCount + 1)] //retriable codes
    [DataRow(HttpStatusCode.TooManyRequests, MaxRetryCount + 1)]
    [DataRow(HttpStatusCode.ServiceUnavailable, MaxRetryCount + 1)]
    [DataRow(HttpStatusCode.InternalServerError, MaxRetryCount + 1)]
    public async Task ExecuteHttpRequestWithRetryAndCachingAsync_RetriesThrowsAndNotCachedOnFailure(HttpStatusCode statusCode, int numberOfTimes)
    {
        var cacheKey = Guid.NewGuid().ToString();
        var mockFactory = new Mock<ITestHttpResponseMessageFactory>();
        mockFactory.Setup(f => f.CreateResponseAsync()).Returns(CreateResponseAsync(statusCode));

        await Assert.ThrowsExceptionAsync<HttpRequestException>(() => cacheAndRetryHandler.ExecuteHttpRequestWithRetryAndCachingAsync(cacheKey, () => mockFactory.Object.CreateResponseAsync()));

        mockFactory.Verify(f => f.CreateResponseAsync(), Times.Exactly(numberOfTimes));
        Assert.IsFalse(appCache.TryGetValue(cacheKey, out HttpResponseMessage _));
    }

    private Task<HttpResponseMessage> CreateResponseAsync(HttpStatusCode statusCode)
        => Task.FromResult<HttpResponseMessage>(new(statusCode));

    public interface ITestHttpResponseMessageFactory
    {
        public Task<HttpResponseMessage> CreateResponseAsync();
    }
}
