// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json;
using Moq;
using Tes.Models;

namespace Tes.SDK.Tests
{
    [TestClass]
    public class TesClientTests
    {
        private ITesClient _client = null!;
        private Mock<HttpClient> _httpClientMock = null!;

        [TestInitialize]
        public void Initialize()
        {
            _httpClientMock = new Mock<HttpClient>();
            _client = new TesClient(_httpClientMock.Object, new("https://example.com"));
        }

        [TestCleanup]
        public void Cleanup()
        {
            _client.Dispose();
        }

        [TestMethod]
        public async Task CreateTaskAsync_Success()
        {
            // Arrange
            var tesTask = CreateTestTask();
            TesCreateTaskResponse expectedResponse = new() { Id = tesTask.Id };

            _httpClientMock
                .Setup(client => client.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StringContent(JsonSerializer.Serialize(expectedResponse))
                });

            // Act
            var result = await _client.CreateTaskAsync(tesTask);

            // Assert
            Assert.AreEqual(expectedResponse.Id, result);
        }

        [TestMethod]
        public async Task GetTaskAsync_Success()
        {
            // Arrange
            var tesTask = CreateTestTask();

            _httpClientMock
                .Setup(client => client.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StringContent(JsonSerializer.Serialize(tesTask))
                });

            // Act
            var result = await _client.GetTaskAsync(tesTask.Id);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(tesTask.Id, result.Id);
        }

        private static TesTask CreateTestTask()
        {
            var task = new TesTask();
            task.Id = task.CreateId();
            task.Resources.Preemptible = true;
            task.Executors.Add(new()
            {
                Image = "ubuntu",
                Command = ["/bin/sh", "-c", "cat /proc/sys/kernel/random/uuid"],
            });

            return task;
        }
    }
}
