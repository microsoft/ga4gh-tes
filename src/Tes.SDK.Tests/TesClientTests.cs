// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Moq;
using Tes.Models;

namespace Tes.SDK.Tests
{
    [TestClass]
    public class TesClientTests
    {
        private ITesClient _client = null!;
        private Mock<HttpClient> _httpClientMock = null!;
        private static Array tesStateEnumValues = Enum.GetValues(typeof(TesState));

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
                    Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(expectedResponse))
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
                    Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(tesTask))
                });

            // Act
            var result = await _client.GetTaskAsync(tesTask.Id);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(tesTask.Id, result.Id);
        }

        [TestMethod]
        public async Task ListTasksAsync_Success()
        {
            var tasks = new List<TesTask>();

            for (int i = 0; i < 100; i++)
            {
                tasks.Add(CreateTestTask());
            }

            var runningTasksCount = tasks.Count(t => t.State == TesState.RUNNINGEnum);

            var response = new TesListTasksResponse { Tasks = tasks };

            _httpClientMock
                .Setup(client => client.SendAsync(It.IsAny<HttpRequestMessage>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(response))
                });

            var result = await _client.ListTasksAsync().ToListAsync();
            Assert.IsNotNull(result);
            Assert.IsTrue(result.Count == tasks.Count);
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

            task.State = (TesState)tesStateEnumValues.GetValue(Random.Shared.Next(tesStateEnumValues.Length));
            return task;
        }
    }
}
