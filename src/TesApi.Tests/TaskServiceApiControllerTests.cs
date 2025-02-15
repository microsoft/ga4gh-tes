// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.Models;
using Tes.Repository;
using TesApi.Controllers;

namespace TesApi.Tests
{
    [TestClass]
    public class TaskServiceApiControllerTests
    {
        private void SetRepository(Mock<IRepository<TesTask>> mock)
        {
            mock.Setup(x => x.CreateItemAsync(It.IsAny<TesTask>(), It.IsAny<CancellationToken>())).Returns<TesTask, CancellationToken>((tesTask, _) => Task.FromResult(tesTask));
        }

        [TestCategory("TES 1.1")]
        [TestMethod]
        public async Task TES_Supports_BackendParameter_vmsize()
        {
            const string backend_parameter_key = "vm_size";

            var backendParameters = new Dictionary<string, string>
            {
                { backend_parameter_key, "VmSize1" }
            };

            var tesTask = new TesTask
            {
                Executors = [new() { Image = "ubuntu", Command = ["cmd"] }],
                Resources = new() { BackendParameters = backendParameters, BackendParametersStrict = true }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: SetRepository);
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, CancellationToken.None) as ObjectResult;

            Assert.IsNotNull(result);
            services.TesTaskRepository.Verify(x => x.CreateItemAsync(tesTask, It.IsAny<CancellationToken>()));
            Assert.AreEqual(32, tesTask.Id.Length);
            Assert.AreEqual(TesState.QUEUED, tesTask.State);
            Assert.IsTrue(tesTask.Resources.BackendParameters.ContainsKey(backend_parameter_key));
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestCategory("TES 1.1")]
        [TestMethod]
        public async Task TES_Supports_BackendParameter_workflow_execution_identity()
        {
            const string backend_parameter_key = "workflow_execution_identity";

            var backendParameters = new Dictionary<string, string>
            {
                { backend_parameter_key, "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/coa/providers/Microsoft.ManagedIdentity/userAssignedIdentities/coa-test-uami" }
            };

            var tesTask = new TesTask
            {
                Executors = [new() { Image = "ubuntu", Command = ["cmd"] }],
                Resources = new() { BackendParameters = backendParameters, BackendParametersStrict = true }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: SetRepository);
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, CancellationToken.None) as ObjectResult;

            Assert.IsNotNull(result);
            services.TesTaskRepository.Verify(x => x.CreateItemAsync(tesTask, It.IsAny<CancellationToken>()));
            Assert.AreEqual(32, tesTask.Id.Length);
            Assert.AreEqual(TesState.QUEUED, tesTask.State);
            Assert.IsTrue(tesTask.Resources.BackendParameters.ContainsKey(backend_parameter_key));
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestCategory("TES 1.1")]
        [TestMethod]
        public async Task CreateTaskAsync_ReturnsTesCreateTaskResponseWithBackendParameters_UnsupportedKey()
        {
            const string unsupportedKey = "unsupported_key_2021";

            var backendParameters = new Dictionary<string, string>
            {
                { unsupportedKey, Guid.NewGuid().ToString() }
            };

            var tesTask = new TesTask
            {
                Executors = [new() { Image = "ubuntu", Command = ["cmd"] }],
                Resources = new() { BackendParameters = backendParameters }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: SetRepository);
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, CancellationToken.None) as ObjectResult;

            Assert.IsNotNull(result);
            services.TesTaskRepository.Verify(x => x.CreateItemAsync(tesTask, It.IsAny<CancellationToken>()));
            Assert.AreEqual(32, tesTask.Id.Length);
            Assert.AreEqual(TesState.QUEUED, tesTask.State);

            // Unsupported keys should not be persisted
            Assert.IsFalse(tesTask?.Resources?.BackendParameters?.ContainsKey(unsupportedKey));
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestCategory("TES 1.1")]
        [TestMethod]
        public async Task CreateTaskAsync_ReturnsBadRequest_ForBackendParametersStrict_UnsupportedKey()
        {
            const string unsupportedKey = "unsupported_key_2021";

            var backendParameters = new Dictionary<string, string>
            {
                { unsupportedKey, Guid.NewGuid().ToString() }
            };

            var tesTask = new TesTask
            {
                Executors = [new() { Image = "ubuntu", Command = ["cmd"] }],
                Resources = new() { BackendParameters = backendParameters, BackendParametersStrict = true }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: SetRepository);
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, CancellationToken.None) as BadRequestObjectResult;

            Assert.IsNotNull(result);

            // Unsupported keys should cause a bad request when BackendParametersStrict = true
            Assert.AreEqual(400, result.StatusCode);

            // Unsupported keys should be returned in the warning message
            Assert.IsTrue(result.Value.ToString().Contains(unsupportedKey));
        }

        [TestCategory("TES 1.1")]
        [TestMethod]
        public async Task CreateTaskAsync_ReturnsBadRequest_ForBackendParametersStrict_DuplicateKeys()
        {
            const string backend_parameter_key = "vmsize";

            var backendParameters = new Dictionary<string, string>
            {
                { backend_parameter_key, Guid.NewGuid().ToString() },
                { "VmSize", Guid.NewGuid().ToString() }
            };

            var tesTask = new TesTask
            {
                Executors = [new() { Image = "ubuntu", Command = ["cmd"] }],
                Resources = new() { BackendParameters = backendParameters, BackendParametersStrict = true }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: SetRepository);
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, CancellationToken.None) as BadRequestObjectResult;

            Assert.IsNotNull(result);

            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_ReturnsBadRequest_ForInvalidId()
        {
            var tesTask = new TesTask { Id = "ClientProvidedId", Executors = [new() { Image = "ubuntu", Command = ["cmd"] }] };
            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: SetRepository);
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, CancellationToken.None) as BadRequestObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_ReturnsBadRequest_ForMissingDockerImage()
        {
            TesTask tesTask = new() { Executors = [new()] };
            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: SetRepository);
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, CancellationToken.None) as ObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_ReturnsBadRequest_ForRelativeInputPath()
        {
            TesTask tesTask = new() { Inputs = [new() { Path = "xyz/path" }] };
            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: SetRepository);
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, CancellationToken.None) as ObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_ReturnsBadRequest_ForInputMissingContentAndPath()
        {
            TesTask tesTask = new() { Inputs = [new() { Url = "http://host/path" }] };
            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: SetRepository);
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, CancellationToken.None) as ObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_ReturnsBadRequest_ForInputContentAndPath()
        {
            TesTask tesTask = new() { Inputs = [new() { Url = "http://host/path", Path = "/path/file", Content = "content" }] };
            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: SetRepository);
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, CancellationToken.None) as ObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_ReturnsTesCreateTaskResponse()
        {
            var tesTask = new TesTask() { Executors = [new() { Image = "ubuntu", Command = ["cmd"] }] };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: SetRepository);
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, CancellationToken.None) as ObjectResult;

            Assert.IsNotNull(result);
            services.TesTaskRepository.Verify(x => x.CreateItemAsync(tesTask, It.IsAny<CancellationToken>()));
            Assert.AreEqual(32, tesTask.Id.Length);
            Assert.AreEqual(TesState.QUEUED, tesTask.State);
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestMethod]
        [DataRow("IdWith@InvalidCharacter$", 400, "Invalid ID")]
        [DataRow("abcde123_ca8e57a5746f4436b864808b0fbf0a64", 200, null)]
        [DataRow("ca8e57a5746f4436b864808b0fbf0a64", 200, null)]
        public async Task CancelTaskAsync_ValidatesIdCorrectly(string testId, int expectedStatusCode, string expectedMessage)
        {
            var mockTesTask = new TesTask { State = TesState.RUNNING };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
            {
                r.Setup(repo => repo.TryGetItemAsync(It.IsAny<string>(), It.IsAny<CancellationToken>(), It.IsAny<Action<TesTask>>()))
                .Callback((string id, CancellationToken ct, Action<TesTask> action) => action(mockTesTask))
                .ReturnsAsync(true);

                // Mock UpdateItemAsync to throw a RepositoryCollisionException
                r.Setup(repo => repo.UpdateItemAsync(It.IsAny<TesTask>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTesTask);
            });

            var controller = services.GetT();

            // Act
            var result = await controller.CancelTaskAsync(testId, CancellationToken.None);

            // Assert
            if (result is ObjectResult objectResult)
            {
                Assert.AreEqual(expectedStatusCode, objectResult.StatusCode);

                if (expectedMessage != null)
                {
                    Assert.AreEqual(expectedMessage, objectResult.Value);
                }
            }
            else
            {
                Assert.Fail("The action result is not of type ObjectResult");
            }
        }

        [TestMethod]
        public async Task CancelTaskAsync_ReturnsConflict_ForRepositoryCollision()
        {
            var mockTesTask = new TesTask { State = TesState.RUNNING };
            var tesTaskId = mockTesTask.CreateId();

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
            {
                // Mock TryGetItemAsync to return true and provide a TesTask object
                r.Setup(repo => repo.TryGetItemAsync(tesTaskId, It.IsAny<CancellationToken>(), It.IsAny<Action<TesTask>>()))
                    .Callback((string id, CancellationToken ct, Action<TesTask> action) => action(mockTesTask))
                    .ReturnsAsync(true);

                // Mock UpdateItemAsync to throw a RepositoryCollisionException
                r.Setup(repo => repo.UpdateItemAsync(It.IsAny<TesTask>(), It.IsAny<CancellationToken>()))
                    .ThrowsAsync(new RepositoryCollisionException<TesTask>(default));
            });

            var controller = services.GetT();

            // Act
            var result = await controller.CancelTaskAsync(tesTaskId, CancellationToken.None) as ConflictObjectResult;

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(409, result.StatusCode);
        }

        [TestMethod]
        public async Task CancelTaskAsync_ReturnsEmptyObject()
        {
            var tesTask = new TesTask() { State = TesState.QUEUED };
            tesTask.Id = tesTask.CreateId();

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
                r.Setup(repo => repo.TryGetItemAsync(tesTask.Id, It.IsAny<CancellationToken>(), It.IsAny<Action<TesTask>>()))
                .Callback<string, CancellationToken, Action<TesTask>>((id, _1, action) => { action(tesTask); })
                .ReturnsAsync(true));
            var controller = services.GetT();

            var result = await controller.CancelTaskAsync(tesTask.Id, CancellationToken.None) as ObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(200, result.StatusCode);
            Assert.AreEqual(TesState.CANCELING, tesTask.State);
            services.TesTaskRepository.Verify(x => x.UpdateItemAsync(tesTask, It.IsAny<CancellationToken>()));
        }

        [TestMethod]
        public void GetServiceInfo_ReturnsInfo()
        {
            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = controller.GetServiceInfo() as ObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestMethod]
        public async Task GetTaskAsync_ReturnsNotFound_ForValidId()
        {
            var tesTaskId = new TesTask().CreateId();

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
                r.Setup(repo => repo.TryGetItemAsync(tesTaskId, It.IsAny<CancellationToken>(), It.IsAny<Action<TesTask>>()))
                    .ReturnsAsync(false));
            var controller = services.GetT();

            var result = await controller.GetTaskAsync(tesTaskId, "MINIMAL", CancellationToken.None) as NotFoundObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(404, result.StatusCode);
        }

        [TestMethod]
        public async Task GetTaskAsync_ReturnsBadRequest_ForInvalidViewValue()
        {
            var tesTask = new TesTask();
            tesTask.Id = tesTask.CreateId();

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
                r.Setup(repo => repo.TryGetItemAsync(tesTask.Id, It.IsAny<CancellationToken>(), It.IsAny<Action<TesTask>>()))
                .Callback<string, CancellationToken, Action<TesTask>>((id, _1, action) =>
                {
                    action(tesTask);
                })
                .ReturnsAsync(true));
            var controller = services.GetT();

            var result = await controller.GetTaskAsync(tesTask.Id, "INVALID", CancellationToken.None) as BadRequestObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task GetTaskAsync_ReturnsJsonResult()
        {
            var tesTask = new TesTask
            {
                State = TesState.RUNNING
            };
            tesTask.Id = tesTask.CreateId();

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
                r.Setup(repo => repo.TryGetItemAsync(tesTask.Id, It.IsAny<CancellationToken>(), It.IsAny<Action<TesTask>>()))
                .Callback<string, CancellationToken, Action<TesTask>>((id, _1, action) =>
                {
                    action(tesTask);
                })
                .ReturnsAsync(true));
            var controller = services.GetT();

            var result = await controller.GetTaskAsync(tesTask.Id, "MINIMAL", CancellationToken.None) as JsonResult;

            Assert.IsNotNull(result);
            services.TesTaskRepository.Verify(x => x.TryGetItemAsync(tesTask.Id, It.IsAny<CancellationToken>(), It.IsAny<Action<TesTask>>()));
            Assert.AreEqual(TesState.RUNNING, tesTask.State);
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestMethod]
        public async Task ListTasks_ReturnsBadRequest_ForInvalidPageSize()
        {
            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.ListTasksAsync(null, null, [], [], 0, null, "BASIC", CancellationToken.None) as BadRequestObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task ListTasks_ReturnsJsonResult()
        {
            var firstTesTask = new TesTask { Id = "tesTaskId1", State = TesState.COMPLETE, Name = "tesTask", ETag = Guid.NewGuid().ToString() };
            var secondTesTask = new TesTask { Id = "tesTaskId2", State = TesState.EXECUTOR_ERROR, Name = "tesTask2", ETag = Guid.NewGuid().ToString() };
            var thirdTesTask = new TesTask { Id = "tesTaskId3", State = TesState.EXECUTOR_ERROR, Name = "someOtherTask2", ETag = Guid.NewGuid().ToString() };
            var namePrefix = "tesTask";

            var tesTasks = new[] { firstTesTask, secondTesTask, thirdTesTask };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
                r.Setup(repo => repo
                    // string continuationToken, int pageSize, CancellationToken cancellationToken, FormattableString predicate, Expression<Func<T, bool>> predicate
                    .GetItemsAsync(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<CancellationToken>(), It.IsAny<FormattableString>(), It.IsAny<IEnumerable<Expression<Func<TesTask, bool>>>>()))
                .ReturnsAsync((string _1, int pageSize, CancellationToken _2, FormattableString _3, IEnumerable<Expression<Func<TesTask, bool>>> predicates) =>
                    new("continuation-token=1", tesTasks.Where(i => predicates.All(p => p.Compile().Invoke(i))).Take(pageSize))));
            var controller = services.GetT();

            var result = await controller.ListTasksAsync(namePrefix, null, [], [], 1, null, "BASIC", CancellationToken.None) as JsonResult;
            var listOfTesTasks = (TesListTasksResponse)result.Value;

            Assert.IsNotNull(result);
            Assert.AreEqual(1, listOfTesTasks.Tasks.Count);
            Assert.AreEqual(200, result.StatusCode);
        }

        private static readonly System.Collections.ObjectModel.ReadOnlyDictionary<string, IEnumerable<(bool IsKey, string Value)>> TagQueries = new Dictionary<string, IEnumerable<(bool IsKey, string Value)>>(StringComparer.OrdinalIgnoreCase)
        {
            { "empty", [] },
            { "singleKey",
            [
                (true, "key"),
            ] },
            { "singleValue",
            [
                (false, "value"),
            ] },
            { "singlePairInOrder",
            [
                (true, "key"),
                (false, "value"),
            ] },
            { "singlePairOutOfOrder",
            [
                (false, "key"),
                (true, "value"),
            ] },
            { "noValueInMiddle",
            [
                (true, "key1"),
                (false, "value1"),
                (true, "key2"),
                (true, "key3"),
                (false, "value3"),
            ] },
            { "groupedByKind",
            [
                (true, "key1"),
                (true, "key2"),
                (true, "key3"),
                (false, "value1"),
                (false, "value2"),
            ] },
            { "noKeyInMiddle",
            [
                (true, "key1"),
                (false, "value1"),
                (false, "value2"),
                (true, "key3"),
                (false, "value3"),
            ] },
            { "repeatedTag",
            [
                (true, "key1"),
                (false, "value1"),
                (true, "key2"),
                (false, "value2"),
                (true, "key1"),
            ] },
            { "groupedRepeatedTag",
            [
                (false, "value1"),
                (false, "value2"),
                (true, "key1"),
                (true, "key2"),
                (true, "key1"),
            ] },
        }.AsReadOnly();

        [DataTestMethod]
        [DataRow("empty", HttpStatusCode.OK, DisplayName = "No tags")]
        [DataRow("singleKey", HttpStatusCode.OK, DisplayName = "Single key")]
        [DataRow("singleValue", HttpStatusCode.BadRequest, DisplayName = "Single value")]
        [DataRow("singlePairInOrder", HttpStatusCode.OK, DisplayName = "Single pair in order")]
        [DataRow("singlePairOutOfOrder", HttpStatusCode.OK, DisplayName = "Single pair out of order")]
        [DataRow("noValueInMiddle", HttpStatusCode.OK, DisplayName = "No value in middle")]
        [DataRow("groupedByKind", HttpStatusCode.OK, DisplayName = "Grouped by kind")]
        [DataRow("noKeyInMiddle", HttpStatusCode.BadRequest, DisplayName = "No key in middle")]
        [DataRow("repeatedTag", HttpStatusCode.BadRequest, DisplayName = "Repeated tag")]
        [DataRow("groupedRepeatedTag", HttpStatusCode.BadRequest, DisplayName = "Grouped by kind repeated tag")]
        public async Task ListTasks_ReturnsBadRequest_ForInvalidTagArguments(string query, HttpStatusCode statusCode)
        {
            using var services = SetupTagArgumentsTest();
            var controller = services.GetT();

            var result = await ListTasksWithTagArgumentsAsync(controller, query);

            switch (result)
            {
                case JsonResult jsonResult:
                    Assert.AreEqual((int)statusCode, jsonResult.StatusCode);
                    break;

                case BadRequestObjectResult badRequest:
                    Assert.AreEqual((int)statusCode, badRequest.StatusCode);
                    break;

                default:
                    Assert.Fail();
                    break;
            }
        }

        [DataTestMethod]
#pragma warning disable CA1861 // Avoid constant arrays as arguments
        [DataRow("empty", null, null, DisplayName = "No tags")]
        [DataRow("singleKey", "Tags: ? {0}", new[] { "key" }, DisplayName = "Single key")]
        [DataRow("singlePairInOrder", "Tags:->>{0} = {1}", new[] { "key", "value" }, DisplayName = "Single pair in order")]
        [DataRow("singlePairOutOfOrder", "Tags:->>{0} = {1}", new[] { "value", "key" }, DisplayName = "Single pair out of order")]
        [DataRow("noValueInMiddle", "Tags:->>{0} = {1} AND Tags: ? {2} AND Tags:->>{3} = {4}", new[] { "key1", "value1", "key2", "key3", "value3" }, DisplayName = "No value in middle")]
        [DataRow("groupedByKind", "Tags:->>{0} = {1} AND Tags:->>{2} = {3} AND Tags: ? {4}", new[] { "key1", "value1", "key2", "value2", "key3" }, DisplayName = "Grouped by kind")]
#pragma warning restore CA1861 // Avoid constant arrays as arguments
        public async Task ListTasks_CreatesCorrectQueryCorrectPairs_ForTagArguments(string query, string format, object[] args)
        {
            using var services = SetupTagArgumentsTest();
            var controller = services.GetT();

            var querystring = format is null ? null : FormattableStringFactory.Create(format, args);

            var result = await ListTasksWithTagArgumentsAsync(controller, query) as JsonResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(200, result.StatusCode);
            Assert.AreEqual(querystring, services.TesTaskRepository.Invocations.FirstOrDefault(i => i.Method.Name == nameof(IRepository<TesTask>.GetItemsAsync)).Arguments.Skip(3).Cast<FormattableString>().First(), new FormattableStringEqualityComparer());
            //services.TesTaskRepository.Verify(x => x.GetItemsAsync(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<CancellationToken>(), querystring, It.IsAny<IEnumerable<Expression<Func<TesTask, bool>>>>()));
        }

        private static TestServices.TestServiceProvider<TaskServiceApiController> SetupTagArgumentsTest()
            => new(tesTaskRepository: r =>
            {
                r.Setup(repo => repo
                    // string continuationToken, int pageSize, CancellationToken cancellationToken, FormattableString predicate, Expression<Func<T, bool>> predicate
                    .GetItemsAsync(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<CancellationToken>(), It.IsAny<FormattableString>(), It.IsAny<IEnumerable<Expression<Func<TesTask, bool>>>>()))
                .ReturnsAsync((string _1, int pageSize, CancellationToken _2, FormattableString _3, IEnumerable<Expression<Func<TesTask, bool>>> predicates) =>
                    new(null, []));
                r.Setup(repo => repo
                    .JsonFormattableRawString(It.IsAny<string>(), It.IsAny<FormattableString>()))
                .Returns((string property, FormattableString sql) => new Tes.Repository.Utilities.PrependableFormattableString($"{property}:", sql));
            });

        private static Task<IActionResult> ListTasksWithTagArgumentsAsync(TaskServiceApiController controller, string query)
        {
            controller.ControllerContext.HttpContext = new DebugHttpContext();
            controller.Request.QueryString = QueryString.Create("view", "BASIC");
            controller.Request.QueryString += QueryString.Create(TagQueries[query].Select(e => new KeyValuePair<string, string>(e.IsKey ? "tag_key" : "tag_value", e.Value)));

            return controller.ListTasksAsync(null, null, TagQueries[query].Where(e => e.IsKey).Select(e => e.Value).ToArray(), TagQueries[query].Where(e => !e.IsKey).Select(e => e.Value).ToArray(), null, null, "BASIC", CancellationToken.None);
        }

        private struct FormattableStringEqualityComparer : IEqualityComparer<FormattableString>
        {
            readonly bool IEqualityComparer<FormattableString>.Equals(FormattableString x, FormattableString y)
            {
                if (x is null && y is null)
                {
                    return true;
                }

                if (x is null || y is null)
                {
                    return false;
                }

                return x.Format.Equals(y.Format, StringComparison.Ordinal)
                    && x.ArgumentCount == y.ArgumentCount
                    && x.GetArguments().SequenceEqual(y.GetArguments());
            }

            readonly int IEqualityComparer<FormattableString>.GetHashCode(FormattableString obj)
                => obj.GetHashCode();
        }

        private class DebugHttpRequest : HttpRequest
        {
            public override HttpContext HttpContext => throw new NotImplementedException();

            public override string Method { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public override string Scheme { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public override bool IsHttps { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public override HostString Host { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public override PathString PathBase { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public override PathString Path { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public override QueryString QueryString { get; set; }
            public override IQueryCollection Query { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public override string Protocol { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

            public override IHeaderDictionary Headers => throw new NotImplementedException();

            public override IRequestCookieCollection Cookies { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public override long? ContentLength { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public override string ContentType { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public override Stream Body { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

            public override bool HasFormContentType => throw new NotImplementedException();

            public override IFormCollection Form { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

            public override Task<IFormCollection> ReadFormAsync(CancellationToken cancellationToken = default)
            {
                throw new NotImplementedException();
            }
        }

        private class DebugHttpContext : HttpContext
        {
            public override Microsoft.AspNetCore.Http.Features.IFeatureCollection Features => throw new NotImplementedException();

            public override HttpRequest Request { get; } = new DebugHttpRequest();

            public override HttpResponse Response => throw new NotImplementedException();

            public override ConnectionInfo Connection => throw new NotImplementedException();

            public override WebSocketManager WebSockets => throw new NotImplementedException();

            public override System.Security.Claims.ClaimsPrincipal User { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public override IDictionary<object, object> Items { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public override IServiceProvider RequestServices { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public override CancellationToken RequestAborted { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public override string TraceIdentifier { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public override ISession Session { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

            public override void Abort()
            {
                throw new NotImplementedException();
            }
        }

        [TestMethod]
        public async Task CreateTaskAsync_InvalidInputsAndPathDoNotThrow()
        {
            var cromwellWorkflowId = "daf1a044-d741-4db9-8eb5-d6fd0519b1f1";
            var taskDescription = $"{cromwellWorkflowId}:BackendJobDescriptorKey_CommandCallNode_wf_hello.hello:-1:1";

            var tesTask1 = new TesTask()
            {
                Description = taskDescription,
                Executors = [new() { Image = "ubuntu", Command = ["cmd"] }]
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: SetRepository);
            var controller = services.GetT();

            await controller.CreateTaskAsync(tesTask1, CancellationToken.None);

            Assert.IsNull(tesTask1.WorkflowId);

            var tesTask2 = new TesTask()
            {
                Description = taskDescription,
                Executors = [new() { Image = "ubuntu", Command = ["cmd"] }],
                Inputs = [new() { Path = "/cromwell-executions/" }]
            };

            await controller.CreateTaskAsync(tesTask2, CancellationToken.None);

            Assert.IsNull(tesTask2.WorkflowId);

            var tesTask3 = new TesTask()
            {
                Description = taskDescription,
                Executors = [new() { Image = "ubuntu", Command = ["cmd"] }],
                Inputs = [new() { Path = "/cromwell-executions/" }]
            };

            await controller.CreateTaskAsync(tesTask3, CancellationToken.None);

            Assert.IsNull(tesTask3.WorkflowId);

            var tesTask4 = new TesTask()
            {
                Description = taskDescription,
                Executors = [new() { Image = "ubuntu", Command = ["cmd"] }],
                Inputs = [new() { Path = "/cromwell-executions/test/" }]
            };

            await controller.CreateTaskAsync(tesTask4, CancellationToken.None);

            Assert.IsNull(tesTask4.WorkflowId);
        }

        // TODO: create similar tests for other submitters
        [TestMethod]
        public async Task CreateTaskAsync_ExtractsCromwellWorkflowId()
        {
            var cromwellWorkflowId = Guid.NewGuid().ToString();
            var cromwellSubWorkflowId = Guid.NewGuid().ToString();
            var taskDescription = $"{cromwellSubWorkflowId}:BackendJobDescriptorKey_CommandCallNode_wf_hello.hello:-1:1";

            var tesTask = new TesTask()
            {
                Description = taskDescription,
                Executors = [new() { Image = "ubuntu", Command = ["cmd"] }],
                Inputs = [new() { Type = TesFileType.FILE, Description = "BackendJobDescriptorKey_CommandCallNode_wf_hello.hello.commandScript", Name = "commandScript", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/script", Content = "command" }],
                Outputs =
                [
                    new() { Name = "commandScript", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/script" },
                    new() { Name = "stderr", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/stderr" },
                    new() { Name = "stdout", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/stdout" },
                    new() { Name = "rc", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/rc" }
                ]
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: SetRepository);
            var controller = services.GetT();

            await controller.CreateTaskAsync(tesTask, CancellationToken.None);

            Assert.AreEqual(cromwellWorkflowId, tesTask.WorkflowId);
        }

        [TestMethod]
        public async Task CreateTaskAsync_CromwellWorkflowIdIsUsedAsTaskIdPrefix()
        {
            var cromwellWorkflowId = Guid.NewGuid().ToString();
            var cromwellSubWorkflowId = Guid.NewGuid().ToString();
            var taskDescription = $"{cromwellSubWorkflowId}:BackendJobDescriptorKey_CommandCallNode_wf_hello.hello:-1:1";

            var tesTask = new TesTask()
            {
                Description = taskDescription,
                Executors = [new() { Image = "ubuntu", Command = ["cmd"] }],
                Inputs = [new() { Type = TesFileType.FILE, Description = "BackendJobDescriptorKey_CommandCallNode_wf_hello.hello.commandScript", Name = "commandScript", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/script", Content = "command" }],
                Outputs =
                [
                    new() { Name = "commandScript", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/script" },
                    new() { Name = "stderr", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/stderr" },
                    new() { Name = "stdout", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/stdout" },
                    new() { Name = "rc", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/rc" }
                ]
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: SetRepository);
            var controller = services.GetT();

            await controller.CreateTaskAsync(tesTask, CancellationToken.None);

            Assert.AreEqual(41, tesTask.Id.Length); // First eight characters of Cromwell's job id + underscore + GUID without dashes
            Assert.IsTrue(tesTask.Id.StartsWith(cromwellWorkflowId[..8] + "_"));
        }

        [DataTestMethod]
        [DataRow("0fbdb535-4afd-45e3-a8a8-c8e50585ee4e:BackendJobDescriptorKey_CommandCallNode_workflow1.Task1:-1:1", "/cromwell-executions/test/0fbdb535-4afd-45e3-a8a8-c8e50585ee4e/call-hello/execution", "0fbdb535-4afd-45e3-a8a8-c8e50585ee4e", "0fbdb535-4afd-45e3-a8a8-c8e50585ee4e:BackendJobDescriptorKey_CommandCallNode_workflow1.Task1", -1, 1)]
        [DataRow("0fbdb535-4afd-45e3-a8a8-c8e50585ee4e:BackendJobDescriptorKey_CommandCallNode_workflow1.Task1:8:1", "/cromwell-executions/test/0fbdb535-4afd-45e3-a8a8-c8e50585ee4e/call-hello/shard-8/execution", "0fbdb535-4afd-45e3-a8a8-c8e50585ee4e", "0fbdb535-4afd-45e3-a8a8-c8e50585ee4e:BackendJobDescriptorKey_CommandCallNode_workflow1.Task1", 8, 1)]
        [DataRow("b16af660-32b7-4aac-a812-3c22409fb385:BackendJobDescriptorKey_CommandCallNode_workflow1.Task1:8:1", "/cromwell-executions/test/0fbdb535-4afd-45e3-a8a8-c8e50585ee4e/call-hello/test-subworkflow/b16af660-32b7-4aac-a812-3c22409fb385/call-subworkflow/shard-8/execution", "0fbdb535-4afd-45e3-a8a8-c8e50585ee4e", "b16af660-32b7-4aac-a812-3c22409fb385:BackendJobDescriptorKey_CommandCallNode_workflow1.Task1", 8, 1)]
        public async Task CreateTaskAsync_CromwellMetadataForTriggerServiceIsGenerated(string taskDescription, string path, string workflowid, string taskName, int? shard, int? attempt)
        {
            var tesTask = new TesTask()
            {
                Description = taskDescription,
                Executors = [new() { Image = "ubuntu", Command = ["cmd"] }],
                Inputs = [new() { Type = TesFileType.FILE, Description = "BackendJobDescriptorKey_CommandCallNode_wf_hello.hello.commandScript", Name = "commandScript", Path = $"{path}/script", Url = $"{path}/script" }],
                Outputs =
                [
                    new() { Name = "commandScript", Path = $"{path}/script" },
                    new() { Name = "stderr", Path = $"{path}/stderr" },
                    new() { Name = "stdout", Path = $"{path}/stdout" },
                    new() { Name = "rc", Path = $"{path}/rc", Url = $"{path}/rc" }
                ]
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: SetRepository);
            var controller = services.GetT();

            await controller.CreateTaskAsync(tesTask, CancellationToken.None);

            Assert.AreEqual(workflowid, tesTask.WorkflowId);
            Assert.AreEqual(taskName, tesTask.CromwellTaskInstanceName);
            Assert.AreEqual(shard, tesTask.CromwellShard);
            Assert.AreEqual(attempt, tesTask.CromwellAttempt);
        }
    }
}
