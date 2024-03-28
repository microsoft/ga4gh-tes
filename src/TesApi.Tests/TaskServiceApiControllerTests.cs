// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
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
                Executors = [new() { Image = "ubuntu" }],
                Resources = new() { BackendParameters = backendParameters, BackendParametersStrict = true }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, System.Threading.CancellationToken.None) as ObjectResult;

            Assert.IsNotNull(result);
            services.TesTaskRepository.Verify(x => x.CreateItemAsync(tesTask, It.IsAny<System.Threading.CancellationToken>()));
            Assert.AreEqual(32, tesTask.Id.Length);
            Assert.AreEqual(TesState.QUEUEDEnum, tesTask.State);
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
                Executors = [new() { Image = "ubuntu" }],
                Resources = new() { BackendParameters = backendParameters, BackendParametersStrict = true }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, System.Threading.CancellationToken.None) as ObjectResult;

            Assert.IsNotNull(result);
            services.TesTaskRepository.Verify(x => x.CreateItemAsync(tesTask, It.IsAny<System.Threading.CancellationToken>()));
            Assert.AreEqual(32, tesTask.Id.Length);
            Assert.AreEqual(TesState.QUEUEDEnum, tesTask.State);
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
                Executors = [new() { Image = "ubuntu" }],
                Resources = new() { BackendParameters = backendParameters }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, System.Threading.CancellationToken.None) as ObjectResult;

            Assert.IsNotNull(result);
            services.TesTaskRepository.Verify(x => x.CreateItemAsync(tesTask, It.IsAny<System.Threading.CancellationToken>()));
            Assert.AreEqual(32, tesTask.Id.Length);
            Assert.AreEqual(TesState.QUEUEDEnum, tesTask.State);

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
                Executors = [new() { Image = "ubuntu" }],
                Resources = new() { BackendParameters = backendParameters, BackendParametersStrict = true }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, System.Threading.CancellationToken.None) as BadRequestObjectResult;

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
                Executors = [new() { Image = "ubuntu" }],
                Resources = new() { BackendParameters = backendParameters, BackendParametersStrict = true }
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, System.Threading.CancellationToken.None) as BadRequestObjectResult;

            Assert.IsNotNull(result);

            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_ReturnsBadRequest_ForInvalidId()
        {
            var tesTask = new TesTask { Id = "ClientProvidedId", Executors = new() { new() { Image = "ubuntu" } } };
            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, System.Threading.CancellationToken.None) as BadRequestObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_ReturnsBadRequest_ForMissingDockerImage()
        {
            var tesTask = new TesTask();
            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, System.Threading.CancellationToken.None) as ObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_ReturnsTesCreateTaskResponse()
        {
            var tesTask = new TesTask() { Executors = [new() { Image = "ubuntu" }] };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.CreateTaskAsync(tesTask, System.Threading.CancellationToken.None) as ObjectResult;

            Assert.IsNotNull(result);
            services.TesTaskRepository.Verify(x => x.CreateItemAsync(tesTask, It.IsAny<System.Threading.CancellationToken>()));
            Assert.AreEqual(32, tesTask.Id.Length);
            Assert.AreEqual(TesState.QUEUEDEnum, tesTask.State);
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestMethod]
        [DataRow("IdWith@InvalidCharacter$", 400, "Invalid ID")]
        [DataRow("abcde123_ca8e57a5746f4436b864808b0fbf0a64", 200, null)]
        [DataRow("ca8e57a5746f4436b864808b0fbf0a64", 200, null)]
        public async Task CancelTaskAsync_ValidatesIdCorrectly(string testId, int expectedStatusCode, string expectedMessage)
        {
            var mockTesTask = new TesTask { State = TesState.RUNNINGEnum };

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
            var result = await controller.CancelTask(testId, CancellationToken.None);

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
            var mockTesTask = new TesTask { State = TesState.RUNNINGEnum };
            var tesTaskId = mockTesTask.CreateId();

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
            {
                // Mock TryGetItemAsync to return true and provide a TesTask object
                r.Setup(repo => repo.TryGetItemAsync(tesTaskId, It.IsAny<CancellationToken>(), It.IsAny<Action<TesTask>>()))
                .Callback((string id, CancellationToken ct, Action<TesTask> action) => action(mockTesTask))
                .ReturnsAsync(true);

                // Mock UpdateItemAsync to throw a RepositoryCollisionException
                r.Setup(repo => repo.UpdateItemAsync(It.IsAny<TesTask>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new RepositoryCollisionException());
            });

            var controller = services.GetT();

            // Act
            var result = await controller.CancelTask(tesTaskId, CancellationToken.None) as ConflictObjectResult;

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(409, result.StatusCode);
        }

        [TestMethod]
        public async Task CancelTaskAsync_ReturnsEmptyObject()
        {
            var tesTask = new TesTask() { State = TesState.QUEUEDEnum };
            tesTask.Id = tesTask.CreateId();

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
                r.Setup(repo => repo.TryGetItemAsync(tesTask.Id, It.IsAny<System.Threading.CancellationToken>(), It.IsAny<Action<TesTask>>()))
                .Callback<string, System.Threading.CancellationToken, Action<TesTask>>((id, _1, action) => { action(tesTask); })
                .ReturnsAsync(true));
            var controller = services.GetT();

            var result = await controller.CancelTask(tesTask.Id, System.Threading.CancellationToken.None) as ObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(200, result.StatusCode);
            Assert.AreEqual(TesState.CANCELEDEnum, tesTask.State);
            services.TesTaskRepository.Verify(x => x.UpdateItemAsync(tesTask, It.IsAny<System.Threading.CancellationToken>()));
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
                r.Setup(repo => repo.TryGetItemAsync(tesTaskId, It.IsAny<System.Threading.CancellationToken>(), It.IsAny<Action<TesTask>>()))
                    .ReturnsAsync(false));
            var controller = services.GetT();

            var result = await controller.GetTaskAsync(tesTaskId, "MINIMAL", System.Threading.CancellationToken.None) as NotFoundObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(404, result.StatusCode);
        }

        [TestMethod]
        public async Task GetTaskAsync_ReturnsBadRequest_ForInvalidViewValue()
        {
            var tesTask = new TesTask();
            tesTask.Id = tesTask.CreateId();

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
                r.Setup(repo => repo.TryGetItemAsync(tesTask.Id, It.IsAny<System.Threading.CancellationToken>(), It.IsAny<Action<TesTask>>()))
                .Callback<string, System.Threading.CancellationToken, Action<TesTask>>((id, _1, action) =>
                {
                    action(tesTask);
                })
                .ReturnsAsync(true));
            var controller = services.GetT();

            var result = await controller.GetTaskAsync(tesTask.Id, "INVALID", System.Threading.CancellationToken.None) as BadRequestObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task GetTaskAsync_ReturnsJsonResult()
        {
            var tesTask = new TesTask
            {
                State = TesState.RUNNINGEnum
            };
            tesTask.Id = tesTask.CreateId();

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
                r.Setup(repo => repo.TryGetItemAsync(tesTask.Id, It.IsAny<System.Threading.CancellationToken>(), It.IsAny<Action<TesTask>>()))
                .Callback<string, System.Threading.CancellationToken, Action<TesTask>>((id, _1, action) =>
                {
                    action(tesTask);
                })
                .ReturnsAsync(true));
            var controller = services.GetT();

            var result = await controller.GetTaskAsync(tesTask.Id, "MINIMAL", System.Threading.CancellationToken.None) as JsonResult;

            Assert.IsNotNull(result);
            services.TesTaskRepository.Verify(x => x.TryGetItemAsync(tesTask.Id, It.IsAny<System.Threading.CancellationToken>(), It.IsAny<Action<TesTask>>()));
            Assert.AreEqual(TesState.RUNNINGEnum, tesTask.State);
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestMethod]
        public async Task ListTasks_ReturnsBadRequest_ForInvalidPageSize()
        {
            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            var result = await controller.ListTasks(null, 0, null, "BASIC", System.Threading.CancellationToken.None) as BadRequestObjectResult;

            Assert.IsNotNull(result);
            Assert.AreEqual(400, result.StatusCode);
        }

        [TestMethod]
        public async Task ListTasks_ReturnsJsonResult()
        {
            var firstTesTask = new TesTask { Id = "tesTaskId1", State = TesState.COMPLETEEnum, Name = "tesTask", ETag = Guid.NewGuid().ToString() };
            var secondTesTask = new TesTask { Id = "tesTaskId2", State = TesState.EXECUTORERROREnum, Name = "tesTask2", ETag = Guid.NewGuid().ToString() };
            var thirdTesTask = new TesTask { Id = "tesTaskId3", State = TesState.EXECUTORERROREnum, Name = "someOtherTask2", ETag = Guid.NewGuid().ToString() };
            var namePrefix = "tesTask";

            var tesTasks = new[] { firstTesTask, secondTesTask, thirdTesTask };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>(tesTaskRepository: r =>
                r.Setup(repo => repo
                .GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>(), It.IsAny<int>(), It.IsAny<string>(), It.IsAny<System.Threading.CancellationToken>()))
                .ReturnsAsync((Expression<Func<TesTask, bool>> predicate, int pageSize, string continuationToken, System.Threading.CancellationToken _1) =>
                    (string.Empty, tesTasks.Where(i => predicate.Compile().Invoke(i)).Take(pageSize))));
            var controller = services.GetT();

            var result = await controller.ListTasks(namePrefix, 1, null, "BASIC", System.Threading.CancellationToken.None) as JsonResult;
            var listOfTesTasks = (TesListTasksResponse)result.Value;

            Assert.IsNotNull(result);
            Assert.AreEqual(1, listOfTesTasks.Tasks.Count);
            Assert.AreEqual(200, result.StatusCode);
        }

        [TestMethod]
        public async Task CreateTaskAsync_InvalidInputsAndPathDoNotThrow()
        {
            var cromwellWorkflowId = "daf1a044-d741-4db9-8eb5-d6fd0519b1f1";
            var taskDescription = $"{cromwellWorkflowId}:BackendJobDescriptorKey_CommandCallNode_wf_hello.hello:-1:1";

            var tesTask1 = new TesTask()
            {
                Description = taskDescription,
                Executors = [new() { Image = "ubuntu" }]
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            await controller.CreateTaskAsync(tesTask1, System.Threading.CancellationToken.None);

            Assert.IsNull(tesTask1.WorkflowId);

            var tesTask2 = new TesTask()
            {
                Description = taskDescription,
                Executors = [new() { Image = "ubuntu" }],
                Inputs = [new() { Path = "/cromwell-executions/" }]
            };

            await controller.CreateTaskAsync(tesTask2, System.Threading.CancellationToken.None);

            Assert.IsNull(tesTask2.WorkflowId);

            var tesTask3 = new TesTask()
            {
                Description = taskDescription,
                Executors = [new() { Image = "ubuntu" }],
                Inputs = [new() { Path = "/cromwell-executions/" }]
            };

            await controller.CreateTaskAsync(tesTask3, System.Threading.CancellationToken.None);

            Assert.IsNull(tesTask3.WorkflowId);

            var tesTask4 = new TesTask()
            {
                Description = taskDescription,
                Executors = [new() { Image = "ubuntu" }],
                Inputs = [new() { Path = "/cromwell-executions/test/" }]
            };

            await controller.CreateTaskAsync(tesTask4, System.Threading.CancellationToken.None);

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
                Executors = [new() { Image = "ubuntu" }],
                Inputs = [new() { Type = TesFileType.FILEEnum, Description = "BackendJobDescriptorKey_CommandCallNode_wf_hello.hello.commandScript", Name = "commandScript", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/script" }],
                Outputs =
                [
                    new() { Name = "commandScript", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/script" },
                    new() { Name = "stderr", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/stderr" },
                    new() { Name = "stdout", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/stdout" },
                    new() { Name = "rc", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/rc" }
                ]
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            await controller.CreateTaskAsync(tesTask, System.Threading.CancellationToken.None);

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
                Executors = [new() { Image = "ubuntu" }],
                Inputs = [new() { Type = TesFileType.FILEEnum, Description = "BackendJobDescriptorKey_CommandCallNode_wf_hello.hello.commandScript", Name = "commandScript", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/script" }],
                Outputs =
                [
                    new() { Name = "commandScript", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/script" },
                    new() { Name = "stderr", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/stderr" },
                    new() { Name = "stdout", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/stdout" },
                    new() { Name = "rc", Path = $"/cromwell-executions/test/{cromwellWorkflowId}/call-hello/test-subworkflow/{cromwellSubWorkflowId}/call-subworkflow/shard-8/execution/rc" }
                ]
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            await controller.CreateTaskAsync(tesTask, System.Threading.CancellationToken.None);

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
                Executors = [new() { Image = "ubuntu" }],
                Inputs = [new() { Type = TesFileType.FILEEnum, Description = "BackendJobDescriptorKey_CommandCallNode_wf_hello.hello.commandScript", Name = "commandScript", Path = $"{path}/script" }],
                Outputs =
                [
                    new() { Name = "commandScript", Path = $"{path}/script" },
                    new() { Name = "stderr", Path = $"{path}/stderr" },
                    new() { Name = "stdout", Path = $"{path}/stdout" },
                    new() { Name = "rc", Path = $"{path}/rc", Url = $"{path}/rc" }
                ]
            };

            using var services = new TestServices.TestServiceProvider<TaskServiceApiController>();
            var controller = services.GetT();

            await controller.CreateTaskAsync(tesTask, System.Threading.CancellationToken.None);

            Assert.AreEqual(workflowid, tesTask.WorkflowId);
            Assert.AreEqual(taskName, tesTask.CromwellTaskInstanceName);
            Assert.AreEqual(shard, tesTask.CromwellShard);
            Assert.AreEqual(attempt, tesTask.CromwellAttempt);
        }
    }
}
