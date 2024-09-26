// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Tes.Models;
using Tes.TaskSubmitters;

namespace TesApi.Tests
{
    [TestClass]
    public class TesTaskTests
    {
        [TestMethod]
        public void CreateId_Generates_Valid_Id()
        {
            var tesTask = new TesTask
            {
                TaskSubmitter = new UnknownTaskSubmitter { WorkflowId = Guid.NewGuid().ToString() }
            };

            var id = tesTask.CreateId();

            // Check that the ID is valid according to the IsValidId method
            Assert.IsTrue(TesTask.IsValidId(id));

            // Check that the ID starts with the first 8 characters of WorkflowId
            Assert.IsTrue(id.StartsWith(tesTask.WorkflowId[..8]));
        }

        [TestMethod]
        public void CreateId_Generates_Valid_Id_Without_WorkflowId()
        {
            var tesTask = new TesTask();
            var id = tesTask.CreateId();

            // Check that the ID is valid according to the IsValidId method
            Assert.IsTrue(TesTask.IsValidId(id));
        }

        [TestMethod]
        public void IsValidId_Returns_False_For_InvalidIds()
        {
            string[] invalidIds = { "invalid-id", "123", "ABC!", "" };

            foreach (var id in invalidIds)
            {
                Assert.IsFalse(TesTask.IsValidId(id));
            }
        }

        [TestMethod]
        public void IsValidId_Returns_True_For_ValidIds()
        {
            string[] validIds = {
                "12345678123456781234567812345678", // 32 characters
                "12345678_12345678123456781234567812345678" // 41 characters
            };

            foreach (var id in validIds)
            {
                Assert.IsTrue(TesTask.IsValidId(id));
            }
        }
    }
}
