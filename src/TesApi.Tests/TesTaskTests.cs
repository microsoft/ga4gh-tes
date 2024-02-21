// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Tes.Models;

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
                WorkflowId = Guid.NewGuid().ToString()
            };

            string id = tesTask.CreateId();

            // Check that the ID is valid according to the IsValidId method
            Assert.IsTrue(TesTask.IsValidId(id));

            // Check that the ID starts with the first 8 characters of WorkflowId
            Assert.IsTrue(id.StartsWith(tesTask.WorkflowId.Substring(0, 8)));
        }

        [TestMethod]
        public void CreateId_Generates_Valid_Id_Without_WorkflowId()
        {
            var tesTask = new TesTask();
            string id = tesTask.CreateId();

            // Check that the ID is valid according to the IsValidId method
            Assert.IsTrue(TesTask.IsValidId(id));
        }

        [TestMethod]
        public void IsValidId_Returns_False_For_InvalidIds()
        {
            string[] invalidIds = { "invalid-id", "123", "ABC!", "" };

            foreach (string id in invalidIds)
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

            foreach (string id in validIds)
            {
                Assert.IsTrue(TesTask.IsValidId(id));
            }
        }
    }
}
