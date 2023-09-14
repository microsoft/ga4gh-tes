// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Models;
using Tes.Runner.Storage;


namespace Tes.Runner.Test
{
    [TestClass]
    public class ResolutionPolicyHandlerTests
    {
        private ResolutionPolicyHandler resolutionPolicyHandler = null!;
        private RuntimeOptions runtimeOptions = null!;

        [TestInitialize]
        public void SetUp()
        {
            runtimeOptions = new RuntimeOptions();
            resolutionPolicyHandler = new ResolutionPolicyHandler(runtimeOptions);
        }

        [TestMethod]
        public async Task ApplyResolutionPolicyAsync_WhenTestTaskOutputsIsNull_ReturnsNull()
        {
            List<FileOutput>? testTaskOutputs = null;
            var result = await resolutionPolicyHandler.ApplyResolutionPolicyAsync(testTaskOutputs);
            Assert.IsNull(result);
        }

        [TestMethod]
        public async Task ApplyResolutionPolicyAsync_WhenTestTaskOutputsIsEmpty_ReturnsEmptyList()
        {
            var testTaskOutputs = new List<FileOutput>();
            var result = await resolutionPolicyHandler.ApplyResolutionPolicyAsync(testTaskOutputs);
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public async Task ApplyResolutionPolicyAsync_WhenTestTaskOutputsIsNotEmpty_ReturnsListWithSameCount()
        {
            var testTaskOutputs = new List<FileOutput>
            {
                new FileOutput(){Path = "file", TargetUrl = "http://foo.bar", TransformationStrategy = TransformationStrategy.None},
                new FileOutput(){Path = "file1", TargetUrl = "http://foo1.bar", TransformationStrategy = TransformationStrategy.None},
                new FileOutput(){Path = "file2", TargetUrl = "http://foo2.bar", TransformationStrategy = TransformationStrategy.None}
            };
            var result = await resolutionPolicyHandler.ApplyResolutionPolicyAsync(testTaskOutputs);
            Assert.IsNotNull(result);
            Assert.AreEqual(testTaskOutputs.Count, result.Count);
        }
        [TestMethod]
        public async Task ApplyResolutionPolicyAsync_WhenTestTaskInputsIsNull_ReturnsNull()
        {
            List<FileInput>? testTaskInputs = null;
            var result = await resolutionPolicyHandler.ApplyResolutionPolicyAsync(testTaskInputs);
            Assert.IsNull(result);
        }

        [TestMethod]
        public async Task ApplyResolutionPolicyAsync_WhenTestTaskInputsIsEmpty_ReturnsEmptyList()
        {
            var testTaskInputs = new List<FileInput>();
            var result = await resolutionPolicyHandler.ApplyResolutionPolicyAsync(testTaskInputs);
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public async Task ApplyResolutionPolicyAsync_WhenTestTaskInputsIsNotEmpty_ReturnsListWithSameCount()
        {
            var testTaskInputs = new List<FileInput>
            {
                new FileInput(){Path = "file", SourceUrl = "http://foo.bar", SasStrategy = TransformationStrategy.None},
                new FileInput(){Path = "file1", SourceUrl = "http://foo1.bar", SasStrategy = TransformationStrategy.None},
                new FileInput(){Path = "file2", SourceUrl = "http://foo2.bar", SasStrategy = TransformationStrategy.None}
            };
            var result = await resolutionPolicyHandler.ApplyResolutionPolicyAsync(testTaskInputs);
            Assert.IsNotNull(result);
            Assert.AreEqual(testTaskInputs.Count, result.Count);
        }

    }
}
