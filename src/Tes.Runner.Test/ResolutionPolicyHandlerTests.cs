// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Moq;
using Tes.Runner.Models;


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
            resolutionPolicyHandler = new ResolutionPolicyHandler(new(runtimeOptions, Runner.Transfer.BlobPipelineOptions.DefaultApiVersion,
                new(() => new Mock<Runner.Storage.IUrlTransformationStrategy>().Object), new(() => new Mock<Runner.Storage.IUrlTransformationStrategy>().Object),
                new(() => new Mock<Runner.Storage.IUrlTransformationStrategy>().Object), new(() => new Mock<Runner.Storage.IUrlTransformationStrategy>().Object),
                new(() => new Mock<Runner.Storage.IUrlTransformationStrategy>().Object)), sinks => new Mock<Runner.Events.EventsPublisher>().Object);
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
            List<FileOutput>? testTaskOutputs =
            [
                new() { Path = "file", TargetUrl = "http://foo.bar", TransformationStrategy = TransformationStrategy.None },
                new() { Path = "file1", TargetUrl = "http://foo1.bar", TransformationStrategy = TransformationStrategy.None },
                new() { Path = "file2", TargetUrl = "http://foo2.bar", TransformationStrategy = TransformationStrategy.None }
            ];
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
            List<FileInput>? testTaskInputs = [];
            var result = await resolutionPolicyHandler.ApplyResolutionPolicyAsync(testTaskInputs);
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Count);
        }

        [TestMethod]
        public async Task ApplyResolutionPolicyAsync_WhenTestTaskInputsIsNotEmpty_ReturnsListWithSameCount()
        {
            List<FileInput>? testTaskInputs =
            [
                new FileInput() { Path = "file", SourceUrl = "http://foo.bar", TransformationStrategy = TransformationStrategy.None },
                new FileInput() { Path = "file1", SourceUrl = "http://foo1.bar", TransformationStrategy = TransformationStrategy.None },
                new FileInput() { Path = "file2", SourceUrl = "http://foo2.bar", TransformationStrategy = TransformationStrategy.None }
            ];
            var result = await resolutionPolicyHandler.ApplyResolutionPolicyAsync(testTaskInputs);
            Assert.IsNotNull(result);
            Assert.AreEqual(testTaskInputs.Count, result.Count);
        }
    }
}
