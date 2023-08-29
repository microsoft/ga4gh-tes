// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Moq;
using Tes.Runner.Docker;
using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test.Docker
{
    [TestClass, TestCategory("Unit")]
    public class VolumeBindingsGeneratorTests
    {
        private VolumeBindingsGenerator volumeBindingsGenerator = null!;
        private Mock<IFileInfoProvider> mockFileInfoProvider = null!;

        [TestInitialize]
        public void SetUp()
        {
            mockFileInfoProvider = new Mock<IFileInfoProvider>();
            mockFileInfoProvider.Setup(p => p.GetExpandedFileName(It.IsAny<string>())).Returns<string>(p => p);
            volumeBindingsGenerator = new VolumeBindingsGenerator();
        }

        [DataTestMethod]
        [DataRow("/wkd/input/file.bam", "/wkd", "/wkd/input:/input")]
        [DataRow("/wkd/dir1/input/file.bam", "/wkd/dir1", "/wkd/dir1/input:/input")]
        [DataRow("/wkd/input/file.bam", "/wkd/", "/wkd/input:/input")]
        public void GenerateVolumeBindings_SingleInputWithWorkingDir_SingleVolumeBinding(string path, string mountParent, string expected)
        {
            var input = new FileInput() { Path = path, MountParentDirectory = mountParent };

            var bindings = volumeBindingsGenerator.GenerateVolumeBindings(new List<FileInput>() { input }, outputs: default);

            Assert.AreEqual(expected, bindings.Single());
        }

        [DataTestMethod]
        [DataRow("/wkd/output/file.bam", "/wkd", "/wkd/output:/output")]
        [DataRow("/wkd/output/file.bam", "/wkd/", "/wkd/output:/output")]
        public void GenerateVolumeBindings_SingleOutputWithWorkingDir_SingleVolumeBinding(string path, string mountParent, string expected)
        {
            var output = new FileOutput() { Path = path, MountParentDirectory = mountParent };

            var bindings = volumeBindingsGenerator.GenerateVolumeBindings(inputs: default, new List<FileOutput>() { output });

            Assert.AreEqual(expected, bindings.Single());
        }

        [DataTestMethod]
        [DataRow("/wkd", "/wkd/output:/output", "/wkd/output/file.bam", "/wkd/output/file1.bam")]
        [DataRow("/wkd", "/wkd/output:/output", "/wkd/output/file.bam", "/wkd/output/dir1/file1.bam")]
        [DataRow("/wkd", "/wkd/output:/output", "/wkd/output/file.bam", "/wkd/output/dir1/file1.bam", "/wkd/output/dir2/file1.bam")]
        public void GenerateVolumeBindings_OutputsWithWorkingDir_SingleVolumeBinding(string mountParent, string expected, params string[] paths)
        {
            var outputs = paths.Select(p => new FileOutput() { Path = p, MountParentDirectory = mountParent }).ToList();

            var bindings = volumeBindingsGenerator.GenerateVolumeBindings(inputs: default, outputs);

            Assert.AreEqual(expected, bindings.Single());
        }
        [DataTestMethod]
        [DataRow("/wkd", "/wkd/output:/output", "/wkd/out:/out", "/wkd/output/file.bam", "/wkd/out/file1.bam")]
        [DataRow("/wkd", "/wkd/output:/output", "/wkd/out:/out", "/wkd/output/file.bam", "/wkd/out/dir1/file1.bam")]
        [DataRow("/wkd", "/wkd/output:/output", "/wkd/out:/out", "/wkd/out/dir1/file1.bam", "/wkd/output/dir2/file1.bam")]
        public void GenerateVolumeBindings_OutputsWitDifferentParentsAfterWd_TwoVolumeBinding(string mountParent, string expected1, string expected2, params string[] paths)
        {
            var outputs = paths.Select(p => new FileOutput() { Path = p, MountParentDirectory = mountParent }).ToList();

            var bindings = volumeBindingsGenerator.GenerateVolumeBindings(inputs: default, outputs);

            Assert.AreEqual(2, bindings.Count);
            Assert.IsTrue(bindings.Any(p => p.Equals(expected1, StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(bindings.Any(p => p.Equals(expected2, StringComparison.OrdinalIgnoreCase)));
        }

        [TestMethod]
        public void GenerateVolumeBindings_MultipleInputsAndOutputsWitDifferentParentsAfterWd_TwoVolumeBinding()
        {
            var mountParent = "/wkd";
            var paths = new string[] { "/wkd/outputs/f.bam", "/wkd/outputs/b.bam" };
            var outputs = paths.Select(p => new FileOutput() { Path = p, MountParentDirectory = mountParent }).ToList();

            paths = new string[] { "/wkd/inputs/f.bam", "/wkd/inputs/b.bam" };
            var inputs = paths.Select(p => new FileInput() { Path = p, MountParentDirectory = mountParent }).ToList();

            var bindings = volumeBindingsGenerator.GenerateVolumeBindings(inputs, outputs);

            Assert.AreEqual(2, bindings.Count);
            Assert.IsTrue(bindings.Any(p => p.Equals("/wkd/inputs:/inputs", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(bindings.Any(p => p.Equals("/wkd/outputs:/outputs", StringComparison.OrdinalIgnoreCase)));
        }
    }
}
