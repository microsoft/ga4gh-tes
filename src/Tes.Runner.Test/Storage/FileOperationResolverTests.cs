// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Moq;
using Tes.Runner.Models;
using Tes.Runner.Storage;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test.Storage
{
    [TestClass]
    [TestCategory("Unit")]
    public class FileOperationResolverTests
    {
        private ResolutionPolicyHandler resolutionPolicyHandler = null!;
        private Mock<IFileInfoProvider> fileInfoProvider = null!;
        private FileOutput singleFileOutput = null!;
        private FileOutput directoryFileOutput = null!;
        private FileOutput patternFileOutput = null!;

        private FileInput singleFileInput = null!;

        [TestInitialize]
        public void SetUp()
        {
            resolutionPolicyHandler = new ResolutionPolicyHandler();
            fileInfoProvider = new Mock<IFileInfoProvider>();

            fileInfoProvider.Setup(x => x.GetExpandedFileName(It.IsAny<string>())).Returns<string>(x => x);
            fileInfoProvider.Setup(x => x.FileExists(It.IsAny<string>())).Returns(true);

            singleFileInput = new FileInput
            {
                Path = "/foo/bar",
                SourceUrl = "https://foo.bar/cont/foo/bar?sig=sasToken",
                SasStrategy = SasResolutionStrategy.None,
            };

            singleFileOutput = new FileOutput
            {
                Path = "/foo/bar",
                TargetUrl = "https://foo.bar/cont/foo/bar?sig=sasToken",
                SasStrategy = SasResolutionStrategy.None,
                FileType = FileType.File
            };

            directoryFileOutput = new FileOutput
            {
                Path = "/root",
                TargetUrl = "https://foo.bar/cont?sig=sasToken",
                SasStrategy = SasResolutionStrategy.None,
                FileType = FileType.Directory
            };

            patternFileOutput = new FileOutput
            {
                Path = "/data/*.foo",
                TargetUrl = "https://foo.bar/cont?sig=sasToken",
                SasStrategy = SasResolutionStrategy.None,
                PathPrefix = "/prefix",
                FileType = FileType.File
            };
        }

        [TestMethod]
        public async Task ResolveOutputsAsync_FileOutputProvided_FileOperationIsResolved()
        {
            var nodeTask = new NodeTask()
            {
                Outputs = new List<FileOutput>
                {
                    singleFileOutput
                }
            };

            var fileOperationInfoResolver = new FileOperationResolver(nodeTask, resolutionPolicyHandler, fileInfoProvider.Object);
            var resolvedOutputs = await fileOperationInfoResolver.ResolveOutputsAsync();

            Assert.AreEqual(1, resolvedOutputs?.Count);
            Assert.IsTrue(resolvedOutputs?.Any(r => r.FullFilePath.Equals(singleFileOutput.Path, StringComparison.InvariantCultureIgnoreCase)));
            Assert.IsTrue(resolvedOutputs?.Any(r => r.TargetUri.ToString().Equals(singleFileOutput.TargetUrl, StringComparison.InvariantCultureIgnoreCase)));
        }

        [TestMethod]
        public async Task ResolveOutputsAsync_PatternOutputProvided_FileOperationsAreResolved()
        {
            var nodeTask = new NodeTask()
            {
                Outputs = new List<FileOutput>
                {
                    patternFileOutput
                }
            };

            fileInfoProvider.Setup(x => x.GetFilesBySearchPattern(It.IsAny<string>(), It.IsAny<string>()))
                .Returns(new List<string> { "/prefix/data/foo.foo", "/prefix/data/bar.foo" }.ToArray);

            var fileOperationInfoResolver = new FileOperationResolver(nodeTask, resolutionPolicyHandler, fileInfoProvider.Object);
            var resolvedOutputs = await fileOperationInfoResolver.ResolveOutputsAsync();

            Assert.AreEqual(2, resolvedOutputs?.Count);
            Assert.IsTrue(resolvedOutputs!.Any(r => r.FullFilePath.Equals("/prefix/data/foo.foo", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.FullFilePath.Equals("/prefix/data/bar.foo", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.TargetUri.ToString().Equals(@"https://foo.bar/cont/data/foo.foo?sig=sasToken", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.TargetUri.ToString().Equals(@"https://foo.bar/cont/data/bar.foo?sig=sasToken", StringComparison.OrdinalIgnoreCase)));
        }

        [TestMethod]
        public async Task ResolveOutputsAsync_DirectoryOutputProvided_TargetUrlDoesNotContainRootDirInPathProperty()
        {
            var nodeTask = new NodeTask()
            {
                Outputs = new List<FileOutput>
                {
                    directoryFileOutput
                }
            };
            var rootDir = directoryFileOutput.Path;
            var file1 = "/dir1/file1.tmp";
            var file2 = "/dir1/dir2/file2.tmp";

            fileInfoProvider.Setup(x => x.GetAllFilesInDirectory(It.IsAny<string>()))
                .Returns(new List<string> { $"{rootDir}{file1}", $"{rootDir}{file2}" }.ToArray);

            var fileOperationInfoResolver = new FileOperationResolver(nodeTask, resolutionPolicyHandler, fileInfoProvider.Object);
            var resolvedOutputs = await fileOperationInfoResolver.ResolveOutputsAsync();


            Assert.AreEqual(2, resolvedOutputs?.Count);
            Assert.IsTrue(resolvedOutputs!.Any(r => r.FullFilePath.Equals($"{rootDir}{file1}", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.FullFilePath.Equals($"{rootDir}{file2}", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.TargetUri.AbsoluteUri.ToString().Equals($@"https://foo.bar/cont{file1}?sig=sasToken", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.TargetUri.AbsoluteUri.ToString().Equals($@"https://foo.bar/cont{file2}?sig=sasToken", StringComparison.OrdinalIgnoreCase)));
        }

        [TestMethod]
        public async Task ResolveOutputsAsync_PatternAndFileOutputProvided_FileOperationsAreResolved()
        {
            var nodeTask = new NodeTask()
            {
                Outputs = new List<FileOutput>
                {
                    patternFileOutput,
                    singleFileOutput
                }
            };

            fileInfoProvider.Setup(x => x.GetFilesBySearchPattern(It.IsAny<string>(), It.IsAny<string>()))
                .Returns(new List<string> { "/prefix/data/foo.foo", "/prefix/data/bar.foo" }.ToArray);

            var fileOperationInfoResolver = new FileOperationResolver(nodeTask, resolutionPolicyHandler, fileInfoProvider.Object);
            var resolvedOutputs = await fileOperationInfoResolver.ResolveOutputsAsync();

            Assert.AreEqual(3, resolvedOutputs?.Count);
            Assert.IsTrue(resolvedOutputs!.Any(r => r.FullFilePath.Equals("/prefix/data/foo.foo", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.FullFilePath.Equals("/prefix/data/bar.foo", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.FullFilePath.Equals("/prefix/data/bar.foo", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.TargetUri.ToString().Equals(@"https://foo.bar/cont/data/foo.foo?sig=sasToken", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.TargetUri.ToString().Equals(@"https://foo.bar/cont/data/bar.foo?sig=sasToken", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs?.Any(r => r.FullFilePath.Equals(singleFileOutput.Path, StringComparison.InvariantCultureIgnoreCase)));
            Assert.IsTrue(resolvedOutputs?.Any(r => r.TargetUri.ToString().Equals(singleFileOutput.TargetUrl, StringComparison.InvariantCultureIgnoreCase)));
        }

        [TestMethod]
        public async Task ResolveInputsAsync_FileInputProvided_FileOperationsAreResolved()
        {
            var nodeTask = new NodeTask()
            {
                Inputs = new List<FileInput>
                {
                    singleFileInput
                }
            };

            var fileOperationInfoResolver = new FileOperationResolver(nodeTask, resolutionPolicyHandler, fileInfoProvider.Object);

            var resolvedInputs = await fileOperationInfoResolver.ResolveInputsAsync();

            Assert.AreEqual(1, resolvedInputs?.Count);
            Assert.IsTrue(resolvedInputs?.Any(r => r.FullFilePath.Equals(singleFileInput.Path, StringComparison.InvariantCultureIgnoreCase)));
            Assert.IsTrue(resolvedInputs?.Any(r => r.SourceUrl.ToString().Equals(singleFileInput.SourceUrl, StringComparison.InvariantCultureIgnoreCase)));
        }

    }
}
