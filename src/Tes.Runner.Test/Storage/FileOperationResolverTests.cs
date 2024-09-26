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
            resolutionPolicyHandler = new(new(), BlobPipelineOptions.DefaultApiVersion);

            singleFileInput = new()
            {
                Path = "/foo/bar",
                SourceUrl = "https://foo.bar/cont?sig=sasToken",
                TransformationStrategy = TransformationStrategy.None,
            };

            singleFileOutput = new()
            {
                Path = "/foo/bar",
                TargetUrl = "https://foo.bar/cont/bar?sig=sasToken",
                TransformationStrategy = TransformationStrategy.None,
                FileType = FileType.File
            };

            directoryFileOutput = new()
            {
                Path = "/foo/",
                TargetUrl = "https://foo.bar/cont?sig=sasToken",
                TransformationStrategy = TransformationStrategy.None,
                FileType = FileType.Directory
            };

            patternFileOutput = new()
            {
                Path = "/data/*",
                TargetUrl = "https://foo.bar/cont?sig=sasToken",
                TransformationStrategy = TransformationStrategy.None,
                FileType = FileType.File
            };

            fileInfoProvider = new();

            fileInfoProvider.Setup(x => x.GetExpandedFileName(It.IsAny<string>())).Returns<string>(f => f);
            fileInfoProvider.Setup(x => x.GetRootPathPair("/foo/bar"))
                .Returns(() => new("/", "foo/bar"));
            fileInfoProvider.Setup(x => x.GetRootPathPair("/data/*"))
                .Returns(() => new("/", "data/*"));
        }

        [TestMethod]
        public async Task ResolveOutputsAsync_FileOutputProvided_TargetUrlIsUsed()
        {
            NodeTask nodeTask = new() { Outputs = [singleFileOutput] };

            //the path in the setup is /foo/bar, and the target URL is https://foo.bar/cont/bar?sig=sasToken
            //therefore the expected output URL is the target URL:
            var expectedSingleFileOutputUrl = singleFileOutput.TargetUrl;


            fileInfoProvider.Setup(p =>
                    p.GetFilesBySearchPattern(It.IsAny<string>(), It.IsAny<string>()))
                     .Returns([new(singleFileOutput.Path!, "bar", "/")]);
            fileInfoProvider.Setup(x => x.FileExists(singleFileOutput.Path!)).Returns(true);  // single file output exists

            var fileOperationInfoResolver =
                new FileOperationResolver(nodeTask, resolutionPolicyHandler, fileInfoProvider.Object);
            var resolvedOutputs = await fileOperationInfoResolver.ResolveOutputsAsync();

            Assert.AreEqual(1, resolvedOutputs?.Count);
            Assert.IsTrue(resolvedOutputs?.Any(r =>
                r.FullFilePath.Equals(singleFileOutput.Path, StringComparison.InvariantCultureIgnoreCase)));
            Assert.IsTrue(resolvedOutputs?.Any(r =>
                r.TargetUri.ToString()
                    .Equals(expectedSingleFileOutputUrl, StringComparison.InvariantCultureIgnoreCase)));
        }

        [TestMethod]
        public async Task ResolveOutputsAsync_PatternOutputProvided_FileOperationsAreResolved()
        {
            var nodeTask = new NodeTask()
            {
                Outputs =
                [
                    patternFileOutput // path: /data/*, target URL: https://foo.bar/cont/data?sig=sasToken
                ]
            };

            fileInfoProvider.Setup(x => x.GetFilesBySearchPattern("/", "data/*"))
                .Returns([
                    new("/data/foo.foo", "foo.foo", "/"),  //result of pattern file output
                    new("/data/dir1/bar.foo", "dir1/bar.foo", "/")
                ]);

            var fileOperationInfoResolver =
                new FileOperationResolver(nodeTask, resolutionPolicyHandler, fileInfoProvider.Object);
            var resolvedOutputs = await fileOperationInfoResolver.ResolveOutputsAsync();

            Assert.AreEqual(2, resolvedOutputs?.Count);
            Assert.IsTrue(resolvedOutputs!.Any(r =>
                r.FullFilePath.Equals("/data/foo.foo", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r =>
                r.FullFilePath.Equals("/data/dir1/bar.foo", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r =>
                r.TargetUri.ToString().Equals(@"https://foo.bar/cont/foo.foo?sig=sasToken",
                    StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r =>
                r.TargetUri.ToString().Equals(@"https://foo.bar/cont/dir1/bar.foo?sig=sasToken",
                    StringComparison.OrdinalIgnoreCase)));
        }

        [TestMethod]
        public async Task ResolveOutputsAsync_DirectoryOutputProvided_TargetUrlDoesNotContainRootDirInPathProperty()
        {
            NodeTask nodeTask = new() { Outputs = [directoryFileOutput] };
            var rootDir = directoryFileOutput.Path;
            var file1 = "/dir1/file1.tmp";
            var file2 = "/dir1/dir2/file2.tmp";

            fileInfoProvider.Setup(x => x.GetAllFilesInDirectory(It.IsAny<string>()))
                .Returns(
                [
                    new($"{rootDir}{file1}", $"{file1.TrimStart('/')}", $"{rootDir}"),
                    new($"{rootDir}{file2}", $"{file2.TrimStart('/')}", $"{rootDir}")
                ]);

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
            NodeTask nodeTask = new()
            {
                Outputs =
                [
                    patternFileOutput,  // path: /data/*, target URL: https://foo.bar/cont/data?sig=sasToken
                    singleFileOutput    // path: /foo/bar, target URL: https://foo.bar/cont/bar?sig=sasToken
                ]
            };

            fileInfoProvider.Setup(x => x.GetFilesBySearchPattern("/", "data/*"))
                .Returns(
                [
                    new("/data/foo.foo", "foo.foo", "/"),  //result of pattern file output
                    new FileResult("/data/dir1/bar.foo", "dir1/bar.foo", "/")
                ]);
            fileInfoProvider.Setup(x => x.GetFilesBySearchPattern("/", "foo/bar"))
                .Returns(
                [
                    new("/foo/bar", "bar", "/") // result of the single file output
                ]);
            fileInfoProvider.Setup(x => x.FileExists(singleFileOutput.Path!)).Returns(true);  // single file output exists

            var fileOperationInfoResolver = new FileOperationResolver(nodeTask, resolutionPolicyHandler, fileInfoProvider.Object);
            var resolvedOutputs = await fileOperationInfoResolver.ResolveOutputsAsync();

            Assert.AreEqual(3, resolvedOutputs?.Count);
            Assert.IsTrue(resolvedOutputs!.Any(r => r.FullFilePath.Equals("/data/foo.foo", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.FullFilePath.Equals("/data/dir1/bar.foo", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.FullFilePath.Equals("/foo/bar", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.TargetUri.ToString().Equals(@"https://foo.bar/cont/foo.foo?sig=sasToken", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.TargetUri.ToString().Equals(@"https://foo.bar/cont/dir1/bar.foo?sig=sasToken", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.TargetUri.ToString().Equals(@"https://foo.bar/cont/bar?sig=sasToken", StringComparison.OrdinalIgnoreCase)));
        }

        [TestMethod]
        public async Task ResolveOutputsAsync_PatternAndFileOutputProvided_FileDoesntExist_FileOperationsAreResolved()
        {
            NodeTask nodeTask = new()
            {
                Outputs =
                [
                    patternFileOutput,  // path: /data/*, target URL: https://foo.bar/cont/data?sig=sasToken
                    singleFileOutput    // path: /foo/bar, target URL: https://foo.bar/cont/bar?sig=sasToken
                ]
            };

            fileInfoProvider.Setup(x => x.GetFilesBySearchPattern("/", "data/*"))
                .Returns(
                [
                    new("/data/foo.foo", "foo.foo", "/"),  //result of pattern file output
                    new("/data/dir1/bar.foo", "dir1/bar.foo", "/")
                ]);
            fileInfoProvider.Setup(x => x.GetFilesBySearchPattern("/", "foo/bar"))
                .Returns(
                [
                    new("/foo/bar", "bar", "/") // result of the single file output
                ]);
            fileInfoProvider.Setup(x => x.FileExists(singleFileOutput.Path!)).Returns(false);  // single file output does NOT exist

            var fileOperationInfoResolver = new FileOperationResolver(nodeTask, resolutionPolicyHandler, fileInfoProvider.Object);
            var resolvedOutputs = await fileOperationInfoResolver.ResolveOutputsAsync();

            Assert.AreEqual(2, resolvedOutputs?.Count);
            Assert.IsTrue(resolvedOutputs!.Any(r => r.FullFilePath.Equals("/data/foo.foo", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.FullFilePath.Equals("/data/dir1/bar.foo", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.TargetUri.ToString().Equals(@"https://foo.bar/cont/foo.foo?sig=sasToken", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.TargetUri.ToString().Equals(@"https://foo.bar/cont/dir1/bar.foo?sig=sasToken", StringComparison.OrdinalIgnoreCase)));
        }

        [TestMethod]
        public async Task ResolveInputsAsync_FileInputProvided_FileOperationsAreResolved()
        {
            NodeTask nodeTask = new()
            {
                Inputs =
                [
                    singleFileInput
                ]
            };

            var fileOperationInfoResolver = new FileOperationResolver(nodeTask, resolutionPolicyHandler, fileInfoProvider.Object);

            var resolvedInputs = await fileOperationInfoResolver.ResolveInputsAsync();

            Assert.AreEqual(1, resolvedInputs?.Count);
            Assert.IsTrue(resolvedInputs?.Any(r => r.FullFilePath.Equals(singleFileInput.Path, StringComparison.InvariantCultureIgnoreCase)));
            Assert.IsTrue(resolvedInputs?.Any(r => r.SourceUrl.ToString().Equals(singleFileInput.SourceUrl, StringComparison.InvariantCultureIgnoreCase)));
        }

    }
}
