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
            resolutionPolicyHandler = new ResolutionPolicyHandler(new RuntimeOptions());

            singleFileInput = new FileInput
            {
                Path = "/foo/bar",
                SourceUrl = "https://foo.bar/cont?sig=sasToken",
                SasStrategy = TransformationStrategy.None,
            };

            singleFileOutput = new FileOutput
            {
                Path = "/foo/bar",
                TargetUrl = "https://foo.bar/cont/outputs?sig=sasToken",
                TransformationStrategy = TransformationStrategy.None,
                FileType = FileType.File
            };

            directoryFileOutput = new FileOutput
            {
                Path = "/foo/",
                TargetUrl = "https://foo.bar/cont?sig=sasToken",
                TransformationStrategy = TransformationStrategy.None,
                FileType = FileType.Directory
            };

            patternFileOutput = new FileOutput
            {
                Path = "/data/*",
                TargetUrl = "https://foo.bar/cont?sig=sasToken",
                TransformationStrategy = TransformationStrategy.None,
                FileType = FileType.File
            };

            fileInfoProvider = new Mock<IFileInfoProvider>();

            fileInfoProvider.Setup(x => x.GetExpandedFileName(It.IsAny<string>())).Returns<string>(f => f);
            fileInfoProvider.Setup(x => x.FileExists(It.IsAny<string>())).Returns(true);
            fileInfoProvider.Setup(x => x.GetRootPathPair("/foo/bar"))
                .Returns(() => new RootPathPair("/", "foo/bar"));
            fileInfoProvider.Setup(x => x.GetRootPathPair("/data/*"))
                .Returns(() => new RootPathPair("/", "data/*"));

        }

        [TestMethod]
        public async Task ResolveOutputsAsync_FileOutputProvided_FileOperationIsResolved()
        {
            var nodeTask = new NodeTask() { Outputs = new List<FileOutput> { singleFileOutput } };

            //the path in the setup is /foo/bar, and the target URL is https://foo.bar/cont/outputs?sig=sasToken
            //therefore the expected output URL is:
            var expectedSingleFileOutputUrl = "https://foo.bar/cont/outputs/bar?sig=sasToken";


            fileInfoProvider.Setup(p =>
                    p.GetFilesBySearchPattern(It.IsAny<string>(), It.IsAny<string>()))
                     .Returns(new List<FileResult> { new FileResult(singleFileOutput.Path!, "bar", "/") });

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
                Outputs = new List<FileOutput>
                {
                    patternFileOutput // path: /data/*, target URL: https://foo.bar/cont/data?sig=sasToken
                }
            };

            fileInfoProvider.Setup(x => x.GetFilesBySearchPattern("/", "data/*"))
                .Returns(new List<FileResult> {
                    new FileResult("/data/foo.foo", "foo.foo", "/"),  //result of pattern file output
                    new FileResult("/data/dir1/bar.foo", "dir1/bar.foo", "/")
                });

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
            var nodeTask = new NodeTask() { Outputs = new List<FileOutput> { directoryFileOutput } };
            var rootDir = directoryFileOutput.Path;
            var file1 = "/dir1/file1.tmp";
            var file2 = "/dir1/dir2/file2.tmp";

            fileInfoProvider.Setup(x => x.GetAllFilesInDirectory(It.IsAny<string>()))
                .Returns(new List<FileResult>
                {
                    new FileResult($"{rootDir}{file1}", $"{file1.TrimStart('/')}", $"{rootDir}"),
                    new FileResult($"{rootDir}{file2}", $"{file2.TrimStart('/')}", $"{rootDir}")
                });

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
                    patternFileOutput,  // path: /data/*, target URL: https://foo.bar/cont/data?sig=sasToken
                    singleFileOutput    // path: /foo/bar, target URL: https://foo.bar/cont/outputs?sig=sasToken
                }
            };

            fileInfoProvider.Setup(x => x.GetFilesBySearchPattern("/", "data/*"))
                .Returns(new List<FileResult> {
                    new FileResult("/data/foo.foo", "foo.foo", "/"),  //result of pattern file output
                    new FileResult("/data/dir1/bar.foo", "dir1/bar.foo", "/")
                });
            fileInfoProvider.Setup(x => x.GetFilesBySearchPattern("/", "foo/bar"))
                .Returns(new List<FileResult> {
                    new FileResult("/foo/bar", "bar", "/") // result of the single file output
                });

            var fileOperationInfoResolver = new FileOperationResolver(nodeTask, resolutionPolicyHandler, fileInfoProvider.Object);
            var resolvedOutputs = await fileOperationInfoResolver.ResolveOutputsAsync();

            Assert.AreEqual(3, resolvedOutputs?.Count);
            Assert.IsTrue(resolvedOutputs!.Any(r => r.FullFilePath.Equals("/data/foo.foo", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.FullFilePath.Equals("/data/dir1/bar.foo", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.FullFilePath.Equals("/foo/bar", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.TargetUri.ToString().Equals(@"https://foo.bar/cont/foo.foo?sig=sasToken", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.TargetUri.ToString().Equals(@"https://foo.bar/cont/dir1/bar.foo?sig=sasToken", StringComparison.OrdinalIgnoreCase)));
            Assert.IsTrue(resolvedOutputs!.Any(r => r.TargetUri.ToString().Equals(@"https://foo.bar/cont/outputs/bar?sig=sasToken", StringComparison.OrdinalIgnoreCase)));
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
