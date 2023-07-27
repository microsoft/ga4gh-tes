// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Transfer;

namespace Tes.Runner.Test.Transfer
{
    [TestClass, TestCategory("Unit")]
    public class DefaultFileInfoProviderTests
    {
        private DirectoryInfo directoryInfo = null!;
        private string testFile = null!;
        private const string TestFilePrefix = "foo";
        private DefaultFileInfoProvider fileInfoProvider = null!;

        [TestInitialize]
        public async Task Setup()
        {
            //this creates a directory structure like this: guid/dir1/dir2/dir3, with a file in each directory with the same prefix
            directoryInfo = RunnerTestUtils.CreateTempFilesInDirectory("dir1/dir2/dir3", TestFilePrefix);
            testFile = await RunnerTestUtils.CreateTempFileWithContentAsync(numberOfMiB: 1, extraBytes: 0);
            fileInfoProvider = new DefaultFileInfoProvider();
        }

        [TestMethod]
        public void GetFileSizeTest_FileExists_ReturnsValidSize()
        {
            var size = fileInfoProvider.GetFileSize(testFile);

            Assert.AreEqual(BlobSizeUtils.MiB, size);
        }

        [TestMethod]
        public void GetFileSizeTest_FileDoesNotExist_ThrowsFileNotFoundException()
        {
            Assert.ThrowsException<FileNotFoundException>(() => fileInfoProvider.GetFileSize("foo"));
        }

        [TestMethod]
        public void GetExpandedFileName_EnvVariableIsInPath_FileNameIsExpanded()
        {
            Environment.SetEnvironmentVariable("AZ_PATH", "foo");

            var expandedFileName = fileInfoProvider.GetExpandedFileName("%AZ_PATH%/bar");

            Assert.AreEqual("foo/bar", expandedFileName);
        }

        [TestMethod]
        public void FileExistsTest_FileExists_ReturnsTrue()
        {
            Assert.IsTrue(fileInfoProvider.FileExists(testFile));
        }

        [TestMethod]
        public void FileExistsTest_FileDoesNotExist_ReturnsFalse()
        {
            Assert.IsFalse(fileInfoProvider.FileExists("foo"));
        }

        [TestMethod]
        [DataRow($"{TestFilePrefix}*")]
        [DataRow($"*")]
        [DataRow($"*.tmp")]
        public void GetFilesBySearchPattern_PatternThatMatchesAllFiles_ReturnsAllFilesInAllLevels(string pattern)
        {
            var files = fileInfoProvider.GetFilesBySearchPattern(directoryInfo.FullName, pattern);

            AssertAllTempFilesAreReturned(files);
        }

        private static void AssertAllTempFilesAreReturned(string[] files)
        {
            //the setup creates 4 files with the same prefix
            Assert.AreEqual(4, files.Length);
        }

        [TestMethod]
        public void GetAllFilesInDirectoryTest_RootDirectoryIsProvided_AllFilesAreReturned()
        {
            var files = fileInfoProvider.GetAllFilesInDirectory(directoryInfo.FullName);

            AssertAllTempFilesAreReturned(files);
        }
    }
}
