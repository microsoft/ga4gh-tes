// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Moq;
using Tes.Runner.Logs;
using Tes.RunnerCLI.Commands;

namespace Tes.Runner.Test.Commands
{
    [TestClass, TestCategory("Unit")]
    [Ignore] //ignoring this test as it fails in the pipeline because the dotnet command is not available. 
    public class ProcessLauncherTests
    {
        private ProcessLauncher processLauncher = null!;
        private Mock<IStreamLogReader> streamLogReaderMock = null!;

        [TestInitialize]
        public void SetUp()
        {
            streamLogReaderMock = new Mock<IStreamLogReader>();
            processLauncher = new ProcessLauncher(streamLogReaderMock.Object);
        }

        [TestMethod]
        public async Task LaunchProcessAndWaitAsync_ValidCommand_NoErrorSuccessExitCode()
        {
            //dotnet version must be available in windows and linux. 
            var result = await processLauncher.LaunchProcessAndWaitAsync(["dotnet --version"]);

            Assert.AreEqual(0, result.ExitCode);
        }

        [TestMethod]
        public async Task LaunchProcessAndWaitAsync_InvalidCommand_NoSuccessExitCode()
        {
            //dotnet version must be available in windows and linux. 
            var result = await processLauncher.LaunchProcessAndWaitAsync(["dotnet -version"]);

            Assert.AreNotEqual(0, result.ExitCode);
        }

        [TestMethod]
        public async Task LaunchProcessAndWaitAsync_MultipleCalls_StandardOutputIsReturnedWithLatestExecutionResult()
        {
            //dotnet version must be available in windows and linux. 
            var result1 = await processLauncher.LaunchProcessAndWaitAsync(["dotnet --version"]);
            var result2 = await processLauncher.LaunchProcessAndWaitAsync(["dotnet --version"]);

            Assert.AreEqual(result2.ExitCode, result1.ExitCode);
        }
    }
}
