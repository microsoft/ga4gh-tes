// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.RunnerCLI.Commands;

namespace Tes.Runner.Test.Commands
{
    [TestClass, TestCategory("Unit")]
    public class ProcessLauncherTests
    {
        private ProcessLauncher processLauncher = null!;

        [TestInitialize]
        public void SetUp()
        {
            processLauncher = new ProcessLauncher();
        }

        [TestMethod]
        public async Task LaunchProcessAndWaitAsync_ValidCommand_StandardOutputIsReturnedNoErrorSuccessExitCode()
        {
            //dotnet version must be available in windows and linux. 
            var result = await processLauncher.LaunchProcessAndWaitAsync(new[] { "dotnet --version" });

            Assert.IsTrue(!string.IsNullOrEmpty(result.StandardOutput));
            Assert.IsTrue(string.IsNullOrEmpty(result.StandardError));
            Assert.AreEqual(0, result.ExitCode);
        }

        [TestMethod]
        public async Task LaunchProcessAndWaitAsync_InvalidCommand_StandardErrorIsReturnedNoSuccessExitCode()
        {
            //dotnet version must be available in windows and linux. 
            var result = await processLauncher.LaunchProcessAndWaitAsync(new[] { "dotnet -version" });

            Assert.IsTrue(!string.IsNullOrEmpty(result.StandardError));
            Assert.AreNotEqual(0, result.ExitCode);
        }

        [TestMethod]
        public async Task LaunchProcessAndWaitAsync_MultipleCalls_StandardOutputIsReturnedWithLatestExecutionResult()
        {
            //dotnet version must be available in windows and linux. 
            var result1 = await processLauncher.LaunchProcessAndWaitAsync(new[] { "dotnet --version" });
            var result2 = await processLauncher.LaunchProcessAndWaitAsync(new[] { "dotnet --version" });

            Assert.AreEqual(result2.StandardOutput, result1.StandardOutput);
            Assert.AreEqual(result2.ExitCode, result1.ExitCode);
            Assert.AreEqual(result2.StandardError, result1.StandardError);
        }
    }
}
