// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Moq;
using Tes.Runner.Exceptions;
using Tes.RunnerCLI.Commands;

namespace Tes.Runner.Test.Commands
{
    [TestClass]
    [TestCategory("Unit")]
    public class CommandLauncherTests
    {

        [TestMethod]
        public void HandleFatalLauncherError_IdentityUnavailableException_ReturnsIdentityUnavailableExitCode()
        {
            var exception = new IdentityUnavailableException();
            var expectedExitCode = (int)ProcessExitCode.IdentityUnavailable;

            var ex = Assert.ThrowsException<CommandExecutionException>(() => CommandLauncher.HandleFatalLauncherError("command", exception));

            Assert.AreEqual(expectedExitCode, ex.ExitCode);
        }

        [TestMethod]
        public void HandleFatalLauncherError_Exception_ReturnsUncategorizedExitCode()
        {
            var exception = new Exception();
            var expectedExitCode = (int)ProcessExitCode.UncategorizedError;

            var ex = Assert.ThrowsException<CommandExecutionException>(() => CommandLauncher.HandleFatalLauncherError("command", exception));

            Assert.AreEqual(expectedExitCode, ex.ExitCode);
        }
    }
}
