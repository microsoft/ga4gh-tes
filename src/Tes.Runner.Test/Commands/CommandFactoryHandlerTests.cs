// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Moq;
using Moq.Protected;
using Tes.Runner.Transfer;
using Tes.RunnerCLI.Commands;

namespace Tes.Runner.Test.Commands
{
    [TestClass, TestCategory("Unit")]
    public class CommandFactoryHandlerTests
    {
        // Prevent breaking other tests
        private readonly string defaultTaskFile = Path.Combine(Environment.CurrentDirectory, CommandFactory.DefaultTaskDefinitionFile);
        // NodeTaskResolverOptions serialization
        private static readonly JsonSerializerOptions jsonSerializerOptions = new() { DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingDefault };

        private readonly Mock<HttpMessageHandler> mockHttpMessageHandler = new();
        private RootCommand rootCommand = default!;
        private FileInfo? taskFile = default;
        private FileInfo? otherFile = default;
        private const int MaxRetryCount = 3;

        // CommandHandlers is not mockable because we don't do strong-name signing
        private class TestableCommandHandlers(Action<FileInfo> action, Func<BlobApiHttpUtils> blobApiUtilsFactory)
            : CommandHandlers(new(blobApiUtilsFactory))
        {
            private readonly Action<FileInfo>? action = action;

            protected override Task ExecuteAllOperationsAsSubProcessesAsync(FileInfo file, int blockSize, int writers, int readers, int bufferCapacity, string apiVersion, Uri dockerUri)
            {
                action?.Invoke(file);
                file?.Refresh();
                return Task.CompletedTask;
            }
        }

        [TestInitialize]
        public void SetUp()
        {
            File.Move(defaultTaskFile, Path.ChangeExtension(defaultTaskFile, ".backup"));

            CommandFactory commandFactory = new(
                new TestableCommandHandlers(file => taskFile = file,
                () => new BlobApiHttpUtils(new(mockHttpMessageHandler.Object), HttpRetryPolicyDefinition.DefaultAsyncRetryPolicy(MaxRetryCount))));

            rootCommand = commandFactory.CreateRootCommand();
            commandFactory.CreateUploadCommand(rootCommand);
            commandFactory.CreateExecutorCommand(rootCommand);
            commandFactory.CreateDownloadCommand(rootCommand);
        }

        [TestCleanup]
        public void Cleanup()
        {
            CleanupFile(taskFile);
            CleanupFile(otherFile);

            File.Move(Path.ChangeExtension(defaultTaskFile, ".backup"), defaultTaskFile);

            static void CleanupFile(FileInfo? file)
            {
                file?.Refresh();
                if (file?.Exists ?? false)
                {
                    file.Delete();
                }
            }
        }

        private void SetEnvironment(Models.NodeTaskResolverOptions? resolverOptions)
        {
            if (resolverOptions is not null)
            {
                Environment.SetEnvironmentVariable(nameof(Models.NodeTaskResolverOptions), JsonSerializer.Serialize(resolverOptions, jsonSerializerOptions));
            }
            else
            {
                Environment.SetEnvironmentVariable(nameof(Models.NodeTaskResolverOptions), null);
            }
        }

        private void SetDownload(HttpContent content)
        {
            mockHttpMessageHandler
                .Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
                .Returns(Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = content }));
        }

        [TestMethod]
        public async Task NoArgumentsUsesDefaultFilenameIfExists()
        {
            SetEnvironment(null);
            otherFile = new(Path.Combine(Environment.CurrentDirectory, CommandFactory.DefaultTaskDefinitionFile));
            File.WriteAllText(otherFile.FullName, @"{}");

            var result = await rootCommand.InvokeAsync(string.Empty);

            Assert.AreEqual(0, result);
            Assert.IsNotNull(taskFile);
            Assert.IsTrue(taskFile.Exists);
        }

        [TestMethod]
        public async Task NoArgumentsDefaultFilenameNotExistsReturnsError()
        {
            SetEnvironment(null);

            var result = await rootCommand.InvokeAsync(string.Empty);

            Assert.AreEqual(1, result);
            Assert.IsNull(taskFile);
        }

        [TestMethod]
        public async Task FileArgumentsUsesFileIfExists()
        {
            SetEnvironment(null);
            otherFile = new(Path.Combine(Environment.CurrentDirectory, "task.json"));
            File.WriteAllText(otherFile.FullName, @"{}");

            var result = await rootCommand.InvokeAsync("-f task.json");

            Assert.AreEqual(0, result);
            Assert.IsNotNull(taskFile);
            Assert.IsTrue(taskFile.Exists);
        }

        [TestMethod]
        public async Task FileArgumentNotExistsReturnsError()
        {
            SetEnvironment(null);
            otherFile = new(Path.Combine(Environment.CurrentDirectory, "task.json"));

            var result = await rootCommand.InvokeAsync("-f task.json");

            Assert.AreEqual(1, result);
            Assert.IsNull(taskFile);
        }

        [TestMethod]
        public async Task UrlArgumentExistsWithoutFileArgumentDownloadsIntoDefaultFilename()
        {
            SetEnvironment(new() { RuntimeOptions = new() });
            SetDownload(JsonContent.Create(new Models.NodeTask()));

            var result = await rootCommand.InvokeAsync("-i http://localhost/task.json");

            Assert.AreEqual(0, result);
            Assert.IsNotNull(taskFile);
            Assert.IsTrue(taskFile.Exists);
        }

        [TestMethod]
        public async Task UrlArgumentExistsWithFileArgumentWithFileExistsReturnsError()
        {
            SetEnvironment(null);
            otherFile = new(Path.Combine(Environment.CurrentDirectory, "task.json"));
            File.WriteAllText(otherFile.FullName, @"{}");

            var result = await rootCommand.InvokeAsync("-f task.json -i http://localhost/task.json");

            Assert.AreEqual(1, result);
            Assert.IsNull(taskFile);
        }

        [TestMethod]
        public async Task UrlArgumentExistsWithFileArgumentNotExistsDownloadsIntoFilename()
        {
            SetEnvironment(new() { RuntimeOptions = new() });
            otherFile = new(Path.Combine(Environment.CurrentDirectory, "task.json"));
            SetDownload(JsonContent.Create(new Models.NodeTask()));

            var result = await rootCommand.InvokeAsync("-f task.json -i http://localhost/task.json");

            Assert.AreEqual(0, result);
            Assert.IsNotNull(taskFile);
            Assert.IsTrue(taskFile.Exists);
        }
    }
}
