// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using Azure.Storage.Sas;
using CommonUtilities;
using Tes.Runner.Models;
using Tes.Runner.Storage;
using Tes.Runner.Transfer;
using Tes.RunnerCLI.Commands;

namespace Tes.Runner.Test.Commands
{
    [TestClass, TestCategory("Unit")]
    public class NodeTaskResolverTests
    {
        // Prevent breaking other tests
        private readonly string defaultTaskFile = Path.Combine(Environment.CurrentDirectory, CommandFactory.DefaultTaskDefinitionFile);
        // NodeTaskResolverOptions serialization
        private static readonly JsonSerializerOptions jsonSerializerOptions = new() { DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingDefault };

        private FileInfo? taskFile = default;
        private const int MaxRetryCount = 3;
        private NodeTaskResolver nodeTaskResolver = default!;

        private class MockableHttpMessageHandler : HttpMessageHandler
        {
            private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> sendAsyncImpl;

            public MockableHttpMessageHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> sendAsyncImpl)
            {
                ArgumentNullException.ThrowIfNull(sendAsyncImpl);
                this.sendAsyncImpl = sendAsyncImpl;
            }

            protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            {
                return sendAsyncImpl(request, cancellationToken);
            }
        }

        private class MockableResolutionPolicyHandler : ResolutionPolicyHandler
        {
            private readonly Func<string?, TransformationStrategy?, BlobSasPermissions, RuntimeOptions, Uri> applySasResolutionToUrl;

            public MockableResolutionPolicyHandler(Func<string?, TransformationStrategy?, BlobSasPermissions, RuntimeOptions, Uri> applySasResolutionToUrl, RuntimeOptions runtimeOptions)
                : base(runtimeOptions)
            {
                ArgumentNullException.ThrowIfNull(applySasResolutionToUrl);
                this.applySasResolutionToUrl = applySasResolutionToUrl;
            }

            protected override Task<Uri> ApplySasResolutionToUrlAsync(string? sourceUrl, TransformationStrategy? strategy, BlobSasPermissions blobSasPermissions, RuntimeOptions runtimeOptions)
            {
                return Task.FromResult(applySasResolutionToUrl(sourceUrl, strategy, blobSasPermissions, runtimeOptions));
            }
        }

        [TestInitialize]
        public void SetUp()
        {
            File.Move(defaultTaskFile, Path.ChangeExtension(defaultTaskFile, ".backup"));
        }

        [TestCleanup]
        public void Cleanup()
        {
            CleanupFile(taskFile);

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

        private void SetEnvironment(NodeTaskResolverOptions? resolverOptions)
        {
            if (resolverOptions is not null)
            {
                Environment.SetEnvironmentVariable(nameof(NodeTaskResolverOptions), JsonSerializer.Serialize(resolverOptions, jsonSerializerOptions));
            }
            else
            {
                Environment.SetEnvironmentVariable(nameof(NodeTaskResolverOptions), null);
            }
        }

        private void ConfigureBlobApiHttpUtils(
            Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> sendAsync,
            Func<RuntimeOptions, ResolutionPolicyHandler>? ResolutionPolicyFactory = default)
        {
            nodeTaskResolver = new(
                () => new BlobApiHttpUtils(new(new MockableHttpMessageHandler(sendAsync)), HttpRetryPolicyDefinition.DefaultAsyncRetryPolicy(MaxRetryCount)),
                ResolutionPolicyFactory);
        }

        [TestMethod]
        public async Task ResolveNodeTaskAsyncWithNoUriWhenFileExistsReturnsContent()
        {
            ConfigureBlobApiHttpUtils((_, _) => throw new NotImplementedException());
            SetEnvironment(null);
            taskFile = new(Path.Combine(Environment.CurrentDirectory, CommandFactory.DefaultTaskDefinitionFile));
            File.WriteAllText(taskFile.FullName, @"{}");
            taskFile.Refresh();

            var result = await nodeTaskResolver.ResolveNodeTaskAsync(file: taskFile, uri: null, apiVersion: BlobPipelineOptions.DefaultApiVersion, saveDownload: false);

            Assert.IsNotNull(result);
        }

        [TestMethod]
        public async Task ResolveNodeTaskAsyncWithNoUriWhenFileNotExistsReturnsError()
        {
            ConfigureBlobApiHttpUtils((_, _) => throw new NotImplementedException());
            SetEnvironment(null);
            taskFile = new(Path.Combine(Environment.CurrentDirectory, CommandFactory.DefaultTaskDefinitionFile));
            Assert.IsFalse(taskFile.Exists);

            var ex = await Assert.ThrowsExceptionAsync<ArgumentNullException>(async () =>
                _ = await nodeTaskResolver.ResolveNodeTaskAsync(file: taskFile, uri: null, apiVersion: BlobPipelineOptions.DefaultApiVersion, saveDownload: false));
            Assert.AreEqual("uri", ex.ParamName);
        }

        [TestMethod]
        public async Task ResolveNodeTaskAsyncWithUriWhenFileExistsDoesNotDownload()
        {
            ConfigureBlobApiHttpUtils((_, _) => throw new NotImplementedException());
            SetEnvironment(null);
            taskFile = new(Path.Combine(Environment.CurrentDirectory, CommandFactory.DefaultTaskDefinitionFile));
            File.WriteAllText(taskFile.FullName, @"{}");
            taskFile.Refresh();

            var result = await nodeTaskResolver.ResolveNodeTaskAsync(file: taskFile, uri: new("http://localhost/task.json"), apiVersion: BlobPipelineOptions.DefaultApiVersion, saveDownload: false);

            Assert.IsNotNull(result);
        }

        [TestMethod]
        public async Task ResolveNodeTaskAsyncWithUriWhenFileNotExistsDoesDownload()
        {
            ConfigureBlobApiHttpUtils((_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(@"{}") }));
            SetEnvironment(new() { RuntimeOptions = new() });
            taskFile = new(Path.Combine(Environment.CurrentDirectory, CommandFactory.DefaultTaskDefinitionFile));
            Assert.IsFalse(taskFile.Exists);

            var result = await nodeTaskResolver.ResolveNodeTaskAsync(file: taskFile, uri: new("http://localhost/task.json"), apiVersion: BlobPipelineOptions.DefaultApiVersion, saveDownload: false);

            Assert.IsNotNull(result);
            Assert.IsFalse(taskFile.Exists);
        }

        [TestMethod]
        public async Task ResolveNodeTaskAsyncWithUriWhenFileNotExistsDoesSave()
        {
            ConfigureBlobApiHttpUtils((_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(@"{}") }));
            SetEnvironment(new() { RuntimeOptions = new() });
            taskFile = new(Path.Combine(Environment.CurrentDirectory, CommandFactory.DefaultTaskDefinitionFile));
            Assert.IsFalse(taskFile.Exists);

            var result = await nodeTaskResolver.ResolveNodeTaskAsync(file: taskFile, uri: new("http://localhost/task.json"), apiVersion: BlobPipelineOptions.DefaultApiVersion, saveDownload: true);

            Assert.IsNotNull(result);
            Assert.IsTrue(taskFile.Exists);
        }

        [TestMethod]
        public async Task ResolveNodeTaskAsyncUsesResolutionPolicyResolver()
        {
            ExceptionDispatchInfo? assertException = default;
            var sendHeadCalled = false;
            var sendGetCalled = false;

            ConfigureBlobApiHttpUtils((request, _) => Task.FromResult(Send(request)), options => new MockableResolutionPolicyHandler(ApplySasResolutionToUrl, options));
            SetEnvironment(new() { RuntimeOptions = new() { Terra = new() }, TransformationStrategy = TransformationStrategy.CombinedTerra });
            taskFile = new(Path.Combine(Environment.CurrentDirectory, CommandFactory.DefaultTaskDefinitionFile));

            var result = await nodeTaskResolver.ResolveNodeTaskAsync(file: taskFile, uri: new("http://localhost/task.json"), apiVersion: BlobPipelineOptions.DefaultApiVersion, saveDownload: false);

            assertException?.Throw();
            Assert.IsNotNull(result);
            Assert.IsTrue(sendHeadCalled);
            Assert.IsTrue(sendGetCalled);

            Uri ApplySasResolutionToUrl(string? sourceUrl, TransformationStrategy? strategy, BlobSasPermissions blobSasPermissions, RuntimeOptions runtimeOptions)
            {
                try
                {
                    Assert.IsNotNull(runtimeOptions.Terra);
                    Assert.AreEqual(TransformationStrategy.CombinedTerra, strategy);
                    Assert.IsTrue(0 != (blobSasPermissions | BlobSasPermissions.Read));
                    Assert.IsTrue(0 == (blobSasPermissions & (BlobSasPermissions.Create | BlobSasPermissions.Write)));
                }
                catch (AssertFailedException ex)
                {
                    assertException ??= ExceptionDispatchInfo.Capture(ex);
                }

                return new UriBuilder(sourceUrl!) { Query = "sas" }.Uri;
            }

            HttpResponseMessage Send(HttpRequestMessage request)
            {
                try
                {
                    Assert.AreEqual(new("http://localhost/task.json?sas"), request.RequestUri);
                }
                catch (AssertFailedException ex)
                {
                    assertException ??= ExceptionDispatchInfo.Capture(ex);
                }

                return request.Method.Method switch
                {
                    "GET" => SendGetCalled(),
                    "HEAD" => SendHeadCalled(),
                    _ => InvalidRequest(),
                };

                HttpResponseMessage SendHeadCalled()
                {
                    sendHeadCalled = true;
                    var response = MakeResponse();
                    var content = response.Content;
                    response.Content = new StringContent(string.Empty);
                    response.Content.Headers.Clear();
                    content.Headers.ForEach(header => response.Content.Headers.Add(header.Key, header.Value));
                    response.Content.Headers.ContentLength = content.Headers.ContentLength;
                    return response;
                }

                HttpResponseMessage SendGetCalled()
                {
                    sendGetCalled = true;
                    return MakeResponse();
                }

                HttpResponseMessage MakeResponse()
                    => new(HttpStatusCode.OK) { Content = new StringContent(@"{}", System.Text.Encoding.UTF8, System.Net.Mime.MediaTypeNames.Application.Json) };

                HttpResponseMessage InvalidRequest()
                {
                    try
                    {
                        Assert.Fail("Invalid HTTP Method.");
                    }
                    catch (AssertFailedException ex)
                    {
                        assertException ??= ExceptionDispatchInfo.Capture(ex);
                    }

                    return new HttpResponseMessage(HttpStatusCode.MethodNotAllowed);
                }
            }
        }
    }
}
