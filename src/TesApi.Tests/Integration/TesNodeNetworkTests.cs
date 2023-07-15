// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Tes.Models;
using Tes.Utilities;

namespace TesApi.Tests.Integration
{
    [TestClass]
    public class TesNodeNetworkTests
    {
        private readonly TestUtility testUtility;

        public TesNodeNetworkTests()
        {
            // TODO wireup using existing JSON schema
            testUtility = new TestUtility { TesEndpoint = "", TesUsername = "", TesPassword = "" };
        }
        /// <summary>
        /// To run this test, specify a testStorageAccountName, a workflowsContainerSasToken
        /// </summary>
        /// <returns></returns>
        [Ignore]
        [TestCategory("Integration")]
        [TestMethod]
        public async Task TestTesNodeNetworkConnectivityAsync()
        {
            // This is set in the Azure Devops pipeline, which writes the file to the .csproj directory
            // The current working directory is this: /mnt/vss/_work/r1/a/CoaArtifacts/AllSource/TriggerService.Tests/bin/Debug/net7.0/
            // And the file is available here: /mnt/vss/_work/r1/a/CoaArtifacts/AllSource/TriggerService.Tests/temp_storage_account_name.txt
            const string storageAccountNamePath = "../../../temp_storage_account_name.txt";
            var path = storageAccountNamePath;

            if (!File.Exists(path))
            {
                Console.WriteLine($"Path not found - exiting integration test: {path}");
                return;
            }

            Console.WriteLine($"Found path: {path}");
            var lines = await File.ReadAllLinesAsync(path);

            var taskId1 = await CreateTesTaskToTestInternetAccessAsync();
            var taskId2 = await CreateTesTaskToTestImdsIsBlockedAsync();
            await WaitForAllTesTasksToCompleteAsync(new List<string>{ taskId1, taskId2 });
        }

        private async Task<string> CreateTesTaskToTestInternetAccessAsync()
        {
            const string commandToTestInternetAccess = """if ! curl -s -H Metadata:true "https://www.example.com" >/dev/null; then exit 1; fi""";
            var task = CreateBasicTesTask(commandToTestInternetAccess);
            return await testUtility.PostTesTaskAsync(task);
        }

        private async Task<string> CreateTesTaskToTestImdsIsBlockedAsync()
        {
            const string commandToFailOnNoConnection = """if ! curl -s -H Metadata:true "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://management.azure.com/" >/dev/null; then exit 0; fi""";
            var task = CreateBasicTesTask(commandToFailOnNoConnection);
            return await testUtility.PostTesTaskAsync(task);
        }

        private TesTask CreateBasicTesTask(string command)
        {
            const string image = "ubuntu:latest";
            return new TesTask(new TesExecutor { Image = image, Command = new List<string> { command } }, resources: new TesResources { Preemptible = true });
        } 

        private async Task WaitForAllTesTasksToCompleteAsync(List<string> ids)
        {
            Assert.IsTrue(await testUtility.AllTasksSuccessfulAfterLongPollingAsync(ids));
        }
    }
}
