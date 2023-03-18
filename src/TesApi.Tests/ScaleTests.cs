using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Tes.Models;
using TesApi.Controllers;

namespace TesApi.Tests
{
    [TestClass]
    public class ScaleTests
    {
        public async Task TestMaxTasksAsync()
        {
            var client = new HttpClient();
            var tesEndpoint = "";
            var preemptible = true;
            var tesUsername = "";
            var tesPassword = "";

            for (int i = 0; i < 5000; i++)
            {
                var task = new TesTask();

                await TestTaskAsync(tesEndpoint, preemptible, tesUsername, tesPassword)
            }

        }

        private static async Task<int> TestTaskAsync(string tesEndpoint, bool preemptible, string tesUsername, string tesPassword)
        {
            using var client = new HttpClient();
            client.SetBasicAuthentication(tesUsername, tesPassword);

            var task = new TesTask()
            {
                Inputs = new List<TesInput>(),
                Outputs = new List<TesOutput>(),
                Executors = new List<TesExecutor>
                {
                    new TesExecutor()
                    {
                        Image = "ubuntu:22.04",
                        Command = new List<string>{@"timeout 20m bash -c 'for ((i=0;i<$(nproc);i++)); do (while true; do sleep 0.1; done) & done'\n" },
                    }
                },
                Resources = new TesResources()
                {
                    Preemptible = preemptible
                }
            };

            var content = new StringContent(JsonConvert.SerializeObject(task), Encoding.UTF8, "application/json");
            var requestUri = $"https://{tesEndpoint}/v1/tasks";
            Dictionary<string, string> response = null;
            await longRetryPolicy.ExecuteAsync(
                    async () =>
                    {
                        var responseBody = await client.PostAsync(requestUri, content);
                        var body = await responseBody.Content.ReadAsStringAsync();
                        response = JsonConvert.DeserializeObject<Dictionary<string, string>>(body);
                    });

            return await IsTaskSuccessfulAfterLongPollingAsync(client, $"{requestUri}/{response["id"]}") ? 0 : 1;
        }
    }
}
