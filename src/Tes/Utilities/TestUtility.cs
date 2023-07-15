// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;
using Tes.Models;

namespace Tes.Utilities
{
    public class TestUtility
    {
        private static HttpClient client = new HttpClient();
        private static readonly AsyncRetryPolicy longRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(60, retryAttempt => TimeSpan.FromSeconds(15));

        public string TesEndpoint{ get; set; }
        public string TesPassword { get; set; }
        public string TesUsername { get; set; }

        public async Task<string> PostTesTaskAsync(TesTask task)
        {
            var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{TesUsername}:{TesPassword}"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);
            var content = new StringContent(JsonConvert.SerializeObject(task), Encoding.UTF8, "application/json");
            var requestUri = $"https://{TesEndpoint}/v1/tasks";

            await longRetryPolicy.ExecuteAsync(
                    async () =>
                    {
                        var responseBody = await client.PostAsync(requestUri, content);
                        var body = await responseBody.Content.ReadAsStringAsync();
                        try
                        {
                            var response = JsonConvert.DeserializeObject<TesCreateTaskResponse>(body);
                            return response.Id;
                        }
                        catch (JsonReaderException exception)
                        {
                            exception.Data.Add("Body", body);
                            throw;
                        }
                    });

            throw new Exception();
        }

        public async Task<bool> AllTasksSuccessfulAfterLongPollingAsync(List<string> ids)
        {
            while (true)
            {
                try
                {
                    int completedCount = 0;
                    int errorCount = 0;

                    foreach (var id in ids)
                    {
                        var responseBody = await client.GetAsync(TesEndpoint);
                        var content = await responseBody.Content.ReadAsStringAsync();
                        var response = JsonConvert.DeserializeObject<TesTask>(content);

                        switch (response.State)
                        {
                            case TesState.COMPLETEEnum:
                                if (string.IsNullOrWhiteSpace(response.FailureReason))
                                {
                                    Console.WriteLine($"TES Task State: {response.State}");
                                    completedCount++;
                                    break;
                                }

                                Console.WriteLine($"Failure reason: {response.FailureReason}");
                                errorCount++;
                                break;
                            case TesState.EXECUTORERROREnum:
                            case TesState.SYSTEMERROREnum:
                            case TesState.CANCELEDEnum:
                                Console.WriteLine($"TES Task State: {response.State}");

                                if (!string.IsNullOrWhiteSpace(response.FailureReason))
                                {
                                    Console.WriteLine($"Failure reason: {response.FailureReason}");
                                }

                                errorCount++;
                                break;
                        }
                    }

                    if (completedCount + errorCount == ids.Count)
                    {
                        return completedCount == ids.Count;
                    }
                }
                catch (HttpRequestException ex)
                {
                    Console.WriteLine($"Server is busy: {ex.Message}. Will retry again in 10s.");
                }
                catch (Exception exc)
                {
                    Console.WriteLine(exc);
                }

                await Task.Delay(TimeSpan.FromSeconds(10));
            }
        }
    }
}
