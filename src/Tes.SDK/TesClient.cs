// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Polly;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Tes.Models;

namespace Tes.SDK
{
    public class TesClient : ITesClient
    {
        private static readonly HttpClient _httpClient = new HttpClient();
        private readonly string _baseUrl;
        private readonly string? _username;
        private readonly string? _password;

        public string SdkVersion { get; } = "0.1.0";

        public TesClient(string baseUrl)
        {
            _baseUrl = baseUrl ?? throw new ArgumentNullException(nameof(baseUrl));
        }

        public TesClient(string baseUrl, string username, string password)
        {
            _baseUrl = baseUrl ?? throw new ArgumentNullException(nameof(baseUrl));
            _username = username ?? throw new ArgumentNullException(nameof(username));
            _password = password ?? throw new ArgumentNullException(nameof(password));
        }

        private void SetAuthorizationHeader(HttpRequestMessage request)
        {
            if (!string.IsNullOrWhiteSpace(_username) && !string.IsNullOrWhiteSpace(_password))
            {
                request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic",
                    Convert.ToBase64String(Encoding.UTF8.GetBytes($"{_username}:{_password}")));
            }
        }

        private async Task<HttpResponseMessage> SendRequestAsync(HttpMethod method, string urlPath, HttpContent? content = null, CancellationToken cancellationToken = default)
        {
            var request = new HttpRequestMessage(method, _baseUrl + urlPath);
            request.Content = content;
            SetAuthorizationHeader(request);
            var response = await _httpClient.SendAsync(request, cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                throw new HttpRequestException($"Failed to {method} task. Status Code: {response.StatusCode}");
            }

            return response;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tesTask"></param>
        /// <returns>The created TES task's ID</returns>
        public async Task<string> CreateTaskAsync(TesTask tesTask, CancellationToken cancellationToken = default)
        {
            var response = await SendRequestAsync(HttpMethod.Post, "/v1/tasks",
                new StringContent(JsonSerializer.Serialize(tesTask), Encoding.UTF8, "application/json"), cancellationToken);

            return JsonSerializer.Deserialize<TesCreateTaskResponse>(await response.Content.ReadAsStringAsync())!.Id;
        }

        public async Task<TesTask> GetTaskAsync(string taskId, TesView view = TesView.MINIMAL, CancellationToken cancellationToken = default)
        {
            var response = await SendRequestAsync(HttpMethod.Get, $"/v1/tasks/{taskId}?view={view}", null, cancellationToken);
            return JsonSerializer.Deserialize<TesTask>(await response.Content.ReadAsStringAsync())!;
        }

        public async Task CancelTaskAsync(string taskId, CancellationToken cancellationToken = default)
        {
            await SendRequestAsync(HttpMethod.Post, $"/v1/tasks/{taskId}:cancel", null, cancellationToken);
        }

        public async IAsyncEnumerable<TesTask> ListTasksAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            string? pageToken = null;

            do
            {
                string url = $"/v1/tasks{(string.IsNullOrWhiteSpace(pageToken) ? "" : $"?pageToken={pageToken}")}";
                var response = await SendRequestAsync(HttpMethod.Get, url, null, cancellationToken);
                var jsonContent = await response.Content.ReadAsStringAsync();
                var tesListTasksResponse = JsonSerializer.Deserialize<TesListTasksResponse>(jsonContent);

                if (tesListTasksResponse?.Tasks.Count > 0)
                {
                    foreach (var task in tesListTasksResponse.Tasks)
                    {
                        yield return task;
                    }

                    pageToken = tesListTasksResponse.NextPageToken;
                }
                else
                {
                    yield break;
                }
            } while (pageToken != null && (cancellationToken == default || !cancellationToken.IsCancellationRequested));
        }

        /// <summary>
        /// Creates a new TES task and blocks forever until it's done
        /// </summary>
        /// <param name="tesTask">The TES task to create</param>
        /// <param name="cancellationToken">The cancellationToken</param>
        /// <returns>The created TES task</returns>
        public async Task<TesTask> CreateAndWaitTilDoneAsync(TesTask tesTask, CancellationToken cancellationToken = default)
        {
            var taskId = await CreateTaskAsync(tesTask, cancellationToken);
            var retryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(10, _ => TimeSpan.FromSeconds(5));

            while (true)
            {
                var task = await retryPolicy.ExecuteAsync(() => GetTaskAsync(taskId, TesView.MINIMAL, cancellationToken));

                if (!task.IsActiveState())
                {
                    return task;
                }

                await Task.Delay(TimeSpan.FromSeconds(1));
            }
        }
    }
}
