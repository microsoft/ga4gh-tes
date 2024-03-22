// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Polly;
using System.Runtime.CompilerServices;
using System.Text;
using Newtonsoft.Json;
using Tes.Models;
using System.Net.Http;
using System.Net.Http.Json;
using System.Net.Http.Headers;

namespace Tes.SDK
{
    public class TesClient : ITesClient
    {
        private static readonly JsonSerializerSettings serializerSettings = new()
        {
            DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate,
            NullValueHandling = NullValueHandling.Ignore,
        };

        private readonly HttpClient _httpClient;
        private readonly Uri _baseUrl;
        private readonly string? _username;
        private readonly string? _password;
        private bool disposedValue;

        private static StringContent Serialize<T>(T obj)
        {
            using StringWriter writer = new();
            JsonSerializer.Create(serializerSettings).Serialize(writer, obj);
            return new(writer.ToString(), Encoding.UTF8, new MediaTypeHeaderValue("application/json"));
        }

        private static async ValueTask<T> DeserializeAsync<T>(HttpContent content, CancellationToken cancellationToken)
        {
            using StreamReader streamReader = new(await content.ReadAsStreamAsync(cancellationToken));
            using JsonTextReader jsonReader = new(streamReader);
            return JsonSerializer.Create(serializerSettings).Deserialize<T>(jsonReader)!;
        }

        /// <inheritdoc/>
        public string SdkVersion { get; } = "0.1.0";

        public TesClient(HttpClient httpClient, Uri baseUrl, string? username = null, string? password = null)
        {
            ArgumentNullException.ThrowIfNull(baseUrl);
            _httpClient = httpClient;
            _baseUrl = baseUrl;
            _username = username;
            _password = password;
        }

        public TesClient(Uri baseUrl)
            : this(new(), baseUrl)
        { }

        public TesClient(Uri baseUrl, string username, string password)
            : this(new(), baseUrl, username, password)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(username);
            ArgumentException.ThrowIfNullOrEmpty(password);
        }

        private void SetAuthorizationHeader(HttpRequestMessage request)
        {
            if (!string.IsNullOrWhiteSpace(_username) && !string.IsNullOrWhiteSpace(_password))
            {
                request.Headers.Authorization = new("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{_username}:{_password}")));
            }
        }

        private HttpRequestMessage GetRequest(HttpMethod method, string urlPath, string? query = null, HttpContent? content = null)
        {
            var uri = new UriBuilder(_baseUrl) { Path = urlPath, Query = query }.Uri;
            return new(method, uri) { Content = content };
        }

        private async Task<HttpResponseMessage> SendRequestAsync(HttpRequestMessage request, CancellationToken cancellationToken = default)
        {
            SetAuthorizationHeader(request);
            var response = await _httpClient.SendAsync(request, cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                throw new HttpRequestException($"Failed to {request.Method} task(s). Status Code: {response.StatusCode}");
            }

            return response;
        }

        /// <inheritdoc/>
        public async Task<string> CreateTaskAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            var response = await SendRequestAsync(GetRequest(HttpMethod.Post, "/v1/tasks", content: Serialize(tesTask)), cancellationToken);

            var result = await DeserializeAsync<TesCreateTaskResponse>(response.Content, cancellationToken)!;
            return result!.Id;
        }

        /// <inheritdoc/>
        public async Task<TesTask> GetTaskAsync(string taskId, TesView view, CancellationToken cancellationToken)
        {
            var response = await SendRequestAsync(GetRequest(HttpMethod.Get, $"/v1/tasks/{taskId}", query: $"view={view}"), cancellationToken: cancellationToken);
            return await DeserializeAsync<TesTask>(response.Content, cancellationToken)!;
        }

        /// <inheritdoc/>
        public async Task CancelTaskAsync(string taskId, CancellationToken cancellationToken)
        {
            await SendRequestAsync(GetRequest(HttpMethod.Post, $"/v1/tasks/{taskId}:cancel"), cancellationToken: cancellationToken);
        }

        /// <inheritdoc/>
        public async IAsyncEnumerable<TesTask> ListTasksAsync(TesView view, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            string? pageToken = null;

            do
            {
                var query = $"view={view}{(string.IsNullOrWhiteSpace(pageToken) ? string.Empty : $"&pageToken={pageToken}")}";
                var response = await SendRequestAsync(GetRequest(HttpMethod.Get, "/v1/tasks", query), cancellationToken: cancellationToken);
                var tesListTasksResponse = await DeserializeAsync<TesListTasksResponse>(response.Content, cancellationToken);

                if (tesListTasksResponse?.Tasks?.Count > 0)
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
            } while (pageToken != null && !cancellationToken.IsCancellationRequested);
        }

        /// <inheritdoc/>
        public async Task<TesTask> CreateAndWaitTilDoneAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            var taskId = await CreateTaskAsync(tesTask, cancellationToken);
            var retryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(60, _ => TimeSpan.FromSeconds(15));

            while (true)
            {
                var task = await retryPolicy.ExecuteAsync(() => GetTaskAsync(taskId, TesView.MINIMAL, cancellationToken));

                if (!task.IsActiveState())
                {
                    return await GetTaskAsync(taskId, TesView.FULL, cancellationToken);
                }

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _httpClient.Dispose();
                }

                disposedValue = true;
            }
        }

        void IDisposable.Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
