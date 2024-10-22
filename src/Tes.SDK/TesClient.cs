// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using Azure.Identity;
using Azure.Storage.Blobs;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;
using Tes.Models;

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
        private readonly bool _httpClientAllocated;
        private readonly Uri _baseUrl;
        private readonly string? _username;
        private readonly string? _password;
        private readonly AsyncRetryPolicy getTaskRetryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(60, _ => TimeSpan.FromSeconds(15));

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
        public string SdkVersion { get; } = "1.1.0-preview";

        private TesClient(bool clientAllocated, HttpClient httpClient, Uri baseUrl, string? username = null, string? password = null)
        {
            ArgumentNullException.ThrowIfNull(httpClient);
            ArgumentNullException.ThrowIfNull(baseUrl);

            if (string.IsNullOrWhiteSpace(username) != string.IsNullOrEmpty(password))
            {
                throw new ArgumentException("'username' and 'password' must be both provided or neither provided.", nameof(password));
            }

            // https://datatracker.ietf.org/doc/html/rfc7617#section-2 paragraph starting "Furthermore, a user-id containing a colon character is invalid,"
            if (username?.Contains(':') ?? false)
            {
                throw new ArgumentException("'username' must not contain ':'.", nameof(username));
            }

            _httpClientAllocated = clientAllocated;
            _httpClient = httpClient;
            _baseUrl = baseUrl;
            _username = username;
            _password = password;
        }

        public TesClient(HttpClient httpClient, Uri baseUrl, string? username = null, string? password = null)
            : this(false, httpClient, baseUrl, username, password)
        { }

        public TesClient(Uri baseUrl)
            : this(true, new(), baseUrl)
        { }

        public TesClient(Uri baseUrl, string username, string password)
            : this(true, new(), baseUrl, username, password)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(username);
            ArgumentException.ThrowIfNullOrEmpty(password);
        }

        public TesClient(TesCredentials tesCredentials, string scheme = "https")
            : this(new($"{scheme}://{tesCredentials.TesHostname}"), tesCredentials.TesUsername, tesCredentials.TesPassword)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(tesCredentials.TesHostname, nameof(tesCredentials));
        }

        public TesClient(HttpClient httpClient, TesCredentials tesCredentials, string scheme = "https")
            : this(httpClient, new($"{scheme}://{tesCredentials.TesHostname}"), tesCredentials.TesUsername, tesCredentials.TesPassword)
        {
            ArgumentNullException.ThrowIfNull(tesCredentials);
            ArgumentException.ThrowIfNullOrWhiteSpace(tesCredentials.TesHostname, nameof(tesCredentials));
            ArgumentException.ThrowIfNullOrWhiteSpace(tesCredentials.TesUsername, nameof(tesCredentials));
            ArgumentException.ThrowIfNullOrEmpty(tesCredentials.TesPassword, nameof(tesCredentials));
        }

        private void SetAuthorizationHeader(HttpRequestMessage request)
        {
            if (!string.IsNullOrWhiteSpace(_username) && !string.IsNullOrEmpty(_password))
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
        public async IAsyncEnumerable<TesTask> ListTasksAsync(TaskQueryOptions? options = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            string? pageToken = null;

            if (options == null)
            {
                options = new TaskQueryOptions();
            }

            do
            {
                var query = GetQuery(options, pageToken);
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

        public async Task<List<TesTask>> CreateAndWaitTilDoneAsync(IEnumerable<TesTask> tesTasks, CancellationToken cancellationToken = default)
        {
            var runningTasks = new Dictionary<string, TesTask>();

            foreach (var tesTask in tesTasks)
            {
                var taskId = await CreateTaskAsync(tesTask, cancellationToken);
                runningTasks.Add(taskId, tesTask);
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                foreach (var taskId in runningTasks.Keys)
                {
                    var task = await getTaskRetryPolicy.ExecuteAsync(() => GetTaskAsync(taskId, TesView.MINIMAL, cancellationToken));
                    runningTasks[taskId] = task;
                }

                if (runningTasks.Values.All(t => !t.IsActiveState()))
                {
                    // All tasks are done; now get their FULL metadata
                    var completedTasks = new List<TesTask>();

                    foreach (var kvp in runningTasks)
                    {
                        var completedTask = await GetTaskAsync(kvp.Key, TesView.FULL, cancellationToken);
                        completedTasks.Add(completedTask);
                    }

                    return completedTasks;
                }

                await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);
            }

            throw new TaskCanceledException();
        }

        /// <inheritdoc/>
        public async Task<TesTask> CreateAndWaitTilDoneAsync(TesTask tesTask, CancellationToken cancellationToken = default)
        {
            var taskId = await CreateTaskAsync(tesTask, cancellationToken);

            while (!cancellationToken.IsCancellationRequested)
            {
                var task = await getTaskRetryPolicy.ExecuteAsync(() => GetTaskAsync(taskId, TesView.MINIMAL, cancellationToken));

                if (!task.IsActiveState())
                {
                    return await GetTaskAsync(taskId, TesView.FULL, cancellationToken);
                }

                await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);
            }

            throw new TaskCanceledException();
        }

        private static string GetQuery(TaskQueryOptions options, string? pageToken)
        {
            var queryBuilder = new StringBuilder();
            queryBuilder.Append($"view={options.View}");

            if (!string.IsNullOrWhiteSpace(pageToken))
            {
                queryBuilder.Append($"&pageToken={pageToken}");
            }

            if (options.Tags?.Count > 0)
            {
                foreach (var key in options.Tags.Keys)
                {
                    queryBuilder.Append($"&tag_key={key}");

                    if (options.Tags.TryGetValue(key, out var val) && !string.IsNullOrWhiteSpace(val))
                    {
                        queryBuilder.Append($"&tag_value={val}");
                    }
                }
            }

            if (options.State.HasValue)
            {
                queryBuilder.Append($"&state={options.State}");
            }

            if (!string.IsNullOrWhiteSpace(options.NamePrefix))
            {
                queryBuilder.Append($"&name_prefix={options.NamePrefix}");
            }

            var query = queryBuilder.ToString();
            return query;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (_httpClientAllocated)
                    {
                        _httpClient.Dispose();
                    }
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
