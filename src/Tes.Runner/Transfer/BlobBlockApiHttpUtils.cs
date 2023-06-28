// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net;
using System.Net.Http.Headers;
using System.Text;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;

namespace Tes.Runner.Transfer;
/// <summary>
/// A class containing the logic to create and make the HTTP requests for the blob block API.
/// </summary>
public class BlobBlockApiHttpUtils
{
    private const string BlobType = "BlockBlob";
    private const int MaxRetryCount = 9;
    private readonly HttpClient httpClient;
    private static readonly ILogger Logger = PipelineLoggerFactory.Create<BlobBlockApiHttpUtils>();
    private readonly AsyncRetryPolicy retryPolicy;


    public const string RootHashMetadataName = "md5_4mib_hashlist_root_hash";

    public BlobBlockApiHttpUtils(HttpClient httpClient, AsyncRetryPolicy retryPolicy)
    {
        this.httpClient = httpClient;
        this.retryPolicy = retryPolicy;
    }

    public BlobBlockApiHttpUtils() : this(new HttpClient(), DefaultAsyncRetryPolicy(MaxRetryCount))
    {
    }

    public static HttpRequestMessage CreatePutBlockRequestAsync(PipelineBuffer buffer, string apiVersion)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, buffer.BlobPartUrl)
        {
            Content = new ByteArrayContent(buffer.Data, 0, buffer.Length)
        };

        AddPutBlockHeaders(request, apiVersion);
        return request;
    }

    public static Uri ParsePutBlockUrl(Uri? baseUri, int ordinal)
    {
        return new Uri($"{baseUri?.AbsoluteUri}&comp=block&blockid={ToBlockId(ordinal)}");
    }

    public static string ToBlockId(int ordinal)
    {
        return Convert.ToBase64String(Encoding.UTF8.GetBytes($"block{ordinal:00000}"));
    }

    private static void AddPutBlockHeaders(HttpRequestMessage request, string apiVersion)
    {
        request.Headers.Add("x-ms-blob-type", BlobType);

        AddBlockBlobServiceHeaders(request, apiVersion);
    }

    private static void AddMetadataHeaderIfValueIsSet(HttpRequestMessage request, string name, string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return;
        }
        request.Headers.Add($"x-ms-meta-{name}", value);
    }

    private static void AddBlockBlobServiceHeaders(HttpRequestMessage request, string apiVersion)
    {
        request.Headers.Add("x-ms-version", apiVersion);
        request.Headers.Add("x-ms-date", DateTime.UtcNow.ToString("R"));
    }

    public static HttpRequestMessage CreateBlobBlockListRequest(long length, Uri blobUrl, int blockSizeBytes, string apiVersion, string? rootHash)
    {
        var content = CreateBlockListContent(length, blockSizeBytes);

        var putBlockUrl = new UriBuilder($"{blobUrl.AbsoluteUri}&comp=blocklist");

        var request = new HttpRequestMessage(HttpMethod.Put, putBlockUrl.Uri)
        {
            Content = content
        };

        AddMetadataHeaderIfValueIsSet(request, RootHashMetadataName, rootHash);
        AddBlockBlobServiceHeaders(request, apiVersion);
        return request;
    }

    public async Task<HttpResponseMessage> ExecuteHttpRequestAsync(Func<HttpRequestMessage> requestFactory)
    {
        return await retryPolicy.ExecuteAsync(() => ExecuteHttpRequestImplAsync(requestFactory));
    }

    private async Task<HttpResponseMessage> ExecuteHttpRequestImplAsync(Func<HttpRequestMessage> request)
    {
        HttpResponseMessage? response = null;

        try
        {
            try
            {
                response = await httpClient.SendAsync(request());

                response.EnsureSuccessStatusCode();
            }
            catch (HttpRequestException ex)
            {
                var status = response?.StatusCode;

                HandleHttpRequestException(status, ex);
            }
            catch (Exception ex)
            {
                if (ContainsRetriableException(ex))
                {
                    throw new RetriableException(ex.Message, ex);
                }

                throw;
            }
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Error executing request");

            response?.Dispose();
            throw;
        }

        return response!;
    }

    private static void HandleHttpRequestException(HttpStatusCode? status, HttpRequestException ex)
    {
        if (IsRetriableStatusCode(status))
        {
            throw new RetriableException(ex.Message, ex);
        }

        if (ContainsRetriableException(ex))
        {
            throw new RetriableException(ex.Message, ex);
        }

        throw ex;
    }


    public async Task<int> ExecuteHttpRequestAndReadBodyResponseAsync(PipelineBuffer buffer, Func<HttpRequestMessage> requestFactory)
    {
        return await retryPolicy.ExecuteAsync(() => ExecuteHttpRequestAndReadBodyResponseImplAsync(buffer, requestFactory));
    }

    private static bool ContainsRetriableException(Exception? ex)
    {
        if (ex is null)
        {
            return false;
        }

        if (ex is TimeoutException or IOException)
        {
            return true;
        }

        return ContainsRetriableException(ex.InnerException);
    }

    private async Task<int> ExecuteHttpRequestAndReadBodyResponseImplAsync(PipelineBuffer buffer, Func<HttpRequestMessage> requestFactory)
    {
        HttpResponseMessage? response = null;
        try
        {
            response = await httpClient.SendAsync(requestFactory(), HttpCompletionOption.ResponseHeadersRead);

            response.EnsureSuccessStatusCode();

            await using var data = await response.Content.ReadAsStreamAsync();

            await data.ReadExactlyAsync(buffer.Data, 0, buffer.Length);

        }
        catch (HttpRequestException ex)
        {
            var status = response?.StatusCode;

            HandleHttpRequestException(status, ex);
        }
        catch (Exception ex)
        {
            if (ContainsRetriableException(ex))
            {
                throw new RetriableException(ex.Message, ex);
            }

            throw;
        }
        finally
        {
            response?.Dispose();
        }

        return buffer.Length;
    }

    private static bool IsRetriableStatusCode(HttpStatusCode? responseStatusCode)
    {
        if (responseStatusCode is null)
        {
            return false;
        }

        if (responseStatusCode is
            HttpStatusCode.ServiceUnavailable or
            HttpStatusCode.GatewayTimeout or
            HttpStatusCode.TooManyRequests or
            HttpStatusCode.InternalServerError or
            HttpStatusCode.RequestTimeout)
        {
            return true;
        }

        return false;
    }

    private static StringContent CreateBlockListContent(long length, int blockSizeBytes)
    {
        var sb = new StringBuilder();

        sb.Append("<?xml version='1.0' encoding='utf-8'?><BlockList>");

        var parts = BlobSizeUtils.GetNumberOfParts(length, blockSizeBytes);

        for (var n = 0; n < parts; n++)
        {
            var blockId = ToBlockId(n);
            sb.Append($"<Latest>{blockId}</Latest>");
        }

        sb.Append("</BlockList>");

        var content = new StringContent(sb.ToString(), Encoding.UTF8, "text/plain");
        return content;
    }

    public static HttpRequestMessage CreateReadByRangeHttpRequest(PipelineBuffer buffer)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, buffer.BlobUrl);

        request.Headers.Range = new RangeHeaderValue(buffer.Offset, buffer.Offset + buffer.Length);
        return request;
    }

    public static AsyncRetryPolicy DefaultAsyncRetryPolicy(int retryAttempts)
    {
        return Policy
            .Handle<RetriableException>()
            .WaitAndRetryAsync(retryAttempts, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                onRetryAsync:
                (exception, _, retryCount, _) =>
                {
                    Logger.LogError(exception, "Retrying failed request. Retry count: {retryCount}", retryCount);
                    return Task.CompletedTask;
                });
    }
}
