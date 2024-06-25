// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;
using Polly.Retry;

namespace Tes.Runner.Transfer;
/// <summary>
/// A class containing the logic to create and make the HTTP requests for the blob block API.
/// </summary>
public class BlobApiHttpUtils(HttpClient httpClient, AsyncRetryPolicy retryPolicy)
{
    //https://learn.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
    public const string DefaultApiVersion = "2023-05-03";
    public const string BlockBlobType = "BlockBlob";
    public const string AppendBlobType = "AppendBlob";

    private readonly HttpClient httpClient = httpClient;
    private static readonly ILogger Logger = PipelineLoggerFactory.Create<BlobApiHttpUtils>();
    private readonly AsyncRetryPolicy retryPolicy = retryPolicy;


    public const string RootHashMetadataName = "md5_4mib_hashlist_root_hash";

    public BlobApiHttpUtils()
        : this(new HttpClient(), HttpRetryPolicyDefinition.DefaultAsyncRetryPolicy())
    { }

    public static HttpRequestMessage CreatePutBlockRequestAsync(PipelineBuffer buffer, string apiVersion)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, buffer.BlobPartUrl)
        {
            Content = new ByteArrayContent(buffer.Data, 0, buffer.Length)
        };

        AddPutBlockHeaders(request, apiVersion);
        return request;
    }

    public static HttpRequestMessage CreatePutAppendBlockRequestAsync(string data, Uri url, string apiVersion)
    {
        var appendBlockUrl = BlobApiHttpUtils.ParsePutAppendBlockUrl(url);

        var request = new HttpRequestMessage(HttpMethod.Put, appendBlockUrl)
        {
            Content = new StringContent(data)
        };

        AddBlobServiceHeaders(request, apiVersion);

        return request;
    }

    public static HttpRequestMessage CreatePutBlobRequestAsync(Uri blobUrl, string? content, string apiVersion,
        IDictionary<string, string>? tags, string blobType = BlockBlobType)
    {
        ArgumentNullException.ThrowIfNull(blobUrl);
        ArgumentException.ThrowIfNullOrEmpty(apiVersion, nameof(apiVersion));

        var request = new HttpRequestMessage(HttpMethod.Put, blobUrl);

        if (blobType == BlockBlobType && content is not null)
        {
            //only add content when creating a block blob
            request.Content = new StringContent(content);
        }

        AddPutBlobHeaders(request, apiVersion, tags, blobType);

        return request;
    }

    private static void AddPutBlobHeaders(HttpRequestMessage request, string apiVersion, IDictionary<string, string>? tags, string blobType)
    {
        request.Headers.Add("x-ms-blob-type", blobType);

        AddBlobServiceHeaders(request, apiVersion);

        if (tags is { Count: > 0 })
        {
            var tagsHeader = string.Join("&", tags.Select(t => $"{t.Key}={WebUtility.UrlEncode(t.Value)}"));

            request.Headers.Add("x-ms-tags", tagsHeader);
        }
    }

    public static Uri ParsePutBlockUrl(Uri? baseUri, int ordinal)
    {
        return new Uri($"{baseUri?.AbsoluteUri}&comp=block&blockid={ToBlockId(ordinal)}");
    }

    public static Uri ParsePutAppendBlockUrl(Uri? baseUri)
    {
        ArgumentNullException.ThrowIfNull(baseUri);

        var uriBuilder = new UriBuilder(baseUri);

        if (string.IsNullOrWhiteSpace(uriBuilder.Query))
        {
            uriBuilder.Query = "comp=appendblock";
        }
        else
        {
            uriBuilder.Query += "&comp=appendblock";
        }
        return uriBuilder.Uri;
    }

    public static string ToBlockId(int ordinal)
    {
        return Convert.ToBase64String(Encoding.UTF8.GetBytes($"block{ordinal:00000}"));
    }

    private static void AddPutBlockHeaders(HttpRequestMessage request, string apiVersion)
    {
        request.Headers.Add("x-ms-blob-type", BlockBlobType);

        AddBlobServiceHeaders(request, apiVersion);
    }

    private static void AddMetadataHeaderIfValueIsSet(HttpRequestMessage request, string name, string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return;
        }
        request.Headers.Add($"x-ms-meta-{name}", value);
    }
    private static void AddContentMd5HeaderIfValueIsSet(HttpRequestMessage request, string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return;
        }

        request.Headers.Add($"x-ms-blob-content-md5", Convert.ToBase64String(Encoding.UTF8.GetBytes(value)));
    }

    public static void AddBlobServiceHeaders(HttpRequestMessage request, string apiVersion)
    {
        request.Headers.Add("x-ms-version", apiVersion);
        request.Headers.Add("x-ms-date", DateTime.UtcNow.ToString("R"));
    }

    public static HttpRequestMessage CreateBlobBlockListRequest(long length, Uri blobUrl, int blockSizeBytes, string apiVersion, string? rootHash, string? contentMd5)
    {
        var content = CreateBlockListContent(length, blockSizeBytes);

        var putBlockUrl = new UriBuilder($"{blobUrl.AbsoluteUri}&comp=blocklist");

        var request = new HttpRequestMessage(HttpMethod.Put, putBlockUrl.Uri)
        {
            Content = content
        };

        AddMetadataHeaderIfValueIsSet(request, RootHashMetadataName, rootHash);
        AddContentMd5HeaderIfValueIsSet(request, contentMd5);
        AddBlobServiceHeaders(request, apiVersion);
        return request;
    }

    public async Task<HttpResponseMessage> ExecuteHttpRequestAsync(Func<HttpRequestMessage> requestFactory, CancellationToken cancellationToken = default)
    {
        return await retryPolicy.ExecuteAsync(ct => ExecuteHttpRequestImplAsync(requestFactory, ct), cancellationToken);
    }

    public static bool UrlContainsSasToken(string sourceUrl)
    {
        if (string.IsNullOrWhiteSpace(sourceUrl))
        {
            return false;
        }

        var blobBuilder = new BlobUriBuilder(new Uri(sourceUrl));

        return !string.IsNullOrWhiteSpace(blobBuilder.Sas?.Signature);
    }
    private async Task<HttpResponseMessage> ExecuteHttpRequestImplAsync(Func<HttpRequestMessage> request, CancellationToken cancellationToken)
    {
        HttpResponseMessage? response = null;

        try
        {
            try
            {
                response = await httpClient.SendAsync(request(), cancellationToken);

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


    public async Task<int> ExecuteHttpRequestAndReadBodyResponseAsync(PipelineBuffer buffer,
        Func<HttpRequestMessage> requestFactory, CancellationToken cancellationToken = default)
    {
        return await retryPolicy.ExecuteAsync(ct => ExecuteHttpRequestAndReadBodyResponseImplAsync(buffer, requestFactory, ct), cancellationToken);
    }

    private static bool ContainsRetriableException(Exception? ex)
    {
        if (ex is null)
        {
            return false;
        }

        if (ex is TimeoutException or IOException or SocketException)
        {
            return true;
        }

        return ContainsRetriableException(ex.InnerException);
    }

    private async Task<int> ExecuteHttpRequestAndReadBodyResponseImplAsync(PipelineBuffer buffer,
        Func<HttpRequestMessage> requestFactory, CancellationToken cancellationToken)
    {
        HttpResponseMessage? response = null;
        try
        {
            var request = requestFactory();

            response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);

            await ReadPartFromBodyAsync(buffer, response, cancellationToken);
        }
        catch (HttpRequestException ex)
        {
            Logger.LogDebug("Failed to process part. Part: {BufferFileName} Ordinal: {BufferOrdinal}. Error: {FailureMessage}", buffer.FileName, buffer.Ordinal, ex.Message);

            var status = response?.StatusCode;

            HandleHttpRequestException(status, ex);
        }
        catch (Exception ex)
        {
            Logger.LogDebug("Failed to process part. Part: {BufferFileName} Ordinal: {BufferOrdinal}. Error: {FailureMessage}", buffer.FileName, buffer.Ordinal, ex.Message);

            if (ContainsRetriableException(ex))
            {
                throw new RetriableException(ex.Message, ex);
            }

            throw;
        }
        finally
        {
            try
            {
                response?.Dispose();
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Error disposing response");
            }
        }

        return buffer.Length;
    }

    private async Task ReadPartFromBodyAsync(PipelineBuffer buffer,
        HttpResponseMessage response, CancellationToken cancellationToken)
    {
        response.EnsureSuccessStatusCode();

        await using var data = await response.Content.ReadAsStreamAsync(cancellationToken)
            .WaitAsync(httpClient.Timeout, cancellationToken);

        await data.ReadExactlyAsync(buffer.Data, 0, buffer.Length, cancellationToken)
            .AsTask()
            .WaitAsync(httpClient.Timeout, cancellationToken);
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
}
