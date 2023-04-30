﻿using System.Net;
using System.Net.Http.Headers;
using System.Text;
using Polly;
using Polly.Retry;

namespace Tes.Runner.Transfer;
/// <summary>
/// A class containing the logic to create and make the HTTP requests for the blob block API.
/// </summary>
public class BlobBlockApiHttpUtils
{
    private const int MaxRetryCount = 10;
    private static readonly HttpClient HttpClient = new HttpClient();
    private static readonly AsyncRetryPolicy RetryPolicy = Policy
        .Handle<RetriableException>()
        .WaitAndRetryAsync(MaxRetryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));

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
        return Convert.ToBase64String(Encoding.UTF8.GetBytes($"block{ordinal}"));
    }

    private static void AddPutBlockHeaders(HttpRequestMessage request, string apiVersion)
    {
        request.Headers.Add("x-ms-blob-type", "BlockBlob");
        AddBlockBlobServiceHeaders(request, apiVersion);
    }

    private static void AddBlockBlobServiceHeaders(HttpRequestMessage request, string apiVersion)
    {
        //TODO: Move this version to the blob options.
        request.Headers.Add("x-ms-version", apiVersion);
        request.Headers.Add("x-ms-date", DateTime.UtcNow.ToString("R"));
    }

    public static HttpRequestMessage CreateBlobBlockListRequest(long length, Uri blobUrl, int blockSize, string apiVersion)
    {
        var content = CreateBlockListContent(length, blockSize);

        var putBlockUrl = new UriBuilder($"{blobUrl.AbsoluteUri}&comp=blocklist");

        var request = new HttpRequestMessage(HttpMethod.Put, putBlockUrl.Uri)
        {
            Content = content
        };

        AddBlockBlobServiceHeaders(request, apiVersion);
        return request;
    }

    public static async Task<HttpResponseMessage> ExecuteHttpRequestAsync(HttpRequestMessage request)
    {
        return await RetryPolicy.ExecuteAsync(() => ExecuteHttpRequestImplAsync(request));
    }

    private static async Task<HttpResponseMessage> ExecuteHttpRequestImplAsync(HttpRequestMessage request)
    {
        var response = await HttpClient.SendAsync(request);

        try
        {
            response.EnsureSuccessStatusCode();
        }
        catch (HttpRequestException ex)
        {
            if (IsRetriableStatusCode(response.StatusCode))
            {
                throw new RetriableException(ex.Message, ex);
            }
            throw;
        }
        return response;
    }

    public static async Task<int> ExecuteHttpRequestAndReadBodyResponseAsync(PipelineBuffer buffer, HttpRequestMessage request)
    {
        return await RetryPolicy.ExecuteAsync(() => ExecuteHttpRequestAndReadBodyResponseImplAsync(buffer, request));
    }

    private static async Task<int> ExecuteHttpRequestAndReadBodyResponseImplAsync(PipelineBuffer buffer, HttpRequestMessage request)
    {
        var response = await HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);

        try
        {
            response.EnsureSuccessStatusCode();

            var data = await response.Content.ReadAsStreamAsync();

            await data.ReadExactlyAsync(buffer.Data, 0, buffer.Length);

            return buffer.Length;
        }
        catch (HttpRequestException ex)
        {
            if (IsRetriableStatusCode(response.StatusCode))
            {
                throw new RetriableException(ex.Message, ex);
            }
            throw;
        }
        catch (IOException ex)
        {
            throw new RetriableException(ex.Message, ex);
        }
        finally
        {
            response.Dispose();
        }
    }

    private static bool IsRetriableStatusCode(HttpStatusCode responseStatusCode)
    {
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

    private static StringContent CreateBlockListContent(long length, int blockSize)
    {
        var sb = new StringBuilder();

        sb.Append("<?xml version='1.0' encoding='utf-8'?><BlockList>");

        var parts = BlobSizeUtils.GetNumberOfParts(length, blockSize);

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
