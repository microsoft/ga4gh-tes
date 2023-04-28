using System.Text;

namespace Tes.Runner.Transfer;
/// <summary>
/// A class containing the logic to create the HTTP requests for the blob block API.
/// </summary>
public class BlobBlockApiHttpUtils
{
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
}
