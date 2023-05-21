// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;

namespace Tes.Runner.Transfer;

public class PipelineBuffer
{
    public string FileName { get; set; } = null!;
    public FileStream? FileStream { get; set; }
    public byte[] Data { get; set; } = null!;
    public long Offset { get; set; }
    public int Length { get; set; }
    public Uri? BlobUrl { get; set; }
    public Uri? BlobPartUrl { get; set; }
    public int Ordinal { get; set; }
    public int NumberOfParts { get; set; }
    public long FileSize { get; set; }
    public Channel<FileStream> FileHandlerPool { get; set; } = null!;
    public Md5Processor? Md5Processor { get; set; }
}

