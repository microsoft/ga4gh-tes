// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Models
{
    public class NodeTask
    {
        public string? ImageTag { get; set; }
        public string? ImageName { get; set; }
        public List<string>? CommandsToExecute { get; set; }
        public List<FileInput>? Inputs { get; set; }
        public List<FileOutput>? Outputs { get; set; }
    }

    public class FileOutput
    {
        public string? FullFileName { get; set; }
        public string? TargetUrl { get; set; }
        public SasResolutionStrategy SasStrategy { get; set; }
    }

    public class FileInput
    {
        public string? FullFileName { get; set; }
        public string? SourceUrl { get; set; }
        public SasResolutionStrategy SasStrategy { get; set; }
    }

    public enum SasResolutionStrategy
    {
        None,
        StorageAccountNameAndKey,
        TerraWsm
    }

    public class BlobTransferOptions
    {
        public int? BlockSize {get; set;} = 1024 * 1024 * 10; //10MiB  
        public int? NumberOfBuffers {get; set;} = 10;
        public int? NumberOfWriters {get; set;} = 10;
        public int? NumberOfReaders {get; set;} = 10;
        public int? BufferCapacity {get; set;} = 10; 
        public int? MemoryBufferCapacity {get; set;} = 10;
    }
}
