// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Models
{
    public class BlockBlobDatabaseOptions
    {
        public const string SectionName = "BlockBlobDatabase";

        public string StorageAccountName { get; set; }
        public string ContainerName { get; set; } = "testasksdb";
        public string ContainerSasToken { get; set; }
    }
}
