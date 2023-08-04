﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Storage;

namespace Tes.Runner.Test.Storage
{
    [TestClass]
    [TestCategory("Unit")]
    public class CloudProviderSchemeConverterTests
    {
        [DataTestMethod]
        [DataRow(@"https://blob.name", @"https://blob.name")]
        [DataRow(@"s3://broad-references/hg38/v0/Homo_sapiens_assembly38.dict", @"https://broad-references.s3.amazonaws.com/hg38/v0/Homo_sapiens_assembly38.dict")]
        [DataRow(@"gs://encode-pipeline-test-samples/encode-atac-seq-pipeline/ENCSR356KRQ/fastq_subsampled/rep2/pair1/ENCFF641SFZ.subsampled.400.fastq.gz", @"https://storage.googleapis.com/encode-pipeline-test-samples/encode-atac-seq-pipeline/ENCSR356KRQ/fastq_subsampled/rep2/pair1/ENCFF641SFZ.subsampled.400.fastq.gz")]
        public async Task CreateSasTokenWithStrategyAsyncTest_URIProvided_MatchesExpected(string sourceUri, string expectedUri)
        {
            var provider = new CloudProviderSchemeConverter();

            var result = await provider.CreateSasTokenWithStrategyAsync(sourceUri);

            Assert.IsNotNull(result);
            Assert.AreEqual(result.AbsoluteUri, new Uri(expectedUri).AbsoluteUri);
        }
    }
}
