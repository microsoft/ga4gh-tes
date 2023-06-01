// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Tes.Runner.Storage;

namespace Tes.Runner.Storage.Tests
{
    [TestClass()]
    public class CloudProviderSchemeConverterTests
    {
        [DataTestMethod]
        [DataRow(@"https://blob.name", @"https://blob.name")]
        [DataRow(@"s3://broad-references/hg38/v0/Homo_sapiens_assembly38.dict", @"https://broad-references.s3.amazonaws.com/hg38/v0/Homo_sapiens_assembly38.dict")]
        [DataRow(@"gs://encode-pipeline-test-samples/encode-atac-seq-pipeline/ENCSR356KRQ/fastq_subsampled/rep2/pair1/ENCFF641SFZ.subsampled.400.fastq.gz", @"https://storage.googleapis.com/encode-pipeline-test-samples/encode-atac-seq-pipeline/ENCSR356KRQ/fastq_subsampled/rep2/pair1/ENCFF641SFZ.subsampled.400.fastq.gz")]
        public async Task CreateSasTokenWithStrategyAsyncTest_URIProvided_MatchesExpected(string sourceURI, string expectedURI)
        {
            var provider = new CloudProviderSchemeConverter();

            var result = await provider.CreateSasTokenWithStrategyAsync(sourceURI);

            Assert.IsNotNull(result);
            Assert.AreEqual(result.AbsoluteUri, new Uri(expectedURI).AbsoluteUri);
        }
    }
}
