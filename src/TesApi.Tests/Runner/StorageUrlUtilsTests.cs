// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TesApi.Web.Options;
using TesApi.Web.Runner;
using TesApi.Web.Storage;

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Tests.Runner
{
    [TestClass, TestCategory("Unit")]
    public class StorageUrlUtilsTests
    {
        private const string CromwellStorageAccount = "default";

        [DataTestMethod]
        [DataRow("/foo/container/blob", $"https://foo{StorageUrlUtils.BlobEndpointHostNameSuffix}/container/blob")]
        [DataRow($"{StorageAccessProvider.CromwellPathPrefix}blob", $"https://{CromwellStorageAccount}{StorageUrlUtils.BlobEndpointHostNameSuffix}{StorageAccessProvider.CromwellPathPrefix}blob")]
        [DataRow("/foo/container", $"https://foo{StorageUrlUtils.BlobEndpointHostNameSuffix}/container/")] //empty blob is a valid scenario...
        [DataRow("/foo/container/", $"https://foo{StorageUrlUtils.BlobEndpointHostNameSuffix}/container/")] //empty blob is a valid scenario...
        public void ConvertLocalPathOrCromwellLocalPathToUrlTest_ValidInputProvided_ExpectedResult(string input, string expectedResult)
        {
            var result = StorageUrlUtils.ConvertLocalPathOrCromwellLocalPathToUrl(input, CromwellStorageAccount);

            Assert.AreEqual(expectedResult, result);
        }

        [DataTestMethod]
        [DataRow("/")]
        [DataRow("/foo")]
        public void ConvertLocalPathOrCromwellLocalPathToUrlTest_InvalidInputProvided_ThrowsException(string input)
        {
            Assert.ThrowsException<InvalidOperationException>(() =>
                StorageUrlUtils.ConvertLocalPathOrCromwellLocalPathToUrl(input, CromwellStorageAccount));
        }

        [TestMethod]
        public void ConvertLocalPathOrCromwellLocalPathToUrlTest_CromwellPathMissingCromwellStorageAccount_ThrowsException()
        {
            var cromwellLocalPath = $"{StorageAccessProvider.CromwellPathPrefix}blob";
            Assert.ThrowsException<ArgumentException>(() => StorageUrlUtils.ConvertLocalPathOrCromwellLocalPathToUrl(cromwellLocalPath, null));
        }

        [DataTestMethod]
        [DataRow($"https://foo{StorageUrlUtils.BlobEndpointHostNameSuffix}", true)]
        [DataRow($"https://foo{StorageUrlUtils.BlobEndpointHostNameSuffix}/", true)]
        [DataRow($"https://foo{StorageUrlUtils.BlobEndpointHostNameSuffix}/container", true)]
        [DataRow($"https://foo{StorageUrlUtils.BlobEndpointHostNameSuffix}/container/", true)]
        [DataRow($"https://foo{StorageUrlUtils.BlobEndpointHostNameSuffix}/container/blob", true)]
        [DataRow($"https://foo.bar.com/container/blob", false)]
        public void IsValidAzureStorageAccountUri_ValidInput_ExpectedResult(string input, bool expectedResult)
        {
            var result = StorageUrlUtils.IsValidAzureStorageAccountUri(input);

            Assert.AreEqual(expectedResult, result);
        }

        [DataTestMethod]
        [DataRow($"https://ext1{StorageUrlUtils.BlobEndpointHostNameSuffix}/cont/?sas=sig,https://ext2{StorageUrlUtils.BlobEndpointHostNameSuffix}/cont/?sas=sig", 2)]
        [DataRow($"https://ext1{StorageUrlUtils.BlobEndpointHostNameSuffix}/cont/?sas=sig;https://ext2{StorageUrlUtils.BlobEndpointHostNameSuffix}/cont/?sas=sig", 2)]
        [DataRow($"https://ext1{StorageUrlUtils.BlobEndpointHostNameSuffix}/cont/?sas=sig\nhttps://ext2{StorageUrlUtils.BlobEndpointHostNameSuffix}/cont/?sas=sig", 2)]
        [DataRow($"https://ext1{StorageUrlUtils.BlobEndpointHostNameSuffix}/cont/?sas=sig\rhttps://ext2{StorageUrlUtils.BlobEndpointHostNameSuffix}/cont/?sas=sig", 2)]
        [DataRow($"https://ext1{StorageUrlUtils.BlobEndpointHostNameSuffix}/cont/?sas=sig", 1)]
        [DataRow("", 0)]
        [DataRow(null, 0)]
        public void GetExternalStorageContainerInfos_ListProvided_ExpectedCount(string externalStorageList, int expectedCount)
        {
            var options = new StorageOptions() { ExternalStorageContainers = externalStorageList };

            var results = StorageUrlUtils.GetExternalStorageContainerInfos(options);

            Assert.AreEqual(expectedCount, results.Count);
        }

        [DataTestMethod]
        [DataRow("https://foo.bar/com?sas=token", "https://foo.bar/com")]
        [DataRow("https://foo.bar/com", "https://foo.bar/com")]
        [DataRow("https://foo.bar/com?", "https://foo.bar/com")]
        public void RemoveQueryStringFromUrl_ValidInput_RemovesQueryString(string input, string expectedUrl)
        {
            var result = StorageUrlUtils.RemoveQueryStringFromUrl(new Uri(input));

            Assert.AreEqual(expectedUrl, result);
        }

        [DataTestMethod]
        [DataRow("a=1", "sg=value", "a=1&sg=value")]
        [DataRow("", "sg=value", "sg=value")]
        [DataRow(null, "sg=value", "sg=value")]
        public void SetOrAddSasTokenToQueryString_ValidInputs_ExpectedQueryString(string existingQs, string sasToken, string expectedResult)
        {
            var result = StorageUrlUtils.SetOrAddSasTokenToQueryString(existingQs, sasToken);

            Assert.AreEqual(expectedResult, result);
        }

        [DataTestMethod]
        [DataRow("/roo/cont", true)]
        [DataRow("/roo", true)]
        [DataRow("https://foo.com", false)]
        [DataRow("", false)]
        [DataRow(null, false)]
        public void IsLocalAbsolutePath_ValidInputs_ExpectedResult(string input, bool expectedResult)
        {
            var result = StorageUrlUtils.IsLocalAbsolutePath(input);

            Assert.AreEqual(expectedResult, result);
        }
    }
}
