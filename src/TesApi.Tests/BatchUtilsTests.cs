// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TesApi.Web;

namespace TesApi.Tests
{
    [TestClass]
    public class BatchUtilsTests
    {
        [DataTestMethod] // https://datatracker.ietf.org/doc/html/rfc4648#section-10
        [DataRow("", "")]
        [DataRow("f", "MY======")]
        [DataRow("fo", "MZXQ====")]
        [DataRow("foo", "MZXW6===")]
        [DataRow("foob", "MZXW6YQ=")]
        [DataRow("fooba", "MZXW6YTB")]
        [DataRow("foobar", "MZXW6YTBOI======")]
        public void ValidateConvertToBase32(string data, string expected)
            => Assert.AreEqual(expected, BatchUtils.ConvertToBase32(Encoding.UTF8.GetBytes(data)));
    }
}
