// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace CommonUtilities.Tests
{
    [TestClass]
    public class DrsUriParserTests
    {
        [TestInitialize]
        public void SetUp()
        {
            if (!UriParser.IsKnownScheme(DrsUriParser.UriSchemeDrs))
            {
                DrsUriParser.Register();
            }
        }

        [DataTestMethod]
        [DataRow(@"drs://hostname/abc123", @"hostname", @"abc123")]
        public void DrsUriParser_CorrectlyParsesDrsNamespaceStyleUrls(string uriString, string host, string path)
        {
            Uri uri = new(uriString);

            Assert.IsNotNull(uri);
            Assert.IsTrue(uri.IsWellFormedOriginalString());
            Assert.AreEqual(host, uri.Host);
            Assert.AreEqual($"/{path}", uri.AbsolutePath);
            Assert.AreEqual(uriString, uri.AbsoluteUri);
            Assert.AreEqual(path, uri.GetComponents(UriComponents.Path, UriFormat.Unescaped));
        }

        [DataTestMethod]
        [DataRow(@"drs://provider/namespace:abc123", @"provider/namespace", @"abc123")]
        [DataRow(@"drs://prefix:abc123", @"prefix", @"abc123")]
        public void DrsUriParser_CorrectlyParsesCompactIdStyleUrls(string uriString, string host, string path)
        {
            Uri uri = new(uriString);

            Assert.IsNotNull(uri);
            Assert.IsTrue(uri.IsWellFormedOriginalString());
            Assert.AreEqual(host, uri.Host);
            Assert.AreEqual($":{path}", uri.AbsolutePath);
            Assert.AreEqual(uriString, uri.AbsoluteUri);
            Assert.AreEqual(path, uri.GetComponents(UriComponents.Path, UriFormat.Unescaped));
        }

        [DataTestMethod]
        [DataRow(@"drs://foo")]
        [DataRow(@"drs://foo-bar:baz")]
        [DataRow(@"drs://hostname/abc&123")]
        [DataRow(@"drs://hostname/abc/123")]
        [DataRow(@"drs://preFix:abc123")]
        [DataRow(@"drs://prefix:abc&123")]
        [DataRow(@"drs://prefix:abc/123")]
        [DataRow(@"drs://prefix:abc:123")]
        [DataRow(@"drs://Provider/namespace:abc123")]
        [DataRow(@"drs://provider/Namespace:abc123")]
        [DataRow(@"drs://provider/namespace:abc&123")]
        [DataRow(@"drs://provider/namespace:abc/123")]
        [DataRow(@"drs://provider/namespace:abc:123")]
        public void DrsUriParser_CorrectlyFailsToCreateMalformedUris(string uriString)
        {
            Assert.ThrowsException<UriFormatException>(() => _ = new Uri(uriString));
        }
    }
}
