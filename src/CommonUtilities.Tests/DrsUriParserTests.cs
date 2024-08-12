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
        [DataRow(@"drs://hostname/abc123", @"hostname", @"abc123", true)]
        [DataRow(@"drs://hostname/abc-123", @"hostname", @"abc-123", true)]
        [DataRow(@"drs://hostname/abc_123", @"hostname", @"abc_123", true)]
        [DataRow(@"drs://hostname/abc.123", @"hostname", @"abc.123", true)]
        [DataRow(@"drs://hostname/abc~123", @"hostname", @"abc~123", true)]
        [DataRow(@"drs://hostname/abc%3A123", @"hostname", @"abc%3A123", true)]
        [DataRow(@"drs://hostname/abc%2F123", @"hostname", @"abc%2F123", true)]
        public void DrsUriParser_CorrectlyParsesDrsNamespaceStyleUrls(string uriString, string host, string path, bool isWellFormed)
        {
            Uri uri = new(uriString);

            Assert.IsNotNull(uri);
            Assert.AreEqual(isWellFormed, uri.IsWellFormedOriginalString());
            Assert.AreEqual(host, uri.Host);
            Assert.AreEqual($"/{path}", uri.AbsolutePath);
            Assert.AreEqual(uriString, uri.AbsoluteUri, ignoreCase: !isWellFormed);
            Assert.AreEqual(uriString, uri.ToString(), ignoreCase: true);
            Assert.AreEqual(path, uri.GetComponents(UriComponents.Path, UriFormat.Unescaped));
        }

        [DataTestMethod]
        [DataRow(@"drs://provider/namespace:abc123", @"provider/namespace", @"abc123", true)]
        [DataRow(@"drs://provider/namespace:abc-123", @"provider/namespace", @"abc-123", true)]
        [DataRow(@"drs://provider/namespace:abc_123", @"provider/namespace", @"abc_123", true)]
        [DataRow(@"drs://provider/namespace:abc.123", @"provider/namespace", @"abc.123", true)]
        [DataRow(@"drs://provider/namespace:abc~123", @"provider/namespace", @"abc~123", true)]
        [DataRow(@"drs://provider/namespace:abc%3A123", @"provider/namespace", @"abc%3A123", true)]
        [DataRow(@"drs://provider/namespace:abc%2F123", @"provider/namespace", @"abc%2F123", true)]
        [DataRow(@"drs://Provider/namespace:abc123", @"provider/namespace", @"abc123", false)]
        [DataRow(@"drs://provider/Namespace:abc123", @"provider/namespace", @"abc123", false)]
        [DataRow(@"drs://prefix:abc123", @"prefix", @"abc123", true)]
        [DataRow(@"drs://prefix:abc%3A123", @"prefix", @"abc%3A123", true)]
        [DataRow(@"drs://prefix:abc%2F123", @"prefix", @"abc%2F123", true)]
        [DataRow(@"drs://pre_fix:abc123", @"pre_fix", @"abc123", true)]
        [DataRow(@"drs://pre.fix:abc123", @"pre.fix", @"abc123", true)]
        public void DrsUriParser_CorrectlyParsesCompactIdStyleUrls(string uriString, string host, string path, bool isWellFormed)
        {
            Uri uri = new(uriString);

            Assert.IsNotNull(uri);
            Assert.AreEqual(isWellFormed, uri.IsWellFormedOriginalString());
            Assert.AreEqual(host, uri.Host);
            Assert.AreEqual($":{path}", uri.AbsolutePath);
            Assert.AreEqual(uriString, uri.AbsoluteUri, ignoreCase: !isWellFormed);
            Assert.AreEqual(uriString, uri.ToString(), ignoreCase: true);
            Assert.AreEqual(path, uri.GetComponents(UriComponents.Path, UriFormat.Unescaped));
        }

        [DataTestMethod]
        [DataRow(@"drs://foo")]
        [DataRow(@"drs://foo:b%az")]
        [DataRow(@"drs://foo-bar:baz")]
        [DataRow(@"drs://hostname/abc&123")]
        [DataRow(@"drs://hostname/abc/123")]
        [DataRow(@"drs://prefix:abc&123")]
        [DataRow(@"drs://prefix:abc/123")]
        [DataRow(@"drs://prefix:abc:123")]
        [DataRow(@"drs://provider/namespace:abc&123")]
        [DataRow(@"drs://provider/namespace:abc/123")]
        [DataRow(@"drs://provider/namespace:abc:123")]
        public void DrsUriParser_CorrectlyFailsToCreateMalformedUris(string uriString)
        {
            Assert.ThrowsException<UriFormatException>(() => _ = new Uri(uriString));
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void DrsUriParser_CorrectlyFailsUsingRelativeUrisWhenRelativeUriIsNotDrsId(bool compact)
        {
            Assert.IsTrue(Uri.TryCreate(compact ? @"drs://prefix:abc123" : @"drs://hostname/abc123", UriKind.Absolute, out var uri));
            Assert.IsFalse(Uri.TryCreate(uri, @"abc/123", out var result));
        }

        [DataTestMethod]
        [DataRow(@"drs://hostname/abc123", @"abc%3A123", @"drs://hostname/abc%3A123")]
        [DataRow(@"drs://prefix:abc123", @"abc%3A123", @"drs://prefix:abc%3A123")]
        public void DrsUriParser_CorrectlyCreatesUsingRelativeUris(string uriString, string relativeUri, string absoluteUri)
        {
            Assert.IsTrue(Uri.TryCreate(uriString, UriKind.Absolute, out var uri));
            Assert.IsTrue(Uri.TryCreate(uri, relativeUri, out var result));
            Assert.AreEqual(absoluteUri, result.AbsoluteUri);
            Assert.IsTrue(result.IsBaseOf(uri));
            Assert.IsTrue(uri.IsBaseOf(result));
        }
    }
}
