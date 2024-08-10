// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.RegularExpressions;

namespace CommonUtilities
{
    /// <summary>
    /// Uri parser for DRS scheme
    /// </summary>
    /// <seealso cref="GenericUriParser" />
    // -------------------------------------------------------------------------------------------------------------
    // DRS URI style |          Host          |    Path    | Description
    // -------------------------------------------------------------------------------------------------------------
    // Compact       | Prefix                 | Accession  | If prefix contains a '/', it is provider_code/namespace.
    // Hostname      | [UserInfo@]Host[:Port] | ID         | Per the spec, ID and Accession are the same thing.
    // -------------------------------------------------------------------------------------------------------------

    // Hostname style are like HTTP, except they don't have fragments and the Path is limited to one level.
    // Spec is silent on Query, it's not currently implemented here.

    // Compact style are not IETL-valid, which is the raison d'être of this implementation.
    public sealed partial class DrsUriParser : GenericUriParser
    {
        private static readonly int _prefixLength = UriSchemeDrs.Length + Uri.SchemeDelimiter.Length;
        private static readonly string _prefix = UriSchemeDrs + Uri.SchemeDelimiter;
        private static readonly char[] _allowedAccessionOthers = ['-', '.', '_', '~'];

        private static readonly Regex _drsCompactId = DrsCompactId();

        /// <summary>
        /// The URI scheme DRS
        /// </summary>
        public const string UriSchemeDrs = "drs";

        /// <summary>
        /// Registers this parser with the runtime.
        /// </summary>
        public static void Register()
        {
            if (!IsKnownScheme(UriSchemeDrs))
            {
                Register(new DrsUriParser(), UriSchemeDrs, defaultPort: -1);
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public DrsUriParser() : base(
            GenericUriParserOptions.GenericAuthority |
            GenericUriParserOptions.NoFragment |
            GenericUriParserOptions.DontCompressPath |
            GenericUriParserOptions.Idn |
            GenericUriParserOptions.IriParsing)
        { }

        private static bool IsWellFormedSchemeAndDelimiter(Uri uri)
            => _prefix.AsSpan().Equals(uri.OriginalString.AsSpan(0, _prefixLength), StringComparison.Ordinal);

        private static bool IsWellFormedNamespaceOriginalString(Uri uri, IDrsParser parser)
        {
            if (!IsWellFormedSchemeAndDelimiter(uri))
            {
                return false;
            }

            UriBuilder builder = new(new Uri($"{Uri.UriSchemeHttps}{uri.OriginalString[UriSchemeDrs.Length..]}"));

            var path = builder.Path ?? string.Empty;
            builder.Path = string.Empty;

            if (!parser.IsWellFormedOriginalString(builder.Uri))
            {
                return false;
            }

            if (path.StartsWith('/'))
            {
                path = path[1..];
            }

            return IsAccessionValid(path);
        }

        private static bool IsWellFormedCompactIdOriginalString(Uri uri)
        {
            if (!IsWellFormedSchemeAndDelimiter(uri))
            {
                return false;
            }

            return _drsCompactId.IsMatch(uri.OriginalString.AsSpan(_prefixLength)) &&
                IsAccessionValid(uri.OriginalString.AsSpan(uri.OriginalString.LastIndexOf(':') + 1).ToString());
        }

        /// <summary>
        /// Determines whether the value is a legal DRS ID.
        /// </summary>
        /// <param name="accession">The accession/id.</param>
        /// <returns>
        ///   <c>true</c> if the value matches the spec; otherwise, <c>false</c>.
        /// </returns>
        private static bool IsAccessionValid(string accession)
        {
            for (var i = 0; i < accession.Length; ++i)
            {
                var ch = accession[i];

                if (char.IsAsciiLetterOrDigit(ch) || _allowedAccessionOthers.Contains(ch))
                {
                    continue;
                }

                var start = i;
                _ = Uri.HexUnescape(accession, ref i);

                if (--i == start)
                {
                    return false;
                }
            }

            return true;
        }

        protected override UriParser OnNewUri() => new DrsParser();

        private interface IDrsParser
        {
            bool IsWellFormedOriginalString(Uri uri);
        }

        private sealed partial class DrsParser : UriParser, IDrsParser
        {
            private static readonly Regex _parseDrsCompactId = ParseDrsCompactId();

            // These objects hold the following parts of the URI as applicable according to uri style: UserInfo, Host, Port, Path, & Query.
            // Only one of them should ever be non null.
            private Uri? _namespaceAsHttp;
            private Match? _compactId;

            /// <summary>
            /// Gets a value indicating whether this instance is compact identifier.
            /// </summary>
            /// <value>
            ///   <c>true</c> if the associated Uri is a drs compact identifier uri; otherwise, <c>false</c>.
            /// </value>
            /// <exception cref="System.InvalidOperationException">Uri initialization and validation is not complete.</exception>
            private bool IsCompactId
            {
                get
                {
                    if (_namespaceAsHttp is null && _compactId is null)
                    {
                        throw new InvalidOperationException("Uri initialization and validation is not complete.");
                    }

                    return _compactId is not null;
                }
            }

            /// <inheritdoc/> 
            protected override bool IsBaseOf(Uri baseUri, Uri relativeUri)
                => UriSchemeDrs.Equals(baseUri.Scheme, StringComparison.OrdinalIgnoreCase) && UriSchemeDrs.Equals(relativeUri.Scheme, StringComparison.OrdinalIgnoreCase)
                ? baseUri.Host.Equals(relativeUri.Host, StringComparison.OrdinalIgnoreCase) &&
                    baseUri.Port.Equals(relativeUri.Port) &&
                    baseUri.Query.Equals(relativeUri.Query, StringComparison.Ordinal) // TODO: review
                : base.IsBaseOf(baseUri, relativeUri);

            /// <inheritdoc/>
            protected override bool IsWellFormedOriginalString(Uri uri)
                => IsCompactId
                ? IsWellFormedCompactIdOriginalString(uri)
                : IsWellFormedNamespaceOriginalString(uri, this);

            /// <inheritdoc/>
            protected override string? Resolve(Uri baseUri, Uri? relativeUri, out UriFormatException? parsingError)
            {
                if (relativeUri is null || string.IsNullOrWhiteSpace(relativeUri.OriginalString) || !IsAccessionValid(relativeUri.OriginalString))
                {
                    parsingError = new("relativeUri is not a valid DRS ID");
                    return null;
                }

                return IsCompactId
                    ? ResolveCompactId(baseUri, relativeUri, out parsingError)
                    : base.Resolve(baseUri, relativeUri, out parsingError);
            }

            private static string? ResolveCompactId(Uri baseUri, Uri relativeUri, out UriFormatException? parsingError)
            {
                parsingError = null;
                return $"{_prefix}{baseUri.Host}:{relativeUri.OriginalString}";
            }

            /// <inheritdoc/>
            protected override string GetComponents(Uri uri, UriComponents components, UriFormat format)
            {
                return IsCompactId
                    ? GetCompactIdComponents(uri, components, format)
                    : GetNamespaceComponents(uri, components, format);
            }

            private string GetCompactIdComponents(Uri uri, UriComponents components, UriFormat format) => components switch
            {
                // UriBuilder
                UriComponents.AbsoluteUri => _prefix + GetCompactIdComponents(uri, UriComponents.Host, UriFormat.UriEscaped) + GetCompactIdComponents(uri, UriComponents.Path | UriComponents.KeepDelimiter, UriFormat.UriEscaped),

                // "provider_code" includes the separating '/' if a provider code was found due to the regex.
                UriComponents.Host => _compactId!.Groups["provider_code"].Value.ToLowerInvariant() + _compactId!.Groups["namespace"].Value.ToLowerInvariant(),

                var c when c.AreComponentExactly(UriComponents.Path, UriComponents.KeepDelimiter) =>
                    (components.AreComponentsIn(UriComponents.KeepDelimiter) ? ":" : string.Empty) + _compactId!.Groups["accession"].Value,

                // Base class implementation will call back here for (some of) the individual elements.
                _ => base.GetComponents(uri, components, format),
            };

            private string GetNamespaceComponents(Uri uri, UriComponents components, UriFormat format) => components switch
            {
                UriComponents.Host => _namespaceAsHttp!.Host,

                UriComponents.StrongPort => _namespaceAsHttp!.IsDefaultPort ? string.Empty : _namespaceAsHttp!.Port.ToString(System.Globalization.CultureInfo.InvariantCulture),

                var c when c.AreComponentExactly(UriComponents.Path, UriComponents.KeepDelimiter) => new Func<string>(() =>
                {
                    var path = _namespaceAsHttp!.AbsolutePath;

                    if (path[0] == '/' && !components.AreComponentsIn(UriComponents.KeepDelimiter))
                    {
                        path = path[1..];
                    }

                    return path;
                })(),

                // Base class implementation will call back here for (some of) the individual elements.
                _ => base.GetComponents(uri, components, format),
            };

            /// <inheritdoc/>
            protected override void InitializeAndValidate(Uri uri, out UriFormatException? parsingError)
            {
                if (!uri.OriginalString.StartsWith(_prefix, StringComparison.OrdinalIgnoreCase))
                {
                    parsingError = new("Invalid DRS URI: Unexpected schema in Uri.");
                    return;
                }

                // Hostname URIs are mostly processed by GenericUriParser.
                base.InitializeAndValidate(uri, out parsingError);

                if (parsingError is not null)
                {
                    return;
                }

                var uriWithoutPrefix = uri.OriginalString.AsSpan(_prefixLength);

                var idxOfEndOfPath = uriWithoutPrefix.IndexOfAny(['?', '#']);
                var authorityAndPath = idxOfEndOfPath == -1 ? uriWithoutPrefix : uriWithoutPrefix[..idxOfEndOfPath];

                if (authorityAndPath.Count('@') == 0 && authorityAndPath.Count(':') == 1 && authorityAndPath.Count('/') < 2) // zero '@', one ':' and zero or one '/'
                {
                    // compact Id style
                    _compactId = _parseDrsCompactId.Match(uriWithoutPrefix.ToString());

                    if (!_compactId.Success)
                    {
                        parsingError = new("Invalid DRS URI: Malformed Compact ID URI");
                    }
                }

                if (_compactId is null)
                {
                    // hostname style
                    // This https uri is used to parse out the escaped Host, Port, and Path uri properties because GenericUriParser is unable to do it correctly with our settings.
                    _namespaceAsHttp = new(Uri.UriSchemeHttps + Uri.SchemeDelimiter + uriWithoutPrefix.ToString());

                    if (!IsHostnameUriValid(uri))
                    {
                        parsingError = new UriFormatException("Invalid DRS URI: Malformed Hostname URI");
                    }
                    else
                    {
                        // This is a hostname URI. However we turned off the host uri parsing in base.InitializeAndValidate (to prevent forming filesystem style URIs), so we'll validate it here.
                        switch (Uri.CheckHostName(uri.Host))
                        {
                            case UriHostNameType.Dns:
                            case UriHostNameType.IPv4:
                            case UriHostNameType.IPv6:
                                break;

                            case UriHostNameType.Basic:
                            default:
                                parsingError = new("Invalid DRS URI: The Authority/Host could not be parsed.");
                                break;
                        }
                    }
                }

                if (!(parsingError is null && IsDrsIdValid(uri)))
                {
                    parsingError = new("Invalid DRS URI: Invalid DSR ID/Accession.");
                }

                static bool IsHostnameUriValid(Uri uri)
                {
                    if (!uri.IsAbsoluteUri)
                    {
                        return false;
                    }

                    // If this was parsed as ither a UNC or a local-file style path, reject it.
                    if (uri.IsUnc || new Uri($"{Uri.UriSchemeHttps}{uri.OriginalString[UriSchemeDrs.Length..]}").HostNameType == UriHostNameType.Basic)
                    {
                        return false;
                    }

                    return true;
                }

                static bool IsDrsIdValid(Uri uri)
                {
                    var path = uri.AbsolutePath;

                    if (path.IndexOfAny(['/', ':']) == 0)
                    {
                        path = path[1..];
                    }

                    if (string.IsNullOrEmpty(path))
                    {
                        return false;
                    }

                    // Return false if it appears a compact id URI with provider_code was parsed. Note that hostname Uris don't allow unescaped embedded slashes in the path.
                    return !path.Any(c => c == '/') && !path.Any(c => c == ':') && IsAccessionValid(path);
                }
            }

            bool IDrsParser.IsWellFormedOriginalString(Uri uri) => base.IsWellFormedOriginalString(uri);

            // Same as DrsCompactId(), except with RegexOptions.IgnoreCase to allow case insensitive comparisons.
            [GeneratedRegex(@"\A(?<provider_code>[\.0-9_a-z]+?/)?(?<namespace>[\.0-9_a-z]+?):(?<accession>[%-\.0-9A-Z_a-z~]+?)\Z", RegexOptions.ExplicitCapture | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase)]
            private static partial Regex ParseDrsCompactId();
        }

        // https://ga4gh.github.io/data-repository-service-schemas/docs/#tag/DRS-API-Principles/DRS-IDs
        // https://ga4gh.github.io/data-repository-service-schemas/docs/more-background-on-compact-identifiers.html#tag/Background-on-Compact-Identifier-Based-URIs
        [GeneratedRegex(@"\A(?<provider_code>[\.0-9_a-z]+?/)?(?<namespace>[\.0-9_a-z]+?):(?<accession>[%-\.0-9A-Z_a-z~]+?)\Z", RegexOptions.ExplicitCapture | RegexOptions.CultureInvariant)]
        private static partial Regex DrsCompactId();
    }

    internal static partial class UriComponentExtensions
    {
        /// <summary>
        /// Determines whether any of the specified components are included in the mask.
        /// </summary>
        /// <param name="components">The components to consider.</param>
        /// <param name="mask">The desired components.</param>
        /// <returns>
        ///   <c>true</c> if any of the specified components is included in the mask; otherwise, <c>false</c>.
        /// </returns>
        public static bool AreComponentsIn(this UriComponents components, UriComponents mask) => (components & mask) != 0;

        /// <summary>
        /// Determines whether the specified components are all included components, except for those in the mask.
        /// </summary>
        /// <param name="components">The components to consider.</param>
        /// <param name="value">The remaining components.</param>
        /// <param name="ignore">The components to ignore.</param>
        /// <returns>
        ///   <c>true</c> if all and only the components in <paramref name="value"/> are found in <paramref name="components"/>, excluding the components in <paramref name="ignore"/>; otherwise, <c>false</c>.
        /// </returns>
        public static bool AreComponentExactly(this UriComponents components, UriComponents value, UriComponents ignore = 0) => (components & ~ignore) == value;
    }
}
