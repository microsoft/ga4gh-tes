// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text;
using System.Text.RegularExpressions;

namespace CommonUtilities
{
    public partial class DrsUriParser : GenericUriParser
    {
        public const string UriSchemeDrs = "drs";

        private static readonly int _prefixLength = UriSchemeDrs.Length + Uri.SchemeDelimiter.Length;
        private static readonly string _prefix = UriSchemeDrs + Uri.SchemeDelimiter;
        private static readonly char[] _allowedAccessionOthers = ['-', '.', '_', '~'];

        private static readonly Regex _drsCompactId = DrsCompactId();

        public static void Register()
        {
            Register(new DrsUriParser(), UriSchemeDrs, defaultPort: -1);
        }

        public DrsUriParser() : base(
            GenericUriParserOptions.GenericAuthority |
            GenericUriParserOptions.NoFragment |
            GenericUriParserOptions.Idn |
            GenericUriParserOptions.IriParsing)
        {
        }

        protected override bool IsWellFormedOriginalString(Uri uri)
        {
            if (!_prefix.AsSpan().Equals(uri.OriginalString.AsSpan(0, _prefixLength), StringComparison.Ordinal))
            {
                return false;
            }

            if (IsCompactIdUri(uri))
            {
                return _drsCompactId.IsMatch(uri.OriginalString.AsSpan(_prefixLength));
            }
            else
            {
                // This is a hostname id
                if (!base.IsWellFormedOriginalString(uri))
                {
                    return false;
                }

                var path = base.GetComponents(uri, UriComponents.Path, UriFormat.Unescaped);

                var segments = path?.Split('/') ?? [];

                if (segments.Length != 1)
                {
                    return false;
                }

                return IsAccessionValid(segments.Last());
            }
        }

        private static bool IsAccessionValid(ReadOnlySpan<char> accession)
        {
            foreach (var ch in accession)
            {
                if (char.IsAsciiLetterOrDigit(ch))
                    continue;

                if (_allowedAccessionOthers.Contains(ch))
                    continue;

                return false;
            }

            return true;
        }

        protected override string GetComponents(Uri uri, UriComponents components, UriFormat format)
        {
            if (IsCompactIdUri(uri))
            {
                var keepDelimiter = IsComponent(components, UriComponents.KeepDelimiter);

                // This is a compact id
                StringBuilder builder = new();
                var match = _drsCompactId.Match(uri.OriginalString[(_prefixLength)..]);

                if (match.Success)
                {
                    if (IsComponent(components, UriComponents.Scheme))
                    {
                        builder.Append(UriSchemeDrs);
                    }

                    if (IsComponent(components, UriComponents.Host))
                    {
                        if (builder.Length > 0)
                        {
                            builder.Append(Uri.SchemeDelimiter);
                        }

                        // "provider_code" includes the separating '/' if a provider code was found due to the regex.
                        builder.Append(match.Groups["provider_code"].Value + match.Groups["namespace"].Value);
                    }

                    if (IsComponent(components, UriComponents.Path))
                    {
                        if (builder.Length > 0 || keepDelimiter)
                        {
                            builder.Append(':');
                        }

                        builder.Append(match.Groups["accession"].Value);
                    }
                }

                return builder.ToString();
            }
            else
            {
                // This is a hostname id
                if (components == UriComponents.Host)
                {
                    return GetModel(uri).Host;
                }

                if (components == UriComponents.StrongPort)
                {
                    var model = GetModel(uri);
                    return model.IsDefaultPort ? string.Empty : model.Port.ToString(System.Globalization.CultureInfo.InvariantCulture);
                }

                if (components == UriComponents.Path || components == (UriComponents.Path | UriComponents.KeepDelimiter))
                {
                    var path = GetModel(uri).LocalPath;

                    if (path[0] == '/' && !IsComponent(components, UriComponents.KeepDelimiter))
                    {
                        path = path.Substring(1);
                    }

                    return path;
                }

                return base.GetComponents(uri, components, format);
            }

            static Uri GetModel(Uri uri)
                => new($"{Uri.UriSchemeHttps}{uri.OriginalString[UriSchemeDrs.Length..]}");
        }

        private static bool IsComponent(UriComponents components, UriComponents mask)
            => (components & mask) != 0;

        protected override string? Resolve(Uri baseUri, Uri? relativeUri, out UriFormatException? parsingError)
        {
            // DRS relative URLs are simply not supported. Force an error.
            return base.Resolve(new Uri(".", UriKind.Relative), relativeUri, out parsingError);
        }

        protected override void InitializeAndValidate(Uri uri, out UriFormatException? parsingError)
        {
            // Hostname URIs are adequately processed by GenericUriParser.
            base.InitializeAndValidate(uri, out parsingError);

            // Validate accessions/ids
            if (parsingError is null && !IsCompactIdUriValid(uri))
            {
                // This is a hostname URI. However we turned off the host uri parsing in base.InitializeAndValidate (to prevent file-based URIs), so we'll do it here.
                if (!IsHostnameUriValid(uri))
                {
                    parsingError = new UriFormatException("Malformed DRS URI: slashes are not allowed in the DRS ID.");
                }
                else
                {
                    switch (Uri.CheckHostName(uri.Host))
                    {
                        case UriHostNameType.Dns:
                        case UriHostNameType.IPv4:
                        case UriHostNameType.IPv6:
                            break;

                        case UriHostNameType.Basic:
                        default:
                            parsingError = new("Invalid URI: The Authority/Host could not be parsed.");
                            break;
                    }
                }
            }

            static bool IsHostnameUriValid(Uri uri)
            {
                if (!uri.IsAbsoluteUri || uri.IsUnc)
                {
                    return false;
                }

                if (new Uri($"{Uri.UriSchemeHttps}{uri.OriginalString[UriSchemeDrs.Length..]}").HostNameType == UriHostNameType.Basic)
                {
                    return false;
                }

                return IsDrsIdValid(uri);
            }

            static bool IsCompactIdUriValid(Uri uri)
            {
                if (!IsCompactIdUri(uri))
                {
                    return false;
                }

                return IsDrsIdValid(uri);
            }

            static bool IsDrsIdValid(Uri uri)
            {
                var path = uri.AbsolutePath;

                // Return false if it appears a compact id URI with provider_code was parsed. Note that hostname Uris don't allow unescaped embedded slashes in the path.
                return !path.TrimStart('/').Any(c => c == '/') && !path.TrimStart(':').Any(c => c == ':') && IsAccessionValid(uri.Segments.Last().TrimStart(':'));
            }
        }

        private static bool IsCompactIdUri(Uri uri)
            => uri.OriginalString.StartsWith(_prefix, StringComparison.OrdinalIgnoreCase) && _drsCompactId.IsMatch(uri.OriginalString.AsSpan(_prefixLength));

        // https://ga4gh.github.io/data-repository-service-schemas/docs/#tag/DRS-API-Principles/DRS-IDs
        // https://ga4gh.github.io/data-repository-service-schemas/docs/more-background-on-compact-identifiers.html#tag/Background-on-Compact-Identifier-Based-URIs
        [GeneratedRegex("\\A(?<provider_code>[\\._a-z]+?/)?(?<namespace>[\\._a-z]+?):(?<accession>[%-\\.0-9A-Z_a-z~]+?)\\Z", RegexOptions.ExplicitCapture | RegexOptions.CultureInvariant)]
        private static partial Regex DrsCompactId();
    }
}
