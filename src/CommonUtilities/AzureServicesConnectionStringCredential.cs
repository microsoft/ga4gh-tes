// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;
using Azure.Core;
using CommonUtilities.AzureCloud;
using Microsoft.Extensions.Configuration;

namespace CommonUtilities
{
    /// <summary>
    /// Enables authentication to Microsoft Entra ID using AzureServicesAuthConnectionString syntax
    /// </summary>
    /// <remarks>This is adapted from <c>Microsoft.Azure.Services.AppAuthentication.AzureServiceTokenProvider</c> and <c>Azure.Identity.EnvironmentCredential</c></remarks>
    public class AzureServicesConnectionStringCredential : TokenCredential
    {
        private readonly TokenCredential credential;
        //private readonly AzureServicesConnectionStringCredentialOptions options;

        public AzureServicesConnectionStringCredential(AzureServicesConnectionStringCredentialOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);
            //options.Validate();

            //this.options = options;
            this.credential = AzureServicesConnectionStringCredentialFactory.Create(options);
        }

        /// <inheritdoc/>
        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            return credential.GetToken(requestContext, cancellationToken);
        }

        /// <inheritdoc/>
        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            return credential.GetTokenAsync(requestContext, cancellationToken);
        }
    }

    /// <summary>
    /// Options used to configure the <see cref="AzureServicesConnectionStringCredential"/>.
    /// </summary>
    /// <remarks>This is adapted from <c>Azure.Identity.EnvironmentCredentialOptions</c></remarks>
    public class AzureServicesConnectionStringCredentialOptions : Azure.Identity.TokenCredentialOptions
    {
        [Microsoft.Extensions.DependencyInjection.ActivatorUtilitiesConstructor]
        public AzureServicesConnectionStringCredentialOptions(IConfiguration? configuration, AzureCloudConfig armEndpoints)
            : this()
        {
            Configuration = configuration;
            SetInitialState(armEndpoints);
            ConnectionString = GetEnvironmentVariable("AzureServicesAuthConnectionString")!;
        }

        public AzureServicesConnectionStringCredentialOptions(string connectionString, AzureCloudConfig armEndpoints)
            : this()
        {
            SetInitialState(armEndpoints);
            ConnectionString = connectionString;
        }

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        private AzureServicesConnectionStringCredentialOptions()
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        {
            AdditionallyAllowedTenants = [];
        }

        private void SetInitialState(AzureCloudConfig armEndpoints)
        {
            (GetEnvironmentVariable("AZURE_ADDITIONALLY_ALLOWED_TENANTS") ?? string.Empty).Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ForEach(AdditionallyAllowedTenants.Add);
            TenantId = GetEnvironmentVariable("AZURE_TENANT_ID")!;
            AuthorityHost = armEndpoints.AuthorityHost ?? new(armEndpoints.Authentication?.LoginEndpointUrl ?? throw new ArgumentException("AuthorityHost is missing", nameof(armEndpoints)));
            Audience = armEndpoints.ArmEnvironment?.Audience ?? armEndpoints.Authentication?.Audiences?.LastOrDefault() ?? throw new ArgumentException("Audience is missing", nameof(armEndpoints));
            Resource = new(armEndpoints.ResourceManagerUrl ?? throw new ArgumentException("ResourceManager is missing", nameof(armEndpoints)));

            if (string.IsNullOrWhiteSpace(TenantId))
            {
                TenantId = armEndpoints.Authentication?.Tenant ?? throw new ArgumentException("TenantId is missing", nameof(armEndpoints));
            }
        }

        /// <summary>
        /// The connection string to connect to azure services. This value defaults to the value of the environment variable AzureServicesAuthConnectionString.
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// The authentication audience. This value is related to <c>Azure.ResourceManager.ArmEnvironment.DefaultScope</c>.
        /// </summary>
        public string Audience { get; set; }

        /// <summary>
        /// The azure service for which to obtain credentials.
        /// </summary>
        public Uri Resource { get; set; }

        /// <summary>
        /// The ID of the tenant to which the credential will authenticate by default. This value defaults to the value of the environment variable AZURE_TENANT_ID.
        /// </summary>
        public string TenantId { get; set; }

        /// <summary>
        /// Gets or sets the setting which determines whether or not instance discovery is performed when attempting to authenticate.
        /// Setting this to true will completely disable both instance discovery and authority validation.
        /// This functionality is intended for use in scenarios where the metadata endpoint cannot be reached, such as in private clouds or Azure Stack.
        /// The process of instance discovery entails retrieving authority metadata from https://login.microsoft.com/ to validate the authority.
        /// By setting this to <c>true</c>, the validation of the authority is disabled.
        /// As a result, it is crucial to ensure that the configured authority host is valid and trustworthy."
        /// </summary>
        public bool DisableInstanceDiscovery { get; set; }

        /// <summary>
        /// Options controlling the storage of the token cache.
        /// </summary>
        public Azure.Identity.TokenCachePersistenceOptions TokenCachePersistenceOptions { get; set; }

        /// <summary>
        /// Specifies tenants in addition to the specified <see cref="TenantId"/> for which the credential may acquire tokens.
        /// Add the wildcard value "*" to allow the credential to acquire tokens for any tenant the logged in account can access.
        /// If no value is specified for <see cref="TenantId"/>, this option will have no effect on that authentication method, and the credential will acquire tokens for any requested tenant when using that method.
        /// This value defaults to the value of the environment variable AZURE_ADDITIONALLY_ALLOWED_TENANTS.
        /// </summary>
        public IList<string> AdditionallyAllowedTenants { get; }

        internal IConfiguration? Configuration { get; }

        private string? GetEnvironmentVariable(string key) => GetConfigurationVariable(key) ?? Environment.GetEnvironmentVariable(key);

        private string? GetConfigurationVariable(string key) => Configuration is null ? default : Configuration[key];

        //internal void Validate()
        //{
        //    throw new NotImplementedException();
        //}

        internal Azure.Identity.AzureCliCredential CreateAzureCliCredential()
        {
            return new(ConfigureOptions(new Azure.Identity.AzureCliCredentialOptions()));
        }

        internal Azure.Identity.VisualStudioCredential CreateVisualStudioCredential()
        {
            return new(ConfigureOptions(new Azure.Identity.VisualStudioCredentialOptions()));
        }

        internal Azure.Identity.VisualStudioCodeCredential CreateVisualStudioCodeCredential()
        {
            return new(ConfigureOptions(new Azure.Identity.VisualStudioCodeCredentialOptions()));
        }

        //internal Azure.Identity.InteractiveBrowserCredential CreateInteractiveBrowserCredential()
        //{
        //    var result = new Azure.Identity.InteractiveBrowserCredentialOptions { TenantId = TenantId, AuthorityHost = AuthorityHost, IsUnsafeSupportLoggingEnabled = IsUnsafeSupportLoggingEnabled, DisableInstanceDiscovery = DisableInstanceDiscovery };
        //    CopyAdditionallyAllowedTenants(result.AdditionallyAllowedTenants);
        //    return new(result);
        //}

        //internal Azure.Identity.ClientCertificateCredential CreateClientCertificateCredential(string appId, X509Certificate2 certificate, string tenantId)
        //{
        //    var result = new Azure.Identity.ClientCertificateCredentialOptions { AuthorityHost = AuthorityHost, IsUnsafeSupportLoggingEnabled = IsUnsafeSupportLoggingEnabled, DisableInstanceDiscovery = DisableInstanceDiscovery };
        //    CopyAdditionallyAllowedTenants(result.AdditionallyAllowedTenants);
        //    return new(string.IsNullOrEmpty(tenantId) ? TenantId : tenantId, appId, certificate, result);
        //}

        internal Azure.Identity.ClientSecretCredential CreateClientSecretCredential(string appId, string appKey, string tenantId)
        {
            return new(string.IsNullOrEmpty(tenantId) ? TenantId : tenantId, appId, appKey, ConfigureOptions(new Azure.Identity.ClientSecretCredentialOptions()));
        }

        internal Azure.Identity.ManagedIdentityCredential CreateManagedIdentityCredential(string appId)
        {
            return new(appId, options: this);
        }

        internal Azure.Identity.ManagedIdentityCredential CreateManagedIdentityCredential()
        {
            return CreateManagedIdentityCredential(null!);
        }

        internal Azure.Identity.WorkloadIdentityCredential CreateWorkloadIdentityCredential(string appId)
        {
            return new(ConfigureOptions(new Azure.Identity.WorkloadIdentityCredentialOptions() { ClientId = appId }));
        }

        internal Azure.Identity.WorkloadIdentityCredential CreateWorkloadIdentityCredential()
        {
            return new(ConfigureOptions(new Azure.Identity.WorkloadIdentityCredentialOptions()));
        }

        // Based on https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/identity/Azure.Identity/src/Credentials/TokenCredentialOptions.cs#L50 method Clone
        private T ConfigureOptions<T>(T options) where T : Azure.Identity.TokenCredentialOptions
        {
            CopyTenantId(options);

            // copy TokenCredentialOptions Properties
            options.AuthorityHost = AuthorityHost;

            options.IsUnsafeSupportLoggingEnabled = IsUnsafeSupportLoggingEnabled;

            // copy TokenCredentialDiagnosticsOptions specific options
            options.Diagnostics.IsAccountIdentifierLoggingEnabled = Diagnostics.IsAccountIdentifierLoggingEnabled;

            // copy ISupportsDisableInstanceDiscovery
            CopyDisableInstanceDiscovery(options);

            // copy ISupportsTokenCachePersistenceOptions
            CopyTokenCachePersistenceOptions(options);

            // copy ISupportsAdditionallyAllowedTenants
            CopyAdditionallyAllowedTenants(options);

            // copy base ClientOptions properties

            // only copy transport if the original has changed from the default so as not to set IsCustomTransportSet unintentionally
            if (Transport != Default.Transport)
            {
                options.Transport = Transport;
            }

            // clone base Diagnostic options
            options.Diagnostics.ApplicationId = Diagnostics.ApplicationId;
            options.Diagnostics.IsLoggingEnabled = Diagnostics.IsLoggingEnabled;
            options.Diagnostics.IsTelemetryEnabled = Diagnostics.IsTelemetryEnabled;
            options.Diagnostics.LoggedContentSizeLimit = Diagnostics.LoggedContentSizeLimit;
            options.Diagnostics.IsDistributedTracingEnabled = Diagnostics.IsDistributedTracingEnabled;
            options.Diagnostics.IsLoggingContentEnabled = Diagnostics.IsLoggingContentEnabled;

            CopyListItems(Diagnostics.LoggedHeaderNames, options.Diagnostics.LoggedHeaderNames);
            CopyListItems(Diagnostics.LoggedQueryParameters, options.Diagnostics.LoggedQueryParameters);

            // clone base RetryOptions
            options.RetryPolicy = RetryPolicy;

            options.Retry.MaxRetries = Retry.MaxRetries;
            options.Retry.Delay = Retry.Delay;
            options.Retry.MaxDelay = Retry.MaxDelay;
            options.Retry.Mode = Retry.Mode;
            options.Retry.NetworkTimeout = Retry.NetworkTimeout;

            return options;
        }

        private static void CopyListItems<TItem>(IList<TItem> source, IList<TItem> destination)
        {
            foreach (var item in source)
            {
                destination.Add(item);
            }
        }

        private void CopyTenantId<T>(T options) where T : Azure.Identity.TokenCredentialOptions
        {
            options?.GetType().GetProperty(nameof(TenantId))?.SetValue(options, TenantId);
        }

        private void CopyDisableInstanceDiscovery<T>(T options) where T : Azure.Identity.TokenCredentialOptions
        {
            options?.GetType().GetProperty(nameof(DisableInstanceDiscovery))?.SetValue(options, DisableInstanceDiscovery);
        }

        private void CopyTokenCachePersistenceOptions<T>(T options) where T : Azure.Identity.TokenCredentialOptions
        {
            options?.GetType().GetProperty(nameof(TokenCachePersistenceOptions))?.SetValue(options, TokenCachePersistenceOptions);
        }

        void CopyAdditionallyAllowedTenants<T>(T options) where T : Azure.Identity.TokenCredentialOptions
        {
            var additionalTenants = options?.GetType().GetProperty(nameof(AdditionallyAllowedTenants))?.GetValue(options) as IList<string>;

            if (additionalTenants is not null)
            {
                CopyListItems(AdditionallyAllowedTenants, additionalTenants);
            }
        }
    }

    // adapted from https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/mgmtcommon/AppAuthentication/Azure.Services.AppAuthentication/AzureServiceTokenProviderFactory.cs
    // Implements https://learn.microsoft.com/en-us/dotnet/api/overview/azure/app-auth-migration?view=azure-dotnet
    internal partial struct AzureServicesConnectionStringCredentialFactory
    {
        private const string RunAs = "RunAs";
        private const string Developer = "Developer";
        private const string AzureCli = "AzureCLI";
        private const string VisualStudio = "VisualStudio";
        private const string VisualStudioCode = "VisualStudioCode";
        private const string DeveloperTool = "DeveloperTool";
        private const string CurrentUser = "CurrentUser";
        private const string Workload = "Workload";
        private const string App = "App";
        private const string AppId = "AppId";
        private const string AppKey = "AppKey";
        private const string TenantId = "TenantId";
        private const string CertificateSubjectName = "CertificateSubjectName";
        private const string CertificateThumbprint = "CertificateThumbprint";
        private const string KeyVaultCertificateSecretIdentifier = "KeyVaultCertificateSecretIdentifier";
        //private const string KeyVaultUserAssignedManagedIdentityId = "KeyVaultUserAssignedManagedIdentityId";
        private const string CertificateStoreLocation = "CertificateStoreLocation";
        private const string MsiRetryTimeout = "MsiRetryTimeout";

        // taken from https://github.com/dotnet/corefx/blob/master/src/Common/src/System/Data/Common/DbConnectionOptions.Common.cs
        [GeneratedRegex(                                                    // may not contain embedded null except trailing last value
                  "([\\s;]*"                                                // leading whitespace and extra semicolons
                + "(?![\\s;])"                                              // key does not start with space or semicolon
                + "(?<key>([^=\\s\\p{Cc}]|\\s+[^=\\s\\p{Cc}]|\\s+==|==)+)"  // allow any visible character for keyname except '=' which must quoted as '=='
                + "\\s*=(?!=)\\s*"                                          // the equal sign divides the key and value parts
                + "(?<value>"
                + "(\"([^\"\u0000]|\"\")*\")"                               // double quoted string, " must be quoted as ""
                + "|"
                + "('([^'\u0000]|'')*')"                                    // single quoted string, ' must be quoted as ''
                + "|"
                + "((?![\"'\\s])"                                           // unquoted value must not start with " or ' or space, would also like = but too late to change
                + "([^;\\s\\p{Cc}]|\\s+[^;\\s\\p{Cc}])*"                    // control characters must be quoted
                + "(?<![\"']))"                                             // unquoted value must not stop with " or '
                + ")(\\s*)(;|[\u0000\\s]*$)"                                // whitespace after value up to semicolon or end-of-line
                + ")*"                                                      // repeat the key-value pair
                + "[\\s;]*[\u0000\\s]*",                                     // trailing whitespace/semicolons (DataSourceLocator), embedded nulls are allowed only in the end
            RegexOptions.ExplicitCapture | RegexOptions.Compiled)]
        private static partial Regex ConnectionStringPatternRegexGenerated();
        private static readonly Regex ConnectionStringPatternRegex = ConnectionStringPatternRegexGenerated();

        internal static TokenCredential Create(AzureServicesConnectionStringCredentialOptions options)
        {
            var connectionString = options.ConnectionString;
            var connectionSettings = ParseConnectionString(connectionString);

            TokenCredential azureServiceTokenCredential;

            ValidateAttribute(connectionSettings, RunAs, connectionString);

            var runAs = connectionSettings[RunAs];

            if (string.Equals(runAs, Developer, StringComparison.OrdinalIgnoreCase))
            {
                // If RunAs=Developer
                ValidateAttribute(connectionSettings, DeveloperTool, connectionString);

                // And Dev Tool equals AzureCLI or VisualStudio
                if (string.Equals(connectionSettings[DeveloperTool], AzureCli,
                    StringComparison.OrdinalIgnoreCase))
                {
                    azureServiceTokenCredential = options.CreateAzureCliCredential();
                }
                else if (string.Equals(connectionSettings[DeveloperTool], VisualStudio,
                    StringComparison.OrdinalIgnoreCase))
                {
                    azureServiceTokenCredential = options.CreateVisualStudioCredential();
                }
                else if (string.Equals(connectionSettings[DeveloperTool], VisualStudioCode,
                    StringComparison.OrdinalIgnoreCase))
                {
                    azureServiceTokenCredential = options.CreateVisualStudioCodeCredential();
                }
                else
                {
                    throw new ArgumentException($"Connection string '{connectionString}' is not valid. {DeveloperTool} '{connectionSettings[DeveloperTool]}' is not valid. " +
                                                $"Allowed values are {AzureCli}, {VisualStudio} or {VisualStudioCode}", nameof(options));
                }
            }
            else if (string.Equals(runAs, CurrentUser, StringComparison.OrdinalIgnoreCase))
            {
                // If RunAs=CurrentUser
                throw new ArgumentException("Connection string " + connectionString + " is not supported for .NET Core.", nameof(options));
            }
            else if (string.Equals(runAs, App, StringComparison.OrdinalIgnoreCase))
            {
                // If RunAs=App
                // If AppId key is present, use certificate, client secret, or MSI (with user assigned identity) based token provider
                if (connectionSettings.TryGetValue(AppId, out var appId))
                {
                    ValidateAttribute(connectionSettings, AppId, options.ConnectionString);

                    if (connectionSettings.ContainsKey(CertificateStoreLocation))
                    {
                        //ValidateAttributes(connectionSettings, new List<string> { CertificateSubjectName, CertificateThumbprint }, options.ConnectionString);
                        //ValidateAttribute(connectionSettings, CertificateStoreLocation, options.ConnectionString);
                        //ValidateStoreLocation(connectionSettings, options.ConnectionString);
                        //ValidateAttribute(connectionSettings, TenantId, options.ConnectionString);

                        //azureServiceTokenCredential =
                        //    options.CreateClientCertificateCredential(
                        //        appId,
                        //        GetCertificates(
                        //            connectionSettings.ContainsKey(CertificateThumbprint)
                        //                ? connectionSettings[CertificateThumbprint]
                        //                : connectionSettings[CertificateSubjectName],
                        //            connectionSettings.ContainsKey(CertificateThumbprint),
                        //            Enum.Parse<StoreLocation>(connectionSettings[CertificateStoreLocation]))
                        //            .Single(),
                        //        connectionSettings[TenantId]);

                        throw new ArgumentException("Connection string '" + connectionString + "' is not supported. CertificateStoreLocation is deprecated.", nameof(options));
                    }
                    else if (connectionSettings.ContainsKey(CertificateThumbprint) ||
                             connectionSettings.ContainsKey(CertificateSubjectName))
                    {
                        // if certificate thumbprint or subject name are specified but certificate store location is not, throw error
                        throw new ArgumentException($"Connection string '{connectionString}' is not valid. Must contain '{CertificateStoreLocation}' attribute and it must not be empty " +
                                                    $"when using '{CertificateThumbprint}' and '{CertificateSubjectName}' attributes", nameof(options));
                    }
                    else if (connectionSettings.ContainsKey(KeyVaultCertificateSecretIdentifier))
                    {
                        throw new ArgumentException("Connection string '" + connectionString + "' is not supported. KeyVaultCertificateSecretIdentifier is deprecated.", nameof(options));

                        //ValidateMsiRetryTimeout(connectionSettings, options.ConnectionString);

                        //var msiRetryTimeout = connectionSettings.ContainsKey(MsiRetryTimeout)
                        //    ? int.Parse(connectionSettings[MsiRetryTimeout])
                        //    : 0;
                        //connectionSettings.TryGetValue(KeyVaultUserAssignedManagedIdentityId, out var keyVaultUserAssignedManagedIdentityId);

                        //azureServiceTokenCredential =
                        //    new ClientCertificateAzureServiceTokenProvider(
                        //        appId,
                        //        connectionSettings[KeyVaultCertificateSecretIdentifier],
                        //        ClientCertificateAzureServiceTokenProvider.CertificateIdentifierType.KeyVaultCertificateSecretIdentifier,
                        //        null, // storeLocation unused
                        //        azureAdInstance,
                        //        connectionSettings.ContainsKey(TenantId) // tenantId can be specified in connection string or retrieved from Key Vault access token later
                        //            ? connectionSettings[TenantId]
                        //            : default,
                        //msiRetryTimeout,
                        //        keyVaultUserAssignedManagedIdentityId,
                        //        new AdalAuthenticationContext(httpClientFactory));
                    }
                    else if (connectionSettings.TryGetValue(AppKey, out var appKey))
                    {
                        ValidateAttribute(connectionSettings, TenantId, options.ConnectionString);

                        azureServiceTokenCredential =
                            options.CreateClientSecretCredential(
                                appId,
                                appKey,
                                connectionSettings[TenantId]);
                    }
                    else
                    {
                        ValidateAndSetMsiRetryTimeout(connectionSettings, options);

                        // If certificate or client secret are not specified, use the specified managed identity
                        azureServiceTokenCredential = options.CreateManagedIdentityCredential(appId);
                    }
                }
                else
                {
                    ValidateAndSetMsiRetryTimeout(connectionSettings, options);

                    // If AppId is not specified, use Managed Service Identity
                    azureServiceTokenCredential = options.CreateManagedIdentityCredential();
                }
            }
            else if (string.Equals(runAs, Workload, StringComparison.OrdinalIgnoreCase))
            {
                // RunAs=Workload
                // Use the specified Workload Identity
                // If AppId key is present, use it as the ClientId
                if (connectionSettings.TryGetValue(AppId, out var appId))
                {
                    ValidateAttribute(connectionSettings, AppId, options.ConnectionString);

                    azureServiceTokenCredential = options.CreateWorkloadIdentityCredential(appId);
                }
                else
                {
                    azureServiceTokenCredential = options.CreateWorkloadIdentityCredential();
                }
            }
            else
            {
                throw new ArgumentException($"Connection string '{connectionString}' is not valid. RunAs value '{connectionSettings[RunAs]}' is not valid. " +
                                            $"Allowed values are {Developer}, {CurrentUser}, {App}, or {Workload}");
            }

            return azureServiceTokenCredential;

        }

        //public static List<X509Certificate2> GetCertificates(string subjectNameOrThumbprint, bool isThumbprint, StoreLocation location)
        //{
        //    var x509Store = new X509Store(StoreName.My, location);
        //    x509Store.Open(OpenFlags.ReadOnly);
        //    return x509Store.Certificates
        //        .Where(current => current is not null && current.HasPrivateKey && (isThumbprint && string.Equals(subjectNameOrThumbprint, current.Thumbprint, StringComparison.OrdinalIgnoreCase) || !isThumbprint && string.Equals(subjectNameOrThumbprint, current.Subject, StringComparison.OrdinalIgnoreCase)))
        //        .ToList();
        //}

        private static void ValidateAttribute(Dictionary<string, string> connectionSettings, string attribute, string connectionString)
        {
            if (connectionSettings != null &&
                (!connectionSettings.ContainsKey(attribute) || string.IsNullOrWhiteSpace(connectionSettings[attribute])))
            {
                throw new ArgumentException($"Connection string '{connectionString}' is not valid. Must contain '{attribute}' attribute and it must not be empty.", nameof(connectionString));
            }
        }

        ///// <summary>
        ///// Throws an exception if none of the attributes are in the connection string
        ///// </summary>
        ///// <param name="connectionSettings">List of key value pairs in the connection string</param>
        ///// <param name="attributes">List of attributes to test</param>
        ///// <param name="connectionString">The connection string specified</param>
        //private static void ValidateAttributes(Dictionary<string, string> connectionSettings, List<string> attributes, string connectionString)
        //{
        //    if (connectionSettings != null)
        //    {
        //        foreach (var attribute in attributes)
        //        {
        //            if (connectionSettings.ContainsKey(attribute))
        //            {
        //                return;
        //            }
        //        }

        //        throw new ArgumentException($"Connection string {connectionString} is not valid. Must contain at least one of {string.Join(" or ", attributes)} attributes.", nameof(connectionString));
        //    }
        //}

        //private static void ValidateStoreLocation(Dictionary<string, string> connectionSettings, string connectionString)
        //{
        //    if (connectionSettings != null && connectionSettings.TryGetValue(CertificateStoreLocation, out var storeLocation))
        //    {
        //        if (!string.IsNullOrWhiteSpace(storeLocation))
        //        {
        //            if (!Enum.TryParse<StoreLocation>(storeLocation, true, out var _))
        //            {
        //                throw new ArgumentException(
        //                    $"Connection string {connectionString} is not valid. StoreLocation {storeLocation} is not valid. Valid values are CurrentUser and LocalMachine.");
        //            }
        //        }
        //    }
        //}

        private static void ValidateAndSetMsiRetryTimeout(Dictionary<string, string> connectionSettings, AzureServicesConnectionStringCredentialOptions options)
        {
            if (connectionSettings != null && connectionSettings.TryGetValue(MsiRetryTimeout, out var value))
            {
                if (!string.IsNullOrWhiteSpace(value))
                {
                    var timeoutString = value;

                    if (int.TryParse(timeoutString, out var timeoutValue) && timeoutValue >= 0)
                    {
                        options.Retry.NetworkTimeout = TimeSpan.FromSeconds(timeoutValue);
                    }
                    else
                    {
                        throw new ArgumentException($"Connection string '{options.ConnectionString}' is not valid. MsiRetryTimeout '{timeoutString}' is not valid. Valid values are integers greater than or equal to 0.", nameof(options));
                    }
                }
            }
        }

        // adapted from https://github.com/dotnet/corefx/blob/master/src/Common/src/System/Data/Common/DbConnectionOptions.Common.cs
        internal static Dictionary<string, string> ParseConnectionString(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                connectionString = string.Empty;
            }

            ArgumentException.ThrowIfNullOrEmpty(connectionString);

            var connectionSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            const int KeyIndex = 1, ValueIndex = 2;
            var match = ConnectionStringPatternRegex.Match(connectionString);
            if (!match.Success || match.Length != connectionString.Length)
            {
                throw new ArgumentException($"Connection string '{connectionString}' is not in a proper format. Expected format is Key1=Value1;Key2=Value2;", nameof(connectionString));
            }

            var indexValue = 0;
            var keyValues = match.Groups[ValueIndex].Captures;
            foreach (var keyNames in match.Groups[KeyIndex].Captures.Cast<Capture>())
            {
                var key = keyNames.Value.Replace("==", "=");
                var value = keyValues[indexValue++].Value;
                if (value.Length > 0)
                {
                    switch (value[0])
                    {
                        case '\"':
                            value = value[1..^1].Replace("\"\"", "\"");
                            break;
                        case '\'':
                            value = value[1..^1].Replace("\'\'", "\'");
                            break;
                        default:
                            break;
                    }
                }

                if (!string.IsNullOrWhiteSpace(key))
                {
                    if (!connectionSettings.ContainsKey(key))
                    {
                        connectionSettings[key] = value;
                    }
                    else
                    {
                        throw new ArgumentException($"Connection string '{connectionString}' is not in a proper format. Key '{key}' is repeated.", nameof(connectionString));
                    }
                }
            }

            return connectionSettings;
        }
    }
}
