﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IdentityModel.Tokens.Jwt;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.ResourceManager;
using Azure.ResourceManager.ApplicationInsights;
using Azure.ResourceManager.ApplicationInsights.Models;
using Azure.ResourceManager.Authorization;
using Azure.ResourceManager.Batch;
using Azure.ResourceManager.Compute;
using Azure.ResourceManager.ContainerRegistry;
using Azure.ResourceManager.ContainerService;
using Azure.ResourceManager.ContainerService.Models;
using Azure.ResourceManager.KeyVault;
using Azure.ResourceManager.KeyVault.Models;
using Azure.ResourceManager.ManagedServiceIdentities;
using Azure.ResourceManager.Network;
using Azure.ResourceManager.Network.Models;
using Azure.ResourceManager.OperationalInsights;
using Azure.ResourceManager.PostgreSql.FlexibleServers;
using Azure.ResourceManager.PostgreSql.FlexibleServers.Models;
using Azure.ResourceManager.PrivateDns;
using Azure.ResourceManager.ResourceGraph;
using Azure.ResourceManager.Resources;
using Azure.ResourceManager.Resources.Models;
using Azure.ResourceManager.Storage;
using Azure.Security.KeyVault.Secrets;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using BuildPushAcr;
using CommonUtilities;
using CommonUtilities.AzureCloud;
using k8s;
using Microsoft.EntityFrameworkCore;
using Microsoft.Graph;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;
using Polly.Utilities;
using Tes.Extensions;
using Tes.Models;
using Tes.SDK;
using Batch = Azure.ResourceManager.Batch.Models;
using Storage = Azure.ResourceManager.Storage.Models;

namespace TesDeployer
{
    public class Deployer(Configuration configuration)
    {
        private static readonly AsyncRetryPolicy roleAssignmentHashConflictRetryPolicy = Policy
            .Handle<RequestFailedException>(requestFailedException =>
                "HashConflictOnDifferentRoleAssignmentIds".Equals(requestFailedException.ErrorCode, StringComparison.OrdinalIgnoreCase))
            .RetryAsync();

        private static readonly AsyncRetryPolicy operationNotAllowedConflictRetryPolicy = Policy
            .Handle<RequestFailedException>(azureException =>
                (int)HttpStatusCode.Conflict == azureException.Status &&
                "OperationNotAllowed".Equals(azureException.ErrorCode, StringComparison.OrdinalIgnoreCase))
            .WaitAndRetryAsync(30, retryAttempt => TimeSpan.FromSeconds(10));

        private static readonly AsyncRetryPolicy buildPushAcrRetryPolicy = Policy
            .Handle<Exception>(AsyncRetryExceptionPolicy)
            .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(1));

        private static bool AsyncRetryExceptionPolicy(Exception ex)
        {
            var dontRetry = ex is InvalidOperationException
                || (ex is Microsoft.Kiota.Abstractions.ApiException ae && (int)HttpStatusCode.Unauthorized == ae.ResponseStatusCode)
                || (ex is GitHub.Models.ValidationError ve && (int)HttpStatusCode.UnprocessableContent == ve.ResponseStatusCode)
                || (ex is GitHub.Models.BasicError be &&
                    ((int)HttpStatusCode.Forbidden == be.ResponseStatusCode
                    || (int)HttpStatusCode.NotFound == be.ResponseStatusCode
                    || (int)HttpStatusCode.Conflict == be.ResponseStatusCode));

            if (!dontRetry)
            {
                Console.WriteLine($"Retrying ACR image build because ({ex.GetType().FullName}): {ex.Message}");
            }

            return !dontRetry;
        }

        private static readonly AsyncRetryPolicy acrGetDigestRetryPolicy = Policy
            .Handle<RequestFailedException>(azureException => (int)HttpStatusCode.NotFound == azureException.Status)
            .WaitAndRetryAsync(30, retryAttempt => TimeSpan.FromSeconds(10));

        private static readonly AsyncRetryPolicy generalRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(1));

        private static readonly AsyncRetryPolicy internalServerErrorRetryPolicy = Policy
            .Handle<RequestFailedException>(azureException =>
                (int)HttpStatusCode.OK == azureException.Status &&
                "InternalServerError".Equals(azureException.ErrorCode, StringComparison.OrdinalIgnoreCase))
            .WaitAndRetryAsync(3, retryAttempt => longRetryWaitTime);

        private static readonly TimeSpan longRetryWaitTime = TimeSpan.FromSeconds(15);

        internal static Azure.Core.Pipeline.RetryPolicy GetRetryPolicy(CommonUtilities.Options.RetryPolicyOptions retryPolicy)
            => new(retryPolicy.MaxRetryCount, DelayStrategy.CreateExponentialDelayStrategy(TimeSpan.FromSeconds(retryPolicy.ExponentialBackOffExponent)));

        public const string ConfigurationContainerName = "configuration";
        public const string TesInternalContainerName = "tes-internal";
        public const string AllowedVmSizesFileName = "allowed-vm-sizes";
        public const string TesCredentialsFileName = "TesCredentials.json";
        public const string InputsContainerName = "inputs";
        public const string StorageAccountKeySecretName = "CoAStorageKey";
        public const string PostgresqlSslMode = "VerifyFull";

        private readonly CancellationTokenSource cts = new();

        private readonly List<string> requiredResourceProviders =
        [
            "Microsoft.Authorization",
            "Microsoft.Batch",
            "Microsoft.Compute",
            "Microsoft.ContainerService",
            "Microsoft.DocumentDB",
            "Microsoft.OperationalInsights",
            "Microsoft.OperationsManagement",
            "Microsoft.insights",
            "Microsoft.Network",
            "Microsoft.Storage",
            "Microsoft.DBforPostgreSQL"
        ];

        private readonly Dictionary<string, List<string>> requiredResourceProviderFeatures = new()
        {
            { "Microsoft.Compute", new() { "EncryptionAtHost" } },
        };

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "We are using the base type everywhere.")]
        private TokenCredential tokenCredential { get; set; }
        private SubscriptionResource armSubscription { get; set; }
        private ArmClient armClient { get; set; }
        private ResourceGroupResource resourceGroup { get; set; }
        private CloudEnvironment cloudEnvironment { get; set; }
        private IEnumerable<SubscriptionResource> subscriptionIds { get; set; }
        private bool isResourceGroupCreated { get; set; }
        private KubernetesManager kubernetesManager { get; set; }
        internal static AzureCloudConfig azureCloudConfig { get; private set; }

        private static async Task<T> EnsureResourceDataAsync<T>(T resource, Predicate<T> HasData, Func<T, Func<CancellationToken, Task<Response<T>>>> GetAsync, CancellationToken cancellationToken, Action<T> OnAcquisition = null) where T : ArmResource
        {
            return HasData(resource)
                ? resource
                : await FetchResourceDataAsync(GetAsync(resource), cancellationToken, OnAcquisition);
        }

        private static async Task<T> FetchResourceDataAsync<T>(Func<CancellationToken, Task<Response<T>>> GetAsync, CancellationToken cancellationToken, Action<T> OnAcquisition = null) where T : ArmResource
        {
            ArgumentNullException.ThrowIfNull(GetAsync);

            var result = await GetAsync(cancellationToken);
            OnAcquisition?.Invoke(result);
            return result;
        }

        private BlobClient GetBlobClient(StorageAccountData storageAccount, string containerName, string blobName)
        {
            return new(new BlobUriBuilder(storageAccount.PrimaryEndpoints.BlobUri) { BlobContainerName = containerName, BlobName = blobName }.ToUri(),
                tokenCredential,
                new()
                {
                    Audience = Azure.Storage.Blobs.Models.BlobAudience.DefaultAudience, // https://github.com/Azure/azure-cli/issues/28708#issuecomment-2047256166
                    RetryPolicy = GetRetryPolicy(new())
                });
        }

        public async Task<int> DeployAsync()
        {
            var mainTimer = Stopwatch.StartNew();

            try
            {
                ConsoleEx.WriteLine("Running...");

                await Execute($"Getting cloud configuration for {configuration.AzureCloudName}...", async () =>
                {
                    azureCloudConfig = await AzureCloudConfig.FromKnownCloudNameAsync(cloudName: configuration.AzureCloudName, retryPolicyOptions: Microsoft.Extensions.Options.Options.Create<CommonUtilities.Options.RetryPolicyOptions>(new()));
                    cloudEnvironment = new(azureCloudConfig.ArmEnvironment.Value, azureCloudConfig.AuthorityHost);
                });

                await Execute("Validating command line arguments...", () =>
                {
                    ValidateInitialCommandLineArgs();
                    return Task.CompletedTask;
                });

                await ValidateTokenProviderAsync();

                await Execute("Connecting to Azure Services...", async () =>
                {
                    tokenCredential = new AzureCliCredential(new() { AuthorityHost = cloudEnvironment.AzureAuthorityHost, RetryPolicy = GetRetryPolicy(new()) });
                    armClient = new ArmClient(tokenCredential, configuration.SubscriptionId, new() { Environment = cloudEnvironment.ArmEnvironment, RetryPolicy = GetRetryPolicy(new()) });
                    armSubscription = armClient.GetSubscriptionResource(SubscriptionResource.CreateResourceIdentifier(configuration.SubscriptionId));
                    subscriptionIds = await armClient.GetSubscriptions().GetAllAsync(cts.Token).ToListAsync(cts.Token);
                });

                await ValidateSubscriptionAndResourceGroupAsync(configuration);
                kubernetesManager = new(configuration, azureCloudConfig, GetBlobClient, cts.Token);

                ContainerServiceManagedClusterResource aksCluster = null;
                BatchAccountResource batchAccount = null;
                OperationalInsightsWorkspaceResource logAnalyticsWorkspace = null;
                ApplicationInsightsComponentResource appInsights = null;
                PostgreSqlFlexibleServerResource postgreSqlFlexServer = null;
                StorageAccountResource storageAccount = null;
                StorageAccountData storageAccountData = null;
                Uri keyVaultUri = null;
                UserAssignedIdentityResource managedIdentity = null;
                PrivateDnsZoneResource postgreSqlDnsZone = null;

                var targetVersion = Utility.DelimitedTextToDictionary(Utility.GetFileContent("scripts", "env-00-tes-version.txt")).GetValueOrDefault("TesOnAzureVersion");

                if (configuration.Update)
                {
                    resourceGroup = (await armSubscription.GetResourceGroupAsync(configuration.ResourceGroupName, cts.Token)).Value;
                    configuration.RegionName = resourceGroup.Id.Location ??
                        ((await EnsureResourceDataAsync(resourceGroup, g => g.HasData, g => g.GetAsync, cts.Token, g => resourceGroup = g)).Data.Location.Name);

                    ConsoleEx.WriteLine($"Upgrading TES on Azure instance in resource group '{resourceGroup.Id.Name}' to version {targetVersion}...");

                    if (string.IsNullOrEmpty(configuration.StorageAccountName))
                    {
                        var storageAccounts = await resourceGroup.GetStorageAccounts().ToListAsync(cts.Token);

                        storageAccount = storageAccounts.Count switch
                        {
                            0 => throw new ValidationException($"Update was requested but resource group {configuration.ResourceGroupName} does not contain any storage accounts.", displayExample: false),
                            1 => storageAccounts.Single(),
                            _ => throw new ValidationException($"Resource group {configuration.ResourceGroupName} contains multiple storage accounts. {nameof(configuration.StorageAccountName)} must be provided.", displayExample: false),
                        };
                    }
                    else
                    {
                        storageAccount = await GetExistingStorageAccountAsync(configuration.StorageAccountName)
                            ?? throw new ValidationException($"Storage account {configuration.StorageAccountName} does not exist in region {configuration.RegionName} or is not accessible to the current user.", displayExample: false);
                    }

                    storageAccountData = (await FetchResourceDataAsync(ct => storageAccount.GetAsync(cancellationToken: ct), cts.Token, account => storageAccount = account)).Data;

                    switch (await AssignRoleForDeployerToStorageAccountAsync(storageAccount))
                    {
                        case true:
                            // 10 minutes for propagation https://learn.microsoft.com/azure/role-based-access-control/troubleshooting
                            await Execute("Waiting 5 minutes for role assignment propagation...",
                                () => Task.Delay(TimeSpan.FromMinutes(5), cts.Token));
                            break;

                        case null:
                            ConsoleEx.WriteLine("Unable to assign 'Storage Blob Data Contributor' for deployment identity to the storage account. If the deployment fails as a result, assign the deploying user the 'Storage Blob Data Contributor' role for the storage account.", ConsoleColor.Yellow);
                            break;
                    }

                    if (string.IsNullOrWhiteSpace(configuration.AksClusterName))
                    {
                        var aksClusters = await resourceGroup.GetContainerServiceManagedClusters().GetAllAsync(cts.Token).ToListAsync(cts.Token);

                        aksCluster = aksClusters.Count switch
                        {
                            0 => throw new ValidationException($"Update was requested but resource group {configuration.ResourceGroupName} does not contain any AKS clusters.", displayExample: false),
                            1 => (await aksClusters.Single().GetAsync(cts.Token)).Value,
                            _ => throw new ValidationException($"Resource group {configuration.ResourceGroupName} contains multiple AKS clusters. {nameof(configuration.AksClusterName)} must be provided.", displayExample: false),
                        };

                        configuration.AksClusterName = aksCluster.Data.Name;
                    }
                    else
                    {
                        aksCluster = (await GetExistingAKSClusterAsync(configuration.AksClusterName))
                            ?? throw new ValidationException($"AKS cluster {configuration.AksClusterName} does not exist in region {configuration.RegionName} or is not accessible to the current user.", displayExample: false);
                    }

                    var aksValues = await kubernetesManager.GetAKSSettingsAsync(storageAccountData);

                    if (0 == aksValues.Count)
                    {
                        throw new ValidationException($"Could not retrieve account names from stored configuration in {storageAccountData.Name}.", displayExample: false);
                    }

                    if (aksValues.TryGetValue("EnableIngress", out var enableIngress) && aksValues.TryGetValue("TesHostname", out var tesHostname))
                    {
                        kubernetesManager.TesHostname = tesHostname;
                        configuration.EnableIngress = bool.TryParse(enableIngress, out var parsed) ? parsed : null;

                        var tesCredentialsFile = new FileInfo(Path.Combine(Directory.GetCurrentDirectory(), TesCredentialsFileName));
                        tesCredentialsFile.Refresh();

                        if (configuration.EnableIngress.GetValueOrDefault() && tesCredentialsFile.Exists)
                        {
                            try
                            {
                                using var stream = tesCredentialsFile.OpenRead();
                                var (hostname, tesUsername, tesPassword) = TesCredentials.Deserialize(stream);

                                if (kubernetesManager.TesHostname.Equals(hostname, StringComparison.InvariantCultureIgnoreCase) && string.IsNullOrEmpty(configuration.TesPassword))
                                {
                                    configuration.TesPassword = tesPassword;
                                    configuration.TesUsername = tesUsername;
                                }
                            }
                            catch (NotSupportedException)
                            { }
                            catch (ArgumentException)
                            { }
                            catch (IOException)
                            { }
                            catch (UnauthorizedAccessException)
                            { }
                            catch (System.Text.Json.JsonException)
                            { }
                        }
                    }

                    if (!configuration.SkipTestWorkflow && configuration.EnableIngress.GetValueOrDefault() && string.IsNullOrEmpty(configuration.TesPassword))
                    {
                        throw new ValidationException($"{nameof(configuration.TesPassword)} is required for update.", false);
                    }

                    if (!aksValues.TryGetValue("BatchAccountName", out var batchAccountName))
                    {
                        throw new ValidationException($"Could not retrieve the Batch account name from stored configuration in {storageAccount.Id.Name}.", displayExample: false);
                    }

                    batchAccount = await GetExistingBatchAccountAsync(batchAccountName)
                        ?? throw new ValidationException($"Batch account {batchAccountName}, referenced by the stored configuration, does not exist in region {configuration.RegionName} or is not accessible to the current user.", displayExample: false);

                    configuration.BatchAccountName = batchAccountName;

                    if (!aksValues.TryGetValue("PostgreSqlServerName", out var postgreSqlServerName))
                    {
                        throw new ValidationException($"Could not retrieve the PostgreSqlServer account name from stored configuration in {storageAccount.Id.Name}.", displayExample: false);
                    }

                    configuration.PostgreSqlServerName = postgreSqlServerName;

                    if (aksValues.TryGetValue("CrossSubscriptionAKSDeployment", out var crossSubscriptionAKSDeployment))
                    {
                        configuration.CrossSubscriptionAKSDeployment = bool.TryParse(crossSubscriptionAKSDeployment, out var parsed) ? parsed : null;
                    }

                    if (aksValues.TryGetValue("KeyVaultName", out var keyVaultName))
                    {
                        keyVaultUri = (await EnsureResourceDataAsync(await GetKeyVaultAsync(keyVaultName), vault => vault.HasData, vault => vault.GetAsync, cts.Token)).Data.Properties.VaultUri;
                    }

                    if (!aksValues.TryGetValue("ManagedIdentityClientId", out var managedIdentityClientId))
                    {
                        throw new ValidationException($"Could not retrieve ManagedIdentityClientId.", displayExample: false);
                    }

                    var clientId = Guid.Parse(managedIdentityClientId);
                    managedIdentity = await resourceGroup.GetUserAssignedIdentities()
                        .SelectAwaitWithCancellation(async (id, ct) => await FetchResourceDataAsync(id.GetAsync, ct))
                        .FirstOrDefaultAsync(id => id.Data.ClientId == clientId, cts.Token)
                        ?? throw new ValidationException($"Managed Identity {managedIdentityClientId} does not exist in region {configuration.RegionName} or is not accessible to the current user.", displayExample: false);

                    // Override any configuration that is used by the update.
                    var versionString = aksValues["TesOnAzureVersion"];
                    var installedVersion = !string.IsNullOrEmpty(versionString) && Version.TryParse(versionString, out var version) ? version : null;

                    if (installedVersion is null || installedVersion < new Version(4, 1)) // Assume 4.0.0. The work needed to upgrade from this version shouldn't apply to other releases of TES.
                    {
                        var tesImageString = aksValues["TesImageName"];
                        if (!string.IsNullOrEmpty(tesImageString) && tesImageString.EndsWith("/tes:4"))
                        {
                            aksValues["TesImageName"] = tesImageString + ".0";
                            installedVersion = new("4.0");
                        }
                    }

                    var settings = ConfigureSettings(managedIdentity.Data.ClientId?.ToString("D"), aksValues, installedVersion);
                    var waitForRoleAssignmentPropagation = false;
                    IEnumerable<string> manualPrecommands = null;
                    Func<IKubernetes, Task> asyncTask = null;

                    if (!string.IsNullOrWhiteSpace(configuration.AcrId) && settings.TryGetValue("AcrId", out var acrId) && !string.IsNullOrEmpty(acrId))
                    {
                        throw new ValidationException("AcrId must not be set if previously configured.", displayExample: false);
                    }

                    if (installedVersion is null || installedVersion < new Version(4, 4))
                    {
                        // Ensure all storage containers are created.
                        await CreateDefaultStorageContainersAsync(storageAccount);

                        if (string.IsNullOrWhiteSpace(settings["BatchNodesSubnetId"]))
                        {
                            settings["BatchNodesSubnetId"] = await UpdateVnetWithBatchSubnet();
                        }
                    }

                    if (installedVersion is null || installedVersion < new Version(4, 8))
                    {
                        var hasAssignedNetworkContributor = !await AssignMIAsNetworkContributorToResourceAsync(managedIdentity, resourceGroup);
                        var hasAssignedDataOwner = !await AssignVmAsDataOwnerToStorageAccountAsync(managedIdentity, storageAccount);
                        waitForRoleAssignmentPropagation |= hasAssignedNetworkContributor || hasAssignedDataOwner;
                    }

                    if (installedVersion is null || installedVersion < new Version(5, 0, 1))
                    {
                        if (string.IsNullOrWhiteSpace(settings["ExecutionsContainerName"]))
                        {
                            settings["ExecutionsContainerName"] = TesInternalContainerName;
                        }
                    }

                    if (installedVersion is null || installedVersion < new Version(5, 4, 7)) // Previous attempt 5.2.2
                    {
                        var connectionString = settings["AzureServicesAuthConnectionString"];
                        if (connectionString.Contains("RunAs=App"))
                        {
                            settings["AzureServicesAuthConnectionString"] = connectionString.Replace("RunAs=App", "RunAs=Workload");
                        }

                        var pool = aksCluster.Data.AgentPoolProfiles.FirstOrDefault(pool => "nodepool1".Equals(pool.Name, StringComparison.OrdinalIgnoreCase));

                        if (!(aksCluster.Data.SecurityProfile.IsWorkloadIdentityEnabled ?? false) ||
                            !(aksCluster.Data.OidcIssuerProfile.IsEnabled ?? false) ||
                            pool?.OSSku == ContainerServiceOSSku.Ubuntu ||
                            !(pool?.EnableEncryptionAtHost ?? false) ||
                            "Standard_D3_v2".Equals(pool?.VmSize, StringComparison.OrdinalIgnoreCase) ||
                            !(aksCluster.Data.AadProfile?.IsAzureRbacEnabled ?? false) ||
                            (await managedIdentity.GetFederatedIdentityCredentials()
                                .SingleOrDefaultAsync(r => "toaFederatedIdentity".Equals(r.Id.Name, StringComparison.OrdinalIgnoreCase), cts.Token)) is null)
                        {
                            await AssignMeAsRbacClusterAdminToManagedClusterAsync(aksCluster);
                            waitForRoleAssignmentPropagation = true;
                            ManagedClusterEnableManagedAad(aksCluster.Data);

                            if (pool?.OSSku == ContainerServiceOSSku.Ubuntu || !(pool?.EnableEncryptionAtHost ?? false))
                            {
                                pool.EnableEncryptionAtHost = true;
                                pool.OSSku = ContainerServiceOSSku.AzureLinux;
                                pool.VmSize = "Standard_D4s_v3";
                            }

                            aksCluster = await EnableWorkloadIdentity(aksCluster, managedIdentity, resourceGroup);
                            await Task.Delay(TimeSpan.FromMinutes(2), cts.Token);

                            if (installedVersion is null || installedVersion < new Version(5, 2, 3))
                            {
                                manualPrecommands = (manualPrecommands ?? []).Append("Include the following HELM command: uninstall aad-pod-identity --namespace kube-system");
                                asyncTask = _ => kubernetesManager.RemovePodAadChart();
                            }
                        }
                    }

                    if (installedVersion is null || installedVersion < new Version(5, 3, 1))
                    {
                        if (string.IsNullOrWhiteSpace(settings["DeploymentCreated"]))
                        {
                            settings["DeploymentCreated"] = settings["DeploymentUpdated"];
                        }
                    }

                    if (installedVersion is null || installedVersion < new Version(5, 3, 3))
                    {
                        if (string.IsNullOrWhiteSpace(settings["AzureCloudName"]))
                        {
                            settings["AzureCloudName"] = configuration.AzureCloudName;
                        }
                    }

                    //if (installedVersion is null || installedVersion < new Version(x, y, z))
                    //{
                    //}

                    await Task.WhenAll(
                    [
                        BuildPushAcrAsync(settings, targetVersion, managedIdentity),
                        Task.Run(async () =>
                        {
                            if (waitForRoleAssignmentPropagation)
                            {
                                // 10 minutes for propagation https://learn.microsoft.com/azure/role-based-access-control/troubleshooting
                                await Execute("Waiting 10 minutes for role assignment propagation...",
                                    () => Task.Delay(TimeSpan.FromMinutes(10), cts.Token));
                            }
                        })
                    ]);

                    await kubernetesManager.UpgradeValuesYamlAsync(storageAccountData, settings, installedVersion);
                    await PerformHelmDeploymentAsync(aksCluster, manualPrecommands, asyncTask);
                }

                if (!configuration.Update)
                {
                    if (string.IsNullOrWhiteSpace(configuration.BatchPrefix))
                    {
                        var blob = new byte[5];
                        RandomNumberGenerator.Fill(blob);
                        configuration.BatchPrefix = blob.ConvertToBase32().TrimEnd('=');
                    }

                    KeyVaultResource keyVault = default;
                    await Execute("Validating existing Azure resources...", async () =>
                    {
                        await ValidateRegionNameAsync(configuration.RegionName);
                        ValidateMainIdentifierPrefix(configuration.MainIdentifierPrefix);
                        storageAccount = await ValidateAndGetExistingStorageAccountAsync();
                        batchAccount = await ValidateAndGetExistingBatchAccountAsync();
                        aksCluster = await ValidateAndGetExistingAKSClusterAsync();
                        postgreSqlFlexServer = await ValidateAndGetExistingPostgresqlServerAsync();
                        var keyVault = await ValidateAndGetExistingKeyVaultAsync();

                        if (aksCluster is null && !configuration.ManualHelmDeployment)
                        {
                            //await ValidateVmAsync();
                        }

                        if (string.IsNullOrWhiteSpace(configuration.PostgreSqlServerNameSuffix))
                        {
                            configuration.PostgreSqlServerNameSuffix = $".{azureCloudConfig.Suffixes.PostgresqlServerEndpointSuffix}";
                        }

                        // Configuration preferences not currently settable by user.
                        if (string.IsNullOrWhiteSpace(configuration.PostgreSqlServerName))
                        {
                            configuration.PostgreSqlServerName = Utility.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                        }

                        configuration.PostgreSqlAdministratorPassword = PasswordGenerator.GeneratePassword();
                        configuration.PostgreSqlTesUserPassword = PasswordGenerator.GeneratePassword();

                        if (string.IsNullOrWhiteSpace(configuration.BatchAccountName))
                        {
                            configuration.BatchAccountName = Utility.RandomResourceName($"{configuration.MainIdentifierPrefix}", 15);
                        }

                        if (string.IsNullOrWhiteSpace(configuration.StorageAccountName))
                        {
                            configuration.StorageAccountName = Utility.RandomResourceName($"{configuration.MainIdentifierPrefix}", 24);
                        }

                        //if (string.IsNullOrWhiteSpace(configuration.NetworkSecurityGroupName))
                        //{
                        //    configuration.NetworkSecurityGroupName = Utility.RandomResourceName($"{configuration.MainIdentifierPrefix}", 15);
                        //}

                        if (string.IsNullOrWhiteSpace(configuration.ApplicationInsightsAccountName))
                        {
                            configuration.ApplicationInsightsAccountName = Utility.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                        }

                        if (string.IsNullOrWhiteSpace(configuration.TesPassword))
                        {
                            configuration.TesPassword = PasswordGenerator.GeneratePassword();
                        }

                        if (string.IsNullOrWhiteSpace(configuration.AksClusterName))
                        {
                            configuration.AksClusterName = Utility.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 25);
                        }

                        if (string.IsNullOrWhiteSpace(configuration.KeyVaultName))
                        {
                            configuration.KeyVaultName = Utility.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                        }

                        await RegisterResourceProvidersAsync();
                        await RegisterResourceProviderFeaturesAsync();

                        if (batchAccount is null)
                        {
                            await ValidateBatchAccountQuotaAsync();
                        }
                    });

                    ConsoleEx.WriteLine($"Deploying TES on Azure version {targetVersion}...");

                    if (!string.IsNullOrEmpty(configuration.BatchNodesSubnetId))
                    {
                        configuration.BatchSubnetName = new ResourceIdentifier(configuration.BatchNodesSubnetId).Name;
                    }

                    var vnetAndSubnet = await ValidateAndGetExistingVirtualNetworkAsync();

                    if (string.IsNullOrWhiteSpace(configuration.ResourceGroupName))
                    {
                        configuration.ResourceGroupName = Utility.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                        resourceGroup = await CreateResourceGroupAsync();
                        isResourceGroupCreated = true;
                    }
                    else
                    {
                        resourceGroup = (await armSubscription.GetResourceGroupAsync(configuration.ResourceGroupName, cts.Token)).Value;
                    }

                    // Derive TES ingress URL from resource group name
                    kubernetesManager.SetTesIngressNetworkingConfiguration(configuration.ResourceGroupName);

                    managedIdentity = await EnsureResourceDataAsync(await CreateUserManagedIdentityAsync(), id => id.HasData, id => id.GetAsync, cts.Token);

                    if (vnetAndSubnet is not null)
                    {
                        ConsoleEx.WriteLine($"Creating VM in existing virtual network {vnetAndSubnet.Value.virtualNetwork.Id.Name} and subnet {vnetAndSubnet.Value.vmSubnet.Id.Name}");
                    }

                    if (storageAccount is not null)
                    {
                        ConsoleEx.WriteLine($"Using existing Storage Account {storageAccount.Id.Name}");
                    }

                    if (batchAccount is not null)
                    {
                        ConsoleEx.WriteLine($"Using existing Batch Account {batchAccount.Id.Name}");
                    }

                    await Task.WhenAll(
                    [
                        Task.Run(async () =>
                        {
                            if (vnetAndSubnet is null)
                            {
                                configuration.VnetName = Utility.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                                configuration.PostgreSqlSubnetName = string.IsNullOrEmpty(configuration.PostgreSqlSubnetName) ? configuration.DefaultPostgreSqlSubnetName : configuration.PostgreSqlSubnetName;
                                configuration.BatchSubnetName = string.IsNullOrEmpty(configuration.BatchSubnetName) ? configuration.DefaultBatchSubnetName : configuration.BatchSubnetName;
                                configuration.VmSubnetName = string.IsNullOrEmpty(configuration.VmSubnetName) ? configuration.DefaultVmSubnetName : configuration.VmSubnetName;
                                vnetAndSubnet = await CreateVnetAndSubnetsAsync();
                            }
                        }),
                        Task.Run(async () =>
                        {
                            if (string.IsNullOrWhiteSpace(configuration.LogAnalyticsArmId))
                            {
                                var workspaceName = Utility.RandomResourceName(configuration.MainIdentifierPrefix, 15);
                                logAnalyticsWorkspace = await CreateLogAnalyticsWorkspaceResourceAsync(workspaceName);
                                configuration.LogAnalyticsArmId = logAnalyticsWorkspace.Id;
                            }
                        }),
                        Task.Run(async () =>
                        {
                            storageAccount = await EnsureResourceDataAsync(storageAccount ?? await CreateStorageAccountAsync(), r => r.HasData, r => ct => r.GetAsync(cancellationToken: ct), cts.Token);
                            await CreateDefaultStorageContainersAsync(storageAccount);
                            storageAccountData = storageAccount.Data;

                            if (await AssignRoleForDeployerToStorageAccountAsync(storageAccount) is null)
                            {
                                ConsoleEx.WriteLine("Unable to assign 'Storage Blob Data Contributor' for deployment identity to the storage account. If the deployment fails as a result, the storage account must be precreated and the deploying user must have the 'Storage Blob Data Contributor' role for the storage account.", ConsoleColor.Yellow);
                            }
                            else
                            {
                                await Task.Delay(TimeSpan.FromMinutes(5), cts.Token);
                            }

                            await AssignVmAsContributorToStorageAccountAsync(managedIdentity, storageAccount);
                            await AssignVmAsDataOwnerToStorageAccountAsync(managedIdentity, storageAccount);
                            await AssignManagedIdOperatorToResourceAsync(managedIdentity, resourceGroup);
                            await AssignMIAsNetworkContributorToResourceAsync(managedIdentity, resourceGroup);
                            await WritePersonalizedFilesToStorageAccountAsync(storageAccountData);
                        }),
                    ]);

                    if (configuration.CrossSubscriptionAKSDeployment.GetValueOrDefault())
                    {
                        await Task.Run(async () =>
                        {
                            keyVault ??= await CreateKeyVaultAsync(configuration.KeyVaultName, managedIdentity, vnetAndSubnet.Value.virtualNetwork, vnetAndSubnet.Value.vmSubnet);
                            keyVaultUri = (await EnsureResourceDataAsync(keyVault, r => r.HasData, r => r.GetAsync, cts.Token)).Data.Properties.VaultUri;
                            var key = await storageAccount.GetKeysAsync(cancellationToken: cts.Token).FirstAsync(cts.Token);
                            await SetStorageKeySecret(keyVaultUri, StorageAccountKeySecretName, key.Value);
                        });
                    }

                    if (postgreSqlFlexServer is null)
                    {
                        postgreSqlDnsZone = await CreatePrivateDnsZoneAsync(vnetAndSubnet.Value.virtualNetwork, $"privatelink.{azureCloudConfig.Suffixes.PostgresqlServerEndpointSuffix}", "PostgreSQL Server");
                    }

                    await Task.WhenAll(
                    [
                        Task.Run(async () =>
                        {
                            if (aksCluster is null && !configuration.ManualHelmDeployment)
                            {
                                aksCluster = await ProvisionManagedClusterAsync(managedIdentity, logAnalyticsWorkspace, vnetAndSubnet?.vmSubnet.Id, configuration.PrivateNetworking.GetValueOrDefault());
                                await AssignMeAsRbacClusterAdminToManagedClusterAsync(aksCluster);
                                aksCluster = await EnableWorkloadIdentity(aksCluster, managedIdentity, resourceGroup);
                            }
                        }),
                        Task.Run(async () =>
                        {
                            batchAccount ??= await CreateBatchAccountAsync(storageAccount.Id);
                            await AssignVmAsContributorToBatchAccountAsync(managedIdentity, batchAccount);
                        }),
                        Task.Run(async () =>
                        {
                            appInsights = await CreateAppInsightsResourceAsync(new(configuration.LogAnalyticsArmId));
                            await AssignVmAsContributorToAppInsightsAsync(managedIdentity, appInsights);
                        }),
                        Task.Run(async () =>
                        {
                            postgreSqlFlexServer ??= await CreatePostgreSqlServerAndDatabaseAsync(vnetAndSubnet.Value.postgreSqlSubnet, postgreSqlDnsZone);
                        })
                    ]);

                    if (string.IsNullOrEmpty(configuration.BatchNodesSubnetId))
                    {
                        configuration.BatchNodesSubnetId = vnetAndSubnet.Value.batchSubnet.Id;
                    }

                    var clientId = managedIdentity.Data.ClientId;
                    var settings = ConfigureSettings(clientId?.ToString("D"));
                    await BuildPushAcrAsync(settings, targetVersion, managedIdentity);

                    await kubernetesManager.UpdateHelmValuesAsync(storageAccountData, keyVaultUri, resourceGroup.Id.Name, settings, managedIdentity.Data);
                    await PerformHelmDeploymentAsync(aksCluster,
                        [
                            "Run the following postgresql command to setup the database.",
                            $"\tPostgreSQL command: psql postgresql://{configuration.PostgreSqlAdministratorLogin}:{configuration.PostgreSqlAdministratorPassword}@{configuration.PostgreSqlServerName}.{azureCloudConfig.Suffixes.PostgresqlServerEndpointSuffix}/{configuration.PostgreSqlTesDatabaseName} -c \"{GetCreateTesUserString()}\""
                        ],
                        async kubernetesClient =>
                        {
                            // Deploy an ubuntu pod to run PSQL commands, then delete it
                            const string deploymentNamespace = "default";
                            var (deploymentName, ubuntuDeployment) = KubernetesManager.GetUbuntuDeploymentTemplate(configuration.PrivatePSQLUbuntuImage);
                            await kubernetesClient.AppsV1.CreateNamespacedDeploymentAsync(ubuntuDeployment, deploymentNamespace, cancellationToken: cts.Token);
                            await ExecuteQueriesOnAzurePostgreSQLDbFromK8(kubernetesClient, deploymentName, deploymentNamespace);
                            await kubernetesClient.AppsV1.DeleteNamespacedDeploymentAsync(deploymentName, deploymentNamespace, cancellationToken: cts.Token);

                            if (configuration.EnableIngress.GetValueOrDefault())
                            {
                                var tmpValues = await kubernetesManager.ConfigureAltLocalValuesYamlAsync("no-ingress.yml", values => values.Service["enableIngress"] = $"{false}");
                                var backupValues = kubernetesManager.SwapLocalValuesYaml(tmpValues);
                                await kubernetesManager.DeployHelmChartToClusterAsync(kubernetesClient);
                                kubernetesManager.RestoreLocalValuesYaml(backupValues);

                                await Execute(
                                    $"Enabling Ingress {kubernetesManager.TesHostname}",
                                    async () =>
                                    {
                                        _ = await kubernetesManager.EnableIngress(configuration.TesUsername, configuration.TesPassword, kubernetesClient);
                                    });
                            }
                        });
                }

                TesCredentials tesCredentials = default;

                if (configuration.OutputTesCredentialsJson.GetValueOrDefault())
                {
                    // Write credentials to JSON file in working directory
                    tesCredentials = new TesCredentials(kubernetesManager.TesHostname, configuration.TesUsername, configuration.TesPassword);
                    var credentialsJson = tesCredentials.Serialize();

                    var credentialsPath = Path.Combine(Directory.GetCurrentDirectory(), TesCredentialsFileName);
                    await File.WriteAllTextAsync(credentialsPath, credentialsJson, cts.Token);
                    ConsoleEx.WriteLine($"TES credentials file written to: {credentialsPath}");
                }

                var batchAccountData = (await EnsureResourceDataAsync(batchAccount, r => r.HasData, r => r.GetAsync, cts.Token)).Data;
                var maxPerFamilyQuota = batchAccountData.IsDedicatedCoreQuotaPerVmFamilyEnforced ?? false ? batchAccountData.DedicatedCoreQuotaPerVmFamily.Select(q => q.CoreQuota ?? 0).Where(q => 0 != q) : Enumerable.Repeat(batchAccountData.DedicatedCoreQuota ?? 0, 1);
                var isBatchQuotaAvailable = batchAccountData.LowPriorityCoreQuota > 0 || (batchAccountData.DedicatedCoreQuota > 0 && maxPerFamilyQuota.Append(0).Max() > 0);
                var isBatchPoolQuotaAvailable = batchAccountData.PoolQuota > 0;
                var isBatchJobQuotaAvailable = batchAccountData.ActiveJobAndJobScheduleQuota > 0;
                var insufficientQuotas = new List<string>();
                int exitCode;

                if (!isBatchQuotaAvailable) insufficientQuotas.Add("core");
                if (!isBatchPoolQuotaAvailable) insufficientQuotas.Add("pool");
                if (!isBatchJobQuotaAvailable) insufficientQuotas.Add("job");

                if (0 != insufficientQuotas.Count)
                {
                    if (!configuration.SkipTestWorkflow)
                    {
                        ConsoleEx.WriteLine("Could not run the test task.", ConsoleColor.Yellow);
                    }

                    var quotaMessage = string.Join(" and ", insufficientQuotas);
                    var batchAccountName = configuration.BatchAccountName;
                    ConsoleEx.WriteLine($"Deployment was successful, but Batch account {batchAccountName} does not have sufficient {quotaMessage} quota to run workflows.", ConsoleColor.Yellow);
                    ConsoleEx.WriteLine($"Request Batch {quotaMessage} quota: https://docs.microsoft.com/en-us/azure/batch/batch-quota-limit", ConsoleColor.Yellow);
                    ConsoleEx.WriteLine("After receiving the quota, read the docs to run a test workflow and confirm successful deployment.", ConsoleColor.Yellow);

                    exitCode = 2;
                }
                else
                {
                    if (configuration.SkipTestWorkflow)
                    {
                        exitCode = 0;
                    }
                    else
                    {
                        using var tokenSource = new CancellationTokenSource();
                        var release = cts.Token.Register(tokenSource.Cancel);
                        var deleteResourceGroupTask = Task.CompletedTask;

                        try
                        {
                            var startPortForward = new Func<CancellationToken, Task>(token =>
                                kubernetesManager.ExecKubectlProcessAsync($"port-forward -n {configuration.AksCoANamespace} svc/tes 8088:80", token, appendKubeconfig: true));

                            var portForwardTask = startPortForward(tokenSource.Token);
                            await Task.Delay(longRetryWaitTime * 2, tokenSource.Token); // Give enough time for kubectl to standup the port forwarding.
                            var testsToRun = Enumerable.Empty<Func<Task<bool>>>()
                                .Append(() => RunTestTaskAsync(
                                    "localhost:8088",
                                    isPreemptible: batchAccountData.LowPriorityCoreQuota > 0));

                            if (configuration.RunIntTests)
                            {
                                testsToRun = testsToRun.Append(() => RunIntegrationTestsAsync(
                                        "localhost:8088",
                                        tesCredentials,
                                        isPreemptible: !(batchAccountData.DedicatedCoreQuota >= 2 && maxPerFamilyQuota.Append(0).Max() >= 2),
                                        storageAccount,
                                        managedIdentity.Data.ClientId?.ToString("D")));
                            }

                            var isTestWorkflowSuccessful = true;

                            foreach (var testFactory in testsToRun)
                            {
                                var runTestTask = testFactory();

                                for (var task = await Task.WhenAny(portForwardTask, runTestTask);
                                    isTestWorkflowSuccessful && runTestTask != task;
                                    task = await Task.WhenAny(portForwardTask, runTestTask))
                                {
                                    try
                                    {
                                        await portForwardTask;
                                    }
                                    catch (Exception ex)
                                    {
                                        ConsoleEx.WriteLine($"kubectl stopped unexpectedly ({ex.Message}).", ConsoleColor.Red);
                                    }

                                    ConsoleEx.WriteLine($"Restarting kubectl...");
                                    portForwardTask = startPortForward(tokenSource.Token);
                                }

                                isTestWorkflowSuccessful &= await runTestTask;
                            }

                            exitCode = isTestWorkflowSuccessful ? 0 : 1;

                            if (!isTestWorkflowSuccessful)
                            {
                                deleteResourceGroupTask = DeleteResourceGroupIfUserConsentsAsync();
                            }
                        }
                        catch (Exception e)
                        {
                            ConsoleEx.WriteLine("Exception occurred running test task.", ConsoleColor.Red);
                            ConsoleEx.Write(e.Message, ConsoleColor.Red);
                            exitCode = 1;
                        }
                        finally
                        {
                            _ = release.Unregister();
                            tokenSource.Cancel();
                            await deleteResourceGroupTask;
                        }
                    }
                }

                ConsoleEx.WriteLine($"Completed in {mainTimer.Elapsed.TotalMinutes:n1} minutes.");
                return exitCode;
            }
            catch (ValidationException validationException)
            {
                DisplayValidationExceptionAndExit(validationException);
                return 1;
            }
            catch (Exception exc)
            {
                if (!(exc is OperationCanceledException && cts.Token.IsCancellationRequested))
                {
                    ConsoleEx.WriteLine();
                    ConsoleEx.WriteLine($"{exc.GetType().FullName}: {exc.Message}", ConsoleColor.Red);

                    if (configuration.DebugLogging)
                    {
                        ConsoleEx.WriteLine(exc.StackTrace, ConsoleColor.Red);

                        if (exc is KubernetesException kExc)
                        {
                            ConsoleEx.WriteLine($"Kubenetes Status: {kExc.Status}");
                        }

                        if (exc is WebSocketException wExc)
                        {
                            ConsoleEx.WriteLine($"WebSocket ErrorCode: {wExc.WebSocketErrorCode}");
                        }

                        if (exc is RequestFailedException fExc)
                        {
                            ConsoleEx.WriteLine($"HTTP Response: {fExc.GetRawResponse().Content}");
                        }

                        if (exc is HttpRequestException rExc)
                        {
                            ConsoleEx.WriteLine($"HTTP Request StatusCode: {rExc.StatusCode}");
                            if (rExc.InnerException is not null)
                            {
                                ConsoleEx.WriteLine($"InnerException: {rExc.InnerException.GetType().FullName}: {rExc.InnerException.Message}");
                            }
                        }

                        if (exc is JsonReaderException jExc)
                        {
                            if (!string.IsNullOrEmpty(jExc.Path))
                            {
                                ConsoleEx.WriteLine($"JSON Path: {jExc.Path}");
                            }

                            if (jExc.Data.Contains("Body"))
                            {
                                ConsoleEx.WriteLine($"HTTP Response: {jExc.Data["Body"]}");
                            }
                        }
                    }
                }

                ConsoleEx.WriteLine();
                Debugger.Break();
                WriteGeneralRetryMessageToConsole();
                await DeleteResourceGroupIfUserConsentsAsync();
                return 1;
            }
            finally
            {
                if (!configuration.ManualHelmDeployment)
                {
                    kubernetesManager?.DeleteTempFiles();
                }
            }
        }

        private async Task PerformHelmDeploymentAsync(ContainerServiceManagedClusterResource cluster, IEnumerable<string> manualPrecommands = default, Func<IKubernetes, Task> asyncTask = default)
        {
            if (configuration.ManualHelmDeployment)
            {
                ConsoleEx.WriteLine($"Helm chart written to disk at: {kubernetesManager.helmScriptsRootDirectory}");
                ConsoleEx.WriteLine($"Please update values file if needed here: {kubernetesManager.TempHelmValuesYamlPath}");

                foreach (var line in manualPrecommands ?? [])
                {
                    ConsoleEx.WriteLine(line);
                }

                ConsoleEx.WriteLine($"Then, deploy the helm chart, and press Enter to continue.");
                ConsoleEx.ReadLine();
            }
            else
            {
                var kubernetesClient = await kubernetesManager.GetKubernetesClientAsync(cluster);
                await (asyncTask?.Invoke(kubernetesClient) ?? Task.CompletedTask);
                await kubernetesManager.DeployHelmChartToClusterAsync(kubernetesClient);
            }
        }

        private async ValueTask<BlobClient> CreateNextflowConfig(TesCredentials tesCredentials, StorageAccountResource storageAccount, string userAssignedManagedIdentityClientId)
        {
            StringBuilder sb = new();
            sb.AppendLine(@"plugins {");
            sb.AppendLine(@"  id 'nf-ga4gh'");
            sb.AppendLine(@"}");
            sb.AppendLine(@"process {");
            sb.AppendLine(@"  executor = 'tes'");
            sb.AppendLine(@"}");
            sb.AppendLine(@"azure {");
            sb.AppendLine(@"  managedIdentity {");
            sb.AppendLine($"    clientId='{userAssignedManagedIdentityClientId}'");
            sb.AppendLine(@"  }");
            sb.AppendLine(@"  storage {");
            sb.AppendLine($"    accountName='{storageAccount.Id.Name}'");
            sb.AppendLine(@"  }");
            sb.AppendLine(@"}");
            sb.AppendLine($"tes.endpoint='https://{tesCredentials.TesHostname}'");
            sb.AppendLine($"tes.basicUsername='{tesCredentials.TesUsername}'");
            sb.AppendLine($"tes.basicPassword='{tesCredentials.TesPassword}'");
            sb.AppendLine(@"process {");
            sb.AppendLine(@"  container='docker.io/library/ubuntu:latest'");
            sb.AppendLine(@"}");
            var configText = sb.ToString();
            var tesConfig = GetBlobClient(storageAccount.Data, InputsContainerName, "test/nextflow/tes.config");
            await UploadTextToStorageAccountAsync(tesConfig, configText, cts.Token);
            return tesConfig;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "We are explicitly using the contract specified in the ITesClient interface.")]
        private async Task<bool> RunNextflowTaskAsync(string tesHostname, bool isPreemptible, StorageAccountResource storageAccount, BlobClient tesConfig)
        {
            TesTask testTesTask = new();
            testTesTask.Resources.Preemptible = isPreemptible;
            testTesTask.Resources.CpuCores = 2;
            testTesTask.Resources.RamGb = 32;
            testTesTask.Resources.DiskGb = 100;

            testTesTask.Executors.Add(new()
            {
                //Image = "nextflow/nextflow:24.04.4",
                Image = "nextflow/nextflow:24.08.0-edge",
                Command = ["/bin/sh", "-c", "nextflow run seqeralabs/nf-canary -r main -c /tmp/tes.config -w 'az://outputs' || cat .nextflow.log"],
            });

            testTesTask.Inputs.Add(new()
            {
                Path = "/tmp/tes.config",
                Url = tesConfig.Uri.ToString()
            });

            using ITesClient tesClient = new TesClient(new($"http://{tesHostname}"));
            var completedTask = await tesClient.CreateAndWaitTilDoneAsync(testTesTask, cts.Token);
            ConsoleEx.WriteLine($"TES Task State: {completedTask.State}");

            if (completedTask.State != TesState.COMPLETE)
            {
                ConsoleEx.WriteLine($"Failure reason: {completedTask.FailureReason}");
            }

            return completedTask.State == TesState.COMPLETE;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "We are explicitly using the contract specified in the ITesClient interface.")]
        private async Task<bool> RunTesTaskImplAsync(string tesHostname, bool isPreemptible)
        {
            TesTask testTesTask = new();
            testTesTask.Resources.Preemptible = isPreemptible;
            testTesTask.Executors.Add(new()
            {
                Image = configuration.PrivateTestUbuntuImage,
                Command = ["/bin/sh", "-c", "cat /proc/sys/kernel/random/uuid"],
            });

            using ITesClient tesClient = new TesClient(new($"http://{tesHostname}"));
            var completedTask = await tesClient.CreateAndWaitTilDoneAsync(testTesTask, cts.Token);
            ConsoleEx.WriteLine($"TES Task State: {completedTask.State}");

            if (completedTask.State != TesState.COMPLETE)
            {
                ConsoleEx.WriteLine($"Failure reason: {completedTask.FailureReason}");
            }

            return completedTask.State == TesState.COMPLETE;
        }

        private async Task<bool> RunTestTaskAsync(string tesEndpoint, bool isPreemptible)
        {
            var startTime = DateTime.UtcNow;
            var line = ConsoleEx.WriteLine("Running a test task...");
            var isTestWorkflowSuccessful = await RunTesTaskImplAsync(tesEndpoint, isPreemptible);
            WriteExecutionTime(line, startTime);

            if (isTestWorkflowSuccessful)
            {
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine($"Test task succeeded.", ConsoleColor.Green);
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("Learn more about how to use Tes on Azure: https://github.com/microsoft/ga4gh-tes");
                ConsoleEx.WriteLine();
            }
            else
            {
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine($"Test task failed.", ConsoleColor.Red);
                ConsoleEx.WriteLine();
                WriteGeneralRetryMessageToConsole();
                ConsoleEx.WriteLine();
            }

            return isTestWorkflowSuccessful;
        }

        private async Task<bool> RunNextflowIntegrationTestAsync(string tesEndpoint, TesCredentials tesCredentials, bool isPreemptible, StorageAccountResource storageAccount, string userAssignedManagedIdentityClientId)
        {
            var tesConfig = await CreateNextflowConfig(tesCredentials, storageAccount, userAssignedManagedIdentityClientId);

            try
            {
                return await RunNextflowTaskAsync(tesEndpoint, isPreemptible, storageAccount, tesConfig);
            }
            finally
            {
                await tesConfig.DeleteIfExistsAsync(cancellationToken: CancellationToken.None);
            }
        }

        private async Task<bool> RunIntegrationTestsAsync(string tesEndpoint, TesCredentials tesCredentials, bool isPreemptible, StorageAccountResource storageAccount, string userAssignedManagedIdentityClientId)
        {
            var startTime = DateTime.UtcNow;
            var line = ConsoleEx.WriteLine("Running integration tests...");
            // TODO: Add more integration tests here
            var isTestWorkflowSuccessful = await RunNextflowIntegrationTestAsync(tesEndpoint, tesCredentials, isPreemptible, storageAccount, userAssignedManagedIdentityClientId);
            WriteExecutionTime(line, startTime);

            if (isTestWorkflowSuccessful)
            {
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine($"Integration test tasks succeeded.", ConsoleColor.Green);
                ConsoleEx.WriteLine();
            }
            else
            {
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine($"Integration test tasks failed.", ConsoleColor.Red);
                ConsoleEx.WriteLine();
                WriteGeneralRetryMessageToConsole();
                ConsoleEx.WriteLine();
            }

            return isTestWorkflowSuccessful;
        }

        private async Task<KeyVaultResource> ValidateAndGetExistingKeyVaultAsync()
        {
            if (string.IsNullOrWhiteSpace(configuration.KeyVaultName))
            {
                return null;
            }

            return (await GetKeyVaultAsync(configuration.KeyVaultName))
                ?? throw new ValidationException($"If key vault name is provided, it must already exist in region {configuration.RegionName}, and be accessible to the current user.", displayExample: false);
        }

        private async Task<PostgreSqlFlexibleServerResource> ValidateAndGetExistingPostgresqlServerAsync()
        {
            if (string.IsNullOrWhiteSpace(configuration.PostgreSqlServerName))
            {
                return null;
            }

            return (await GetExistingPostgresqlServiceAsync(configuration.PostgreSqlServerName))
                ?? throw new ValidationException($"If Postgresql server name is provided, the server must already exist in region {configuration.RegionName}, and be accessible to the current user.", displayExample: false);
        }

        private async Task<ContainerServiceManagedClusterResource> ValidateAndGetExistingAKSClusterAsync()
        {
            if (string.IsNullOrWhiteSpace(configuration.AksClusterName))
            {
                return null;
            }

            return (await GetExistingAKSClusterAsync(configuration.AksClusterName))
                ?? throw new ValidationException($"If AKS cluster name is provided, the cluster must already exist in region {configuration.RegionName}, and be accessible to the current user.", displayExample: false);
        }

        private async Task<PostgreSqlFlexibleServerResource> GetExistingPostgresqlServiceAsync(string serverName)
        {
            return await subscriptionIds.ToAsyncEnumerable().Select(s =>
            {
                try
                {
                    return s.GetPostgreSqlFlexibleServersAsync(cts.Token);
                }
                catch (Exception e)
                {
                    ConsoleEx.WriteLine(e.Message);
                    return null;
                }
            })
            .Where(a => a is not null)
            .SelectMany(a => a)
            .SelectAwaitWithCancellation(async (a, ct) => await FetchResourceDataAsync(a.GetAsync, ct))
            .SingleOrDefaultAsync(a =>
                    a.Id.Name.Equals(serverName, StringComparison.OrdinalIgnoreCase) &&
                    a.Data.Location.Name.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase),
                cts.Token);
        }

        private async Task<ContainerServiceManagedClusterResource> GetExistingAKSClusterAsync(string aksClusterName)
        {
            return await subscriptionIds.ToAsyncEnumerable()
                .SelectAwaitWithCancellation((sub, token) => ValueTask.FromResult<IAsyncEnumerable<ContainerServiceManagedClusterResource>>(
                    sub.GetContainerServiceManagedClustersAsync(token)))
                .Where(a => a is not null)
                .SelectMany(a => a)
                .SelectAwaitWithCancellation((resource, token) =>
                    SafeSelectAsync(async () => (await resource.GetAsync(token)).Value))
                .Where(a => a is not null)
                .SingleOrDefaultAsync(a =>
                        a.Data.Name.Equals(aksClusterName, StringComparison.OrdinalIgnoreCase) &&
                        a.Data.Location.Name.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase),
                    cts.Token);

            static async ValueTask<TOut> SafeSelectAsync<TOut>(Func<ValueTask<TOut>> selector) where TOut : class
            {
                try
                {
                    return await selector();
                }
                catch (Exception e)
                {
                    ConsoleEx.WriteLine(e.Message);
                    return null;
                }
            }
        }

        private static void ManagedClusterEnableManagedAad(ContainerServiceManagedClusterData managedCluster)
        {
            managedCluster.EnableRbac = true;
            managedCluster.AadProfile ??= new();
            managedCluster.AadProfile.IsAzureRbacEnabled = true;
            managedCluster.AadProfile.IsManagedAadEnabled = true;
            managedCluster.AadProfile.AdminGroupObjectIds.ToList().ForEach(item => _ = managedCluster.AadProfile.AdminGroupObjectIds.Remove(item));
        }

        private async Task<ContainerServiceManagedClusterResource> ProvisionManagedClusterAsync(UserAssignedIdentityResource managedIdentity, OperationalInsightsWorkspaceResource logAnalyticsWorkspace, ResourceIdentifier subnetId, bool privateNetworking)
        {
            var uami = await EnsureResourceDataAsync(managedIdentity, r => r.HasData, r => r.GetAsync, cts.Token);
            var nodePoolName = "nodepool1";
            ContainerServiceManagedClusterData cluster = new(new(configuration.RegionName))
            {
                DnsPrefix = configuration.AksClusterName,
                NetworkProfile = new()
                {
                    NetworkPlugin = ContainerServiceNetworkPlugin.Azure,
                    ServiceCidr = configuration.KubernetesServiceCidr,
                    DnsServiceIP = configuration.KubernetesDnsServiceIP,
                    DockerBridgeCidr = configuration.KubernetesDockerBridgeCidr,
                    NetworkPolicy = ContainerServiceNetworkPolicy.Azure
                }
            };

            ManagedClusterAddonProfile clusterAddonProfile = new(isEnabled: true);
            clusterAddonProfile.Config.Add("logAnalyticsWorkspaceResourceID", logAnalyticsWorkspace.Id);
            cluster.AddonProfiles.Add("omsagent", clusterAddonProfile);
            ManagedClusterEnableManagedAad(cluster);
            Azure.ResourceManager.Models.ManagedServiceIdentity identity = new(Azure.ResourceManager.Models.ManagedServiceIdentityType.UserAssigned);
            identity.UserAssignedIdentities.Add(uami.Id, new());
            cluster.Identity = identity;
            cluster.IdentityProfile.Add("kubeletidentity", new() { ResourceId = uami.Id, ClientId = uami.Data.ClientId, ObjectId = uami.Data.PrincipalId });

            cluster.AgentPoolProfiles.Add(new(nodePoolName)
            {
                Count = configuration.AksPoolSize,
                VmSize = configuration.VmSize,
                OSDiskSizeInGB = 128,
                OSDiskType = ContainerServiceOSDiskType.Managed,
                EnableEncryptionAtHost = true,
                AgentPoolType = AgentPoolType.VirtualMachineScaleSets,
                EnableAutoScaling = false,
                EnableNodePublicIP = false,
                OSType = ContainerServiceOSType.Linux,
                OSSku = ContainerServiceOSSku.AzureLinux,
                Mode = AgentPoolMode.System,
                VnetSubnetId = subnetId,
            });

            if (privateNetworking)
            {
                cluster.ApiServerAccessProfile = new()
                {
                    EnablePrivateCluster = true,
                    EnablePrivateClusterPublicFqdn = false
                };

                cluster.PublicNetworkAccess = ContainerServicePublicNetworkAccess.Disabled;
            }

            return await Execute(
                $"Creating AKS Cluster: {configuration.AksClusterName}...",
                async () => (await resourceGroup.GetContainerServiceManagedClusters().CreateOrUpdateAsync(Azure.WaitUntil.Completed, configuration.AksClusterName, cluster, cts.Token)).Value);
        }

        private async Task<ContainerServiceManagedClusterResource> EnableWorkloadIdentity(ContainerServiceManagedClusterResource aksCluster, UserAssignedIdentityResource managedIdentity, ResourceGroupResource resourceGroup)
        {
            aksCluster.Data.SecurityProfile.IsWorkloadIdentityEnabled = true;
            aksCluster.Data.OidcIssuerProfile.IsEnabled = true;
            var aksClusterCollection = resourceGroup.GetContainerServiceManagedClusters();

            var cluster = await Execute("Updating AKS cluster...",
                async () => await operationNotAllowedConflictRetryPolicy.ExecuteAsync(token => aksClusterCollection.CreateOrUpdateAsync(WaitUntil.Completed, aksCluster.Data.Name, aksCluster.Data, token), cts.Token));

            var aksOidcIssuer = cluster.Value.Data.OidcIssuerProfile.IssuerUriInfo;

            var federatedCredentialsCollection = managedIdentity.GetFederatedIdentityCredentials();

            if ((await federatedCredentialsCollection.SingleOrDefaultAsync(r => "toaFederatedIdentity".Equals(r.Id.Name, StringComparison.OrdinalIgnoreCase), cts.Token)) is null)
            {
                var data = new FederatedIdentityCredentialData()
                {
                    IssuerUri = new Uri(aksOidcIssuer),
                    Subject = $"system:serviceaccount:{configuration.AksCoANamespace}:{managedIdentity.Id.Name}-sa"
                };
                data.Audiences.Add("api://AzureADTokenExchange");

                await Execute("Enabling workload identity...",
                    async () => _ = await operationNotAllowedConflictRetryPolicy.ExecuteAsync(token => federatedCredentialsCollection.CreateOrUpdateAsync(WaitUntil.Completed, "toaFederatedIdentity", data, token), cts.Token));
            }

            return cluster.Value;
        }

        private async Task BuildPushAcrAsync(Dictionary<string, string> settings, string targetVersion, UserAssignedIdentityResource managedIdentity)
        {
            ContainerRegistryResource acr = default;
            Azure.Containers.ContainerRegistry.ContainerRegistryClient client = default;

            if (settings.TryGetValue("AcrId", out var acrId) && !string.IsNullOrEmpty(acrId))
            {
                acr = await EnsureResourceDataAsync(armClient.GetContainerRegistryResource(new(acrId)), r => r.HasData, r => r.GetAsync, cts.Token);
            }

            // No build needed if the image is not in the registries this deployer manages or if the same version is being upgraded with no explicit source code provided.
            {
                var acrRequested = !((string[])[configuration.AcrId, configuration.GitHubCommit, configuration.SolutionDir]).All(string.IsNullOrWhiteSpace);
                var sameVersionUpgrade = bool.Parse(settings["SameVersionUpgrade"]);
                var tesUpgraded = !settings["TesImageName"].Equals(settings["PreviousTesImageName"]);
                var canReturnEarly = sameVersionUpgrade;

                if (acr is null && !acrRequested &&
                    ((string[])[settings["TesImageName"]]).All(name => !name.StartsWith("mcr.microsoft.com/")))
                {
                    settings["ActualTesImageName"] = settings["TesImageName"];
                }
                else
                {
                    canReturnEarly = sameVersionUpgrade && !tesUpgraded && !string.IsNullOrEmpty(settings["ActualTesImageName"]);
                }

                if (canReturnEarly)
                {
                    return; // No ACR build needed
                }
            }

            if (acr is null)
            {
                if (string.IsNullOrWhiteSpace(configuration.AcrId))
                {
                    var name = Utility.RandomResourceName(configuration.MainIdentifierPrefix, 25);
                    acr = await Execute($"Creating Container Registry: {name}...",
                        async () => (await resourceGroup.GetContainerRegistries().CreateOrUpdateAsync(WaitUntil.Completed, name, new(new(configuration.RegionName), new(Azure.ResourceManager.ContainerRegistry.Models.ContainerRegistrySkuName.Standard)), cts.Token)).Value);
                    await AssignManagedIdAcrPullToResourceAsync(managedIdentity, acr);
                    settings["AcrId"] = acr.Id;
                }
                else
                {
                    acr = await EnsureResourceDataAsync(armClient.GetContainerRegistryResource(new(configuration.AcrId)), r => r.HasData, r => r.GetAsync, cts.Token);
                    ConsoleEx.WriteLine($"Using existing Container Registry {acr.Id.Name}");
                    await AssignManagedIdAcrPullToResourceAsync(managedIdentity, acr);
                    settings["AcrId"] = acr.Id;
                }
            }

            var build = await Execute($"Building TES image on {acr.Id.Name}...",
                () => buildPushAcrRetryPolicy.ExecuteAsync(async () =>
                {
                    AcrBuild build = default;
                    await Policy.Handle<Microsoft.Kiota.Abstractions.ApiException>(ae => (int)HttpStatusCode.Unauthorized == ae.ResponseStatusCode)
                        .WaitAndRetryAsync([TimeSpan.FromSeconds(1)], (ae, _) =>
                        {
                            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("GITHUB_TOKEN")))
                            {
                                throw new InvalidOperationException("GitHub returned an authentication error.", ae);
                            }

                            Console.WriteLine("GitHub returned an authentication error. Retrying anonymously.");
                            Environment.SetEnvironmentVariable("GITHUB_TOKEN", null);
                        })
                        .ExecuteAsync(async token =>
                        {
                            IAsyncDisposable tarDisposable = default;

                            try
                            {
                                IArchive tar;

                                if (string.IsNullOrWhiteSpace(configuration.SolutionDir))
                                {
                                    tar = GitHubArchive.Create(BuildType.Tes, string.IsNullOrWhiteSpace(configuration.GitHubCommit) ? new Version(targetVersion).ToString(3) : configuration.GitHubCommit, GitHubArchive.GetAccessTokenProvider());
                                    tarDisposable = tar as IAsyncDisposable;
                                }
                                else
                                {
                                    tar = LocalGitArchive.Create(new(configuration.SolutionDir));
                                }

                                build = new(BuildType.Tes, await tar.GetTagAsync(token), acr.Id, tokenCredential, new Azure.Containers.ContainerRegistry.ContainerRegistryAudience(azureCloudConfig.ArmEnvironment.Value.Endpoint.AbsoluteUri));
                                await build.LoadAsync(tar, azureCloudConfig.ArmEnvironment.Value, token);
                            }
                            finally
                            {
                                await (tarDisposable?.DisposeAsync() ?? ValueTask.CompletedTask);
                            }
                        },
                        cts.Token);

                    if (!await Policy
                        .HandleResult(false)
                        .WaitAndRetryAsync(Enumerable.Repeat(TimeSpan.FromSeconds(1), 2), (_, _) => Console.WriteLine("Retrying build."))
                        .ExecuteAsync(async token =>
                        {
                            var (buildSuccess, buildLog) = await build.BuildAsync(configuration.DebugLogging ? LogType.Interactive : LogType.CapturedOnError, token);

                            if (!buildSuccess && !string.IsNullOrWhiteSpace(buildLog))
                            {
                                ConsoleEx.WriteLine(buildLog);
                            }

                            return buildSuccess;
                        },
                        cts.Token))
                    {
                        throw new InvalidOperationException("Build failed.");
                    }

                    return build;
                }));

            var tesDigest = (await acrGetDigestRetryPolicy.ExecuteAsync(token => (client ??= GetClient()).GetArtifact("cromwellonazure/tes", build.Tag.ToString()).GetManifestPropertiesAsync(token), cts.Token)).Value.Digest;
            settings["ActualTesImageName"] = $"{acr.Data.LoginServer}/cromwellonazure/tes@{tesDigest}";

            Azure.Containers.ContainerRegistry.ContainerRegistryClient GetClient()
                => new(new UriBuilder() { Scheme = Uri.UriSchemeHttps, Host = acr.Data.LoginServer }.Uri, tokenCredential, new() { Audience = cloudEnvironment.ArmEnvironment.Audience, RetryPolicy = GetRetryPolicy(new()) });
        }

        private static Dictionary<string, string> GetDefaultValues(string[] files)
        {
            var settings = new Dictionary<string, string>();

            foreach (var file in files)
            {
                settings = settings.Union(Utility.DelimitedTextToDictionary(Utility.GetFileContent("scripts", file))).ToDictionary(kv => kv.Key, kv => kv.Value);
            }

            return settings;
        }

        private Dictionary<string, string> ConfigureSettings(string managedIdentityClientId, Dictionary<string, string> settings = null, Version installedVersion = null)
        {
            settings ??= [];
            var defaults = GetDefaultValues(["env-00-tes-version.txt", "env-01-account-names.txt", "env-02-internal-images.txt", "env-04-settings.txt"]);
            var currentTime = DateTime.UtcNow;

            // We always overwrite the CoA version
            UpdateSetting(settings, defaults, "TesOnAzureVersion", default(string), ignoreDefaults: false);
            settings["SameVersionUpgrade"] = (installedVersion?.ToString() ?? string.Empty).Equals(new(defaults["TesOnAzureVersion"])).ToString();
            UpdateSetting(settings, defaults, "ResourceGroupName", configuration.ResourceGroupName, ignoreDefaults: false);
            UpdateSetting(settings, defaults, "RegionName", configuration.RegionName, ignoreDefaults: false);
            UpdateSetting(settings, defaults, "DeploymentUpdated", currentTime.ToString("O"), ignoreDefaults: false);

            // Process images
            CopySetting("TesImageName", "PreviousTesImageName");
            UpdateSetting(settings, defaults, "TesImageName", configuration.TesImageName,
                ignoreDefaults: ImageNameIgnoreDefaults(settings, defaults, "TesImageName", configuration.TesImageName is null, installedVersion));

            // Additional non-personalized settings
            UpdateSetting(settings, defaults, "BatchNodesSubnetId", configuration.BatchNodesSubnetId);
            UpdateSetting(settings, defaults, "DisableBatchNodesPublicIpAddress", configuration.DisableBatchNodesPublicIpAddress, b => b.GetValueOrDefault().ToString(), configuration.DisableBatchNodesPublicIpAddress.GetValueOrDefault().ToString());
            UpdateSetting(settings, defaults, "DeploymentOrganizationName", configuration.DeploymentOrganizationName);
            UpdateSetting(settings, defaults, "DeploymentOrganizationUrl", configuration.DeploymentOrganizationUrl);
            UpdateSetting(settings, defaults, "DeploymentContactUri", configuration.DeploymentContactUri);
            UpdateSetting(settings, defaults, "DeploymentEnvironment", configuration.DeploymentEnvironment);

            if (installedVersion is null)
            {
                UpdateSetting(settings, defaults, "AzureCloudName", configuration.AzureCloudName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "BatchPrefix", configuration.BatchPrefix, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "DefaultStorageAccountName", configuration.StorageAccountName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "ExecutionsContainerName", TesInternalContainerName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "BatchAccountName", configuration.BatchAccountName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "ApplicationInsightsAccountName", configuration.ApplicationInsightsAccountName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "ManagedIdentityClientId", managedIdentityClientId, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "AzureServicesAuthConnectionString", $"RunAs=Workload;AppId={managedIdentityClientId}", ignoreDefaults: true);
                UpdateSetting(settings, defaults, "KeyVaultName", configuration.KeyVaultName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "AksCoANamespace", configuration.AksCoANamespace, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "CrossSubscriptionAKSDeployment", configuration.CrossSubscriptionAKSDeployment);
                UpdateSetting(settings, defaults, "PostgreSqlServerName", configuration.PostgreSqlServerName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlServerNameSuffix", configuration.PostgreSqlServerNameSuffix, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlServerPort", configuration.PostgreSqlServerPort.ToString(), ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlServerSslMode", configuration.PostgreSqlServerSslMode, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlTesDatabaseName", configuration.PostgreSqlTesDatabaseName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlTesDatabaseUserLogin", configuration.PostgreSqlTesUserLogin, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlTesDatabaseUserPassword", configuration.PostgreSqlTesUserPassword, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "EnableIngress", configuration.EnableIngress);
                UpdateSetting(settings, defaults, "LetsEncryptEmail", configuration.LetsEncryptEmail);
                UpdateSetting(settings, defaults, "TesHostname", kubernetesManager.TesHostname, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "DeploymentCreated", currentTime.ToString("O"), ignoreDefaults: true);
            }

            BackFillSettings(settings, defaults);
            return settings;

            void CopySetting(string key, string newKey, string @default = null)
                => settings[newKey] = settings.TryGetValue(key, out var value) ? value : @default;
        }

        /// <summary>
        /// Determines if current setting should be ignored (used for product image names)
        /// </summary>
        /// <param name="settings">Property bag being updated.</param>
        /// <param name="defaults">Property bag containing default values.</param>
        /// <param name="key">Key of value in both <paramref name="settings"/> and <paramref name="defaults"/>.</param>
        /// <param name="valueIsNull">True if configuration value to set is null, otherwise False.</param>
        /// <param name="installedVersion"><see cref="Version"/> of currently installed deployment, or null if not an update.</param>
        /// <returns>False if current setting should be ignored, null otherwise.</returns>
        /// <remarks>This method provides a value for the "ignoreDefaults" parameter to <see cref="UpdateSetting{T}(Dictionary{string, string}, Dictionary{string, string}, string, T, Func{T, string}, string, bool?)"/> for use with container image names.</remarks>
        private static bool? ImageNameIgnoreDefaults(Dictionary<string, string> settings, Dictionary<string, string> defaults, string key, bool valueIsNull, Version installedVersion)
        {
            if (installedVersion is null || !valueIsNull)
            {
                return null;
            }

            var sameVersionUpgrade = bool.Parse(settings["SameVersionUpgrade"]);
            _ = settings.TryGetValue(key, out var installed);
            _ = defaults.TryGetValue(key, out var @default);

            var defaultPath = @default?[..@default.LastIndexOf(':')];
            var installedTag = installed?[(installed.LastIndexOf(':') + 1)..];
            bool? result;

            try
            {
                // Determine if the installed image is from our official repository
                result = installed.StartsWith(defaultPath + ":")
                        // Attempt to parse the tag as a version (ignoring any decorations)
                        && Version.TryParse(installedTag, out var version)
                        // Check if the parsed version matches the installed version
                        && version.Equals(installedVersion)
                    // If not customized, consider it as not requiring an upgrade
                    ? false
                    // If customized, preserve the configured image without upgrading
                    : null;
            }
            catch (ArgumentException)
            {
                result = null;
            }

            if (result is null && !sameVersionUpgrade)
            {
                ConsoleEx.WriteLine($"Warning: TES on Azure is being upgraded, but {key} was customized, and is not being upgraded, which might not be what you want. (To remove the customization of {key}, set it to the empty string.)", ConsoleColor.Yellow);
            }

            return result;
        }

        /// <summary>
        /// Populates <paramref name="settings"/> with missing values.
        /// </summary>
        /// <param name="settings">Property bag being updated.</param>
        /// <param name="defaults">Property bag containing default values.</param>
        /// <remarks>Copy to settings any missing values found in defaults.</remarks>
        private static void BackFillSettings(Dictionary<string, string> settings, Dictionary<string, string> defaults)
        {
            foreach (var key in defaults.Keys.Except(settings.Keys))
            {
                settings[key] = defaults[key];
            }
        }

        /// <summary>
        /// Updates <paramref name="settings"/>.
        /// </summary>
        /// <typeparam name="T">Type of <paramref name="value"/>.</typeparam>
        /// <param name="settings">Property bag being updated.</param>
        /// <param name="defaults">Property bag containing default values.</param>
        /// <param name="key">Key of value in both <paramref name="settings"/> and <paramref name="defaults"/>.</param>
        /// <param name="value">Configuration value to set. Nullable. See remarks.</param>
        /// <param name="ConvertValue">Function that converts <paramref name="value"/> to a string. Can be used for formatting. Defaults to returning the value's string.</param>
        /// <param name="defaultValue">Value to use if <paramref name="defaults"/> does not contain a record for <paramref name="key"/> when <paramref name="value"/> is null.</param>
        /// <param name="ignoreDefaults">True to never use value from <paramref name="defaults"/>, False to never keep the value from <paramref name="settings"/>, null to follow remarks.</param>
        /// <remarks>
        /// If value is null, keep the value already in <paramref name="settings"/>. If the key is not in <paramref name="settings"/>, set the corresponding value from <paramref name="defaults"/>. If key is not found in <paramref name="defaults"/>, use <paramref name="defaultValue"/>.
        /// Otherwise, convert value to a string using <paramref name="ConvertValue"/>.
        /// </remarks>
        private static void UpdateSetting<T>(Dictionary<string, string> settings, Dictionary<string, string> defaults, string key, T value, Func<T, string> ConvertValue = default, string defaultValue = "", bool? ignoreDefaults = null)
        {
            ConvertValue ??= new(v => v switch
            {
                string s => s,
                _ => v?.ToString(),
            });

            var valueIsNull = value is null;
            var valueIsNullOrEmpty = valueIsNull || value switch
            {
                string s => string.IsNullOrWhiteSpace(s),
                _ => string.IsNullOrWhiteSpace(value?.ToString()),
            };

            if (valueIsNull && settings.ContainsKey(key) && ignoreDefaults != false)
            {
                return; // No changes to this setting, no need to rewrite it.
            }

            var GetDefault = new Func<string>(() => ignoreDefaults switch
            {
                true => defaultValue,
                _ => defaults.TryGetValue(key, out var @default) ? @default : defaultValue,
            });

            settings[key] = valueIsNullOrEmpty ? GetDefault() : ConvertValue(value);
        }

        private async Task RegisterResourceProvidersAsync()
        {
            var unregisteredResourceProviders = await GetRequiredResourceProvidersNotRegisteredAsync();

            if (unregisteredResourceProviders.Count == 0)
            {
                return;
            }

            try
            {
                await Execute(
                    $"Registering resource providers...",
                    async () =>
                    {
                        await Task.WhenAll(
                            unregisteredResourceProviders.Select(rp =>
                                rp.RegisterAsync(cancellationToken: cts.Token))
                        );

                        // RP registration takes a few minutes; poll until done registering

                        while (!cts.IsCancellationRequested)
                        {
                            unregisteredResourceProviders = await GetRequiredResourceProvidersNotRegisteredAsync();

                            if (unregisteredResourceProviders.Count == 0)
                            {
                                break;
                            }

                            await Task.Delay(TimeSpan.FromSeconds(15), cts.Token);
                        }
                    });
            }
            catch (RequestFailedException ex) when (ex.ErrorCode.Equals("AuthorizationFailed", StringComparison.OrdinalIgnoreCase))
            {
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("Unable to programmatically register the required resource providers.", ConsoleColor.Red);
                ConsoleEx.WriteLine("This can happen if you don't have the Owner or Contributor role assignment for the subscription.", ConsoleColor.Red);
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("Please contact the Owner or Contributor of your Azure subscription, and have them:", ConsoleColor.Yellow);
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine($"1. Navigate to {azureCloudConfig.PortalUrl}", ConsoleColor.Yellow);
                ConsoleEx.WriteLine("2. Select Subscription -> Resource Providers", ConsoleColor.Yellow);
                ConsoleEx.WriteLine("3. Select each of the following and click Register:", ConsoleColor.Yellow);
                ConsoleEx.WriteLine();
                unregisteredResourceProviders.ForEach(rp => ConsoleEx.WriteLine($"- {rp.Data.Namespace}", ConsoleColor.Yellow));
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("After completion, please re-attempt deployment.");

                Environment.Exit(1);
            }
        }

        private async ValueTask<List<ResourceProviderResource>> GetRequiredResourceProvidersNotRegisteredAsync()
        {
            var cloudResourceProviders = armSubscription.GetResourceProviders().GetAllAsync(cancellationToken: cts.Token);

            var notRegisteredResourceProviders = await cloudResourceProviders
                .SelectAwaitWithCancellation(async (rp, ct) => await FetchResourceDataAsync(token => rp.GetAsync(cancellationToken: token), ct))
                .Where(rp => requiredResourceProviders.Contains(rp.Data.Namespace, StringComparer.OrdinalIgnoreCase))
                .Where(rp => !rp.Data.RegistrationState.Equals("Registered", StringComparison.OrdinalIgnoreCase))
                .ToListAsync(cts.Token);

            return notRegisteredResourceProviders;
        }

        private async Task RegisterResourceProviderFeaturesAsync()
        {
            var unregisteredFeatures = new List<FeatureResource>();
            try
            {
                await Execute(
                    $"Registering resource provider features...",
                    async () =>
                    {
                        foreach (var rpName in requiredResourceProviderFeatures.Keys)
                        {
                            var rp = await armSubscription.GetResourceProviderAsync(rpName, cancellationToken: cts.Token);

                            foreach (var featureName in requiredResourceProviderFeatures[rpName])
                            {
                                var feature = await rp.Value.GetFeatureAsync(featureName, cts.Token);

                                if (!string.Equals(feature.Value.Data.FeatureState, "Registered", StringComparison.OrdinalIgnoreCase))
                                {
                                    unregisteredFeatures.Add(feature);
                                    _ = await feature.Value.RegisterAsync(cts.Token);
                                }
                            }
                        }

                        while (!cts.IsCancellationRequested)
                        {
                            if (unregisteredFeatures.Count == 0)
                            {
                                break;
                            }

                            await Task.Delay(TimeSpan.FromSeconds(30), cts.Token);
                            var finished = new List<FeatureResource>();

                            foreach (var feature in unregisteredFeatures)
                            {
                                var update = await feature.GetAsync(cts.Token);

                                if (string.Equals(update.Value.Data.FeatureState, "Registered", StringComparison.OrdinalIgnoreCase))
                                {
                                    finished.Add(feature);
                                }
                            }
                            unregisteredFeatures.RemoveAll(x => finished.Contains(x));
                        }
                    });
            }
            catch (RequestFailedException ex) when (ex.ErrorCode.Equals("AuthorizationFailed", StringComparison.OrdinalIgnoreCase))
            {
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("Unable to programmatically register the required features.", ConsoleColor.Red);
                ConsoleEx.WriteLine("This can happen if you don't have the Owner or Contributor role assignment for the subscription.", ConsoleColor.Red);
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("Please contact the Owner or Contributor of your Azure subscription, and have them:", ConsoleColor.Yellow);
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("1. For each of the following, execute 'az feature register --namespace {RESOURCE_PROVIDER_NAME} --name {FEATURE_NAME}'", ConsoleColor.Yellow);
                ConsoleEx.WriteLine();
                unregisteredFeatures.ForEach(f => ConsoleEx.WriteLine($"- {f.Data.ResourceType.Namespace} - {f.Data.Name}", ConsoleColor.Yellow));
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("After completion, please re-attempt deployment.");

                Environment.Exit(1);
            }
        }

        private async Task<bool?> AssignRoleForDeployerToStorageAccountAsync(StorageAccountResource storageAccount)
        {
            Azure.ResourceManager.Authorization.Models.RoleManagementPrincipalType type;
            string id;

            if (string.IsNullOrWhiteSpace(configuration.ServicePrincipalId))
            {
                var user = await GetUserObjectAsync();

                if (user is null)
                {
                    return null;
                }

                id = user.Id;
                type = Azure.ResourceManager.Authorization.Models.RoleManagementPrincipalType.User;
            }
            else
            {
                id = configuration.ServicePrincipalId;
                type = Azure.ResourceManager.Authorization.Models.RoleManagementPrincipalType.ServicePrincipal;
            }

            return await AssignRoleToResourceAsync(
                        [new Guid(id)],
                        type,
                        storageAccount,
                        GetSubscriptionRoleDefinition(RoleDefinitions.Storage.StorageBlobDataContributor),
                        $"Assigning '{RoleDefinitions.GetDisplayName(RoleDefinitions.Storage.StorageBlobDataContributor)}' role for the deployment identity to Storage Account resource scope...");
        }

        private Task AssignManagedIdAcrPullToResourceAsync(UserAssignedIdentityResource managedIdentity, ContainerRegistryResource resource)
            => AssignRoleToResourceAsync(managedIdentity, resource, GetSubscriptionRoleDefinition(RoleDefinitions.Containers.AcrPull),
                $"Assigning '{RoleDefinitions.GetDisplayName(RoleDefinitions.Containers.AcrPull)}' role for user-managed identity to container registry resource scope...");

        private Task<bool> AssignMIAsNetworkContributorToResourceAsync(UserAssignedIdentityResource managedIdentity, ArmResource resource)
            => AssignRoleToResourceAsync(managedIdentity, resource, GetSubscriptionRoleDefinition(RoleDefinitions.Networking.NetworkContributor),
                $"Assigning '{RoleDefinitions.GetDisplayName(RoleDefinitions.Networking.NetworkContributor)}' role for user-managed identity to resource group scope...");

        private Task AssignManagedIdOperatorToResourceAsync(UserAssignedIdentityResource managedIdentity, ArmResource resource)
            => AssignRoleToResourceAsync(managedIdentity, resource, GetSubscriptionRoleDefinition(RoleDefinitions.Identity.ManagedIdentityOperator),
                $"Assigning '{RoleDefinitions.GetDisplayName(RoleDefinitions.Identity.ManagedIdentityOperator)}' role for user-managed identity to resource group scope...");

        private Task<bool> AssignVmAsDataOwnerToStorageAccountAsync(UserAssignedIdentityResource managedIdentity, StorageAccountResource storageAccount, bool cancelOnException = true)
            => AssignRoleToResourceAsync(managedIdentity, storageAccount, GetSubscriptionRoleDefinition(RoleDefinitions.Storage.StorageBlobDataOwner),
                $"Assigning '{RoleDefinitions.GetDisplayName(RoleDefinitions.Storage.StorageBlobDataOwner)}' role for user-managed identity to Storage Account resource scope...");

        private Task AssignVmAsContributorToStorageAccountAsync(UserAssignedIdentityResource managedIdentity, StorageAccountResource storageAccount)
            => AssignRoleToResourceAsync(managedIdentity, storageAccount, GetSubscriptionRoleDefinition(RoleDefinitions.General.Contributor),
                $"Assigning '{RoleDefinitions.GetDisplayName(RoleDefinitions.General.Contributor)}' role for user-managed identity to Storage Account resource scope...");

        private async Task AssignMeAsRbacClusterAdminToManagedClusterAsync(ContainerServiceManagedClusterResource managedCluster)
        {
            var message = $"Assigning '{RoleDefinitions.GetDisplayName(RoleDefinitions.Containers.RbacClusterAdmin)}' role for {{Admins}} to AKS cluster resource scope...";
            var roleDefinitionId = GetSubscriptionRoleDefinition(RoleDefinitions.Containers.RbacClusterAdmin);

            var adminGroupObjectIds = managedCluster.Data.AadProfile?.AdminGroupObjectIds ?? [];
            adminGroupObjectIds = adminGroupObjectIds.Count != 0 ? adminGroupObjectIds : (string.IsNullOrWhiteSpace(configuration.AadGroupIds) ? [] : configuration.AadGroupIds.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(Guid.Parse).ToList());

            Azure.ResourceManager.Authorization.Models.RoleManagementPrincipalType type;
            IEnumerable<Guid> principalIds;
            string admins;
            Func<Exception, Exception> transformException = default;

            if (adminGroupObjectIds.Count == 0)
            {
                admins = "deployer user";
                transformException = new(e =>
                {
                    if (e is RequestFailedException ex && ex.Status == 403 && "AuthorizationFailed".Equals(ex.ErrorCode, StringComparison.OrdinalIgnoreCase))
                    {
                        return new System.ComponentModel.WarningException("Insufficient authorization for role assignment. Skipping role assignment to AKS cluster resource scope.", e);
                    }

                    return e;
                });

                if (string.IsNullOrWhiteSpace(configuration.ServicePrincipalId))
                {
                    principalIds = [new(configuration.ServicePrincipalId)];
                    type = Azure.ResourceManager.Authorization.Models.RoleManagementPrincipalType.ServicePrincipal;
                }
                else
                {
                    var user = (await GetUserObjectAsync()) ?? throw new System.ComponentModel.WarningException($"The {admins} could not be determined. Skipping role assignment to AKS cluster resource scope.");
                    principalIds = [new(user.Id)];
                    type = Azure.ResourceManager.Authorization.Models.RoleManagementPrincipalType.User;
                }
            }
            else
            {
                admins = "designated groups";
                principalIds = adminGroupObjectIds;
                type = Azure.ResourceManager.Authorization.Models.RoleManagementPrincipalType.Group;
            }

            await AssignRoleToResourceAsync(
                principalIds,
                type,
                managedCluster,
                roleDefinitionId,
                message.Replace(@"{Admins}", admins));
        }

        private Task<StorageAccountResource> CreateStorageAccountAsync()
            => Execute(
                $"Creating Storage Account: {configuration.StorageAccountName}...",
                async () => (await resourceGroup.GetStorageAccounts().CreateOrUpdateAsync(WaitUntil.Completed,
                    configuration.StorageAccountName,
                    new(
                        new(Storage.StorageSkuName.StandardLrs),
                        Storage.StorageKind.StorageV2,
                        new(configuration.RegionName))
                    {
                        AllowSharedKeyAccess = false,
                        EnableHttpsTrafficOnly = true
                    },
                    cts.Token)).Value);

        private async Task<StorageAccountResource> GetExistingStorageAccountAsync(string storageAccountName)
            => await subscriptionIds.ToAsyncEnumerable().Select(s =>
            {
                try
                {
                    return s.GetStorageAccountsAsync(cts.Token);
                }
                catch (Exception)
                {
                    // Ignore exception if a user does not have the required role to list storage accounts in a subscription
                    return null;
                }
            })
            .Where(a => a is not null)
            .SelectMany(a => a)
            .SelectAwaitWithCancellation(async (a, ct) => await FetchResourceDataAsync(token => a.GetAsync(cancellationToken: token), ct))
            .SingleOrDefaultAsync(a =>
                    a.Id.Name.Equals(storageAccountName, StringComparison.OrdinalIgnoreCase) &&
                    a.Data.Location.Name.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase),
                cts.Token);

        private async Task<BatchAccountResource> GetExistingBatchAccountAsync(string batchAccountName)
            => await subscriptionIds.ToAsyncEnumerable().Select(s =>
            {
                try
                {
                    return s.GetBatchAccountsAsync(cts.Token);
                }
                catch (Exception e)
                {
                    ConsoleEx.WriteLine(e.Message);
                    return null;
                }
            })
            .Where(a => a is not null)
            .SelectMany(a => a)
            .SelectAwaitWithCancellation(async (a, ct) => await FetchResourceDataAsync(a.GetAsync, ct))
            .SingleOrDefaultAsync(a =>
                    a.Id.Name.Equals(batchAccountName, StringComparison.OrdinalIgnoreCase) &&
                    a.Data.Location.Value.Name.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase),
                cts.Token);

        private async Task CreateDefaultStorageContainersAsync(StorageAccountResource storageAccount)
        {
            List<string> defaultContainers = [TesInternalContainerName, InputsContainerName, "outputs", ConfigurationContainerName];

            var containerCollection = storageAccount.GetBlobService().GetBlobContainers();
            await Task.WhenAll(await defaultContainers.ToAsyncEnumerable()
                .Select(name => containerCollection.CreateOrUpdateAsync(WaitUntil.Completed, name, new(), cts.Token))
                .ToArrayAsync(cts.Token));
        }

        private Task WritePersonalizedFilesToStorageAccountAsync(StorageAccountData storageAccount)
            => Execute(
                $"Writing {AllowedVmSizesFileName} file to '{TesInternalContainerName}' storage container...",
                async () =>
                {
                    await UploadTextToStorageAccountAsync(GetBlobClient(storageAccount, TesInternalContainerName, $"{ConfigurationContainerName}/{AllowedVmSizesFileName}"), Utility.GetFileContent("scripts", AllowedVmSizesFileName), cts.Token);
                });

        private Task AssignVmAsContributorToBatchAccountAsync(UserAssignedIdentityResource managedIdentity, BatchAccountResource batchAccount)
            => AssignRoleToResourceAsync(managedIdentity, batchAccount, GetSubscriptionRoleDefinition(RoleDefinitions.General.Contributor),
                $"Assigning '{RoleDefinitions.GetDisplayName(RoleDefinitions.General.Contributor)}' role for user-managed identity to Batch Account resource scope...");

        private async Task<PostgreSqlFlexibleServerResource> CreatePostgreSqlServerAndDatabaseAsync(SubnetResource subnet, PrivateDnsZoneResource postgreSqlDnsZone)
        {
            subnet = await EnsureResourceDataAsync(subnet, r => r.HasData, r => ct => r.GetAsync(cancellationToken: ct), cts.Token);

            if (!subnet.Data.Delegations.Any())
            {
                subnet.Data.Delegations.Add(NewServiceDelegation("Microsoft.DBforPostgreSQL/flexibleServers"));
                await subnet.UpdateAsync(WaitUntil.Completed, subnet.Data, cts.Token);
            }

            PostgreSqlFlexibleServerData data = new(new(configuration.RegionName))
            {
                Version = new(configuration.PostgreSqlVersion),
                Sku = new(configuration.PostgreSqlSkuName, configuration.PostgreSqlTier),
                StorageSizeInGB = configuration.PostgreSqlStorageSize,
                AdministratorLogin = configuration.PostgreSqlAdministratorLogin,
                AdministratorLoginPassword = configuration.PostgreSqlAdministratorPassword,
                Network = new()
                {
                    /*PublicNetworkAccess = PostgreSqlFlexibleServerPublicNetworkAccessState.Disabled,*/
                    DelegatedSubnetResourceId = subnet.Id,
                    PrivateDnsZoneArmResourceId = postgreSqlDnsZone.Id
                },
                HighAvailability = new() { Mode = PostgreSqlFlexibleServerHighAvailabilityMode.Disabled },
            };

            var server = await Execute(
                $"Creating Azure Flexible Server for PostgreSQL: {configuration.PostgreSqlServerName}...",
                async () => (await internalServerErrorRetryPolicy.ExecuteAsync(token => resourceGroup.GetPostgreSqlFlexibleServers().CreateOrUpdateAsync(WaitUntil.Completed, configuration.PostgreSqlServerName, data, token), cts.Token)).Value);

            await Execute(
                $"Creating PostgreSQL tes database: {configuration.PostgreSqlTesDatabaseName}...",
                () => server.GetPostgreSqlFlexibleServerDatabases().CreateOrUpdateAsync(WaitUntil.Completed, configuration.PostgreSqlTesDatabaseName, new(), cts.Token));

            return server;
        }

        private string GetCreateTesUserString()
        {
            return $"CREATE USER {configuration.PostgreSqlTesUserLogin} WITH PASSWORD '{configuration.PostgreSqlTesUserPassword}'; GRANT ALL PRIVILEGES ON DATABASE {configuration.PostgreSqlTesDatabaseName} TO {configuration.PostgreSqlTesUserLogin};";
        }

        private Task ExecuteQueriesOnAzurePostgreSQLDbFromK8(IKubernetes kubernetesClient, string podName, string aksNamespace)
            => Execute(
                "Executing scripts on postgresql...",
                async () =>
                {
                    var tesScript = GetCreateTesUserString();
                    var serverPath = $"{configuration.PostgreSqlServerName}.{azureCloudConfig.Suffixes.PostgresqlServerEndpointSuffix}";
                    var adminUser = configuration.PostgreSqlAdministratorLogin;

                    List<string[]> commands =
                    [
                        ["apt", "-qq", "update"],
                        ["apt", "-qq", "install", "-y", "postgresql-client"],
                        ["bash", "-lic", $"echo '{configuration.PostgreSqlServerName}{configuration.PostgreSqlServerNameSuffix}:{configuration.PostgreSqlServerPort}:{configuration.PostgreSqlTesDatabaseName}:{adminUser}:{configuration.PostgreSqlAdministratorPassword}' >> ~/.pgpass"],
                        ["bash", "-lic", "chmod 0600 ~/.pgpass"],
                        // Set the PGPASSFILE environment variable to point to the .pgpass file
                        ["bash", "-lic", "export PGPASSFILE=~/.pgpass"],
                        ["/usr/bin/psql", "-h", serverPath, "-U", adminUser, "-d", configuration.PostgreSqlTesDatabaseName, "-c", tesScript]
                    ];

                    await kubernetesManager.ExecuteCommandsOnPodAsync(kubernetesClient, podName, commands, aksNamespace);
                });

        private Task AssignVmAsContributorToAppInsightsAsync(UserAssignedIdentityResource managedIdentity, ArmResource appInsights)
            => AssignRoleToResourceAsync(managedIdentity, appInsights, GetSubscriptionRoleDefinition(RoleDefinitions.General.Contributor),
                $"Assigning '{RoleDefinitions.GetDisplayName(RoleDefinitions.General.Contributor)}' role for user-managed identity to App Insights resource scope...");

        private ResourceIdentifier GetSubscriptionRoleDefinition(Guid roleDefinition)
            => AuthorizationRoleDefinitionResource.CreateResourceIdentifier(SubscriptionResource.CreateResourceIdentifier(configuration.SubscriptionId), new(roleDefinition.ToString("D")));

        private Task<bool> AssignRoleToResourceAsync(UserAssignedIdentityResource managedIdentity, ArmResource resource, ResourceIdentifier roleDefinitionId, string message)
            => AssignRoleToResourceAsync([managedIdentity.Data.PrincipalId.Value], Azure.ResourceManager.Authorization.Models.RoleManagementPrincipalType.ServicePrincipal, resource, roleDefinitionId, message);

        private async Task<bool> AssignRoleToResourceAsync(IEnumerable<Guid> principalIds, Azure.ResourceManager.Authorization.Models.RoleManagementPrincipalType principalType, ArmResource resource, ResourceIdentifier roleDefinitionId, string message, Func<Exception, Exception> transformException = default)
        {
            var changed = false;

            foreach (var principal in principalIds)
            {
                if (await resource.GetRoleAssignments().GetAllAsync(filter: "atScope()", cancellationToken: cts.Token)
                    .SelectAwaitWithCancellation(async (a, ct) => await EnsureResourceDataAsync(a, r => r.HasData, CallGetAsync, ct))
                    .Where(a => a?.HasData ?? false)
                    .Where(a => principal.Equals(a.Data.PrincipalId.Value))
                    .Where(a => roleDefinitionId.Equals(a.Data.RoleDefinitionId))
                    .AnyAsync(cts.Token))
                {
                    continue;
                }

                changed |= await Execute(message, async () =>
                {
                    try
                    {
                        await roleAssignmentHashConflictRetryPolicy.ExecuteAsync(token =>
                            (Task)resource.GetRoleAssignments().CreateOrUpdateAsync(WaitUntil.Completed, Guid.NewGuid().ToString(),
                                new(roleDefinitionId, principal)
                                {
                                    PrincipalType = principalType
                                },
                                token),
                            cts.Token);

                        return true;
                    }
                    catch (Exception ex)
                    {
                        Exception e;

                        if (transformException is not null)
                        {
                            e = transformException(ex);

                            if (e is null)
                            {
                                return false;
                            }
                        }
                        else
                        {
                            e = ex;
                        }

                        e.RethrowWithOriginalStackTraceIfDiffersFrom(ex);
                        throw;
                    }
                });
            }

            return changed;

            static Func<CancellationToken, Task<Response<RoleAssignmentResource>>> CallGetAsync(RoleAssignmentResource resource)
            {
                return new Func<CancellationToken, Task<Response<RoleAssignmentResource>>>(async cancellationToken =>
                {
                    try
                    {
                        return await resource.GetAsync(cancellationToken: cancellationToken);
                    }
                    catch (RequestFailedException ex) when ("AuthorizationFailed".Equals(ex.ErrorCode, StringComparison.OrdinalIgnoreCase))
                    {
                        return new NullResponse<RoleAssignmentResource>();
                    }
                });
            }
        }

        private class NullResponse<T> : Response<T>
        {
            public override bool HasValue => false;

            public override T Value => default;

            public override Response GetRawResponse()
            {
                throw new NotImplementedException();
            }
        }

        private Task<(VirtualNetworkResource virtualNetwork, SubnetResource vmSubnet, SubnetResource postgreSqlSubnet, SubnetResource batchSubnet)> CreateVnetAndSubnetsAsync()
          => Execute(
                $"Creating virtual network and subnets: {configuration.VnetName}...",
                async () =>
                {
                    List<int> tesPorts = [];

                    if (configuration.EnableIngress.GetValueOrDefault())
                    {
                        tesPorts = [80, 443];
                    }

                    var defaultNsg = (await EnsureResourceDataAsync(await CreateNetworkSecurityGroupAsync($"{configuration.VnetName}-default-nsg"), nsg => nsg.HasData, nsg => ct => nsg.GetAsync(cancellationToken: ct), cts.Token)).Data;
                    var aksNsg = (await EnsureResourceDataAsync(await CreateNetworkSecurityGroupAsync($"{configuration.VnetName}-aks-nsg", tesPorts), nsg => nsg.HasData, nsg => ct => nsg.GetAsync(cancellationToken: ct), cts.Token)).Data;

                    VirtualNetworkData vnetDefinition = new() { Location = new(configuration.RegionName) };
                    vnetDefinition.AddressPrefixes.Add(configuration.VnetAddressSpace);

                    vnetDefinition.Subnets.Add(new()
                    {
                        Name = configuration.VmSubnetName,
                        AddressPrefix = configuration.VmSubnetAddressSpace,
                        NetworkSecurityGroup = aksNsg,
                    });

                    SubnetData postgreSqlSubnet = new()
                    {
                        Name = configuration.PostgreSqlSubnetName,
                        AddressPrefix = configuration.PostgreSqlSubnetAddressSpace,
                        NetworkSecurityGroup = defaultNsg,
                    };
                    postgreSqlSubnet.Delegations.Add(NewServiceDelegation("Microsoft.DBforPostgreSQL/flexibleServers"));
                    vnetDefinition.Subnets.Add(postgreSqlSubnet);

                    SubnetData batchSubnet = new()
                    {
                        Name = configuration.BatchSubnetName,
                        AddressPrefix = configuration.BatchNodesSubnetAddressSpace,
                        NetworkSecurityGroup = defaultNsg,
                    };
                    AddServiceEndpointsToSubnet(batchSubnet);
                    vnetDefinition.Subnets.Add(batchSubnet);

                    var vnet = (await resourceGroup.GetVirtualNetworks().CreateOrUpdateAsync(WaitUntil.Completed, configuration.VnetName, vnetDefinition, cts.Token)).Value;
                    var subnets = await vnet.GetSubnets().ToListAsync(cts.Token);

                    return (vnet,
                        subnets.FirstOrDefault(s => s.Id.Name.Equals(configuration.VmSubnetName, StringComparison.OrdinalIgnoreCase)),
                        subnets.FirstOrDefault(s => s.Id.Name.Equals(configuration.PostgreSqlSubnetName, StringComparison.OrdinalIgnoreCase)),
                        subnets.FirstOrDefault(s => s.Id.Name.Equals(configuration.BatchSubnetName, StringComparison.OrdinalIgnoreCase)));
                });

        private static ServiceDelegation NewServiceDelegation(string serviceDelegation) =>
            new() { Name = serviceDelegation, ServiceName = serviceDelegation };

        private async Task<NetworkSecurityGroupResource> CreateNetworkSecurityGroupAsync(string networkSecurityGroupName, IEnumerable<int> openPorts = null)
        {
            NetworkSecurityGroupData data = new() { Location = new(configuration.RegionName) };

            if (openPorts is not null)
            {
                foreach (var (port, i) in openPorts.Select((p, i) => (p, i)))
                {
                    data.SecurityRules.Add(new()
                    {
                        Name = $"ALLOW-{port}",
                        Access = SecurityRuleAccess.Allow,
                        Direction = SecurityRuleDirection.Inbound,
                        SourceAddressPrefix = "*",
                        SourcePortRange = "*",
                        DestinationAddressPrefix = "*",
                        DestinationPortRange = port.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        Protocol = SecurityRuleProtocol.Asterisk,
                        Priority = 1000 + i,
                    });
                }
            }

            return (await resourceGroup.GetNetworkSecurityGroups().CreateOrUpdateAsync(WaitUntil.Completed, networkSecurityGroupName, data, cts.Token)).Value;
        }

        private Task<PrivateDnsZoneResource> CreatePrivateDnsZoneAsync(VirtualNetworkResource virtualNetwork, string name, string title)
            => Execute(
                $"Creating private DNS Zone for {title}...",
                async () =>
                {
                    var dnsZone = (await resourceGroup.GetPrivateDnsZones()
                        .CreateOrUpdateAsync(WaitUntil.Completed, name, new(new("global")), cancellationToken: cts.Token)).Value;
                    VirtualNetworkLinkData data = new(new("global"))
                    {
                        VirtualNetworkId = virtualNetwork.Id,
                        RegistrationEnabled = false
                    };
                    _ = await dnsZone.GetVirtualNetworkLinks().CreateOrUpdateAsync(WaitUntil.Completed, $"{virtualNetwork.Id.Name}-link", data, cancellationToken: cts.Token);
                    return dnsZone;
                });

        private async Task SetStorageKeySecret(Uri vaultUrl, string secretName, string secretValue)
        {
            var client = new SecretClient(vaultUrl, tokenCredential, new() { RetryPolicy = GetRetryPolicy(new()) });
            await client.SetSecretAsync(secretName, secretValue, cts.Token);
        }

        private async Task<KeyVaultResource> GetKeyVaultAsync(string vaultName)
        {
            return resourceGroup is null
                ? (await armSubscription.GetKeyVaultsAsync(cancellationToken: cts.Token).FirstOrDefaultAsync(r => r.Id.ResourceGroupName.Equals(configuration.ResourceGroupName, StringComparison.OrdinalIgnoreCase), cts.Token))
                : (await resourceGroup.GetKeyVaultAsync(vaultName, cts.Token)).Value;
        }

        private Task<KeyVaultResource> CreateKeyVaultAsync(string vaultName, UserAssignedIdentityResource managedIdentity, VirtualNetworkResource virtualNetwork, SubnetResource subnet)
            => Execute(
                $"Creating Key Vault: {vaultName}...",
                async () =>
                {
                    if (!managedIdentity.HasData)
                    {
                        throw new ArgumentException("Resource data has not been fetched.", nameof(managedIdentity));
                    }

                    var tenantId = managedIdentity.Data.TenantId;
                    IdentityAccessPermissions permissions = new();
                    permissions.Secrets.Add(IdentityAccessSecretPermission.Get);
                    permissions.Secrets.Add(IdentityAccessSecretPermission.List);
                    permissions.Secrets.Add(IdentityAccessSecretPermission.Set);
                    permissions.Secrets.Add(IdentityAccessSecretPermission.Delete);
                    permissions.Secrets.Add(IdentityAccessSecretPermission.Backup);
                    permissions.Secrets.Add(IdentityAccessSecretPermission.Restore);
                    permissions.Secrets.Add(IdentityAccessSecretPermission.Recover);
                    permissions.Secrets.Add(IdentityAccessSecretPermission.Purge);

                    KeyVaultProperties properties = new(tenantId.Value, new(KeyVaultSkuFamily.A, KeyVaultSkuName.Standard))
                    {
                        NetworkRuleSet = new()
                        {
                            DefaultAction = configuration.PrivateNetworking.GetValueOrDefault() ? KeyVaultNetworkRuleAction.Deny : KeyVaultNetworkRuleAction.Allow
                        },
                    };

                    properties.AccessPolicies.AddRange(
                    [
                        new(tenantId.Value, string.IsNullOrWhiteSpace(configuration.ServicePrincipalId) ? (await GetUserObjectAsync()).Id : configuration.ServicePrincipalId, permissions),
                        new(tenantId.Value, managedIdentity.Data.PrincipalId.Value.ToString("D"), permissions),
                    ]);

                    var vault = (await resourceGroup.GetKeyVaults().CreateOrUpdateAsync(WaitUntil.Completed, vaultName, new(new(configuration.RegionName), properties), cts.Token)).Value;

                    if (configuration.PrivateNetworking.GetValueOrDefault())
                    {
                        var connection = new NetworkPrivateLinkServiceConnection
                        {
                            Name = "pe-coa-keyvault",
                            PrivateLinkServiceId = vault.Id
                        };
                        connection.GroupIds.Add("vault");

                        var endpointData = new PrivateEndpointData
                        {
                            CustomNetworkInterfaceName = "pe-coa-keyvault",
                            ExtendedLocation = new() { Name = configuration.RegionName },
                            Subnet = new() { Id = subnet.Id, Name = subnet.Id.Name }
                        };
                        endpointData.PrivateLinkServiceConnections.Add(connection);

                        var privateEndpoint = (await resourceGroup
                                .GetPrivateEndpoints()
                                .CreateOrUpdateAsync(WaitUntil.Completed, "pe-keyvault", endpointData, cts.Token))
                            .Value.Data;

                        var networkInterface = privateEndpoint.NetworkInterfaces[0];

                        var dnsZone = await CreatePrivateDnsZoneAsync(virtualNetwork, "privatelink.vaultcore.azure.net", "KeyVault");
                        PrivateDnsARecordData aRecordData = new();
                        aRecordData.PrivateDnsARecords.Add(new()
                        {
                            IPv4Address = IPAddress.Parse(networkInterface.IPConfigurations.First(c => NetworkIPVersion.IPv4.Equals(c.PrivateIPAddressVersion)).PrivateIPAddress)
                        });
                        _ = await dnsZone
                            .GetPrivateDnsARecords()
                            .CreateOrUpdateAsync(WaitUntil.Completed, vault.Id.Name, aRecordData, cancellationToken: cts.Token);
                    }

                    return vault;
                });

        private Microsoft.Graph.Models.User _me = null;

        async Task<Microsoft.Graph.Models.User> GetUserObjectAsync()
        {
            // TODO: async blocking
            if (_me is null)
            {
                Dictionary<Uri, string> nationalClouds = new(
                [
                    new(ArmEnvironment.AzurePublicCloud.Endpoint, GraphClientFactory.Global_Cloud),
                    new(ArmEnvironment.AzureChina.Endpoint, GraphClientFactory.China_Cloud),
                    // Note that there are two different values for USGovernment.
                    new(ArmEnvironment.AzureGovernment.Endpoint, GraphClientFactory.USGOV_Cloud), // TODO: when should we return GraphClientFactory.USGOV_DOD_Cloud?
                ]);

                using var httpClient = GraphClientFactory.Create(nationalCloud: nationalClouds.TryGetValue(cloudEnvironment.ArmEnvironment.Endpoint, out var value) ? value : GraphClientFactory.Global_Cloud);
                var scope = new UriBuilder(httpClient.BaseAddress) { Path = ".default" };
                using var client = new GraphServiceClient(httpClient, tokenCredential, scopes: [scope.Uri.AbsoluteUri], baseUrl: httpClient.BaseAddress.AbsoluteUri);

                try
                {
                    _me = await client.Me.GetAsync(cancellationToken: cts.Token);
                }
                catch (AuthenticationFailedException afe)
                {
                    Console.WriteLine($"AuthenticationFailedException: {afe.Message}");
                }
                catch (Microsoft.Graph.Models.ODataErrors.ODataError ex) when ("BadRequest".Equals(ex.Error?.Code, StringComparison.OrdinalIgnoreCase) && ex.Message.Contains("/me", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine($"ODataError: {ex.Message}");
                    // "/me request is only valid with delegated authentication flow."
                }
            }

            return _me;
        }

        private Task<OperationalInsightsWorkspaceResource> CreateLogAnalyticsWorkspaceResourceAsync(string workspaceName)
            => Execute(
                $"Creating Log Analytics Workspace: {workspaceName}...",
               async () =>
               {
                   OperationalInsightsWorkspaceData data = new(new(configuration.RegionName));
                   return (await resourceGroup.GetOperationalInsightsWorkspaces()
                       .CreateOrUpdateAsync(WaitUntil.Completed, workspaceName, data, cts.Token)).Value;
               });

        private Task<ApplicationInsightsComponentResource> CreateAppInsightsResourceAsync(ResourceIdentifier logAnalyticsArmId)
            => Execute(
                $"Creating Application Insights: {configuration.ApplicationInsightsAccountName}...",
                async () =>
                {
                    ApplicationInsightsComponentData data = new(new(configuration.RegionName), "other")
                    {
                        FlowType = ComponentFlowType.Bluefield,
                        RequestSource = ComponentRequestSource.Rest,
                        ApplicationType = ApplicationInsightsApplicationType.Other,
                        WorkspaceResourceId = logAnalyticsArmId,
                    };
                    return (await resourceGroup.GetApplicationInsightsComponents()
                        .CreateOrUpdateAsync(WaitUntil.Completed, configuration.ApplicationInsightsAccountName, data, cts.Token)).Value;
                });

        private Task<BatchAccountResource> CreateBatchAccountAsync(ResourceIdentifier storageAccountId)
            => Execute(
                $"Creating Batch Account: {configuration.BatchAccountName}...",
                async () =>
                {
                    Batch.BatchAccountCreateOrUpdateContent data = new(new(configuration.RegionName))
                    {
                        AutoStorage = configuration.PrivateNetworking.GetValueOrDefault() ? new(storageAccountId) : null,
                    };
                    return (await resourceGroup.GetBatchAccounts()
                        .CreateOrUpdateAsync(WaitUntil.Completed, configuration.BatchAccountName, data, cts.Token)).Value;
                });

        private Task<ResourceGroupResource> CreateResourceGroupAsync()
        {
            var tags = !string.IsNullOrWhiteSpace(configuration.Tags) ? Utility.DelimitedTextToDictionary(configuration.Tags, "=", ",") : null;

            ResourceGroupData data = new(new(configuration.RegionName));
            (tags ?? []).ForEach(data.Tags.Add);

            return Execute(
                $"Creating Resource Group: {configuration.ResourceGroupName}...",
                async () => (await armSubscription.GetResourceGroups().CreateOrUpdateAsync(WaitUntil.Completed, configuration.ResourceGroupName, data, cts.Token)).Value);
        }

        private Task<UserAssignedIdentityResource> CreateUserManagedIdentityAsync()
        {
            // Resource group name supports periods and parenthesis but identity doesn't. Replacing them with hyphens.
            var managedIdentityName = $"{resourceGroup.Id.Name.Replace(".", "-").Replace("(", "-").Replace(")", "-")}-identity";

            return Execute(
                $"Obtaining user-managed identity: {managedIdentityName}...",
                async () =>
                {
                    try
                    {
                        return (await resourceGroup.GetUserAssignedIdentityAsync(managedIdentityName, cts.Token)).Value;
                    }
                    catch (RequestFailedException ex) when (ex.Status == (int)HttpStatusCode.NotFound)
                    {
                        return (await resourceGroup.GetUserAssignedIdentities().CreateOrUpdateAsync(
                                WaitUntil.Completed,
                                managedIdentityName,
                                new(new(configuration.RegionName)),
                                cts.Token))
                            .Value;
                    }
                });
        }

        private async Task DeleteResourceGroupAsync(CancellationToken cancellationToken)
        {
            var startTime = DateTime.UtcNow;
            var line = ConsoleEx.WriteLine("Deleting resource group...");
            await resourceGroup.DeleteAsync(WaitUntil.Completed, cancellationToken: cancellationToken);
            WriteExecutionTime(line, startTime);
        }

        private static void ValidateMainIdentifierPrefix(string prefix)
        {
            const int maxLength = 12;

            if (prefix.Any(c => !char.IsLetter(c)))
            {
                throw new ValidationException($"MainIdentifierPrefix must only contain letters.");
            }

            if (prefix.Length > maxLength)
            {
                throw new ValidationException($"MainIdentifierPrefix too long - must be {maxLength} characters or less.");
            }
        }

        private async Task ValidateRegionNameAsync(string regionName)
        {
            // GetAvailableLocations*() does not work https://github.com/Azure/azure-sdk-for-net/issues/28914
            var validRegionNames = await armSubscription.GetLocationsAsync(cancellationToken: cts.Token)
                .Where(x => x.Metadata.RegionType == RegionType.Physical)
                .Select(loc => loc.Name).Distinct().ToListAsync(cts.Token);

            if (!validRegionNames.Contains(regionName, StringComparer.OrdinalIgnoreCase))
            {
                throw new ValidationException($"Invalid region name '{regionName}'. Valid names are: {string.Join(", ", validRegionNames)}");
            }
        }

        private async Task ValidateSubscriptionAndResourceGroupAsync(Configuration configuration)
        {
            var ownerRoleId = RoleDefinitions.General.Owner.ToString("D");
            var contributorRoleId = RoleDefinitions.General.Contributor.ToString("D");

            bool rgExists;

            try
            {
                rgExists = !string.IsNullOrEmpty(configuration.ResourceGroupName) && (await armSubscription.GetResourceGroups().ExistsAsync(configuration.ResourceGroupName, cts.Token)).Value;
            }
            catch (Exception)
            {
                throw new ValidationException($"Invalid or inaccessible subscription id '{configuration.SubscriptionId}'. Make sure that subscription exists and that you are either an Owner or have Contributor and User Access Administrator roles on the subscription.", displayExample: false);
            }

            if (!string.IsNullOrEmpty(configuration.ResourceGroupName) && !rgExists)
            {
                throw new ValidationException($"If ResourceGroupName is provided, the resource group must already exist.", displayExample: false);
            }

            var token = (await tokenCredential.GetTokenAsync(new([cloudEnvironment.ArmEnvironment.DefaultScope]), cts.Token));
            var currentPrincipalObjectId = new JwtSecurityTokenHandler().ReadJwtToken(token.Token).Claims.FirstOrDefault(c => c.Type == "oid").Value;

            var currentPrincipalSubscriptionRoleIds = armSubscription.GetRoleAssignments().GetAllAsync($"atScope() and assignedTo('{currentPrincipalObjectId}')", cancellationToken: cts.Token)
                .SelectAwaitWithCancellation(async (b, c) => await FetchResourceDataAsync(t => b.GetAsync(cancellationToken: t), c)).Select(b => b.Data.RoleDefinitionId.Name);

            if (!await currentPrincipalSubscriptionRoleIds.AnyAsync(role => ownerRoleId.Equals(role, StringComparison.OrdinalIgnoreCase) || contributorRoleId.Equals(role, StringComparison.OrdinalIgnoreCase), cts.Token))
            {
                if (!rgExists)
                {
                    throw new ValidationException($"Insufficient access to deploy. You must be: 1) Owner of the subscription, or 2) Contributor and User Access Administrator of the subscription, or 3) Owner of the resource group", displayExample: false);
                }

                var currentPrincipalRgRoleIds = resourceGroup.GetRoleAssignments().GetAllAsync($"atScope() and assignedTo('{currentPrincipalObjectId}')", cancellationToken: cts.Token)
                    .SelectAwaitWithCancellation(async (b, c) => await FetchResourceDataAsync(t => b.GetAsync(cancellationToken: t), c)).Select(b => b.Data.RoleDefinitionId.Name);

                if (!await currentPrincipalRgRoleIds.AnyAsync(role => ownerRoleId.Equals(role, StringComparison.OrdinalIgnoreCase), cts.Token))
                {
                    throw new ValidationException($"Insufficient access to deploy. You must be: 1) Owner of the subscription, or 2) Contributor and User Access Administrator of the subscription, or 3) Owner of the resource group", displayExample: false);
                }
            }
        }

        private async Task<StorageAccountResource> ValidateAndGetExistingStorageAccountAsync()
        {
            if (configuration.StorageAccountName is null)
            {
                return null;
            }

            return (await GetExistingStorageAccountAsync(configuration.StorageAccountName))
                ?? throw new ValidationException($"If StorageAccountName is provided, the storage account must already exist in region {configuration.RegionName}, and be accessible to the current user.", displayExample: false);
        }

        private async Task<BatchAccountResource> ValidateAndGetExistingBatchAccountAsync()
        {
            if (configuration.BatchAccountName is null)
            {
                return null;
            }

            return (await GetExistingBatchAccountAsync(configuration.BatchAccountName))
                ?? throw new ValidationException($"If BatchAccountName is provided, the batch account must already exist in region {configuration.RegionName}, and be accessible to the current user.", displayExample: false);
        }

        private async Task<(VirtualNetworkResource virtualNetwork, SubnetResource vmSubnet, SubnetResource postgreSqlSubnet, SubnetResource batchSubnet)?> ValidateAndGetExistingVirtualNetworkAsync()
        {
            static bool AllOrNoneSet(params string[] values) => values.All(v => !string.IsNullOrEmpty(v)) || values.All(v => string.IsNullOrEmpty(v));
            static bool NoneSet(params string[] values) => values.All(v => string.IsNullOrEmpty(v));

            if (NoneSet(configuration.VnetResourceGroupName, configuration.VnetName, configuration.VmSubnetName))
            {
                if (configuration.PrivateNetworking.GetValueOrDefault())
                {
                    throw new ValidationException($"{nameof(configuration.VnetResourceGroupName)}, {nameof(configuration.VnetName)} and {nameof(configuration.VmSubnetName)} are required when using private networking.");
                }

                return null;
            }

            if (!AllOrNoneSet(configuration.VnetResourceGroupName, configuration.VnetName, configuration.VmSubnetName, configuration.PostgreSqlSubnetName))
            {
                throw new ValidationException($"{nameof(configuration.VnetResourceGroupName)}, {nameof(configuration.VnetName)}, {nameof(configuration.VmSubnetName)} and {nameof(configuration.PostgreSqlSubnetName)} are required when using an existing virtual network.");
            }

            if (!AllOrNoneSet(configuration.VnetResourceGroupName, configuration.VnetName, configuration.VmSubnetName))
            {
                throw new ValidationException($"{nameof(configuration.VnetResourceGroupName)}, {nameof(configuration.VnetName)} and {nameof(configuration.VmSubnetName)} are required when using an existing virtual network.");
            }

            if (!await armSubscription.GetResourceGroups().GetAllAsync(cancellationToken: cts.Token).AnyAsync(rg => rg.Id.Name.Equals(configuration.VnetResourceGroupName, StringComparison.OrdinalIgnoreCase), cts.Token))
            {
                throw new ValidationException($"Resource group '{configuration.VnetResourceGroupName}' does not exist.");
            }

            var vnet = (await (await armSubscription.GetResourceGroupAsync(configuration.VnetResourceGroupName, cts.Token)).Value.GetVirtualNetworks().GetIfExistsAsync(configuration.VnetName, cancellationToken: cts.Token)).Value ??
                throw new ValidationException($"Virtual network '{configuration.VnetName}' does not exist in resource group '{configuration.VnetResourceGroupName}'.");

            if (!(await FetchResourceDataAsync(ct => vnet.GetAsync(cancellationToken: ct), cts.Token, net => vnet = net)).Data.Location.Value.Name.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase))
            {
                throw new ValidationException($"Virtual network '{configuration.VnetName}' must be in the same region that you are deploying to ({configuration.RegionName}).");
            }

            var vmSubnet = await vnet.GetSubnets().GetAllAsync(cts.Token).FirstOrDefaultAsync(s => s.Id.Name.Equals(configuration.VmSubnetName, StringComparison.OrdinalIgnoreCase), cts.Token) ??
                throw new ValidationException($"Virtual network '{configuration.VnetName}' does not contain subnet '{configuration.VmSubnetName}'");

            var postgreSqlSubnet = await vnet.GetSubnets().GetAllAsync(cts.Token).FirstOrDefaultAsync(s => s.Id.Name.Equals(configuration.PostgreSqlSubnetName, StringComparison.OrdinalIgnoreCase), cts.Token) ??
                throw new ValidationException($"Virtual network '{configuration.VnetName}' does not contain subnet '{configuration.PostgreSqlSubnetName}'");

            postgreSqlSubnet = await FetchResourceDataAsync(ct => postgreSqlSubnet.GetAsync(cancellationToken: ct), cts.Token);
            var delegatedServices = postgreSqlSubnet.Data.Delegations.Select(d => d.ServiceName).ToList();
            var hasOtherDelegations = delegatedServices.Any(s => s != "Microsoft.DBforPostgreSQL/flexibleServers");
            var hasNoDelegations = 0 == delegatedServices.Count;

            if (hasOtherDelegations)
            {
                throw new ValidationException($"Subnet '{configuration.PostgreSqlSubnetName}' can have 'Microsoft.DBforPostgreSQL/flexibleServers' delegation only.");
            }

            Azure.ResourceManager.ResourceGraph.Models.ResourceQueryContent resourcesInPostgreSqlSubnetQuery = new($"where type =~ 'Microsoft.Network/networkInterfaces' | where properties.ipConfigurations[0].properties.subnet.id == '{postgreSqlSubnet.Id}'");
            resourcesInPostgreSqlSubnetQuery.Subscriptions.Add(configuration.SubscriptionId);
            var resourcesExist = (await (await armClient.GetTenants().GetAllAsync(cts.Token).FirstAsync(cts.Token)).GetResourcesAsync(resourcesInPostgreSqlSubnetQuery, cts.Token)).Value.TotalRecords > 0;

            if (hasNoDelegations && resourcesExist)
            {
                throw new ValidationException($"Subnet '{configuration.PostgreSqlSubnetName}' must be either empty or have 'Microsoft.DBforPostgreSQL/flexibleServers' delegation.");
            }

            var batchSubnet = await vnet.GetSubnets().GetAllAsync(cts.Token).FirstOrDefaultAsync(s => s.Id.Name.Equals(configuration.BatchSubnetName, StringComparison.OrdinalIgnoreCase), cts.Token) ??
                throw new ValidationException($"Virtual network '{configuration.VnetName}' does not contain subnet '{configuration.BatchSubnetName}'");

            return (vnet, vmSubnet, postgreSqlSubnet, batchSubnet);
        }

        private async Task ValidateBatchAccountQuotaAsync()
        {
            var accountQuota = (await armSubscription.GetBatchQuotasAsync(new(configuration.RegionName), cts.Token)).Value.AccountQuota;
            var existingBatchAccountCount = await armSubscription.GetBatchAccountsAsync(cts.Token)
                .SelectAwaitWithCancellation(async (a, t) => await FetchResourceDataAsync(a.GetAsync, cts.Token))
                .CountAsync(b => b.Data.Location.Value.Name.Equals(configuration.RegionName), cts.Token);

            if (existingBatchAccountCount >= accountQuota)
            {
                throw new ValidationException($"The regional Batch account quota ({accountQuota} account(s) per region) for the specified subscription has been reached. Submit a support request to increase the quota or choose another region.", displayExample: false);
            }
        }

        private Task<string> UpdateVnetWithBatchSubnet()
            => Execute(
                $"Creating batch subnet...",
                async () =>
                {
                    var vnetCollection = resourceGroup.GetVirtualNetworks();
                    var vnet = vnetCollection.FirstOrDefault();

                    if (vnetCollection.Count() != 1)
                    {
                        ConsoleEx.WriteLine("There are multiple vnets found in the resource group so the deployer cannot automatically create the subnet.", ConsoleColor.Red);
                        ConsoleEx.WriteLine("In order to avoid unnecessary load balancer charges we suggest manually configuring your deployment to use a subnet for batch pools with service endpoints.", ConsoleColor.Red);
                        ConsoleEx.WriteLine("See: https://github.com/microsoft/CromwellOnAzure/wiki/Using-a-batch-pool-subnet-with-service-endpoints-to-avoid-load-balancer-charges.", ConsoleColor.Red);

                        return null;
                    }

                    var vnetData = vnet.Data;
                    var ipRange = vnetData.AddressPrefixes.Single();
                    var defaultSubnetNames = new List<string> { configuration.DefaultVmSubnetName, configuration.DefaultPostgreSqlSubnetName, configuration.DefaultBatchSubnetName };

                    if (!string.Equals(ipRange, configuration.VnetAddressSpace, StringComparison.OrdinalIgnoreCase) ||
                        vnetData.Subnets.Select(x => x.Name).Except(defaultSubnetNames).Any())
                    {
                        ConsoleEx.WriteLine("We detected a customized networking setup so the deployer will not automatically create the subnet.", ConsoleColor.Red);
                        ConsoleEx.WriteLine("In order to avoid unnecessary load balancer charges we suggest manually configuring your deployment to use a subnet for batch pools with service endpoints.", ConsoleColor.Red);
                        ConsoleEx.WriteLine("See: https://github.com/microsoft/CromwellOnAzure/wiki/Using-a-batch-pool-subnet-with-service-endpoints-to-avoid-load-balancer-charges.", ConsoleColor.Red);

                        return null;
                    }

                    var batchSubnet = new SubnetData
                    {
                        Name = configuration.DefaultBatchSubnetName,
                        AddressPrefix = configuration.BatchNodesSubnetAddressSpace,
                    };

                    AddServiceEndpointsToSubnet(batchSubnet);

                    vnetData.Subnets.Add(batchSubnet);
                    var updatedVnet = (await vnetCollection.CreateOrUpdateAsync(Azure.WaitUntil.Completed, vnetData.Name, vnetData, cts.Token)).Value;

                    return (await updatedVnet.GetSubnetAsync(configuration.DefaultBatchSubnetName, cancellationToken: cts.Token)).Value.Id.ToString();
                });

        private static void AddServiceEndpointsToSubnet(SubnetData subnet)
        {
            subnet.ServiceEndpoints.Add(new()
            {
                Service = "Microsoft.Storage.Global",
            });

            subnet.ServiceEndpoints.Add(new()
            {
                Service = "Microsoft.Sql",
            });

            subnet.ServiceEndpoints.Add(new()
            {
                Service = "Microsoft.ContainerRegistry",
            });

            subnet.ServiceEndpoints.Add(new()
            {
                Service = "Microsoft.KeyVault",
            });
        }

        private async Task ValidateVmAsync()
        {
            var computeSkus = await generalRetryPolicy.ExecuteAsync(async ct =>
                    await armSubscription.GetComputeResourceSkusAsync(
                        filter: $"location eq '{configuration.RegionName}'",
                        cancellationToken: ct)
                        .Where(s => "virtualMachines".Equals(s.ResourceType, StringComparison.OrdinalIgnoreCase))
                        .Where(s => !s.Restrictions.Any())
                        .Select(s => s.Name)
                        .ToListAsync(ct),
                    cts.Token);

            if (0 == computeSkus.Count)
            {
                throw new ValidationException($"Your subscription doesn't support virtual machine creation in {configuration.RegionName}.  Please create an Azure Support case: https://docs.microsoft.com/en-us/azure/azure-portal/supportability/how-to-create-azure-support-request", displayExample: false);
            }
            else if (!computeSkus.Any(s => s.Equals(configuration.VmSize, StringComparison.OrdinalIgnoreCase)))
            {
                throw new ValidationException($"The VmSize {configuration.VmSize} is not available or does not exist in {configuration.RegionName}.  You can use 'az vm list-skus --location {configuration.RegionName} --output table' to find an available VM.", displayExample: false);
            }
        }

        private async Task ValidateTokenProviderAsync()
        {
            try
            {
                _ = await Execute("Retrieving Azure management token...",
                    async () => await new AzureCliCredential(new()
                    {
                        AuthorityHost = cloudEnvironment.AzureAuthorityHost
                    }).GetTokenAsync(new([cloudEnvironment.ArmEnvironment.DefaultScope]), cancellationToken: cts.Token));
            }
            catch (AuthenticationFailedException ex)
            {
                ConsoleEx.WriteLine("No access token found.  Please install the Azure CLI and login with 'az login'", ConsoleColor.Red);
                ConsoleEx.WriteLine("Link: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli");
                ConsoleEx.WriteLine($"Error details: {ex.Message}");
                Environment.Exit(1);
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1861:Avoid constant arrays as arguments", Justification = "Called only once")]
        private void ValidateInitialCommandLineArgs()
        {
            void ThrowIfProvidedForUpdate(object attributeValue, string attributeName)
            {
                if (configuration.Update && attributeValue is not null)
                {
                    throw new ValidationException($"{attributeName} must not be provided when updating", false);
                }
            }

            void ThrowIfNotProvidedForUpdate(string attributeValue, string attributeName)
            {
                if (configuration.Update && string.IsNullOrWhiteSpace(attributeValue))
                {
                    throw new ValidationException($"{attributeName} is required for update.", false);
                }
            }

            //void ThrowIfEitherNotProvidedForUpdate(string attributeValue1, string attributeName1, string attributeValue2, string attributeName2)
            //{
            //    if (configuration.Update && string.IsNullOrWhiteSpace(attributeValue1) && string.IsNullOrWhiteSpace(attributeValue2))
            //    {
            //        throw new ValidationException($"Either {attributeName1} or {attributeName2} is required for update.", false);
            //    }
            //}

            void ThrowIfNotProvided(string attributeValue, string attributeName)
            {
                if (string.IsNullOrWhiteSpace(attributeValue))
                {
                    throw new ValidationException($"{attributeName} is required.", false);
                }
            }

            void ThrowIfNotProvidedForInstall(string attributeValue, string attributeName)
            {
                if (!configuration.Update && string.IsNullOrWhiteSpace(attributeValue))
                {
                    throw new ValidationException($"{attributeName} is required.", false);
                }
            }

            void ThrowIfTagsFormatIsUnacceptable(string attributeValue, string attributeName)
            {
                if (string.IsNullOrWhiteSpace(attributeValue))
                {
                    return;
                }

                try
                {
                    Utility.DelimitedTextToDictionary(attributeValue, "=", ",");
                }
                catch
                {
                    throw new ValidationException($"{attributeName} is specified in incorrect format. Try as TagName=TagValue,TagName=TagValue in double quotes", false);
                }
            }

            void ValidateDependantFeature(bool feature1Enabled, string feature1Name, bool feature2Enabled, string feature2Name)
            {
                if (feature1Enabled && !feature2Enabled)
                {
                    throw new ValidationException($"{feature2Name} must be enabled to use flag {feature1Name}");
                }
            }

            void ThrowIfBothProvided(string feature1Value, string feature1Name, string feature2Value, string feature2Name)
            {
                if (!string.IsNullOrWhiteSpace(feature1Value) && !string.IsNullOrWhiteSpace(feature2Value))
                {
                    throw new ValidationException($"{feature2Name} is incompatible with {feature1Name}");
                }
            }

            void ValidateHelmInstall(string helmPath, string featureName)
            {
                if (!File.Exists(helmPath))
                {
                    throw new ValidationException($"Helm must be installed and set with the {featureName} flag. You can find instructions for install Helm here: https://helm.sh/docs/intro/install/");
                }
            }

            void ValidateKubectlInstall(string kubectlPath, string featureName)
            {
                if (!File.Exists(kubectlPath))
                {
                    throw new ValidationException($"Kubectl must be installed and set with the {featureName} flag. You can find instructions for install Kubectl here: https://kubernetes.io/docs/tasks/tools/#kubectl");
                }
            }

            ThrowIfNotProvided(configuration.SubscriptionId, nameof(configuration.SubscriptionId));

            ThrowIfBothProvided(configuration.GitHubCommit, nameof(configuration.GitHubCommit), configuration.SolutionDir, nameof(configuration.SolutionDir));

            ThrowIfNotProvidedForInstall(configuration.RegionName, nameof(configuration.RegionName));

            ThrowIfNotProvidedForUpdate(configuration.ResourceGroupName, nameof(configuration.ResourceGroupName));

            ThrowIfProvidedForUpdate(configuration.BatchPrefix, nameof(configuration.BatchPrefix));
            ThrowIfProvidedForUpdate(configuration.RegionName, nameof(configuration.RegionName));
            ThrowIfProvidedForUpdate(configuration.BatchAccountName, nameof(configuration.BatchAccountName));
            ThrowIfProvidedForUpdate(configuration.CrossSubscriptionAKSDeployment, nameof(configuration.CrossSubscriptionAKSDeployment));
            ThrowIfProvidedForUpdate(configuration.ApplicationInsightsAccountName, nameof(configuration.ApplicationInsightsAccountName));
            ThrowIfProvidedForUpdate(configuration.PrivateNetworking, nameof(configuration.PrivateNetworking));
            ThrowIfProvidedForUpdate(configuration.EnableIngress, nameof(configuration.EnableIngress));
            ThrowIfProvidedForUpdate(configuration.VnetName, nameof(configuration.VnetName));
            ThrowIfProvidedForUpdate(configuration.VnetResourceGroupName, nameof(configuration.VnetResourceGroupName));
            ThrowIfProvidedForUpdate(configuration.SubnetName, nameof(configuration.SubnetName));
            ThrowIfProvidedForUpdate(configuration.Tags, nameof(configuration.Tags));
            ThrowIfProvidedForUpdate(configuration.AadGroupIds, nameof(configuration.AadGroupIds));

            ThrowIfTagsFormatIsUnacceptable(configuration.Tags, nameof(configuration.Tags));

            if (!configuration.ManualHelmDeployment)
            {
                ValidateHelmInstall(configuration.HelmBinaryPath, nameof(configuration.HelmBinaryPath));
            }

            if (!configuration.SkipTestWorkflow)
            {
                ValidateKubectlInstall(configuration.KubectlBinaryPath, nameof(configuration.KubectlBinaryPath));
            }

            ValidateDependantFeature(configuration.EnableIngress.GetValueOrDefault(), nameof(configuration.EnableIngress), !string.IsNullOrEmpty(configuration.LetsEncryptEmail), nameof(configuration.LetsEncryptEmail));

            if (!configuration.Update)
            {
                if (configuration.BatchPrefix?.Length > 11 || (configuration.BatchPrefix?.Any(c => !char.IsAsciiLetterOrDigit(c)) ?? false))
                {
                    throw new ValidationException("BatchPrefix must not be longer than 11 chars and may contain only ASCII letters or digits", false);
                }
            }

            if (!string.IsNullOrWhiteSpace(configuration.BatchNodesSubnetId) && !string.IsNullOrWhiteSpace(configuration.BatchSubnetName))
            {
                throw new ValidationException("Invalid configuration options BatchNodesSubnetId and BatchSubnetName are mutually exclusive.");
            }

            if (!new[] { "AzureCloud", "AzureUSGovernment", "AzureChinaCloud" }.Contains(configuration.AzureCloudName, StringComparer.OrdinalIgnoreCase))
            {
                throw new ValidationException("AzureCloudName must be either 'AzureCloud','AzureUSGovernment', or 'AzureChinaCloud'");
            }

            if (!string.IsNullOrWhiteSpace(configuration.AadGroupIds))
            {
                try
                {
                    if (!configuration.AadGroupIds.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(Guid.Parse).Any())
                    {
                        throw new FormatException();
                    }
                }
                catch (FormatException)
                {
                    throw new ValidationException("Invalid configuration option AadGroupIds is not formatted correctly.");
                }
            }
        }

        private static void DisplayValidationExceptionAndExit(ValidationException validationException)
        {
            ConsoleEx.WriteLine(validationException.Reason, ConsoleColor.Red);

            if (validationException.DisplayExample)
            {
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine($"Example: ", ConsoleColor.Green).Write($"deploy-tes-on-azure --subscriptionid {Guid.NewGuid()} --regionname westus2 --mainidentifierprefix coa", ConsoleColor.White);
            }

            Environment.Exit(1);
        }

        private async Task DeleteResourceGroupIfUserConsentsAsync()
        {
            if (!isResourceGroupCreated)
            {
                return;
            }

            var userResponse = string.Empty;

            if (!configuration.Silent)
            {
                ConsoleEx.WriteLine();
                ConsoleEx.Write("Delete the resource group?  Type 'yes' and press enter, or, press any key to exit: ");
                userResponse = ConsoleEx.ReadLine();
            }

            if (userResponse.Equals("yes", StringComparison.OrdinalIgnoreCase) || (configuration.Silent && configuration.DeleteResourceGroupOnFailure))
            {
                using var token = new CancellationTokenSource();
                Console.CancelKeyPress += (o, a) => token.Cancel(true);
                await DeleteResourceGroupAsync(token.Token);
            }
        }

        private static void WriteGeneralRetryMessageToConsole()
            => ConsoleEx.WriteLine("Please try deployment again, and create an issue if this continues to fail: https://github.com/microsoft/ga4gh-tes/issues");

        public Task Execute(string message, Func<Task> func, bool cancelOnException = true)
            => Execute(message, async () => { await func(); return false; }, cancelOnException);

        private async Task<T> Execute<T>(string message, Func<Task<T>> func, bool cancelOnException = true)
        {
            const int retryCount = 3;

            var startTime = DateTime.UtcNow;
            var line = ConsoleEx.WriteLine(message);

            for (var i = 0; i < retryCount; i++)
            {
                try
                {
                    cts.Token.ThrowIfCancellationRequested();
                    var result = await func();
                    WriteExecutionTime(line, startTime);
                    return result;
                }
                catch (System.ComponentModel.WarningException warningException)
                {
                    line.Write($" Warning: {warningException.Message}", ConsoleColor.Yellow);
                    return default;
                }
                catch (RequestFailedException requestFailedException) when (requestFailedException.ErrorCode.Equals("ExpiredAuthenticationToken", StringComparison.OrdinalIgnoreCase))
                {
                }
                catch (RequestFailedException requestFailedException) when (requestFailedException.ErrorCode.Equals("RoleAssignmentExists", StringComparison.OrdinalIgnoreCase))
                {
                    line.Write($" skipped. Role assignment already exists.", ConsoleColor.Yellow);
                    return default;
                }
                catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
                {
                    line.Write(" Cancelled", ConsoleColor.Red);
                    return await Task.FromCanceled<T>(cts.Token);
                }
                catch (Exception ex)
                {
                    line.Write($" Failed. {ex.GetType().Name}: {ex.Message}", ConsoleColor.Red);

                    if (cancelOnException)
                    {
                        cts.Cancel();
                    }

                    throw;
                }
            }

            line.Write($" Failed", ConsoleColor.Red);
            cts.Cancel();
            throw new Exception($"Failed after {retryCount} attempts");
        }

        private static void WriteExecutionTime(ConsoleEx.Line line, DateTime startTime)
            => line.Write($" Completed in {DateTime.UtcNow.Subtract(startTime).TotalSeconds:n0}s", ConsoleColor.Green);

        public static async Task<string> DownloadTextFromStorageAccountAsync(BlobClient blobClient, CancellationToken cancellationToken)
        {
            return (await blobClient.DownloadContentAsync(cancellationToken)).Value.Content.ToString();
        }

        public static async Task UploadTextToStorageAccountAsync(BlobClient blobClient, string content, CancellationToken cancellationToken)
        {
            await blobClient.GetParentBlobContainerClient().CreateIfNotExistsAsync(cancellationToken: cancellationToken);
            await blobClient.UploadAsync(BinaryData.FromString(content), true, cancellationToken);
        }

        private class ValidationException(string reason, bool displayExample = true) : Exception
        {
            public string Reason { get; } = reason;
            public bool DisplayExample { get; } = displayExample;
        }
    }
}
