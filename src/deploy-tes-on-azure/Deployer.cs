// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IdentityModel.Tokens.Jwt;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Identity;
using Azure.ResourceManager;
using Azure.ResourceManager.Network;
using Azure.ResourceManager.Network.Models;
using Azure.ResourceManager.Resources;
using Azure.Security.KeyVault.Secrets;
using Azure.Storage;
using Azure.Storage.Blobs;
using CommonUtilities;
using k8s;
using Microsoft.Azure.Management.Batch;
using Microsoft.Azure.Management.Batch.Models;
using Microsoft.Azure.Management.Compute.Fluent;
using Microsoft.Azure.Management.ContainerRegistry.Fluent;
using Microsoft.Azure.Management.ContainerService;
using Microsoft.Azure.Management.ContainerService.Fluent;
using Microsoft.Azure.Management.ContainerService.Models;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.Graph.RBAC.Fluent;
using Microsoft.Azure.Management.KeyVault;
using Microsoft.Azure.Management.KeyVault.Fluent;
using Microsoft.Azure.Management.KeyVault.Models;
using Microsoft.Azure.Management.Msi.Fluent;
using Microsoft.Azure.Management.Network.Fluent;
using Microsoft.Azure.Management.PostgreSQL;
using Microsoft.Azure.Management.PrivateDns.Fluent;
using Microsoft.Azure.Management.ResourceGraph;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Azure.Management.Storage.Fluent;
using Microsoft.Rest;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;
using Tes.Models;
using static Microsoft.Azure.Management.PostgreSQL.FlexibleServers.DatabasesOperationsExtensions;
using static Microsoft.Azure.Management.PostgreSQL.FlexibleServers.ServersOperationsExtensions;
using static Microsoft.Azure.Management.PostgreSQL.ServersOperationsExtensions;
using static Microsoft.Azure.Management.ResourceManager.Fluent.Core.RestClient;
using FlexibleServer = Microsoft.Azure.Management.PostgreSQL.FlexibleServers;
using FlexibleServerModel = Microsoft.Azure.Management.PostgreSQL.FlexibleServers.Models;
using IResource = Microsoft.Azure.Management.ResourceManager.Fluent.Core.IResource;
using KeyVaultManagementClient = Microsoft.Azure.Management.KeyVault.KeyVaultManagementClient;

namespace TesDeployer
{
    public class Deployer
    {
        private static readonly AsyncRetryPolicy roleAssignmentHashConflictRetryPolicy = Policy
            .Handle<Microsoft.Rest.Azure.CloudException>(cloudException => cloudException.Body.Code.Equals("HashConflictOnDifferentRoleAssignmentIds"))
            .RetryAsync();

        private static readonly AsyncRetryPolicy generalRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(3, retryAttempt => System.TimeSpan.FromSeconds(1));

        private static readonly System.TimeSpan longRetryWaitTime = System.TimeSpan.FromSeconds(15);

        private static readonly AsyncRetryPolicy longRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(60, retryAttempt => longRetryWaitTime,
            (exception, timespan) => ConsoleEx.WriteLine($"Retrying task creation in {timespan} due to {exception.GetType().FullName}: {exception.Message}"));

        public const string ConfigurationContainerName = "configuration";
        public const string TesInternalContainerName = "tes-internal";
        public const string AllowedVmSizesFileName = "allowed-vm-sizes";
        public const string TesCredentialsFileName = "TesCredentials.json";
        public const string InputsContainerName = "inputs";
        public const string StorageAccountKeySecretName = "CoAStorageKey";
        public const string PostgresqlSslMode = "VerifyFull";

        private record TesCredentials(string TesHostname, string TesUsername, string TesPassword);

        private readonly CancellationTokenSource cts = new();

        private readonly List<string> requiredResourceProviders = new()
        {
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
        };

        private readonly Dictionary<string, List<string>> requiredResourceProviderFeatures = new()
        {
            { "Microsoft.Compute", new() { "EncryptionAtHost" } }
        };

        private Configuration configuration { get; set; }
        private ITokenProvider tokenProvider;
        private TokenCredentials tokenCredentials;
        private IAzure azureSubscriptionClient { get; set; }
        private Microsoft.Azure.Management.Fluent.Azure.IAuthenticated azureClient { get; set; }
        private IResourceManager resourceManagerClient { get; set; }
        private ArmClient armClient { get; set; }
        private AzureCredentials azureCredentials { get; set; }
        private FlexibleServer.IPostgreSQLManagementClient postgreSqlFlexManagementClient { get; set; }
        private IEnumerable<string> subscriptionIds { get; set; }
        private bool isResourceGroupCreated { get; set; }
        private KubernetesManager kubernetesManager { get; set; }

        public Deployer(Configuration configuration)
            => this.configuration = configuration;

        public async Task<int> DeployAsync()
        {
            var mainTimer = Stopwatch.StartNew();

            try
            {
                ValidateInitialCommandLineArgs();

                ConsoleEx.WriteLine("Running...");

                await ValidateTokenProviderAsync();

                await Execute("Connecting to Azure Services...", async () =>
                {
                    tokenProvider = new RefreshableAzureServiceTokenProvider("https://management.azure.com//.default");
                    tokenCredentials = new(tokenProvider);
                    azureCredentials = new(tokenCredentials, null, null, AzureEnvironment.AzureGlobalCloud);
                    armClient = new ArmClient(new DefaultAzureCredential());
                    azureClient = GetAzureClient(azureCredentials);
                    armClient = new ArmClient(new AzureCliCredential());
                    azureSubscriptionClient = azureClient.WithSubscription(configuration.SubscriptionId);
                    subscriptionIds = await (await azureClient.Subscriptions.ListAsync(cancellationToken: cts.Token)).ToAsyncEnumerable().Select(s => s.SubscriptionId).ToListAsync(cts.Token);
                    resourceManagerClient = GetResourceManagerClient(azureCredentials);
                    postgreSqlFlexManagementClient = new FlexibleServer.PostgreSQLManagementClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId, LongRunningOperationRetryTimeout = 1200 };
                });

                await ValidateSubscriptionAndResourceGroupAsync(configuration);
                kubernetesManager = new(configuration, azureCredentials, cts.Token);
                IResourceGroup resourceGroup = null;
                ManagedCluster aksCluster = null;
                BatchAccount batchAccount = null;
                IGenericResource logAnalyticsWorkspace = null;
                IGenericResource appInsights = null;
                FlexibleServerModel.Server postgreSqlFlexServer = null;
                IStorageAccount storageAccount = null;
                var keyVaultUri = string.Empty;
                IIdentity managedIdentity = null;
                IPrivateDnsZone postgreSqlDnsZone = null;

                var targetVersion = Utility.DelimitedTextToDictionary(Utility.GetFileContent("scripts", "env-00-tes-version.txt")).GetValueOrDefault("TesOnAzureVersion");

                if (configuration.Update)
                {
                    resourceGroup = await azureSubscriptionClient.ResourceGroups.GetByNameAsync(configuration.ResourceGroupName, cts.Token);
                    configuration.RegionName = resourceGroup.RegionName;

                    ConsoleEx.WriteLine($"Upgrading TES on Azure instance in resource group '{resourceGroup.Name}' to version {targetVersion}...");

                    if (string.IsNullOrEmpty(configuration.StorageAccountName))
                    {
                        var storageAccounts = await (await azureSubscriptionClient.StorageAccounts.ListByResourceGroupAsync(configuration.ResourceGroupName, cancellationToken: cts.Token))
                            .ToAsyncEnumerable().ToListAsync(cts.Token);

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

                    ManagedCluster existingAksCluster = default;

                    if (string.IsNullOrWhiteSpace(configuration.AksClusterName))
                    {
                        using var client = new ContainerServiceClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId };
                        var aksClusters = await (await client.ManagedClusters.ListByResourceGroupAsync(configuration.ResourceGroupName, cts.Token))
                            .ToAsyncEnumerable(client.ManagedClusters.ListByResourceGroupNextAsync).ToListAsync(cts.Token);

                        existingAksCluster = aksClusters.Count switch
                        {
                            0 => throw new ValidationException($"Update was requested but resource group {configuration.ResourceGroupName} does not contain any AKS clusters.", displayExample: false),
                            1 => aksClusters.Single(),
                            _ => throw new ValidationException($"Resource group {configuration.ResourceGroupName} contains multiple AKS clusters. {nameof(configuration.AksClusterName)} must be provided.", displayExample: false),
                        };

                        configuration.AksClusterName = existingAksCluster.Name;
                    }
                    else
                    {
                        existingAksCluster = (await GetExistingAKSClusterAsync(configuration.AksClusterName))
                            ?? throw new ValidationException($"AKS cluster {configuration.AksClusterName} does not exist in region {configuration.RegionName} or is not accessible to the current user.", displayExample: false);
                    }

                    var aksValues = await kubernetesManager.GetAKSSettingsAsync(storageAccount);

                    if (!aksValues.Any())
                    {
                        throw new ValidationException($"Could not retrieve account names from stored configuration in {storageAccount.Name}.", displayExample: false);
                    }

                    if (aksValues.TryGetValue("EnableIngress", out var enableIngress) && aksValues.TryGetValue("TesHostname", out var tesHostname))
                    {
                        kubernetesManager.TesHostname = tesHostname;
                        configuration.EnableIngress = bool.TryParse(enableIngress, out var parsed) ? parsed : null;

                        var tesCredentials = new FileInfo(Path.Combine(Directory.GetCurrentDirectory(), TesCredentialsFileName));
                        tesCredentials.Refresh();

                        if (configuration.EnableIngress.GetValueOrDefault() && tesCredentials.Exists)
                        {
                            try
                            {
                                using var stream = tesCredentials.OpenRead();
                                var (hostname, tesUsername, tesPassword) = System.Text.Json.JsonSerializer.Deserialize<TesCredentials>(stream,
                                    new System.Text.Json.JsonSerializerOptions() { IncludeFields = true, PropertyNameCaseInsensitive = true });

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
                        throw new ValidationException($"Could not retrieve the Batch account name from stored configuration in {storageAccount.Name}.", displayExample: false);
                    }

                    batchAccount = await GetExistingBatchAccountAsync(batchAccountName)
                        ?? throw new ValidationException($"Batch account {batchAccountName}, referenced by the stored configuration, does not exist in region {configuration.RegionName} or is not accessible to the current user.", displayExample: false);

                    configuration.BatchAccountName = batchAccountName;

                    if (!aksValues.TryGetValue("PostgreSqlServerName", out var postgreSqlServerName))
                    {
                        throw new ValidationException($"Could not retrieve the PostgreSqlServer account name from stored configuration in {storageAccount.Name}.", displayExample: false);
                    }

                    configuration.PostgreSqlServerName = postgreSqlServerName;

                    if (aksValues.TryGetValue("CrossSubscriptionAKSDeployment", out var crossSubscriptionAKSDeployment))
                    {
                        configuration.CrossSubscriptionAKSDeployment = bool.TryParse(crossSubscriptionAKSDeployment, out var parsed) ? parsed : null;
                    }

                    if (aksValues.TryGetValue("KeyVaultName", out var keyVaultName))
                    {
                        var keyVault = await GetKeyVaultAsync(keyVaultName);
                        keyVaultUri = keyVault.Properties.VaultUri;
                    }

                    if (!aksValues.TryGetValue("ManagedIdentityClientId", out var managedIdentityClientId))
                    {
                        throw new ValidationException($"Could not retrieve ManagedIdentityClientId.", displayExample: false);
                    }

                    managedIdentity = await (await azureSubscriptionClient.Identities.ListByResourceGroupAsync(configuration.ResourceGroupName, cancellationToken: cts.Token))
                            .ToAsyncEnumerable().FirstOrDefaultAsync(id => id.ClientId == managedIdentityClientId, cts.Token)
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

                    var settings = ConfigureSettings(managedIdentity.ClientId, aksValues, installedVersion);
                    var waitForRoleAssignmentPropagation = false;

                    if (installedVersion is null || installedVersion < new Version(4, 4))
                    {
                        // Ensure all storage containers are created.
                        await CreateDefaultStorageContainersAsync(storageAccount);

                        if (string.IsNullOrWhiteSpace(settings["BatchNodesSubnetId"]))
                        {
                            settings["BatchNodesSubnetId"] = await UpdateVnetWithBatchSubnet(resourceGroup.Inner.Id);
                        }
                    }

                    if (installedVersion is null || installedVersion < new Version(4, 8))
                    {
                        var hasAssignedNetworkContributor = await TryAssignMIAsNetworkContributorToResourceAsync(managedIdentity, resourceGroup);
                        var hasAssignedDataOwner = await TryAssignVmAsDataOwnerToStorageAccountAsync(managedIdentity, storageAccount);

                        waitForRoleAssignmentPropagation |= hasAssignedNetworkContributor || hasAssignedDataOwner;
                    }

                    if (installedVersion is null || installedVersion < new Version(5, 0, 1))
                    {
                        if (string.IsNullOrWhiteSpace(settings["ExecutionsContainerName"]))
                        {
                            settings["ExecutionsContainerName"] = TesInternalContainerName;
                        }
                    }

                    //if (installedVersion is null || installedVersion < new Version(5, 0, 2))
                    //{
                    //}

                    if (waitForRoleAssignmentPropagation)
                    {
                        await Execute("Waiting 5 minutes for role assignment propagation...",
                            () => Task.Delay(System.TimeSpan.FromMinutes(5), cts.Token));
                    }

                    await kubernetesManager.UpgradeValuesYamlAsync(storageAccount, settings);
                    await PerformHelmDeploymentAsync(resourceGroup);
                }

                if (!configuration.Update)
                {
                    if (string.IsNullOrWhiteSpace(configuration.BatchPrefix))
                    {
                        var blob = new byte[5];
                        RandomNumberGenerator.Fill(blob);
                        configuration.BatchPrefix = blob.ConvertToBase32().TrimEnd('=');
                    }

                    ValidateRegionName(configuration.RegionName);
                    ValidateMainIdentifierPrefix(configuration.MainIdentifierPrefix);
                    storageAccount = await ValidateAndGetExistingStorageAccountAsync();
                    batchAccount = await ValidateAndGetExistingBatchAccountAsync();
                    aksCluster = await ValidateAndGetExistingAKSClusterAsync();
                    postgreSqlFlexServer = await ValidateAndGetExistingPostgresqlServerAsync();
                    var keyVault = await ValidateAndGetExistingKeyVaultAsync();

                    if (aksCluster is null && !configuration.ManualHelmDeployment)
                    {
                        await ValidateVmAsync();
                    }

                    ConsoleEx.WriteLine($"Deploying TES on Azure version {targetVersion}...");

                    // Configuration preferences not currently settable by user.
                    if (string.IsNullOrWhiteSpace(configuration.PostgreSqlServerName))
                    {
                        configuration.PostgreSqlServerName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                    }

                    configuration.PostgreSqlAdministratorPassword = PasswordGenerator.GeneratePassword();
                    configuration.PostgreSqlTesUserPassword = PasswordGenerator.GeneratePassword();

                    if (string.IsNullOrWhiteSpace(configuration.BatchAccountName))
                    {
                        configuration.BatchAccountName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}", 15);
                    }

                    if (string.IsNullOrWhiteSpace(configuration.StorageAccountName))
                    {
                        configuration.StorageAccountName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}", 24);
                    }

                    //if (string.IsNullOrWhiteSpace(configuration.NetworkSecurityGroupName))
                    //{
                    //    configuration.NetworkSecurityGroupName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}", 15);
                    //}

                    if (string.IsNullOrWhiteSpace(configuration.ApplicationInsightsAccountName))
                    {
                        configuration.ApplicationInsightsAccountName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                    }

                    if (string.IsNullOrWhiteSpace(configuration.TesPassword))
                    {
                        configuration.TesPassword = PasswordGenerator.GeneratePassword();
                    }

                    if (string.IsNullOrWhiteSpace(configuration.AksClusterName))
                    {
                        configuration.AksClusterName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 25);
                    }

                    if (string.IsNullOrWhiteSpace(configuration.KeyVaultName))
                    {
                        configuration.KeyVaultName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                    }

                    await RegisterResourceProvidersAsync();
                    await RegisterResourceProviderFeaturesAsync();

                    if (batchAccount is null)
                    {
                        await ValidateBatchAccountQuotaAsync();
                    }

                    var vnetAndSubnet = await ValidateAndGetExistingVirtualNetworkAsync();

                    if (string.IsNullOrWhiteSpace(configuration.ResourceGroupName))
                    {
                        configuration.ResourceGroupName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                        resourceGroup = await CreateResourceGroupAsync();
                        isResourceGroupCreated = true;
                    }
                    else
                    {
                        resourceGroup = await azureSubscriptionClient.ResourceGroups.GetByNameAsync(configuration.ResourceGroupName, cts.Token);
                    }

                    // Derive TES ingress URL from resource group name
                    kubernetesManager.SetTesIngressNetworkingConfiguration(configuration.ResourceGroupName);

                    managedIdentity = await CreateUserManagedIdentityAsync(resourceGroup);

                    if (vnetAndSubnet is not null)
                    {
                        ConsoleEx.WriteLine($"Creating VM in existing virtual network {vnetAndSubnet.Value.virtualNetwork.Name} and subnet {vnetAndSubnet.Value.vmSubnet.Name}");
                    }

                    if (storageAccount is not null)
                    {
                        ConsoleEx.WriteLine($"Using existing Storage Account {storageAccount.Name}");
                    }

                    if (batchAccount is not null)
                    {
                        ConsoleEx.WriteLine($"Using existing Batch Account {batchAccount.Name}");
                    }

                    await Task.WhenAll(new Task[]
                    {
                            Task.Run(async () =>
                            {
                                if (vnetAndSubnet is null)
                                {
                                    configuration.VnetName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                                    configuration.PostgreSqlSubnetName = string.IsNullOrEmpty(configuration.PostgreSqlSubnetName) ? configuration.DefaultPostgreSqlSubnetName : configuration.PostgreSqlSubnetName;
                                    configuration.BatchSubnetName = string.IsNullOrEmpty(configuration.BatchSubnetName) ? configuration.DefaultBatchSubnetName : configuration.BatchSubnetName;
                                    configuration.VmSubnetName = string.IsNullOrEmpty(configuration.VmSubnetName) ? configuration.DefaultVmSubnetName : configuration.VmSubnetName;
                                    vnetAndSubnet = await CreateVnetAndSubnetsAsync(resourceGroup);

                                    if (string.IsNullOrEmpty(this.configuration.BatchNodesSubnetId))
                                    {
                                        this.configuration.BatchNodesSubnetId = vnetAndSubnet.Value.batchSubnet.Inner.Id;
                                    }
                                }
                            }),
                            Task.Run(async () =>
                            {
                                if (string.IsNullOrWhiteSpace(configuration.LogAnalyticsArmId))
                                {
                                    var workspaceName = SdkContext.RandomResourceName(configuration.MainIdentifierPrefix, 15);
                                    logAnalyticsWorkspace = await CreateLogAnalyticsWorkspaceResourceAsync(workspaceName);
                                    configuration.LogAnalyticsArmId = logAnalyticsWorkspace.Id;
                                }
                            }),
                            Task.Run(async () =>
                            {
                                storageAccount ??= await CreateStorageAccountAsync();
                                await CreateDefaultStorageContainersAsync(storageAccount);
                                await WritePersonalizedFilesToStorageAccountAsync(storageAccount);
                                await AssignVmAsContributorToStorageAccountAsync(managedIdentity, storageAccount);
                                await AssignVmAsDataOwnerToStorageAccountAsync(managedIdentity, storageAccount);
                                await AssignManagedIdOperatorToResourceAsync(managedIdentity, resourceGroup);
                                await AssignMIAsNetworkContributorToResourceAsync(managedIdentity, resourceGroup);
                            }),
                    });

                    if (configuration.CrossSubscriptionAKSDeployment.GetValueOrDefault())
                    {
                        await Task.Run(async () =>
                        {
                            keyVault ??= await CreateKeyVaultAsync(configuration.KeyVaultName, managedIdentity, vnetAndSubnet.Value.vmSubnet);
                            keyVaultUri = keyVault.Properties.VaultUri;
                            var keys = await storageAccount.GetKeysAsync();
                            await SetStorageKeySecret(keyVaultUri, StorageAccountKeySecretName, keys[0].Value);
                        });
                    }

                    if (postgreSqlFlexServer is null)
                    {
                        postgreSqlDnsZone = await CreatePrivateDnsZoneAsync(vnetAndSubnet.Value.virtualNetwork, $"privatelink.postgres.database.azure.com", "PostgreSQL Server");
                    }

                    await Task.WhenAll(new[]
                    {
                            Task.Run(async () =>
                            {
                                if (aksCluster is null && !configuration.ManualHelmDeployment)
                                {
                                    await ProvisionManagedClusterAsync(resourceGroup, managedIdentity, logAnalyticsWorkspace, vnetAndSubnet?.virtualNetwork, vnetAndSubnet?.vmSubnet.Name, configuration.PrivateNetworking.GetValueOrDefault());
                                }
                            }),
                            Task.Run(async () =>
                            {
                                batchAccount ??= await CreateBatchAccountAsync(storageAccount.Id);
                                await AssignVmAsContributorToBatchAccountAsync(managedIdentity, batchAccount);
                            }),
                            Task.Run(async () =>
                            {
                                appInsights = await CreateAppInsightsResourceAsync(configuration.LogAnalyticsArmId);
                                await AssignVmAsContributorToAppInsightsAsync(managedIdentity, appInsights);
                            }),
                            Task.Run(async () => {
                                postgreSqlFlexServer ??= await CreatePostgreSqlServerAndDatabaseAsync(postgreSqlFlexManagementClient, vnetAndSubnet.Value.postgreSqlSubnet, postgreSqlDnsZone);
                            })
                        });

                    var clientId = managedIdentity.ClientId;
                    var settings = ConfigureSettings(clientId);

                    await kubernetesManager.UpdateHelmValuesAsync(storageAccount, keyVaultUri, resourceGroup.Name, settings, managedIdentity);
                    await PerformHelmDeploymentAsync(resourceGroup,
                        new[]
                        {
                                "Run the following postgresql command to setup the database.",
                                $"\tPostgreSQL command: psql postgresql://{configuration.PostgreSqlAdministratorLogin}:{configuration.PostgreSqlAdministratorPassword}@{configuration.PostgreSqlServerName}.postgres.database.azure.com/{configuration.PostgreSqlTesDatabaseName} -c \"{GetCreateTesUserString()}\""
                        },
                        async kubernetesClient =>
                        {
                            await kubernetesManager.DeployCoADependenciesAsync();

                            // Deploy an ubuntu pod to run PSQL commands, then delete it
                            const string deploymentNamespace = "default";
                            var (deploymentName, ubuntuDeployment) = KubernetesManager.GetUbuntuDeploymentTemplate();
                            await kubernetesClient.AppsV1.CreateNamespacedDeploymentAsync(ubuntuDeployment, deploymentNamespace, cancellationToken: cts.Token);
                            await ExecuteQueriesOnAzurePostgreSQLDbFromK8(kubernetesClient, deploymentName, deploymentNamespace);
                            await kubernetesClient.AppsV1.DeleteNamespacedDeploymentAsync(deploymentName, deploymentNamespace, cancellationToken: cts.Token);


                            if (configuration.EnableIngress.GetValueOrDefault())
                            {
                                await Execute(
                                    $"Enabling Ingress {kubernetesManager.TesHostname}",
                                    async () =>
                                    {
                                        _ = await kubernetesManager.EnableIngress(configuration.TesUsername, configuration.TesPassword, kubernetesClient);
                                    });
                            }
                        });
                }

                if (configuration.OutputTesCredentialsJson.GetValueOrDefault())
                {
                    // Write credentials to JSON file in working directory
                    var credentialsJson = System.Text.Json.JsonSerializer.Serialize<TesCredentials>(
                        new(kubernetesManager.TesHostname, configuration.TesUsername, configuration.TesPassword));

                    var credentialsPath = Path.Combine(Directory.GetCurrentDirectory(), TesCredentialsFileName);
                    await File.WriteAllTextAsync(credentialsPath, credentialsJson, cts.Token);
                    ConsoleEx.WriteLine($"TES credentials file written to: {credentialsPath}");
                }

                var maxPerFamilyQuota = batchAccount.DedicatedCoreQuotaPerVMFamilyEnforced ? batchAccount.DedicatedCoreQuotaPerVMFamily.Select(q => q.CoreQuota).Where(q => 0 != q) : Enumerable.Repeat(batchAccount.DedicatedCoreQuota ?? 0, 1);
                var isBatchQuotaAvailable = batchAccount.LowPriorityCoreQuota > 0 || (batchAccount.DedicatedCoreQuota > 0 && maxPerFamilyQuota.Append(0).Max() > 0);
                var isBatchPoolQuotaAvailable = batchAccount.PoolQuota > 0;
                var isBatchJobQuotaAvailable = batchAccount.ActiveJobAndJobScheduleQuota > 0;
                var insufficientQuotas = new List<string>();
                int exitCode;

                if (!isBatchQuotaAvailable) insufficientQuotas.Add("core");
                if (!isBatchPoolQuotaAvailable) insufficientQuotas.Add("pool");
                if (!isBatchJobQuotaAvailable) insufficientQuotas.Add("job");

                if (insufficientQuotas.Any())
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
                            var runTestTask = RunTestTask("localhost:8088", batchAccount.LowPriorityCoreQuota > 0, configuration.TesUsername, configuration.TesPassword);

                            for (var task = await Task.WhenAny(portForwardTask, runTestTask);
                                runTestTask != task;
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

                            var isTestWorkflowSuccessful = await runTestTask;
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
                    ConsoleEx.WriteLine($"{exc.GetType().Name}: {exc.Message}", ConsoleColor.Red);

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

                        if (exc is HttpOperationException hExc)
                        {
                            ConsoleEx.WriteLine($"HTTP Response: {hExc.Response.Content}");
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

        private async Task PerformHelmDeploymentAsync(IResourceGroup resourceGroup, IEnumerable<string> manualPrecommands = default, Func<IKubernetes, Task> asyncTask = default)
        {
            if (configuration.ManualHelmDeployment)
            {
                ConsoleEx.WriteLine($"Helm chart written to disk at: {kubernetesManager.helmScriptsRootDirectory}");
                ConsoleEx.WriteLine($"Please update values file if needed here: {kubernetesManager.TempHelmValuesYamlPath}");

                foreach (var line in manualPrecommands ?? Enumerable.Empty<string>())
                {
                    ConsoleEx.WriteLine(line);
                }

                ConsoleEx.WriteLine($"Then, deploy the helm chart, and press Enter to continue.");
                ConsoleEx.ReadLine();
            }
            else
            {
                var kubernetesClient = await kubernetesManager.GetKubernetesClientAsync(resourceGroup);
                await (asyncTask?.Invoke(kubernetesClient) ?? Task.CompletedTask);
                await kubernetesManager.DeployHelmChartToClusterAsync(kubernetesClient);
            }
        }

        private async Task<int> TestTaskAsync(string tesEndpoint, bool preemptible, string tesUsername, string tesPassword)
        {
            using var client = new HttpClient();

            var task = new TesTask()
            {
                Inputs = new(),
                Outputs = new(),
                Executors = new()
                {
                    new()
                    {
                        Image = "ubuntu:22.04",
                        Command = new() { "echo", "hello world" },
                    }
                },
                Resources = new()
                {
                    Preemptible = preemptible
                }
            };

            var content = new StringContent(JsonConvert.SerializeObject(task), Encoding.UTF8, "application/json");
            var requestUri = $"http://{tesEndpoint}/v1/tasks";

            Dictionary<string, string> response = null;
            await longRetryPolicy.ExecuteAsync(
                async ct =>
                {
                    var responseBody = await client.PostAsync(requestUri, content, ct);
                    var body = await responseBody.Content.ReadAsStringAsync(ct);
                    try
                    {
                        response = JsonConvert.DeserializeObject<Dictionary<string, string>>(body);
                    }
                    catch (JsonReaderException exception)
                    {
                        exception.Data.Add("Body", body);
                        throw;
                    }
                },
                cts.Token);

            return await IsTaskSuccessfulAfterLongPollingAsync(client, $"{requestUri}/{response["id"]}?view=full") ? 0 : 1;
        }

        private async Task<bool> RunTestTask(string tesEndpoint, bool preemptible, string tesUsername, string tesPassword)
        {
            var startTime = DateTime.UtcNow;
            var line = ConsoleEx.WriteLine("Running a test task...");
            var isTestWorkflowSuccessful = (await TestTaskAsync(tesEndpoint, preemptible, tesUsername, tesPassword)) < 1;
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

        private async Task<bool> IsTaskSuccessfulAfterLongPollingAsync(HttpClient client, string taskEndpoint)
        {
            while (true)
            {
                try
                {
                    var responseBody = await client.GetAsync(taskEndpoint, cts.Token);
                    var content = await responseBody.Content.ReadAsStringAsync(cts.Token);
                    var response = JsonConvert.DeserializeObject<TesTask>(content);

                    if (response.State == TesState.COMPLETEEnum)
                    {
                        if (string.IsNullOrWhiteSpace(response.FailureReason))
                        {
                            ConsoleEx.WriteLine($"TES Task State: {response.State}");
                            return true;
                        }

                        ConsoleEx.WriteLine($"Failure reason: {response.FailureReason}");
                        return false;
                    }
                    else if (response.State == TesState.EXECUTORERROREnum || response.State == TesState.SYSTEMERROREnum || response.State == TesState.CANCELEDEnum)
                    {
                        ConsoleEx.WriteLine($"TES Task State: {response.State}");
                        ConsoleEx.WriteLine(content);

                        if (!string.IsNullOrWhiteSpace(response.FailureReason))
                        {
                            ConsoleEx.WriteLine($"Failure reason: {response.FailureReason}");
                        }

                        return false;
                    }
                }
                catch (Exception exc)
                {
                    // "Server is busy" occasionally can be ignored
                    ConsoleEx.WriteLine($"Transient error: '{exc.Message}' Will retry again in 10s.");
                }

                await Task.Delay(System.TimeSpan.FromSeconds(10), cts.Token);
            }
        }

        private async Task<Vault> ValidateAndGetExistingKeyVaultAsync()
        {
            if (string.IsNullOrWhiteSpace(configuration.KeyVaultName))
            {
                return null;
            }

            return (await GetKeyVaultAsync(configuration.KeyVaultName))
                ?? throw new ValidationException($"If key vault name is provided, it must already exist in region {configuration.RegionName}, and be accessible to the current user.", displayExample: false);
        }

        private async Task<FlexibleServerModel.Server> ValidateAndGetExistingPostgresqlServerAsync()
        {
            if (string.IsNullOrWhiteSpace(configuration.PostgreSqlServerName))
            {
                return null;
            }

            return (await GetExistingPostgresqlServiceAsync(configuration.PostgreSqlServerName))
                ?? throw new ValidationException($"If Postgresql server name is provided, the server must already exist in region {configuration.RegionName}, and be accessible to the current user.", displayExample: false);
        }

        private async Task<ManagedCluster> ValidateAndGetExistingAKSClusterAsync()
        {
            if (string.IsNullOrWhiteSpace(configuration.AksClusterName))
            {
                return null;
            }

            return (await GetExistingAKSClusterAsync(configuration.AksClusterName))
                ?? throw new ValidationException($"If AKS cluster name is provided, the cluster must already exist in region {configuration.RegionName}, and be accessible to the current user.", displayExample: false);
        }

        private async Task<FlexibleServerModel.Server> GetExistingPostgresqlServiceAsync(string serverName)
        {
            var regex = new Regex(@"\s+");
            return await subscriptionIds.ToAsyncEnumerable().SelectAwait(async s =>
            {
                try
                {
                    var client = new FlexibleServer.PostgreSQLManagementClient(tokenCredentials) { SubscriptionId = s };
                    return (await client.Servers.ListAsync(cts.Token)).ToAsyncEnumerable(client.Servers.ListNextAsync);
                }
                catch (Exception e)
                {
                    ConsoleEx.WriteLine(e.Message);
                    return null;
                }
            })
            .Where(a => a is not null)
            .SelectMany(a => a)
            .SingleOrDefaultAsync(a =>
                    a.Name.Equals(serverName, StringComparison.OrdinalIgnoreCase) &&
                    regex.Replace(a.Location, string.Empty).Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase),
                cts.Token);
        }

        private async Task<ManagedCluster> GetExistingAKSClusterAsync(string aksClusterName)
        {
            return await subscriptionIds.ToAsyncEnumerable().SelectAwait(async s =>
            {
                try
                {
                    var client = new ContainerServiceClient(tokenCredentials) { SubscriptionId = s };
                    return (await client.ManagedClusters.ListAsync(cts.Token)).ToAsyncEnumerable(client.ManagedClusters.ListNextAsync);
                }
                catch (Exception e)
                {
                    ConsoleEx.WriteLine(e.Message);
                    return null;
                }
            })
            .Where(a => a is not null)
            .SelectMany(a => a)
            .SingleOrDefaultAsync(a =>
                    a.Name.Equals(aksClusterName, StringComparison.OrdinalIgnoreCase) &&
                    a.Location.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase),
                cts.Token);
        }

        private async Task<ManagedCluster> ProvisionManagedClusterAsync(IResource resourceGroupObject, IIdentity managedIdentity, IGenericResource logAnalyticsWorkspace, INetwork virtualNetwork, string subnetName, bool privateNetworking)
        {
            var resourceGroup = resourceGroupObject.Name;
            var nodePoolName = "nodepool1";
            var containerServiceClient = new ContainerServiceClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId };
            var cluster = new ManagedCluster
            {
                AddonProfiles = new Dictionary<string, ManagedClusterAddonProfile>
                {
                    { "omsagent", new(true, new Dictionary<string, string>() { { "logAnalyticsWorkspaceResourceID", logAnalyticsWorkspace.Id } }) }
                },
                Location = configuration.RegionName,
                DnsPrefix = configuration.AksClusterName,
                NetworkProfile = new()
                {
                    NetworkPlugin = NetworkPlugin.Azure,
                    ServiceCidr = configuration.KubernetesServiceCidr,
                    DnsServiceIP = configuration.KubernetesDnsServiceIP,
                    DockerBridgeCidr = configuration.KubernetesDockerBridgeCidr,
                    NetworkPolicy = NetworkPolicy.Azure
                },
                Identity = new(managedIdentity.PrincipalId, managedIdentity.TenantId, Microsoft.Azure.Management.ContainerService.Models.ResourceIdentityType.UserAssigned)
                {
                    UserAssignedIdentities = new Dictionary<string, ManagedClusterIdentityUserAssignedIdentitiesValue>()
                }
            };

            if (!string.IsNullOrWhiteSpace(configuration.AadGroupIds))
            {
                cluster.EnableRBAC = true;
                cluster.AadProfile = new()
                {
                    AdminGroupObjectIDs = configuration.AadGroupIds.Split(",", StringSplitOptions.RemoveEmptyEntries),
                    EnableAzureRBAC = false,
                    Managed = true
                };
            }

            cluster.Identity.UserAssignedIdentities.Add(managedIdentity.Id, new(managedIdentity.PrincipalId, managedIdentity.ClientId));
            cluster.IdentityProfile = new Dictionary<string, ManagedClusterPropertiesIdentityProfileValue>
            {
                { "kubeletidentity", new(managedIdentity.Id, managedIdentity.ClientId, managedIdentity.PrincipalId) }
            };

            cluster.AgentPoolProfiles = new List<ManagedClusterAgentPoolProfile>
            {
                new()
                {
                    Name = nodePoolName,
                    Count = configuration.AksPoolSize,
                    VmSize = configuration.VmSize,
                    OsDiskSizeGB = 128,
                    OsDiskType = OSDiskType.Managed,
                    EnableEncryptionAtHost = true,
                    Type = "VirtualMachineScaleSets",
                    EnableAutoScaling = false,
                    EnableNodePublicIP = false,
                    OsType = "Linux",
                    OsSKU = "AzureLinux",
                    Mode = "System",
                    VnetSubnetID = virtualNetwork.Subnets[subnetName].Inner.Id,
                }
            };

            if (privateNetworking)
            {
                cluster.ApiServerAccessProfile = new()
                {
                    EnablePrivateCluster = true,
                    EnablePrivateClusterPublicFQDN = true
                };
            }

            return await Execute(
                $"Creating AKS Cluster: {configuration.AksClusterName}...",
                () => containerServiceClient.ManagedClusters.CreateOrUpdateAsync(resourceGroup, configuration.AksClusterName, cluster, cts.Token));
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
            settings ??= new();
            var defaults = GetDefaultValues(new[] { "env-00-tes-version.txt", "env-01-account-names.txt", "env-02-internal-images.txt", "env-04-settings.txt" });

            // We always overwrite the CoA version
            UpdateSetting(settings, defaults, "TesOnAzureVersion", default(string), ignoreDefaults: false);
            UpdateSetting(settings, defaults, "ResourceGroupName", configuration.ResourceGroupName, ignoreDefaults: false);
            UpdateSetting(settings, defaults, "RegionName", configuration.RegionName, ignoreDefaults: false);

            // Process images
            UpdateSetting(settings, defaults, "TesImageName", configuration.TesImageName,
                ignoreDefaults: ImageNameIgnoreDefaults(settings, defaults, "TesImageName", configuration.TesImageName is null, installedVersion));

            // Additional non-personalized settings
            UpdateSetting(settings, defaults, "BatchNodesSubnetId", configuration.BatchNodesSubnetId);
            UpdateSetting(settings, defaults, "DisableBatchNodesPublicIpAddress", configuration.DisableBatchNodesPublicIpAddress, b => b.GetValueOrDefault().ToString(), configuration.DisableBatchNodesPublicIpAddress.GetValueOrDefault().ToString());

            if (installedVersion is null)
            {
                UpdateSetting(settings, defaults, "BatchPrefix", configuration.BatchPrefix, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "DefaultStorageAccountName", configuration.StorageAccountName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "ExecutionsContainerName", TesInternalContainerName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "BatchAccountName", configuration.BatchAccountName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "ApplicationInsightsAccountName", configuration.ApplicationInsightsAccountName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "ManagedIdentityClientId", managedIdentityClientId, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "AzureServicesAuthConnectionString", $"RunAs=App;AppId={managedIdentityClientId}", ignoreDefaults: true);
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
            }

            BackFillSettings(settings, defaults);
            return settings;
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

            var sameVersionUpgrade = installedVersion.Equals(new(defaults["TesOnAzureVersion"]));
            _ = settings.TryGetValue(key, out var installed);
            _ = defaults.TryGetValue(key, out var @default);
            var defaultPath = @default?[..@default.LastIndexOf(':')];
            var installedTag = installed?[(installed.LastIndexOf(':') + 1)..];
            bool? result;

            try
            {
                // Is this our official prepository/image?
                result = installed.StartsWith(defaultPath + ":")
                        // Is the tag a version (without decorations)?
                        && Version.TryParse(installedTag, out var version)
                        // Is the image version the same as the installed version?
                        && version.Equals(installedVersion)
                    // Upgrade image
                    ? false
                    // Preserve configured image
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
        /// Pupulates <paramref name="settings"/> with missing values.
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

        private static Microsoft.Azure.Management.Fluent.Azure.IAuthenticated GetAzureClient(AzureCredentials azureCredentials)
            => Microsoft.Azure.Management.Fluent.Azure
                .Configure()
                .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                .Authenticate(azureCredentials);

        private IResourceManager GetResourceManagerClient(AzureCredentials azureCredentials)
            => ResourceManager
                .Configure()
                .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                .Authenticate(azureCredentials)
                .WithSubscription(configuration.SubscriptionId);

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
                                resourceManagerClient.Providers.RegisterAsync(rp, cts.Token))
                        );

                        // RP registration takes a few minutes; poll until done registering

                        while (!cts.IsCancellationRequested)
                        {
                            unregisteredResourceProviders = await GetRequiredResourceProvidersNotRegisteredAsync();

                            if (unregisteredResourceProviders.Count == 0)
                            {
                                break;
                            }

                            await Task.Delay(System.TimeSpan.FromSeconds(15), cts.Token);
                        }
                    });
            }
            catch (Microsoft.Rest.Azure.CloudException ex) when (ex.ToCloudErrorType() == CloudErrorType.AuthorizationFailed)
            {
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("Unable to programatically register the required resource providers.", ConsoleColor.Red);
                ConsoleEx.WriteLine("This can happen if you don't have the Owner or Contributor role assignment for the subscription.", ConsoleColor.Red);
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("Please contact the Owner or Contributor of your Azure subscription, and have them:", ConsoleColor.Yellow);
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("1. Navigate to https://portal.azure.com", ConsoleColor.Yellow);
                ConsoleEx.WriteLine("2. Select Subscription -> Resource Providers", ConsoleColor.Yellow);
                ConsoleEx.WriteLine("3. Select each of the following and click Register:", ConsoleColor.Yellow);
                ConsoleEx.WriteLine();
                unregisteredResourceProviders.ForEach(rp => ConsoleEx.WriteLine($"- {rp}", ConsoleColor.Yellow));
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("After completion, please re-attempt deployment.");

                Environment.Exit(1);
            }
        }

        private async Task<List<string>> GetRequiredResourceProvidersNotRegisteredAsync()
        {
            var cloudResourceProviders = (await resourceManagerClient.Providers.ListAsync(cancellationToken: cts.Token)).ToAsyncEnumerable();

            var notRegisteredResourceProviders = await requiredResourceProviders.ToAsyncEnumerable()
                .Intersect(cloudResourceProviders
                    .Where(rp => !rp.RegistrationState.Equals("Registered", StringComparison.OrdinalIgnoreCase))
                    .Select(rp => rp.Namespace), StringComparer.OrdinalIgnoreCase)
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
                    var subscription = armClient.GetSubscriptionResource(new($"/subscriptions/{configuration.SubscriptionId}"));

                    foreach (var rpName in requiredResourceProviderFeatures.Keys)
                    {
                        var rp = await subscription.GetResourceProviderAsync(rpName, cancellationToken: cts.Token);

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

                        await Task.Delay(System.TimeSpan.FromSeconds(30), cts.Token);
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
            catch (Microsoft.Rest.Azure.CloudException ex) when (ex.ToCloudErrorType() == CloudErrorType.AuthorizationFailed)
            {
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("Unable to programatically register the required features.", ConsoleColor.Red);
                ConsoleEx.WriteLine("This can happen if you don't have the Owner or Contributor role assignment for the subscription.", ConsoleColor.Red);
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("Please contact the Owner or Contributor of your Azure subscription, and have them:", ConsoleColor.Yellow);
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("1. For each of the following, execute 'az feature register --namespace {RESOURCE_PROVIDER_NAME} --name {FEATURE_NAME}'", ConsoleColor.Yellow);
                ConsoleEx.WriteLine();
                unregisteredFeatures.ForEach(f => ConsoleEx.WriteLine($"- {f.Data.Name}", ConsoleColor.Yellow));
                ConsoleEx.WriteLine();
                ConsoleEx.WriteLine("After completion, please re-attempt deployment.");

                Environment.Exit(1);
            }
        }

        private async Task<bool> TryAssignMIAsNetworkContributorToResourceAsync(IIdentity managedIdentity, IResource resource)
        {
            try
            {
                await AssignMIAsNetworkContributorToResourceAsync(managedIdentity, resource, cancelOnException: false);
                return true;
            }
            catch (Exception)
            {
                // Already exists
                ConsoleEx.WriteLine("Network Contributor role for the managed id likely already exists.  Skipping", ConsoleColor.Yellow);
                return false;
            }
        }

        private Task AssignMIAsNetworkContributorToResourceAsync(IIdentity managedIdentity, IResource resource, bool cancelOnException = true)
        {
            // https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#network-contributor
            var roleDefinitionId = $"/subscriptions/{configuration.SubscriptionId}/providers/Microsoft.Authorization/roleDefinitions/4d97b98b-1d4f-4787-a291-c67834d212e7";
            return Execute(
                $"Assigning Network Contributor role for the managed id to resource group scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithRoleDefinition(roleDefinitionId)
                        .WithResourceScope(resource)
                        .CreateAsync(ct),
                    cts.Token),
                cancelOnException: cancelOnException);
        }

        private Task AssignManagedIdOperatorToResourceAsync(IIdentity managedIdentity, IResource resource)
        {
            // https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#managed-identity-operator
            var roleDefinitionId = $"/subscriptions/{configuration.SubscriptionId}/providers/Microsoft.Authorization/roleDefinitions/f1a07417-d97a-45cb-824c-7a7467783830";
            return Execute(
                $"Assigning Managed ID Operator role for the managed id to resource group scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithRoleDefinition(roleDefinitionId)
                        .WithResourceScope(resource)
                        .CreateAsync(ct),
                    cts.Token));
        }

        private async Task<bool> TryAssignVmAsDataOwnerToStorageAccountAsync(IIdentity managedIdentity, IStorageAccount storageAccount)
        {
            try
            {
                await AssignVmAsDataOwnerToStorageAccountAsync(managedIdentity, storageAccount, cancelOnException: false);
                return true;
            }
            catch (Exception)
            {
                // Already exists
                ConsoleEx.WriteLine("Storage Blob Data Owner role for the managed id likely already exists.  Skipping", ConsoleColor.Yellow);
                return false;
            }
        }

        private Task AssignVmAsDataOwnerToStorageAccountAsync(IIdentity managedIdentity, IStorageAccount storageAccount, bool cancelOnException = true)
        {
            //https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#storage-blob-data-owner
            var roleDefinitionId = $"/subscriptions/{configuration.SubscriptionId}/providers/Microsoft.Authorization/roleDefinitions/b7e6dc6d-f1e8-4753-8033-0f276bb0955b";

            return Execute(
                $"Assigning Storage Blob Data Owner role for user-managed identity to Storage Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithRoleDefinition(roleDefinitionId)
                        .WithResourceScope(storageAccount)
                        .CreateAsync(ct),
                    cts.Token),
                cancelOnException: cancelOnException);
        }

        private Task AssignVmAsContributorToStorageAccountAsync(IIdentity managedIdentity, IResource storageAccount)
            => Execute(
                $"Assigning {BuiltInRole.Contributor} role for user-managed identity to Storage Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithResourceScope(storageAccount)
                        .CreateAsync(ct),
                    cts.Token));

        private Task<IStorageAccount> CreateStorageAccountAsync()
            => Execute(
                $"Creating Storage Account: {configuration.StorageAccountName}...",
                () => azureSubscriptionClient.StorageAccounts
                    .Define(configuration.StorageAccountName)
                    .WithRegion(configuration.RegionName)
                    .WithExistingResourceGroup(configuration.ResourceGroupName)
                    .WithGeneralPurposeAccountKindV2()
                    .WithOnlyHttpsTraffic()
                    .WithSku(StorageAccountSkuType.Standard_LRS)
                    .CreateAsync(cts.Token));

        private async Task<IStorageAccount> GetExistingStorageAccountAsync(string storageAccountName)
            => await subscriptionIds.ToAsyncEnumerable().SelectAwait(async s =>
            {
                try
                {
                    return (await azureClient.WithSubscription(s).StorageAccounts.ListAsync(cancellationToken: cts.Token)).ToAsyncEnumerable();
                }
                catch (Exception)
                {
                    // Ignore exception if a user does not have the required role to list storage accounts in a subscription
                    return null;
                }
            })
            .Where(a => a is not null)
            .SelectMany(a => a)
            .SingleOrDefaultAsync(a =>
                    a.Name.Equals(storageAccountName, StringComparison.OrdinalIgnoreCase) &&
                    a.RegionName.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase),
                cts.Token);

        private async Task<BatchAccount> GetExistingBatchAccountAsync(string batchAccountName)
            => await subscriptionIds.ToAsyncEnumerable().SelectAwait(async s =>
            {
                try
                {
                    var client = new BatchManagementClient(tokenCredentials) { SubscriptionId = s };
                    return (await client.BatchAccount.ListAsync(cts.Token))
                        .ToAsyncEnumerable(client.BatchAccount.ListNextAsync);
                }
                catch (Exception e)
                {
                    ConsoleEx.WriteLine(e.Message);
                    return null;
                }
            })
            .Where(a => a is not null)
            .SelectMany(a => a)
            .SingleOrDefaultAsync(a =>
                    a.Name.Equals(batchAccountName, StringComparison.OrdinalIgnoreCase) &&
                    a.Location.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase),
                cts.Token);

        private async Task CreateDefaultStorageContainersAsync(IStorageAccount storageAccount)
        {
            var blobClient = await GetBlobClientAsync(storageAccount, cts.Token);

            var defaultContainers = new List<string> { TesInternalContainerName, InputsContainerName, "outputs", ConfigurationContainerName };
            await Task.WhenAll(defaultContainers.Select(c => blobClient.GetBlobContainerClient(c).CreateIfNotExistsAsync(cancellationToken: cts.Token)));
        }

        private Task WritePersonalizedFilesToStorageAccountAsync(IStorageAccount storageAccount)
            => Execute(
                $"Writing {AllowedVmSizesFileName} file to '{TesInternalContainerName}' storage container...",
                async () =>
                {
                    await UploadTextToStorageAccountAsync(storageAccount, TesInternalContainerName, $"{ConfigurationContainerName}/{AllowedVmSizesFileName}", Utility.GetFileContent("scripts", AllowedVmSizesFileName), cts.Token);
                });

        private Task AssignVmAsContributorToBatchAccountAsync(IIdentity managedIdentity, BatchAccount batchAccount)
            => Execute(
                $"Assigning {BuiltInRole.Contributor} role for user-managed identity to Batch Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithScope(batchAccount.Id)
                        .CreateAsync(ct),
                    cts.Token));

        private async Task<FlexibleServerModel.Server> CreatePostgreSqlServerAndDatabaseAsync(FlexibleServer.IPostgreSQLManagementClient postgresManagementClient, ISubnet subnet, IPrivateDnsZone postgreSqlDnsZone)
        {
            if (!subnet.Inner.Delegations.Any())
            {
                subnet.Parent.Update().UpdateSubnet(subnet.Name).WithDelegation("Microsoft.DBforPostgreSQL/flexibleServers");
                await subnet.Parent.Update().ApplyAsync();
            }

            FlexibleServerModel.Server server = null;

            await Execute(
                $"Creating Azure Flexible Server for PostgreSQL: {configuration.PostgreSqlServerName}...",
                async () =>
                {
                    server = await postgresManagementClient.Servers.CreateAsync(
                        configuration.ResourceGroupName, configuration.PostgreSqlServerName,
                        new(
                           location: configuration.RegionName,
                           version: configuration.PostgreSqlVersion,
                           sku: new(configuration.PostgreSqlSkuName, configuration.PostgreSqlTier),
                           storage: new(configuration.PostgreSqlStorageSize),
                           administratorLogin: configuration.PostgreSqlAdministratorLogin,
                           administratorLoginPassword: configuration.PostgreSqlAdministratorPassword,
                           network: new(publicNetworkAccess: "Disabled", delegatedSubnetResourceId: subnet.Inner.Id, privateDnsZoneArmResourceId: postgreSqlDnsZone.Id),
                           highAvailability: new("Disabled")
                        ));
                });

            await Execute(
                $"Creating PostgreSQL tes database: {configuration.PostgreSqlTesDatabaseName}...",
                () => postgresManagementClient.Databases.CreateAsync(
                    configuration.ResourceGroupName, configuration.PostgreSqlServerName, configuration.PostgreSqlTesDatabaseName,
                    new()));

            return server;
        }

        private string GetCreateTesUserString()
        {
            return $"CREATE USER {configuration.PostgreSqlTesUserLogin} WITH PASSWORD '{configuration.PostgreSqlTesUserPassword}'; GRANT ALL PRIVILEGES ON DATABASE {configuration.PostgreSqlTesDatabaseName} TO {configuration.PostgreSqlTesUserLogin};";
        }

        private Task ExecuteQueriesOnAzurePostgreSQLDbFromK8(IKubernetes kubernetesClient, string podName, string aksNamespace)
            => Execute(
                $"Executing scripts on postgresql...",
                async () =>
                {
                    var tesScript = GetCreateTesUserString();
                    var serverPath = $"{configuration.PostgreSqlServerName}.postgres.database.azure.com";
                    var adminUser = configuration.PostgreSqlAdministratorLogin;

                    var commands = new List<string[]> {
                        new string[] { "apt", "-qq", "update" },
                        new string[] { "apt", "-qq", "install", "-y", "postgresql-client" },
                        new string[] { "bash", "-lic", $"echo {configuration.PostgreSqlServerName}{configuration.PostgreSqlServerNameSuffix}:{configuration.PostgreSqlServerPort}:{configuration.PostgreSqlTesDatabaseName}:{adminUser}:{configuration.PostgreSqlAdministratorPassword} >> ~/.pgpass" },
                        new string[] { "bash", "-lic", "chmod 0600 ~/.pgpass" },
                        new string[] { "/usr/bin/psql", "-h", serverPath, "-U", adminUser, "-d", configuration.PostgreSqlTesDatabaseName, "-c", tesScript }
                    };

                    await kubernetesManager.ExecuteCommandsOnPodAsync(kubernetesClient, podName, commands, aksNamespace);
                });

        private Task AssignVmAsContributorToAppInsightsAsync(IIdentity managedIdentity, IResource appInsights)
            => Execute(
                $"Assigning {BuiltInRole.Contributor} role for user-managed identity to App Insights resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithResourceScope(appInsights)
                        .CreateAsync(ct),
                    cts.Token));

        private Task<(INetwork virtualNetwork, ISubnet vmSubnet, ISubnet postgreSqlSubnet, ISubnet batchSubnet)> CreateVnetAndSubnetsAsync(IResourceGroup resourceGroup)
          => Execute(
                $"Creating virtual network and subnets: {configuration.VnetName}...",
                async () =>
                {
                    var tesPorts = new List<int> { };

                    if (configuration.EnableIngress.GetValueOrDefault())
                    {
                        tesPorts = new List<int> { 80, 443 };
                    }

                    var defaultNsg = await CreateNetworkSecurityGroupAsync(resourceGroup, $"{configuration.VnetName}-default-nsg");
                    var aksNsg = await CreateNetworkSecurityGroupAsync(resourceGroup, $"{configuration.VnetName}-aks-nsg", tesPorts);

                    var vnetDefinition = azureSubscriptionClient.Networks
                        .Define(configuration.VnetName)
                        .WithRegion(configuration.RegionName)
                        .WithExistingResourceGroup(resourceGroup)
                        .WithAddressSpace(configuration.VnetAddressSpace)
                        .DefineSubnet(configuration.VmSubnetName)
                        .WithAddressPrefix(configuration.VmSubnetAddressSpace)
                        .WithExistingNetworkSecurityGroup(aksNsg)
                        .Attach();

                    vnetDefinition = vnetDefinition.DefineSubnet(configuration.PostgreSqlSubnetName)
                        .WithAddressPrefix(configuration.PostgreSqlSubnetAddressSpace)
                        .WithExistingNetworkSecurityGroup(defaultNsg)
                        .WithDelegation("Microsoft.DBforPostgreSQL/flexibleServers")
                        .Attach();

                    vnetDefinition = vnetDefinition.DefineSubnet(configuration.BatchSubnetName)
                        .WithAddressPrefix(configuration.BatchNodesSubnetAddressSpace)
                        .WithExistingNetworkSecurityGroup(defaultNsg)
                        .Attach();

                    var vnet = await vnetDefinition.CreateAsync(cts.Token);
                    var batchSubnet = vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.BatchSubnetName, StringComparison.OrdinalIgnoreCase)).Value;

                    // Use the new ResourceManager sdk to add the ACR service endpoint since it is absent from the fluent sdk.
                    var armBatchSubnet = (await armClient.GetSubnetResource(new ResourceIdentifier(batchSubnet.Inner.Id)).GetAsync(cancellationToken: cts.Token)).Value;

                    AddServiceEndpointsToSubnet(armBatchSubnet.Data);

                    await armBatchSubnet.UpdateAsync(Azure.WaitUntil.Completed, armBatchSubnet.Data, cts.Token);

                    return (vnet,
                        vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.VmSubnetName, StringComparison.OrdinalIgnoreCase)).Value,
                        vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.PostgreSqlSubnetName, StringComparison.OrdinalIgnoreCase)).Value,
                        batchSubnet);
                });

        private Task<INetworkSecurityGroup> CreateNetworkSecurityGroupAsync(IResourceGroup resourceGroup, string networkSecurityGroupName, List<int> openPorts = null)
        {
            var icreate = azureSubscriptionClient.NetworkSecurityGroups.Define(networkSecurityGroupName)
                    .WithRegion(configuration.RegionName)
                    .WithExistingResourceGroup(resourceGroup);

            if (openPorts is not null)
            {
                var i = 0;
                foreach (var port in openPorts)
                {
                    icreate = icreate
                        .DefineRule($"ALLOW-{port}")
                        .AllowInbound()
                        .FromAnyAddress()
                        .FromAnyPort()
                        .ToAnyAddress()
                        .ToPort(port)
                        .WithAnyProtocol()
                        .WithPriority(1000 + i)
                        .Attach();
                    i++;
                }
            }

            return icreate.CreateAsync(cts.Token);
        }

        private Task<IPrivateDnsZone> CreatePrivateDnsZoneAsync(INetwork virtualNetwork, string name, string title)
            => Execute(
                $"Creating private DNS Zone for {title}...",
                async () =>
                {
                    // Note: for a potential future implementation of this method without Fluent,
                    // please see commit cbffa28 in #392
                    var dnsZone = await azureSubscriptionClient.PrivateDnsZones
                        .Define(name)
                        .WithExistingResourceGroup(configuration.ResourceGroupName)
                        .DefineVirtualNetworkLink($"{virtualNetwork.Name}-link")
                        .WithReferencedVirtualNetworkId(virtualNetwork.Id)
                        .DisableAutoRegistration()
                        .Attach()
                        .CreateAsync(cts.Token);
                    return dnsZone;
                });

        private async Task SetStorageKeySecret(string vaultUrl, string secretName, string secretValue)
        {
            var client = new SecretClient(new(vaultUrl), new DefaultAzureCredential());
            await client.SetSecretAsync(secretName, secretValue, cts.Token);
        }

        private Task<Vault> GetKeyVaultAsync(string vaultName)
        {
            var keyVaultManagementClient = new KeyVaultManagementClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId };
            return keyVaultManagementClient.Vaults.GetAsync(configuration.ResourceGroupName, vaultName, cts.Token);
        }

        private Task<Vault> CreateKeyVaultAsync(string vaultName, IIdentity managedIdentity, ISubnet subnet)
            => Execute(
                $"Creating Key Vault: {vaultName}...",
                async () =>
                {
                    var tenantId = managedIdentity.TenantId;
                    var secrets = new List<string>
                    {
                        "get",
                        "list",
                        "set",
                        "delete",
                        "backup",
                        "restore",
                        "recover",
                        "purge"
                    };

                    var keyVaultManagementClient = new KeyVaultManagementClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId };
                    var properties = new VaultCreateOrUpdateParameters()
                    {
                        Location = configuration.RegionName,
                        Properties = new()
                        {
                            TenantId = new(tenantId),
                            Sku = new(SkuName.Standard),
                            NetworkAcls = new()
                            {
                                DefaultAction = configuration.PrivateNetworking.GetValueOrDefault() ? "Deny" : "Allow"
                            },
                            AccessPolicies = new List<AccessPolicyEntry>()
                            {
                                new()
                                {
                                    TenantId = new(tenantId),
                                    ObjectId = await GetUserObjectId(),
                                    Permissions = new()
                                    {
                                        Secrets = secrets
                                    }
                                },
                                new()
                                {
                                    TenantId = new(tenantId),
                                    ObjectId = managedIdentity.PrincipalId,
                                    Permissions = new()
                                    {
                                        Secrets = secrets
                                    }
                                }
                            }
                        }
                    };

                    var vault = await keyVaultManagementClient.Vaults.CreateOrUpdateAsync(configuration.ResourceGroupName, vaultName, properties, cts.Token);

                    if (configuration.PrivateNetworking.GetValueOrDefault())
                    {
                        var connection = new NetworkPrivateLinkServiceConnection
                        {
                            Name = "pe-coa-keyvault",
                            PrivateLinkServiceId = new(vault.Id)
                        };
                        connection.GroupIds.Add("vault");

                        var endpointData = new PrivateEndpointData
                        {
                            CustomNetworkInterfaceName = "pe-coa-keyvault",
                            ExtendedLocation = new() { Name = configuration.RegionName },
                            Subnet = new() { Id = new(subnet.Inner.Id), Name = subnet.Name }
                        };
                        endpointData.PrivateLinkServiceConnections.Add(connection);

                        var privateEndpoint = (await armClient
                                .GetResourceGroupResource(new ResourceIdentifier(subnet.Parent.Inner.Id).Parent)
                                .GetPrivateEndpoints()
                                .CreateOrUpdateAsync(Azure.WaitUntil.Completed, "pe-keyvault", endpointData, cts.Token))
                            .Value.Data;

                        var networkInterface = privateEndpoint.NetworkInterfaces[0];

                        var dnsZone = await CreatePrivateDnsZoneAsync(subnet.Parent, "privatelink.vaultcore.azure.net", "KeyVault");
                        await dnsZone
                            .Update()
                            .DefineARecordSet(vault.Name)
                            .WithIPv4Address(networkInterface.IPConfigurations.First().PrivateIPAddress)
                            .Attach()
                            .ApplyAsync(cts.Token);
                    }

                    return vault;

                    async ValueTask<string> GetUserObjectId()
                    {
                        const string graphUri = "https://graph.windows.net//.default";
                        var credentials = new AzureCredentials(default, new TokenCredentials(new RefreshableAzureServiceTokenProvider(graphUri)), tenantId, AzureEnvironment.AzureGlobalCloud);
                        using GraphRbacManagementClient rbacClient = new(Configure().WithEnvironment(AzureEnvironment.AzureGlobalCloud).WithCredentials(credentials).WithBaseUri(graphUri).Build()) { TenantID = tenantId };
                        credentials.InitializeServiceClient(rbacClient);
                        return (await rbacClient.SignedInUser.GetAsync(cts.Token)).ObjectId;
                    }
                });

        private Task<IGenericResource> CreateLogAnalyticsWorkspaceResourceAsync(string workspaceName)
            => Execute(
                $"Creating Log Analytics Workspace: {workspaceName}...",
                () => ResourceManager
                    .Configure()
                    .Authenticate(azureCredentials)
                    .WithSubscription(configuration.SubscriptionId)
                    .GenericResources.Define(workspaceName)
                    .WithRegion(configuration.RegionName)
                    .WithExistingResourceGroup(configuration.ResourceGroupName)
                    .WithResourceType("workspaces")
                    .WithProviderNamespace("Microsoft.OperationalInsights")
                    .WithoutPlan()
                    .WithApiVersion("2020-08-01")
                    .WithParentResource(string.Empty)
                    .CreateAsync(cts.Token));

        private Task<IGenericResource> CreateAppInsightsResourceAsync(string logAnalyticsArmId)
            => Execute(
                $"Creating Application Insights: {configuration.ApplicationInsightsAccountName}...",
                () => ResourceManager
                    .Configure()
                    .Authenticate(azureCredentials)
                    .WithSubscription(configuration.SubscriptionId)
                    .GenericResources.Define(configuration.ApplicationInsightsAccountName)
                    .WithRegion(configuration.RegionName)
                    .WithExistingResourceGroup(configuration.ResourceGroupName)
                    .WithResourceType("components")
                    .WithProviderNamespace("microsoft.insights")
                    .WithoutPlan()
                    .WithApiVersion("2020-02-02")
                    .WithParentResource(string.Empty)
                    .WithProperties(new Dictionary<string, string>() {
                        { "Application_Type", "other" } ,
                        { "WorkspaceResourceId", logAnalyticsArmId }
                    })
                    .CreateAsync(cts.Token));

        private Task<BatchAccount> CreateBatchAccountAsync(string storageAccountId)
            => Execute(
                $"Creating Batch Account: {configuration.BatchAccountName}...",
                () => new BatchManagementClient(tokenCredentials) { SubscriptionId = configuration.SubscriptionId }
                    .BatchAccount
                    .CreateAsync(
                        configuration.ResourceGroupName,
                        configuration.BatchAccountName,
                        new(
                            configuration.RegionName,
                            autoStorage: configuration.PrivateNetworking.GetValueOrDefault() ? new() { StorageAccountId = storageAccountId } : null),
                        cts.Token));

        private Task<IResourceGroup> CreateResourceGroupAsync()
        {
            var tags = !string.IsNullOrWhiteSpace(configuration.Tags) ? Utility.DelimitedTextToDictionary(configuration.Tags, "=", ",") : null;

            var resourceGroupDefinition = azureSubscriptionClient
                .ResourceGroups
                .Define(configuration.ResourceGroupName)
                .WithRegion(configuration.RegionName);

            resourceGroupDefinition = tags is not null ? resourceGroupDefinition.WithTags(tags) : resourceGroupDefinition;

            return Execute(
                $"Creating Resource Group: {configuration.ResourceGroupName}...",
                () => resourceGroupDefinition.CreateAsync(cts.Token));
        }

        private Task<IIdentity> CreateUserManagedIdentityAsync(IResourceGroup resourceGroup)
        {
            // Resource group name supports periods and parenthesis but identity doesn't. Replacing them with hyphens.
            var managedIdentityName = $"{resourceGroup.Name.Replace(".", "-").Replace("(", "-").Replace(")", "-")}-identity";

            return Execute(
                $"Obtaining user-managed identity: {managedIdentityName}...",
                async () => await azureSubscriptionClient.Identities.GetByResourceGroupAsync(configuration.ResourceGroupName, managedIdentityName)
                    ?? await azureSubscriptionClient.Identities.Define(managedIdentityName)
                        .WithRegion(configuration.RegionName)
                        .WithExistingResourceGroup(resourceGroup)
                        .CreateAsync(cts.Token));
        }

        private async Task DeleteResourceGroupAsync()
        {
            var startTime = DateTime.UtcNow;
            var line = ConsoleEx.WriteLine("Deleting resource group...");
            await azureSubscriptionClient.ResourceGroups.DeleteByNameAsync(configuration.ResourceGroupName, CancellationToken.None);
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

        private void ValidateRegionName(string regionName)
        {
            var validRegionNames = azureSubscriptionClient.GetCurrentSubscription().ListLocations().Select(loc => loc.Region.Name).Distinct();

            if (!validRegionNames.Contains(regionName, StringComparer.OrdinalIgnoreCase))
            {
                throw new ValidationException($"Invalid region name '{regionName}'. Valid names are: {string.Join(", ", validRegionNames)}");
            }
        }

        private async Task ValidateSubscriptionAndResourceGroupAsync(Configuration configuration)
        {
            const string ownerRoleId = "8e3af657-a8ff-443c-a75c-2fe8c4bcb635";
            const string contributorRoleId = "b24988ac-6180-42a0-ab88-20f7382dd24c";

            var azure = Microsoft.Azure.Management.Fluent.Azure
                .Configure()
                .WithLogLevel(HttpLoggingDelegatingHandler.Level.Basic)
                .Authenticate(azureCredentials);

            var subscriptionExists = await (await azure.Subscriptions.ListAsync(cancellationToken: cts.Token)).ToAsyncEnumerable()
                .AnyAsync(sub => sub.SubscriptionId.Equals(configuration.SubscriptionId, StringComparison.OrdinalIgnoreCase), cts.Token);

            if (!subscriptionExists)
            {
                throw new ValidationException($"Invalid or inaccessible subcription id '{configuration.SubscriptionId}'. Make sure that subscription exists and that you are either an Owner or have Contributor and User Access Administrator roles on the subscription.", displayExample: false);
            }

            var rgExists = !string.IsNullOrEmpty(configuration.ResourceGroupName) && await azureSubscriptionClient.ResourceGroups.ContainAsync(configuration.ResourceGroupName, cts.Token);

            if (!string.IsNullOrEmpty(configuration.ResourceGroupName) && !rgExists)
            {
                throw new ValidationException($"If ResourceGroupName is provided, the resource group must already exist.", displayExample: false);
            }

            var token = (await tokenProvider.GetAuthenticationHeaderAsync(cts.Token)).Parameter;
            var currentPrincipalObjectId = new JwtSecurityTokenHandler().ReadJwtToken(token).Claims.FirstOrDefault(c => c.Type == "oid").Value;

            var currentPrincipalSubscriptionRoleIds = (await azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeWithHttpMessagesAsync(
                    $"/subscriptions/{configuration.SubscriptionId}", new($"atScope() and assignedTo('{currentPrincipalObjectId}')"), cancellationToken: cts.Token)).Body
                .ToAsyncEnumerable(async (link, ct) => (await azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeNextWithHttpMessagesAsync(link, cancellationToken: ct)).Body)
                .Select(b => b.RoleDefinitionId.Split(new[] { '/' }).Last());

            if (!await currentPrincipalSubscriptionRoleIds.AnyAsync(role => ownerRoleId.Equals(role, StringComparison.OrdinalIgnoreCase) || contributorRoleId.Equals(role, StringComparison.OrdinalIgnoreCase), cts.Token))
            {
                if (!rgExists)
                {
                    throw new ValidationException($"Insufficient access to deploy. You must be: 1) Owner of the subscription, or 2) Contributor and User Access Administrator of the subscription, or 3) Owner of the resource group", displayExample: false);
                }

                var currentPrincipalRgRoleIds = (await azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeWithHttpMessagesAsync(
                        $"/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}", new($"atScope() and assignedTo('{currentPrincipalObjectId}')"), cancellationToken: cts.Token)).Body
                    .ToAsyncEnumerable(async (link, ct) => (await azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeNextWithHttpMessagesAsync(link, cancellationToken: ct)).Body)
                    .Select(b => b.RoleDefinitionId.Split(new[] { '/' }).Last());

                if (!await currentPrincipalRgRoleIds.AnyAsync(role => ownerRoleId.Equals(role, StringComparison.OrdinalIgnoreCase), cts.Token))
                {
                    throw new ValidationException($"Insufficient access to deploy. You must be: 1) Owner of the subscription, or 2) Contributor and User Access Administrator of the subscription, or 3) Owner of the resource group", displayExample: false);
                }
            }
        }

        private async Task<IStorageAccount> ValidateAndGetExistingStorageAccountAsync()
        {
            if (configuration.StorageAccountName is null)
            {
                return null;
            }

            return (await GetExistingStorageAccountAsync(configuration.StorageAccountName))
                ?? throw new ValidationException($"If StorageAccountName is provided, the storage account must already exist in region {configuration.RegionName}, and be accessible to the current user.", displayExample: false);
        }

        private async Task<BatchAccount> ValidateAndGetExistingBatchAccountAsync()
        {
            if (configuration.BatchAccountName is null)
            {
                return null;
            }

            return (await GetExistingBatchAccountAsync(configuration.BatchAccountName))
                ?? throw new ValidationException($"If BatchAccountName is provided, the batch account must already exist in region {configuration.RegionName}, and be accessible to the current user.", displayExample: false);
        }

        private async Task<(INetwork virtualNetwork, ISubnet vmSubnet, ISubnet postgreSqlSubnet, ISubnet batchSubnet)?> ValidateAndGetExistingVirtualNetworkAsync()
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

            if (!await (await azureSubscriptionClient.ResourceGroups.ListAsync(true, cts.Token)).ToAsyncEnumerable().AnyAsync(rg => rg.Name.Equals(configuration.VnetResourceGroupName, StringComparison.OrdinalIgnoreCase), cts.Token))
            {
                throw new ValidationException($"Resource group '{configuration.VnetResourceGroupName}' does not exist.");
            }

            var vnet = await azureSubscriptionClient.Networks.GetByResourceGroupAsync(configuration.VnetResourceGroupName, configuration.VnetName, cts.Token) ??
                throw new ValidationException($"Virtual network '{configuration.VnetName}' does not exist in resource group '{configuration.VnetResourceGroupName}'.");

            if (!vnet.RegionName.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase))
            {
                throw new ValidationException($"Virtual network '{configuration.VnetName}' must be in the same region that you are deploying to ({configuration.RegionName}).");
            }

            var vmSubnet = vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.VmSubnetName, StringComparison.OrdinalIgnoreCase)).Value ??
                throw new ValidationException($"Virtual network '{configuration.VnetName}' does not contain subnet '{configuration.VmSubnetName}'");

            var resourceGraphClient = new ResourceGraphClient(tokenCredentials);
            var postgreSqlSubnet = vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.PostgreSqlSubnetName, StringComparison.OrdinalIgnoreCase)).Value;

            if (postgreSqlSubnet is null)
            {
                throw new ValidationException($"Virtual network '{configuration.VnetName}' does not contain subnet '{configuration.PostgreSqlSubnetName}'");
            }

            var delegatedServices = postgreSqlSubnet.Inner.Delegations.Select(d => d.ServiceName);
            var hasOtherDelegations = delegatedServices.Any(s => s != "Microsoft.DBforPostgreSQL/flexibleServers");
            var hasNoDelegations = !delegatedServices.Any();

            if (hasOtherDelegations)
            {
                throw new ValidationException($"Subnet '{configuration.PostgreSqlSubnetName}' can have 'Microsoft.DBforPostgreSQL/flexibleServers' delegation only.");
            }

            var resourcesInPostgreSqlSubnetQuery = $"where type =~ 'Microsoft.Network/networkInterfaces' | where properties.ipConfigurations[0].properties.subnet.id == '{postgreSqlSubnet.Inner.Id}'";
            var resourcesExist = (await resourceGraphClient.ResourcesAsync(new(new[] { configuration.SubscriptionId }, resourcesInPostgreSqlSubnetQuery), cts.Token)).TotalRecords > 0;

            if (hasNoDelegations && resourcesExist)
            {
                throw new ValidationException($"Subnet '{configuration.PostgreSqlSubnetName}' must be either empty or have 'Microsoft.DBforPostgreSQL/flexibleServers' delegation.");
            }

            var batchSubnet = vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.BatchSubnetName, StringComparison.OrdinalIgnoreCase)).Value;

            return (vnet, vmSubnet, postgreSqlSubnet, batchSubnet);
        }

        private async Task ValidateBatchAccountQuotaAsync()
        {
            var batchManagementClient = new BatchManagementClient(tokenCredentials) { SubscriptionId = configuration.SubscriptionId };
            var accountQuota = (await batchManagementClient.Location.GetQuotasAsync(configuration.RegionName, cts.Token)).AccountQuota;
            var existingBatchAccountCount = await (await batchManagementClient.BatchAccount.ListAsync(cts.Token)).ToAsyncEnumerable(batchManagementClient.BatchAccount.ListNextAsync)
                .CountAsync(b => b.Location.Equals(configuration.RegionName), cts.Token);

            if (existingBatchAccountCount >= accountQuota)
            {
                throw new ValidationException($"The regional Batch account quota ({accountQuota} account(s) per region) for the specified subscription has been reached. Submit a support request to increase the quota or choose another region.", displayExample: false);
            }
        }

        private Task<string> UpdateVnetWithBatchSubnet(string resourceGroupId)
            => Execute(
                $"Creating batch subnet...",
                async () =>
                {
                    var coaRg = armClient.GetResourceGroupResource(new(resourceGroupId));

                    var vnetCollection = coaRg.GetVirtualNetworks();
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
            subnet.ServiceEndpoints.Add(new ServiceEndpointProperties()
            {
                Service = "Microsoft.Storage.Global",
            });

            subnet.ServiceEndpoints.Add(new ServiceEndpointProperties()
            {
                Service = "Microsoft.Sql",
            });

            subnet.ServiceEndpoints.Add(new ServiceEndpointProperties()
            {
                Service = "Microsoft.ContainerRegistry",
            });

            subnet.ServiceEndpoints.Add(new ServiceEndpointProperties()
            {
                Service = "Microsoft.KeyVault",
            });
        }

        private async Task ValidateVmAsync()
        {
            var computeSkus = await generalRetryPolicy.ExecuteAsync(async ct =>
                    await (await azureSubscriptionClient.ComputeSkus.ListbyRegionAndResourceTypeAsync(
                        Region.Create(configuration.RegionName),
                        ComputeResourceType.VirtualMachines,
                        ct))
                        .ToAsyncEnumerable()
                        .Where(s => !s.Restrictions.Any())
                        .Select(s => s.Name.Value)
                        .ToListAsync(ct),
                    cts.Token);

            if (!computeSkus.Any())
            {
                throw new ValidationException($"Your subscription doesn't support virtual machine creation in {configuration.RegionName}.  Please create an Azure Support case: https://docs.microsoft.com/en-us/azure/azure-portal/supportability/how-to-create-azure-support-request", displayExample: false);
            }
            else if (!computeSkus.Any(s => s.Equals(configuration.VmSize, StringComparison.OrdinalIgnoreCase)))
            {
                throw new ValidationException($"The VmSize {configuration.VmSize} is not available or does not exist in {configuration.RegionName}.  You can use 'az vm list-skus --location {configuration.RegionName} --output table' to find an available VM.", displayExample: false);
            }
        }

        private static async Task<BlobServiceClient> GetBlobClientAsync(IStorageAccount storageAccount, CancellationToken cancellationToken)
            => new(
                new($"https://{storageAccount.Name}.blob.core.windows.net"),
                new StorageSharedKeyCredential(
                    storageAccount.Name,
                    (await storageAccount.GetKeysAsync(cancellationToken))[0].Value));

        private async Task ValidateTokenProviderAsync()
        {
            try
            {
                _ = await Execute("Retrieving Azure management token...", async () => (await (new DefaultAzureCredential()).GetTokenAsync(new Azure.Core.TokenRequestContext(new string[] { "https://management.azure.com//.default" }))).Token);
            }
            catch (AuthenticationFailedException ex)
            {
                ConsoleEx.WriteLine("No access token found.  Please install the Azure CLI and login with 'az login'", ConsoleColor.Red);
                ConsoleEx.WriteLine("Link: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli");
                ConsoleEx.WriteLine($"Error details: {ex.Message}");
                Environment.Exit(1);
            }
        }

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

            //void ThrowIfBothProvided(bool feature1Enabled, string feature1Name, bool feature2Enabled, string feature2Name)
            //{
            //    if (feature1Enabled && feature2Enabled)
            //    {
            //        throw new ValidationException($"{feature2Name} is incompatible with {feature1Name}");
            //    }
            //}

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
                throw new Exception("Invalid configuration options BatchNodesSubnetId and BatchSubnetName are mutually exclusive.");
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
                await DeleteResourceGroupAsync();
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
                catch (Microsoft.Rest.Azure.CloudException cloudException) when (cloudException.ToCloudErrorType() == CloudErrorType.ExpiredAuthenticationToken)
                {
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

        public static async Task<string> DownloadTextFromStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName, CancellationToken cancellationToken)
        {
            var blobClient = await GetBlobClientAsync(storageAccount, cancellationToken);
            var container = blobClient.GetBlobContainerClient(containerName);

            return (await container.GetBlobClient(blobName).DownloadContentAsync(cancellationToken)).Value.Content.ToString();
        }

        public static async Task UploadTextToStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName, string content, CancellationToken token)
        {
            var blobClient = await GetBlobClientAsync(storageAccount, token);
            var container = blobClient.GetBlobContainerClient(containerName);

            await container.CreateIfNotExistsAsync(cancellationToken: token);
            await container.GetBlobClient(blobName).UploadAsync(BinaryData.FromString(content), true, token);
        }

        private class ValidationException : Exception
        {
            public string Reason { get; set; }
            public bool DisplayExample { get; set; }

            public ValidationException(string reason, bool displayExample = true)
            {
                Reason = reason;
                DisplayExample = displayExample;
            }
        }
    }
}
