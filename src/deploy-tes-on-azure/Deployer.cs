// Copyright (c) Microsoft Corporation.
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
using Azure.ResourceManager.Authorization;
using Azure.ResourceManager.Batch;
using Azure.ResourceManager.Compute;
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
using CommonUtilities;
using k8s;
using Microsoft.Graph;
using Microsoft.Rest;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;
using Tes.Models;
using ApplicationInsights = Azure.ResourceManager.ApplicationInsights.Models;
using Batch = Azure.ResourceManager.Batch.Models;
using Storage = Azure.ResourceManager.Storage.Models;

namespace TesDeployer
{
    public class Deployer
    {
        private static readonly AsyncRetryPolicy roleAssignmentHashConflictRetryPolicy = Policy
            .Handle<Microsoft.Rest.Azure.CloudException>(cloudException => cloudException.Body.Code.Equals("HashConflictOnDifferentRoleAssignmentIds"))
            .RetryAsync();

        private static readonly AsyncRetryPolicy generalRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(1));

        private static readonly TimeSpan longRetryWaitTime = TimeSpan.FromSeconds(15);

        private static readonly AsyncRetryPolicy longRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(60, retryAttempt => longRetryWaitTime,
            (exception, timespan) => ConsoleEx.WriteLine($"Retrying task creation in {timespan} due to {exception.GetType().FullName}: {exception.Message}"));

        /// <summary>
        /// Grants full access to manage all resources, but does not allow you to assign roles in Azure RBAC, manage assignments in Azure Blueprints, or share image galleries.
        /// </summary>
        private static ResourceIdentifier All_Role_Contributor = ResourceIdentifier.Parse("/providers/Microsoft.Authorization/roleDefinitions/b24988ac-6180-42a0-ab88-20f7382dd24c");

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
            { "Microsoft.Compute", new() { "EncryptionAtHost" } },
        };

        private Configuration configuration { get; set; }
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "<Pending>")]
        private TokenCredential tokenCredential { get; set; }
        private SubscriptionResource armSubscription { get; set; }
        private ArmClient armClient { get; set; }
        ResourceGroupResource resourceGroup = null;
        CloudEnvironment cloudEnvironment { get; set; }

        private IEnumerable<SubscriptionResource> subscriptionIds { get; set; }
        private bool isResourceGroupCreated { get; set; }
        private KubernetesManager kubernetesManager { get; set; }

        public Deployer(Configuration configuration)
            => this.configuration = configuration;

        private static async Task<T> FetchResourceDataAsync<T>(Func<CancellationToken, Task<Response<T>>> GetAsync, CancellationToken cancellationToken, Action<T> OnAcquisition = null) where T : ArmResource
        {
            ArgumentNullException.ThrowIfNull(GetAsync);

            var result = await GetAsync(cancellationToken);
            OnAcquisition?.Invoke(result);
            return result;
        }

        private BlobClient GetBlobClient(StorageAccountData storageAccount, string containerName, string blobName)
        {
            var key = armClient
                .GetStorageAccountResource(storageAccount.Id)
                .GetKeysAsync(cancellationToken: cts.Token)
                .FirstOrDefaultAsync(cts.Token)
                .AsTask().GetAwaiter().GetResult();
            return new(new BlobUriBuilder(storageAccount.PrimaryEndpoints.BlobUri) { BlobContainerName = containerName, BlobName = blobName }.ToUri(),
                new Azure.Storage.StorageSharedKeyCredential(storageAccount.Name, key.Value),
                new() { Audience = storageAccount.PrimaryEndpoints.BlobUri.AbsoluteUri });
        }

        public async Task<int> DeployAsync()
        {
            cloudEnvironment = CloudEnvironment.GetCloud("AzurePublicCloud");
            var mainTimer = Stopwatch.StartNew();

            try
            {
                ValidateInitialCommandLineArgs();

                ConsoleEx.WriteLine("Running...");

                await ValidateTokenProviderAsync();

                await Execute("Connecting to Azure Services...", async () =>
                {
                    tokenCredential = new AzureCliCredential(new() { AuthorityHost = cloudEnvironment.AzureAuthorityHost });
                    armClient = new ArmClient(tokenCredential, configuration.SubscriptionId, new() { Environment = cloudEnvironment.ArmEnvironment });
                    armSubscription = armClient.GetSubscriptionResource(new($"/subscriptions/{configuration.SubscriptionId}"));
                    subscriptionIds = await armClient.GetSubscriptions().GetAllAsync(cts.Token).ToListAsync(cts.Token);
                });

                await ValidateSubscriptionAndResourceGroupAsync(configuration);
                kubernetesManager = new(configuration, armClient, GetBlobClient, cts.Token);
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
                    configuration.RegionName = resourceGroup.Id.Location ?? (resourceGroup.HasData
                        ? resourceGroup.Data.Location.Name
                        : (await FetchResourceDataAsync(resourceGroup.GetAsync, cts.Token, resource => resourceGroup = resource)).Data.Location.Name);

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

                    ContainerServiceManagedClusterResource existingAksCluster = default;

                    if (string.IsNullOrWhiteSpace(configuration.AksClusterName))
                    {
                        var aksClusters = await resourceGroup.GetContainerServiceManagedClusters().ToListAsync(cts.Token);

                        existingAksCluster = aksClusters.Count switch
                        {
                            0 => throw new ValidationException($"Update was requested but resource group {configuration.ResourceGroupName} does not contain any AKS clusters.", displayExample: false),
                            1 => aksClusters.Single(),
                            _ => throw new ValidationException($"Resource group {configuration.ResourceGroupName} contains multiple AKS clusters. {nameof(configuration.AksClusterName)} must be provided.", displayExample: false),
                        };

                        configuration.AksClusterName = existingAksCluster.Id.Name;
                    }
                    else
                    {
                        existingAksCluster = (await GetExistingAKSClusterAsync(configuration.AksClusterName))
                            ?? throw new ValidationException($"AKS cluster {configuration.AksClusterName} does not exist in region {configuration.RegionName} or is not accessible to the current user.", displayExample: false);
                    }

                    var aksValues = await kubernetesManager.GetAKSSettingsAsync(storageAccountData);

                    if (!aksValues.Any())
                    {
                        throw new ValidationException($"Could not retrieve account names from stored configuration in {storageAccount.Id.Name}.", displayExample: false);
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
                        var keyVault = await GetKeyVaultAsync(keyVaultName);
                        keyVaultUri = (keyVault.HasData ? keyVault : await FetchResourceDataAsync(keyVault.GetAsync, cts.Token)).Data.Properties.VaultUri;
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

                    //if (installedVersion is null || installedVersion < new Version(5, 2, 2))
                    //{
                    //}

                    if (waitForRoleAssignmentPropagation)
                    {
                        await Execute("Waiting 5 minutes for role assignment propagation...",
                            () => Task.Delay(TimeSpan.FromMinutes(5), cts.Token));
                    }

                    await kubernetesManager.UpgradeValuesYamlAsync(storageAccountData, settings);
                    await PerformHelmDeploymentAsync(existingAksCluster);
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
                        keyVault = await ValidateAndGetExistingKeyVaultAsync();

                        if (aksCluster is null && !configuration.ManualHelmDeployment)
                        {
                            await ValidateVmAsync();
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

                    managedIdentity = await CreateUserManagedIdentityAsync();
                    managedIdentity = managedIdentity.HasData ? managedIdentity : await FetchResourceDataAsync(managedIdentity.GetAsync, cts.Token);

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

                                if (string.IsNullOrEmpty(this.configuration.BatchNodesSubnetId))
                                {
                                    this.configuration.BatchNodesSubnetId = vnetAndSubnet.Value.batchSubnet.Id;
                                }
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
                            storageAccount ??= await CreateStorageAccountAsync();
                            await CreateDefaultStorageContainersAsync(storageAccount);
                            storageAccountData = (await FetchResourceDataAsync(ct => storageAccount.GetAsync(cancellationToken: ct), cts.Token, account => storageAccount = account)).Data;
                            await WritePersonalizedFilesToStorageAccountAsync(storageAccountData);
                            await AssignVmAsContributorToStorageAccountAsync(managedIdentity, storageAccount);
                            await AssignVmAsDataOwnerToStorageAccountAsync(managedIdentity, storageAccount);
                            await AssignManagedIdOperatorToResourceAsync(managedIdentity, resourceGroup);
                            await AssignMIAsNetworkContributorToResourceAsync(managedIdentity, resourceGroup);
                        }),
                    ]);

                    if (configuration.CrossSubscriptionAKSDeployment.GetValueOrDefault())
                    {
                        await Task.Run(async () =>
                        {
                            keyVault ??= await CreateKeyVaultAsync(configuration.KeyVaultName, managedIdentity, vnetAndSubnet.Value.virtualNetwork, vnetAndSubnet.Value.vmSubnet);
                            keyVaultUri = (keyVault.HasData ? keyVault : await FetchResourceDataAsync(keyVault.GetAsync, cts.Token)).Data.Properties.VaultUri;
                            var key = await storageAccount.GetKeysAsync().FirstOrDefaultAsync(cts.Token);
                            await SetStorageKeySecret(keyVaultUri, StorageAccountKeySecretName, key.Value);
                        });
                    }

                    if (postgreSqlFlexServer is null)
                    {
                        postgreSqlDnsZone = await CreatePrivateDnsZoneAsync(vnetAndSubnet.Value.virtualNetwork, $"privatelink.postgres.database.azure.com", "PostgreSQL Server");
                    }

                    await Task.WhenAll(
                    [
                        Task.Run(async () =>
                        {
                            if (aksCluster is null && !configuration.ManualHelmDeployment)
                            {
                                aksCluster = await ProvisionManagedClusterAsync(managedIdentity, logAnalyticsWorkspace, vnetAndSubnet?.vmSubnet.Id, configuration.PrivateNetworking.GetValueOrDefault());
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
                            postgreSqlFlexServer ??= await CreatePostgreSqlServerAndDatabaseAsync(vnetAndSubnet.Value.postgreSqlSubnet, postgreSqlDnsZone);
                        })
                    ]);

                    var clientId = managedIdentity.Data.ClientId;
                    var settings = ConfigureSettings(clientId?.ToString("D"));

                    await kubernetesManager.UpdateHelmValuesAsync(storageAccountData, keyVaultUri, resourceGroup.Id.Name, settings, managedIdentity.Data);
                    await PerformHelmDeploymentAsync(aksCluster,
                        [
                            "Run the following postgresql command to setup the database.",
                            $"\tPostgreSQL command: psql postgresql://{configuration.PostgreSqlAdministratorLogin}:{configuration.PostgreSqlAdministratorPassword}@{configuration.PostgreSqlServerName}.postgres.database.azure.com/{configuration.PostgreSqlTesDatabaseName} -c \"{GetCreateTesUserString()}\""
                        ],
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

                var batchAccountData = (batchAccount.HasData ? batchAccount : await FetchResourceDataAsync(batchAccount.GetAsync, cts.Token)).Data;
                var maxPerFamilyQuota = batchAccountData.IsDedicatedCoreQuotaPerVmFamilyEnforced ?? false ? batchAccountData.DedicatedCoreQuotaPerVmFamily.Select(q => q.CoreQuota ?? 0).Where(q => 0 != q) : Enumerable.Repeat(batchAccountData.DedicatedCoreQuota ?? 0, 1);
                var isBatchQuotaAvailable = batchAccountData.LowPriorityCoreQuota > 0 || (batchAccountData.DedicatedCoreQuota > 0 && maxPerFamilyQuota.Append(0).Max() > 0);
                var isBatchPoolQuotaAvailable = batchAccountData.PoolQuota > 0;
                var isBatchJobQuotaAvailable = batchAccountData.ActiveJobAndJobScheduleQuota > 0;
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
                            var runTestTask = RunTestTask("localhost:8088", batchAccountData.LowPriorityCoreQuota > 0, configuration.TesUsername, configuration.TesPassword);

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

        private async Task<int> TestTaskAsync(string tesEndpoint, bool preemptible, string tesUsername, string tesPassword)
        {
            using HttpClient client = new();

            TesTask task = new()
            {
                Inputs = [],
                Outputs = [],
                Executors =
                [
                    new()
                    {
                        Image = "ubuntu:22.04",
                        Command = ["echo", "hello world"],
                    }
                ],
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

                await Task.Delay(TimeSpan.FromSeconds(10), cts.Token);
            }
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
            return await subscriptionIds.ToAsyncEnumerable().Select(s =>
            {
                try
                {
                    return s.GetContainerServiceManagedClustersAsync(cts.Token);
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
                    a.Id.Name.Equals(aksClusterName, StringComparison.OrdinalIgnoreCase) &&
                    a.Data.Location.Name.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase),
                cts.Token);
        }

        private async Task<ContainerServiceManagedClusterResource> ProvisionManagedClusterAsync(UserAssignedIdentityResource managedIdentity, OperationalInsightsWorkspaceResource logAnalyticsWorkspace, ResourceIdentifier subnetId, bool privateNetworking)
        {
            if (!managedIdentity.HasData)
            {
                throw new ArgumentException("The resource must have been retrieved from the cloud.", nameof(managedIdentity));
            }

            var nodePoolName = "nodepool1";
            ContainerServiceManagedClusterData cluster = new(new(configuration.RegionName))
            {
                DnsPrefix = configuration.AksClusterName,
                Identity = Azure.ResourceManager.Models.ResourceManagerModelFactory.ManagedServiceIdentity(
                    managedServiceIdentityType: Azure.ResourceManager.Models.ManagedServiceIdentityType.UserAssigned,
                    userAssignedIdentities: new Dictionary<ResourceIdentifier, Azure.ResourceManager.Models.UserAssignedIdentity>(
                        [new(managedIdentity.Id, new())])),
                NetworkProfile = new()
                {
                    NetworkPlugin = ContainerServiceNetworkPlugin.Azure,
                    ServiceCidr = configuration.KubernetesServiceCidr,
                    DnsServiceIP = configuration.KubernetesDnsServiceIP,
                    DockerBridgeCidr = configuration.KubernetesDockerBridgeCidr,
                    NetworkPolicy = ContainerServiceNetworkPolicy.Azure
                }
            };

            ManagedClusterAddonProfile clusterAddonProfile = new(true);
            clusterAddonProfile.Config.Add("logAnalyticsWorkspaceResourceID", logAnalyticsWorkspace.Id);
            cluster.AddonProfiles.Add("omsagent", clusterAddonProfile);

            if (!string.IsNullOrWhiteSpace(configuration.AadGroupIds))
            {
                cluster.EnableRbac = true;
                cluster.AadProfile = new()
                {
                    IsAzureRbacEnabled = false,
                    IsManagedAadEnabled = true
                };

                foreach (var id in configuration.AadGroupIds.Split(",", StringSplitOptions.RemoveEmptyEntries).Select(Guid.Parse))
                {
                    cluster.AadProfile.AdminGroupObjectIds.Add(id);
                }
            }

            cluster.IdentityProfile.Add("kubeletidentity", new() { ResourceId = managedIdentity.Id, ClientId = managedIdentity.Data.ClientId, ObjectId = managedIdentity.Data.PrincipalId });

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
                    EnablePrivateClusterPublicFqdn = true
                };
            }

            return await Execute(
                $"Creating AKS Cluster: {configuration.AksClusterName}...",
                async () => (await resourceGroup.GetContainerServiceManagedClusters().CreateOrUpdateAsync(WaitUntil.Completed, configuration.AksClusterName, cluster, cts.Token)).Value);
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
                                rp.RegisterAsync())
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

            async ValueTask<List<ResourceProviderResource>> GetRequiredResourceProvidersNotRegisteredAsync()
            {
                var cloudResourceProviders = armSubscription.GetResourceProviders().GetAllAsync(cancellationToken: cts.Token);

                var notRegisteredResourceProviders = await cloudResourceProviders
                    .SelectAwaitWithCancellation(async (rp, ct) => await FetchResourceDataAsync(token => rp.GetAsync(cancellationToken: token), ct))
                    .Where(rp => requiredResourceProviders.Contains(rp.Data.Namespace, StringComparer.OrdinalIgnoreCase))
                    .Where(rp => !rp.Data.RegistrationState.Equals("Registered", StringComparison.OrdinalIgnoreCase))
                    .ToListAsync(cts.Token);

                return notRegisteredResourceProviders;
            }
        }

        private async Task RegisterResourceProviderFeaturesAsync()
        {
            var unregisteredFeatures = new List<FeatureResource>();
            try
            {
                foreach (var rpName in requiredResourceProviderFeatures.Keys)
                {
                    var rp = await armSubscription.GetResourceProviderAsync(rpName, cancellationToken: cts.Token);

                    foreach (var featureName in requiredResourceProviderFeatures[rpName])
                    {
                        var feature = await rp.Value.GetFeatureAsync(featureName, cts.Token);

                        if (!string.Equals(feature.Value.Data.FeatureState, "Registered", StringComparison.OrdinalIgnoreCase))
                        {
                            unregisteredFeatures.Add(feature.Value);
                        }
                    }
                }

                if (unregisteredFeatures.Count == 0)
                {
                    return;
                }

                await Execute(
                    $"Registering resource provider features...",
                async () =>
                {
                    foreach (var feature in unregisteredFeatures)
                    {
                        _ = await feature.RegisterAsync(cts.Token);
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

        private async Task<bool> TryAssignMIAsNetworkContributorToResourceAsync(UserAssignedIdentityResource managedIdentity, ArmResource resource)
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

        private Task AssignMIAsNetworkContributorToResourceAsync(UserAssignedIdentityResource managedIdentity, ArmResource resource, bool cancelOnException = true)
        {
            // https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#network-contributor
            ResourceIdentifier roleDefinitionId = new($"/subscriptions/{configuration.SubscriptionId}/providers/Microsoft.Authorization/roleDefinitions/4d97b98b-1d4f-4787-a291-c67834d212e7");
            return Execute(
                $"Assigning Network Contributor role for the managed id to resource group scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => resource.GetRoleAssignments().CreateOrUpdateAsync(WaitUntil.Completed, Guid.NewGuid().ToString(),
                        new(roleDefinitionId, managedIdentity.Data.PrincipalId.Value)
                        {
                            PrincipalType = Azure.ResourceManager.Authorization.Models.RoleManagementPrincipalType.ServicePrincipal
                        }, ct),
                    cts.Token),
                cancelOnException: cancelOnException);
        }

        private Task AssignManagedIdOperatorToResourceAsync(UserAssignedIdentityResource managedIdentity, ArmResource resource)
        {
            // https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#managed-identity-operator
            ResourceIdentifier roleDefinitionId = new($"/subscriptions/{configuration.SubscriptionId}/providers/Microsoft.Authorization/roleDefinitions/f1a07417-d97a-45cb-824c-7a7467783830");
            return Execute(
                $"Assigning Managed ID Operator role for the managed id to resource group scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => resource.GetRoleAssignments().CreateOrUpdateAsync(WaitUntil.Completed, Guid.NewGuid().ToString(),
                        new(roleDefinitionId, managedIdentity.Data.PrincipalId.Value)
                        {
                            PrincipalType = Azure.ResourceManager.Authorization.Models.RoleManagementPrincipalType.ServicePrincipal
                        }, ct), cts.Token));
        }

        private async Task<bool> TryAssignVmAsDataOwnerToStorageAccountAsync(UserAssignedIdentityResource managedIdentity, StorageAccountResource storageAccount)
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

        private Task AssignVmAsDataOwnerToStorageAccountAsync(UserAssignedIdentityResource managedIdentity, StorageAccountResource storageAccount, bool cancelOnException = true)
        {
            //https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#storage-blob-data-owner
            ResourceIdentifier roleDefinitionId = new($"/subscriptions/{configuration.SubscriptionId}/providers/Microsoft.Authorization/roleDefinitions/b7e6dc6d-f1e8-4753-8033-0f276bb0955b");

            return Execute(
                $"Assigning Storage Blob Data Owner role for user-managed identity to Storage Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => storageAccount.GetRoleAssignments().CreateOrUpdateAsync(WaitUntil.Completed, Guid.NewGuid().ToString(),
                        new(roleDefinitionId, managedIdentity.Data.PrincipalId.Value)
                        {
                            PrincipalType = Azure.ResourceManager.Authorization.Models.RoleManagementPrincipalType.ServicePrincipal
                        }, ct),
                    cts.Token),
                cancelOnException: cancelOnException);
        }

        private Task AssignVmAsContributorToStorageAccountAsync(UserAssignedIdentityResource managedIdentity, StorageAccountResource storageAccount)
            => Execute(
                $"Assigning 'Contributor' role for user-managed identity to Storage Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => storageAccount.GetRoleAssignments().CreateOrUpdateAsync(WaitUntil.Completed, Guid.NewGuid().ToString(),
                        new(All_Role_Contributor, managedIdentity.Data.PrincipalId.Value)
                        {
                            PrincipalType = Azure.ResourceManager.Authorization.Models.RoleManagementPrincipalType.ServicePrincipal
                        }, ct), cts.Token));

        private Task<StorageAccountResource> CreateStorageAccountAsync()
            => Execute(
                $"Creating Storage Account: {configuration.StorageAccountName}...",
                async () => (await resourceGroup.GetStorageAccounts().CreateOrUpdateAsync(WaitUntil.Completed, configuration.StorageAccountName,
                    new(new(Storage.StorageSkuName.StandardLrs), Storage.StorageKind.StorageV2, new(configuration.RegionName))
                    { EnableHttpsTrafficOnly = true }, cts.Token)).Value);

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
                //.SelectAwaitWithCancellation(async (name, ct) =>
                //{
                //    return await containerCollection.ExistsAsync(name, ct)
                //        ? Task.CompletedTask
                //        : containerCollection.CreateOrUpdateAsync(WaitUntil.Completed, name, new(), ct);
                //})
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
            => Execute(
                $"Assigning 'Contributor' role for user-managed identity to Batch Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => batchAccount.GetRoleAssignments().CreateOrUpdateAsync(WaitUntil.Completed, Guid.NewGuid().ToString(),
                        new(All_Role_Contributor, managedIdentity.Data.PrincipalId.Value)
                        {
                            PrincipalType = Azure.ResourceManager.Authorization.Models.RoleManagementPrincipalType.ServicePrincipal
                        }, ct), cts.Token));

        private async Task<PostgreSqlFlexibleServerResource> CreatePostgreSqlServerAndDatabaseAsync(SubnetResource subnet, PrivateDnsZoneResource postgreSqlDnsZone)
        {
            subnet = subnet.HasData ? subnet : await FetchResourceDataAsync(t => subnet.GetAsync(cancellationToken: t), cts.Token);

            if (!subnet.Data.Delegations.Any())
            {
                subnet.Data.Delegations.Add(new() { ServiceName = "Microsoft.DBforPostgreSQL/flexibleServers" });
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
                async () => (await resourceGroup.GetPostgreSqlFlexibleServers().CreateOrUpdateAsync(WaitUntil.Completed, configuration.PostgreSqlServerName, data, cts.Token)).Value);

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
                $"Executing scripts on postgresql...",
                async () =>
                {
                    var tesScript = GetCreateTesUserString();
                    var serverPath = $"{configuration.PostgreSqlServerName}.postgres.database.azure.com";
                    var adminUser = configuration.PostgreSqlAdministratorLogin;

                    List<string[]> commands = [
                        ["apt", "-qq", "update"],
                        ["apt", "-qq", "install", "-y", "postgresql-client"],
                        ["bash", "-lic", $"echo {configuration.PostgreSqlServerName}{configuration.PostgreSqlServerNameSuffix}:{configuration.PostgreSqlServerPort}:{configuration.PostgreSqlTesDatabaseName}:{adminUser}:{configuration.PostgreSqlAdministratorPassword} >> ~/.pgpass"],
                        ["bash", "-lic", "chmod 0600 ~/.pgpass"],
                        ["/usr/bin/psql", "-h", serverPath, "-U", adminUser, "-d", configuration.PostgreSqlTesDatabaseName, "-c", tesScript]
                    ];

                    await kubernetesManager.ExecuteCommandsOnPodAsync(kubernetesClient, podName, commands, aksNamespace);
                });

        private Task AssignVmAsContributorToAppInsightsAsync(UserAssignedIdentityResource managedIdentity, ArmResource appInsights)
            => Execute(
                $"Assigning 'Contributor' role for user-managed identity to App Insights resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    ct => appInsights.GetRoleAssignments().CreateOrUpdateAsync(WaitUntil.Completed, Guid.NewGuid().ToString(),
                        new(All_Role_Contributor, managedIdentity.Data.PrincipalId.Value)
                        {
                            PrincipalType = Azure.ResourceManager.Authorization.Models.RoleManagementPrincipalType.ServicePrincipal
                        }, ct), cts.Token));

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

                    var defaultNsg = await CreateNetworkSecurityGroupAsync($"{configuration.VnetName}-default-nsg");
                    var aksNsg = await CreateNetworkSecurityGroupAsync($"{configuration.VnetName}-aks-nsg", tesPorts);

                    VirtualNetworkData vnetDefinition = new() { Location = new(configuration.RegionName) };
                    vnetDefinition.AddressPrefixes.Add(configuration.VnetAddressSpace);

                    vnetDefinition.Subnets.Add(new()
                    {
                        Name = configuration.VmSubnetName,
                        AddressPrefix = configuration.VmSubnetAddressSpace,
                        NetworkSecurityGroup = aksNsg.HasData ? aksNsg.Data : (await FetchResourceDataAsync(ct => aksNsg.GetAsync(cancellationToken: ct), cts.Token)).Data,
                    });

                    SubnetData postgreSqlSubnet =
                    new()
                    {
                        Name = configuration.PostgreSqlSubnetName,
                        AddressPrefix = configuration.PostgreSqlSubnetAddressSpace,
                        NetworkSecurityGroup = defaultNsg.HasData ? defaultNsg.Data : (await FetchResourceDataAsync(ct => defaultNsg.GetAsync(cancellationToken: ct), cts.Token)).Data,
                    };
                    postgreSqlSubnet.Delegations.Add(NewServiceDelegation("Microsoft.DBforPostgreSQL/flexibleServers"));
                    vnetDefinition.Subnets.Add(postgreSqlSubnet);

                    SubnetData batchSubnet = new()
                    {
                        Name = configuration.BatchSubnetName,
                        AddressPrefix = configuration.BatchNodesSubnetAddressSpace,
                        NetworkSecurityGroup = defaultNsg.HasData ? defaultNsg.Data : (await FetchResourceDataAsync(ct => defaultNsg.GetAsync(cancellationToken: ct), cts.Token)).Data,
                    };
                    AddServiceEndpointsToSubnet(batchSubnet);
                    vnetDefinition.Subnets.Add(batchSubnet);

                    var vnet = (await resourceGroup.GetVirtualNetworks().CreateOrUpdateAsync(WaitUntil.Completed, configuration.VnetName, vnetDefinition, cts.Token)).Value;
                    var subnets = await vnet.GetSubnets().ToListAsync(cts.Token);

                    return (vnet,
                        subnets.FirstOrDefault(s => s.Id.Name.Equals(configuration.VmSubnetName, StringComparison.OrdinalIgnoreCase)),
                        subnets.FirstOrDefault(s => s.Id.Name.Equals(configuration.PostgreSqlSubnetName, StringComparison.OrdinalIgnoreCase)),
                        subnets.FirstOrDefault(s => s.Id.Name.Equals(configuration.BatchSubnetName, StringComparison.OrdinalIgnoreCase)));

                    static ServiceDelegation NewServiceDelegation(string serviceDelegation) =>
                        new() { Name = serviceDelegation, ServiceName = serviceDelegation };
                });

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
            var client = new SecretClient(vaultUrl, tokenCredential);
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

                    Azure.ResourceManager.KeyVault.Models.KeyVaultProperties properties = new(tenantId.Value, new(KeyVaultSkuFamily.A, KeyVaultSkuName.Standard))
                    {
                        NetworkRuleSet = new()
                        {
                            DefaultAction = configuration.PrivateNetworking.GetValueOrDefault() ? KeyVaultNetworkRuleAction.Deny : KeyVaultNetworkRuleAction.Allow
                        },
                    };

                    properties.AccessPolicies.Add(new(tenantId.Value, await GetUserObjectId(), permissions));
                    properties.AccessPolicies.Add(new(tenantId.Value, managedIdentity.Data.PrincipalId.Value.ToString("D"), permissions));

                    var vault = (await resourceGroup.GetKeyVaults().CreateOrUpdateAsync(WaitUntil.Completed, vaultName, new(new(configuration.RegionName), properties), cts.Token)).Value;

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

                    async ValueTask<string> GetUserObjectId()
                    {
                        string baseUrl;
                        {
                            // TODO: convert cloud to appropriate field. Note that there are two different values for USGovernment.
                            using var client = GraphClientFactory.Create(nationalCloud: GraphClientFactory.Global_Cloud);
                            baseUrl = client.BaseAddress.AbsoluteUri;
                        }

                        {
                            using var client = new GraphServiceClient(tokenCredential, baseUrl: baseUrl);
                            return (await client.Me.GetAsync(cancellationToken: cts.Token)).Id;
                        }
                    }
                });

        private Task<OperationalInsightsWorkspaceResource> CreateLogAnalyticsWorkspaceResourceAsync(string workspaceName)
            => Execute(
                $"Creating Log Analytics Workspace: {workspaceName}...",
                async () =>
                {
                    OperationalInsightsWorkspaceData data = new(new(configuration.RegionName));

                    return (await resourceGroup.GetOperationalInsightsWorkspaces()
                        .CreateOrUpdateAsync(WaitUntil.Completed, workspaceName, data, cts.Token)).Value;
                });

        private Task<ApplicationInsightsComponentResource> CreateAppInsightsResourceAsync(string logAnalyticsArmId)
            => Execute(
                $"Creating Application Insights: {configuration.ApplicationInsightsAccountName}...",
                async () =>
                {
                    ApplicationInsightsComponentData data = new(new(configuration.RegionName), "other")
                    {
                        FlowType = ApplicationInsights.FlowType.Bluefield,
                        RequestSource = ApplicationInsights.RequestSource.Rest,
                        ApplicationType = ApplicationInsights.ApplicationType.Other,
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

            foreach (var tag in tags ?? [])
            {
                data.Tags.Add(tag);
            }

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
            const string ownerRoleId = "8e3af657-a8ff-443c-a75c-2fe8c4bcb635";
            const string contributorRoleId = "b24988ac-6180-42a0-ab88-20f7382dd24c";

            bool rgExists;

            try
            {
                rgExists = !string.IsNullOrEmpty(configuration.ResourceGroupName) && (await armSubscription.GetResourceGroups().ExistsAsync(configuration.ResourceGroupName, cts.Token)).Value;
            }
            catch (Exception)
            {
                throw new ValidationException($"Invalid or inaccessible subcription id '{configuration.SubscriptionId}'. Make sure that subscription exists and that you are either an Owner or have Contributor and User Access Administrator roles on the subscription.", displayExample: false);
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
            var hasNoDelegations = !delegatedServices.Any();

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

            if (!computeSkus.Any())
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

        public static async Task<string> DownloadTextFromStorageAccountAsync(BlobClient blobClient, CancellationToken cancellationToken)
        {
            return (await blobClient.DownloadContentAsync(cancellationToken)).Value.Content.ToString();
        }

        public static async Task UploadTextToStorageAccountAsync(BlobClient blobClient, string content, CancellationToken cancellationToken)
        {
            await blobClient.GetParentBlobContainerClient().CreateIfNotExistsAsync(cancellationToken: cancellationToken);
            await blobClient.UploadAsync(BinaryData.FromString(content), true, cancellationToken);
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
