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
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Azure.Storage;
using Azure.Storage.Blobs;
using CommonUtilities;
using IdentityModel.Client;
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
using Microsoft.Azure.Management.Network;
using Microsoft.Azure.Management.Network.Fluent;
using Microsoft.Azure.Management.PostgreSQL;
using Microsoft.Azure.Management.PrivateDns.Fluent;
using Microsoft.Azure.Management.ResourceGraph;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Azure.Management.Storage.Fluent;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Rest;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;
using Tes.Models;
using static Microsoft.Azure.Management.PostgreSQL.FlexibleServers.DatabasesOperationsExtensions;
using static Microsoft.Azure.Management.PostgreSQL.FlexibleServers.ServersOperationsExtensions;
using static Microsoft.Azure.Management.PostgreSQL.ServersOperationsExtensions;
using static Microsoft.Azure.Management.ResourceManager.Fluent.Core.RestClient;
using Extensions = Microsoft.Azure.Management.ResourceManager.Fluent.Core.Extensions;
using FlexibleServer = Microsoft.Azure.Management.PostgreSQL.FlexibleServers;
using FlexibleServerModel = Microsoft.Azure.Management.PostgreSQL.FlexibleServers.Models;
using IResource = Microsoft.Azure.Management.ResourceManager.Fluent.Core.IResource;
using KeyVaultManagementClient = Microsoft.Azure.Management.KeyVault.KeyVaultManagementClient;
using SingleServer = Microsoft.Azure.Management.PostgreSQL;
using SingleServerModel = Microsoft.Azure.Management.PostgreSQL.Models;

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

        private static readonly AsyncRetryPolicy longRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(60, retryAttempt => System.TimeSpan.FromSeconds(15));

        public const string ConfigurationContainerName = "configuration";
        public const string ContainersToMountFileName = "containers-to-mount";
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

        private Configuration configuration { get; set; }
        private ITokenProvider tokenProvider;
        private TokenCredentials tokenCredentials;
        private IAzure azureSubscriptionClient { get; set; }
        private Microsoft.Azure.Management.Fluent.Azure.IAuthenticated azureClient { get; set; }
        private IResourceManager resourceManagerClient { get; set; }
        private Microsoft.Azure.Management.Network.INetworkManagementClient networkManagementClient { get; set; }
        private AzureCredentials azureCredentials { get; set; }
        private FlexibleServer.IPostgreSQLManagementClient postgreSqlFlexManagementClient { get; set; }
        private SingleServer.IPostgreSQLManagementClient postgreSqlSingleManagementClient { get; set; }
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
                    tokenProvider = new RefreshableAzureServiceTokenProvider("https://management.azure.com/");
                    tokenCredentials = new(tokenProvider);
                    azureCredentials = new(tokenCredentials, null, null, AzureEnvironment.AzureGlobalCloud);
                    azureClient = GetAzureClient(azureCredentials);
                    azureSubscriptionClient = azureClient.WithSubscription(configuration.SubscriptionId);
                    subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);
                    resourceManagerClient = GetResourceManagerClient(azureCredentials);
                    networkManagementClient = new Microsoft.Azure.Management.Network.NetworkManagementClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId };
                    postgreSqlFlexManagementClient = new FlexibleServer.PostgreSQLManagementClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId, LongRunningOperationRetryTimeout = 1200 };
                    postgreSqlSingleManagementClient = new SingleServer.PostgreSQLManagementClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId, LongRunningOperationRetryTimeout = 1200 };
                });

                await ValidateSubscriptionAndResourceGroupAsync(configuration);
                kubernetesManager = new(configuration, azureCredentials, cts);
                IResourceGroup resourceGroup = null;
                ManagedCluster aksCluster = null;
                BatchAccount batchAccount = null;
                IGenericResource logAnalyticsWorkspace = null;
                IGenericResource appInsights = null;
                FlexibleServerModel.Server postgreSqlFlexServer = null;
                SingleServerModel.Server postgreSqlSingleServer = null;
                IStorageAccount storageAccount = null;
                var keyVaultUri = string.Empty;
                IIdentity managedIdentity = null;
                IPrivateDnsZone postgreSqlDnsZone = null;

                try
                {
                    if (configuration.Update)
                    {
                        resourceGroup = await azureSubscriptionClient.ResourceGroups.GetByNameAsync(configuration.ResourceGroupName);
                        configuration.RegionName = resourceGroup.RegionName;

                        var targetVersion = Utility.DelimitedTextToDictionary(Utility.GetFileContent("scripts", "env-00-tes-version.txt")).GetValueOrDefault("TesOnAzureVersion");

                        ConsoleEx.WriteLine($"Upgrading TES on Azure instance in resource group '{resourceGroup.Name}' to version {targetVersion}...");

                        if (!string.IsNullOrEmpty(configuration.StorageAccountName))
                        {
                            storageAccount = await GetExistingStorageAccountAsync(configuration.StorageAccountName)
                                ?? throw new ValidationException($"Storage account {configuration.StorageAccountName} does not exist in region {configuration.RegionName} or is not accessible to the current user.", displayExample: false);
                        }
                        else
                        {
                            var storageAccounts = (await azureSubscriptionClient.StorageAccounts.ListByResourceGroupAsync(configuration.ResourceGroupName)).ToList();

                            storageAccount = storageAccounts.Count switch
                            {
                                0 => throw new ValidationException($"Update was requested but resource group {configuration.ResourceGroupName} does not contain any storage accounts.", displayExample: false),
                                1 => storageAccounts.Single(),
                                _ => throw new ValidationException($"Resource group {configuration.ResourceGroupName} contains multiple storage accounts. {nameof(configuration.StorageAccountName)} must be provided.", displayExample: false),
                            };
                        }

                        ManagedCluster existingAksCluster = default;

                        if (!string.IsNullOrWhiteSpace(configuration.AksClusterName))
                        {
                            existingAksCluster = (await GetExistingAKSClusterAsync(configuration.AksClusterName))
                                ?? throw new ValidationException($"AKS cluster {configuration.AksClusterName} does not exist in region {configuration.RegionName} or is not accessible to the current user.", displayExample: false);
                        }
                        else
                        {
                            using var client = new ContainerServiceClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId };
                            var aksClusters = (await client.ManagedClusters.ListByResourceGroupAsync(configuration.ResourceGroupName)).ToList();

                            existingAksCluster = aksClusters.Count switch
                            {
                                0 => throw new ValidationException($"Update was requested but resource group {configuration.ResourceGroupName} does not contain any AKS clusters.", displayExample: false),
                                1 => aksClusters.Single(),
                                _ => throw new ValidationException($"Resource group {configuration.ResourceGroupName} contains multiple AKS clusters. {nameof(configuration.AksClusterName)} must be provided.", displayExample: false),
                            };

                            configuration.AksClusterName = existingAksCluster.Name;
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

                        // Note: Current behavior is to block switching from Docker MySQL to Azure PostgreSql on Update.
                        // However we do ancitipate including this change, this code is here to facilitate this future behavior.
                        configuration.PostgreSqlServerName = aksValues.GetValueOrDefault("PostgreSqlServerName");

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

                        managedIdentity = azureSubscriptionClient.Identities.ListByResourceGroup(configuration.ResourceGroupName).Where(id => id.ClientId == managedIdentityClientId).FirstOrDefault()
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

                        //if (installedVersion is null || installedVersion < new Version(4, 2))
                        //{
                        //}

                        await kubernetesManager.UpgradeValuesYamlAsync(storageAccount, settings);
                        await PerformHelmDeploymentAsync(resourceGroup);
                    }

                    if (!configuration.Update)
                    {
                        configuration.ProvisionPostgreSqlOnAzure ??= true;

                        if (string.IsNullOrWhiteSpace(configuration.BatchPrefix))
                        {
                            var blob = new byte[5];
                            RandomNumberGenerator.Fill(blob);
                            configuration.BatchPrefix = CommonUtilities.Base32.ConvertToBase32(blob).TrimEnd('=');
                        }

                        ValidateRegionName(configuration.RegionName);
                        ValidateMainIdentifierPrefix(configuration.MainIdentifierPrefix);
                        storageAccount = await ValidateAndGetExistingStorageAccountAsync();
                        batchAccount = await ValidateAndGetExistingBatchAccountAsync();
                        aksCluster = await ValidateAndGetExistingAKSClusterAsync();
                        postgreSqlFlexServer = await ValidateAndGetExistingPostgresqlServerAsync();
                        var keyVault = await ValidateAndGetExistingKeyVaultAsync();

                        // Configuration preferences not currently settable by user.
                        if (string.IsNullOrWhiteSpace(configuration.PostgreSqlServerName) && configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault())
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
                        await ValidateVmAsync();

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
                            resourceGroup = await azureSubscriptionClient.ResourceGroups.GetByNameAsync(configuration.ResourceGroupName);
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

                        if (vnetAndSubnet is null)
                        {
                            configuration.VnetName = SdkContext.RandomResourceName($"{configuration.MainIdentifierPrefix}-", 15);
                            configuration.PostgreSqlSubnetName = String.IsNullOrEmpty(configuration.PostgreSqlSubnetName) && configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault() ? configuration.DefaultPostgreSqlSubnetName : configuration.PostgreSqlSubnetName;
                            configuration.VmSubnetName = String.IsNullOrEmpty(configuration.VmSubnetName) ? configuration.DefaultVmSubnetName : configuration.VmSubnetName;
                            vnetAndSubnet = await CreateVnetAndSubnetsAsync(resourceGroup);
                        }

                        if (string.IsNullOrWhiteSpace(configuration.LogAnalyticsArmId))
                        {
                            var workspaceName = SdkContext.RandomResourceName(configuration.MainIdentifierPrefix, 15);
                            logAnalyticsWorkspace = await CreateLogAnalyticsWorkspaceResourceAsync(workspaceName);
                            configuration.LogAnalyticsArmId = logAnalyticsWorkspace.Id;
                        }

                        await Task.Run(async () =>
                        {
                            storageAccount ??= await CreateStorageAccountAsync();
                            await CreateDefaultStorageContainersAsync(storageAccount);
                            await WritePersonalizedFilesToStorageAccountAsync(storageAccount, managedIdentity.Name);
                            await AssignVmAsContributorToStorageAccountAsync(managedIdentity, storageAccount);
                            await AssignVmAsDataReaderToStorageAccountAsync(managedIdentity, storageAccount);
                            await AssignManagedIdOperatorToResourceAsync(managedIdentity, resourceGroup);
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

                        if (configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault() && postgreSqlFlexServer is null)
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
                                if (configuration.UsePostgreSqlSingleServer)
                                {
                                    postgreSqlSingleServer ??= await CreateSinglePostgreSqlServerAndDatabaseAsync(postgreSqlSingleManagementClient, vnetAndSubnet.Value.vmSubnet, postgreSqlDnsZone);
                                }
                                else
                                {
                                    postgreSqlFlexServer ??= await CreatePostgreSqlServerAndDatabaseAsync(postgreSqlFlexManagementClient, vnetAndSubnet.Value.postgreSqlSubnet, postgreSqlDnsZone);
                                }
                            })
                        });

                        var clientId = managedIdentity.ClientId;
                        var settings = ConfigureSettings(clientId);

                        await kubernetesManager.UpdateHelmValuesAsync(storageAccount, keyVaultUri, resourceGroup.Name, settings, managedIdentity);
                        await PerformHelmDeploymentAsync(resourceGroup,
                            new[]
                            {
                                "Run the following postgresql command to setup the database.",
                                "\tPostgreSQL command: " + GetPostgreSQLCreateUserCommand(configuration.UsePostgreSqlSingleServer, configuration.PostgreSqlTesDatabaseName, GetCreateTesUserString()),
                            },
                            async kubernetesClient =>
                            {
                                await kubernetesManager.DeployCoADependenciesAsync();

                                // Deploy an ubuntu pod to run PSQL commands, then delete it
                                const string deploymentNamespace = "default";
                                var (deploymentName, ubuntuDeployment) = KubernetesManager.GetUbuntuDeploymentTemplate();
                                await kubernetesClient.AppsV1.CreateNamespacedDeploymentAsync(ubuntuDeployment, deploymentNamespace);
                                await ExecuteQueriesOnAzurePostgreSQLDbFromK8(kubernetesClient, deploymentName, deploymentNamespace);
                                await kubernetesClient.AppsV1.DeleteNamespacedDeploymentAsync(deploymentName, deploymentNamespace);

                                if (configuration.EnableIngress.GetValueOrDefault())
                                {
                                    _ = await kubernetesManager.EnableIngress(configuration.TesUsername, configuration.TesPassword, kubernetesClient);
                                }
                            });

                        if (configuration.EnableIngress.GetValueOrDefault() && !configuration.SkipTestWorkflow)
                        {
                            await Task.Delay(System.TimeSpan.FromMinutes(3)); // Give Ingress a moment longer to complete its standup.
                        }
                    }
                }
                finally
                {
                    if (!configuration.ManualHelmDeployment)
                    {
                        kubernetesManager.DeleteTempFiles();
                    }
                }

                var maxPerFamilyQuota = batchAccount.DedicatedCoreQuotaPerVMFamilyEnforced ? batchAccount.DedicatedCoreQuotaPerVMFamily.Select(q => q.CoreQuota).Where(q => 0 != q) : Enumerable.Repeat(batchAccount.DedicatedCoreQuota ?? 0, 1);
                var isBatchQuotaAvailable = batchAccount.LowPriorityCoreQuota > 0 || (batchAccount.DedicatedCoreQuota > 0 && maxPerFamilyQuota.Append(0).Max() > 0);

                int exitCode;

                if (configuration.EnableIngress.GetValueOrDefault())
                {
                    ConsoleEx.WriteLine($"TES ingress is enabled");
                    ConsoleEx.WriteLine($"TES is secured with basic auth at {kubernetesManager.TesHostname}");

                    if (configuration.OutputTesCredentialsJson.GetValueOrDefault())
                    {
                        // Write credentials to JSON file in working directory
                        var credentialsJson = System.Text.Json.JsonSerializer.Serialize<TesCredentials>(
                            new(kubernetesManager.TesHostname, configuration.TesUsername, configuration.TesPassword));

                        var credentialsPath = Path.Combine(Directory.GetCurrentDirectory(), TesCredentialsFileName);
                        await File.WriteAllTextAsync(credentialsPath, credentialsJson);
                        ConsoleEx.WriteLine($"TES credentials file written to: {credentialsPath}");
                    }



                    if (isBatchQuotaAvailable)
                    {
                        if (configuration.SkipTestWorkflow)
                        {
                            exitCode = 0;
                        }
                        else
                        {
                            var isTestWorkflowSuccessful = await RunTestTask(kubernetesManager.TesHostname, batchAccount.LowPriorityCoreQuota > 0, configuration.TesUsername, configuration.TesPassword);

                            if (!isTestWorkflowSuccessful)
                            {
                                await DeleteResourceGroupIfUserConsentsAsync();
                            }

                            exitCode = isTestWorkflowSuccessful ? 0 : 1;
                        }
                    }
                    else
                    {
                        if (!configuration.SkipTestWorkflow)
                        {
                            ConsoleEx.WriteLine($"Could not run the test task.", ConsoleColor.Yellow);
                        }

                        ConsoleEx.WriteLine($"Deployment was successful, but Batch account {configuration.BatchAccountName} does not have sufficient core quota to run workflows.", ConsoleColor.Yellow);
                        ConsoleEx.WriteLine($"Request Batch core quota: https://docs.microsoft.com/en-us/azure/batch/batch-quota-limit", ConsoleColor.Yellow);
                        ConsoleEx.WriteLine($"After receiving the quota, read the docs to run a test workflow and confirm successful deployment.", ConsoleColor.Yellow);
                        exitCode = 2;
                    }
                }
                else
                {
                    ConsoleEx.WriteLine($"TES ingress is not enabled, skipping test tasks.");
                    exitCode = 2;
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
                            ConsoleEx.WriteLine($"HTTP Request StatusCode: {rExc.StatusCode.ToString()}");
                            if (rExc.InnerException is not null)
                            {
                                ConsoleEx.WriteLine($"InnerException: {rExc.InnerException.GetType().FullName}: {rExc.InnerException.Message}");
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

        private static async Task<int> TestTaskAsync(string tesEndpoint, bool preemptible, string tesUsername, string tesPassword)
        {
            using var client = new HttpClient();
            client.SetBasicAuthentication(tesUsername, tesPassword);

            var task = new TesTask()
            {
                Inputs = new List<TesInput>(),
                Outputs = new List<TesOutput>(),
                Executors = new List<TesExecutor>
                {
                    new TesExecutor()
                    {
                        Image = "ubuntu:22.04",
                        Command = new List<string>{"echo 'hello world'" },
                    }
                },
                Resources = new TesResources()
                {
                    Preemptible = preemptible
                }
            };

            var content = new StringContent(JsonConvert.SerializeObject(task), Encoding.UTF8, "application/json");
            var requestUri = $"https://{tesEndpoint}/v1/tasks";
            Dictionary<string, string> response = null;
            await longRetryPolicy.ExecuteAsync(
                    async () =>
                    {
                        var responseBody = await client.PostAsync(requestUri, content);
                        var body = await responseBody.Content.ReadAsStringAsync();
                        response = JsonConvert.DeserializeObject<Dictionary<string, string>>(body);
                    });

            return await IsTaskSuccessfulAfterLongPollingAsync(client, $"{requestUri}/{response["id"]}") ? 0 : 1;
        }

        private static async Task<bool> RunTestTask(string tesEndpoint, bool preemptible, string tesUsername, string tesPassword)
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

        private static async Task<bool> IsTaskSuccessfulAfterLongPollingAsync(HttpClient client, string taskEndpoint)
        {
            while (true)
            {
                try
                {
                    var responseBody = await client.GetAsync(taskEndpoint);
                    var content = await responseBody.Content.ReadAsStringAsync();
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

                await Task.Delay(System.TimeSpan.FromSeconds(10));
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
            return (await Task.WhenAll(subscriptionIds.Select(async s =>
            {
                try
                {
                    var client = new FlexibleServer.PostgreSQLManagementClient(tokenCredentials) { SubscriptionId = s };
                    return await client.Servers.ListAsync();
                }
                catch (Exception e)
                {
                    ConsoleEx.WriteLine(e.Message);
                    return null;
                }
            })))
                .Where(a => a is not null)
                .SelectMany(a => a)
                .SingleOrDefault(a => a.Name.Equals(serverName, StringComparison.OrdinalIgnoreCase) && regex.Replace(a.Location, "").Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase));
        }

        private async Task<ManagedCluster> GetExistingAKSClusterAsync(string aksClusterName)
        {
            return (await Task.WhenAll(subscriptionIds.Select(async s =>
            {
                try
                {
                    var client = new ContainerServiceClient(tokenCredentials) { SubscriptionId = s };
                    return await client.ManagedClusters.ListAsync();
                }
                catch (Exception e)
                {
                    ConsoleEx.WriteLine(e.Message);
                    return null;
                }
            })))
                .Where(a => a is not null)
                .SelectMany(a => a)
                .SingleOrDefault(a => a.Name.Equals(aksClusterName, StringComparison.OrdinalIgnoreCase) && a.Location.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase));
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
                    Type = "VirtualMachineScaleSets",
                    EnableAutoScaling = false,
                    EnableNodePublicIP = false,
                    OsType = "Linux",
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
                () => containerServiceClient.ManagedClusters.CreateOrUpdateAsync(resourceGroup, configuration.AksClusterName, cluster));
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
            UpdateSetting(settings, defaults, "DockerInDockerImageName", configuration.DockerInDockerImageName);
            UpdateSetting(settings, defaults, "BlobxferImageName", configuration.BlobxferImageName);
            UpdateSetting(settings, defaults, "DisableBatchNodesPublicIpAddress", configuration.DisableBatchNodesPublicIpAddress, b => b.GetValueOrDefault().ToString(), configuration.DisableBatchNodesPublicIpAddress.GetValueOrDefault().ToString());

            if (installedVersion is null)
            {
                UpdateSetting(settings, defaults, "BatchPrefix", configuration.BatchPrefix, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "DefaultStorageAccountName", configuration.StorageAccountName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "BatchAccountName", configuration.BatchAccountName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "ApplicationInsightsAccountName", configuration.ApplicationInsightsAccountName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "ManagedIdentityClientId", managedIdentityClientId, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "AzureServicesAuthConnectionString", $"RunAs=App;AppId={managedIdentityClientId}", ignoreDefaults: true);
                UpdateSetting(settings, defaults, "KeyVaultName", configuration.KeyVaultName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "AksCoANamespace", configuration.AksCoANamespace, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "ProvisionPostgreSqlOnAzure", configuration.ProvisionPostgreSqlOnAzure, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "CrossSubscriptionAKSDeployment", configuration.CrossSubscriptionAKSDeployment);
                UpdateSetting(settings, defaults, "PostgreSqlServerName", configuration.PostgreSqlServerName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlServerNameSuffix", configuration.PostgreSqlServerNameSuffix, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlServerPort", configuration.PostgreSqlServerPort.ToString(), ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlServerSslMode", configuration.PostgreSqlServerSslMode, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlTesDatabaseName", configuration.PostgreSqlTesDatabaseName, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlTesDatabaseUserLogin", GetFormattedPostgresqlUser(), ignoreDefaults: true);
                UpdateSetting(settings, defaults, "PostgreSqlTesDatabaseUserPassword", configuration.PostgreSqlTesUserPassword, ignoreDefaults: true);
                UpdateSetting(settings, defaults, "UsePostgreSqlSingleServer", configuration.UsePostgreSqlSingleServer.ToString(), ignoreDefaults: true);
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
                                resourceManagerClient.Providers.RegisterAsync(rp))
                        );

                        // RP registration takes a few minutes; poll until done registering

                        while (!cts.IsCancellationRequested)
                        {
                            unregisteredResourceProviders = await GetRequiredResourceProvidersNotRegisteredAsync();

                            if (unregisteredResourceProviders.Count == 0)
                            {
                                break;
                            }

                            await Task.Delay(System.TimeSpan.FromSeconds(15));
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
            var cloudResourceProviders = await resourceManagerClient.Providers.ListAsync();

            var notRegisteredResourceProviders = requiredResourceProviders
                .Intersect(cloudResourceProviders
                    .Where(rp => !rp.RegistrationState.Equals("Registered", StringComparison.OrdinalIgnoreCase))
                    .Select(rp => rp.Namespace), StringComparer.OrdinalIgnoreCase)
                .ToList();

            return notRegisteredResourceProviders;
        }

        private Task AssignManagedIdOperatorToResourceAsync(IIdentity managedIdentity, IResource resource)
        {
            // https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#managed-identity-operator
            var roleDefinitionId = $"/subscriptions/{configuration.SubscriptionId}/providers/Microsoft.Authorization/roleDefinitions/f1a07417-d97a-45cb-824c-7a7467783830";
            return Execute(
                $"Assigning Managed ID Operator role for the managed id to resource group scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithRoleDefinition(roleDefinitionId)
                        .WithResourceScope(resource)
                        .CreateAsync(cts.Token)));
        }

        private Task AssignVmAsDataReaderToStorageAccountAsync(IIdentity managedIdentity, IStorageAccount storageAccount)
        {
            // https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#storage-blob-data-reader
            var roleDefinitionId = $"/subscriptions/{configuration.SubscriptionId}/providers/Microsoft.Authorization/roleDefinitions/2a2b9908-6ea1-4ae2-8e65-a410df84e7d1";

            return Execute(
                $"Assigning Storage Blob Data Reader role for user-managed identity to Storage Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithRoleDefinition(roleDefinitionId)
                        .WithResourceScope(storageAccount)
                        .CreateAsync(cts.Token)));
        }

        private Task AssignVmAsContributorToStorageAccountAsync(IIdentity managedIdentity, IResource storageAccount)
            => Execute(
                $"Assigning {BuiltInRole.Contributor} role for user-managed identity to Storage Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithResourceScope(storageAccount)
                        .CreateAsync(cts.Token)));

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
            => (await Task.WhenAll(subscriptionIds.Select(async s =>
            {
                try
                {
                    return await azureClient.WithSubscription(s).StorageAccounts.ListAsync();
                }
                catch (Exception)
                {
                    // Ignore exception if a user does not have the required role to list storage accounts in a subscription
                    return null;
                }
            })))
                .Where(a => a is not null)
                .SelectMany(a => a)
                .SingleOrDefault(a => a.Name.Equals(storageAccountName, StringComparison.OrdinalIgnoreCase) && a.RegionName.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase));

        private async Task<BatchAccount> GetExistingBatchAccountAsync(string batchAccountName)
            => (await Task.WhenAll(subscriptionIds.Select(async s =>
            {
                try
                {
                    var client = new BatchManagementClient(tokenCredentials) { SubscriptionId = s };
                    return await client.BatchAccount.ListAsync();
                }
                catch (Exception e)
                {
                    ConsoleEx.WriteLine(e.Message);
                    return null;
                }
            })))
                .Where(a => a is not null)
                .SelectMany(a => a)
                .SingleOrDefault(a => a.Name.Equals(batchAccountName, StringComparison.OrdinalIgnoreCase) && a.Location.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase));

        private async Task CreateDefaultStorageContainersAsync(IStorageAccount storageAccount)
        {
            var blobClient = await GetBlobClientAsync(storageAccount);

            var defaultContainers = new List<string> { "executions", InputsContainerName, "outputs", ConfigurationContainerName };
            await Task.WhenAll(defaultContainers.Select(c => blobClient.GetBlobContainerClient(c).CreateIfNotExistsAsync(cancellationToken: cts.Token)));
        }

        private Task WritePersonalizedFilesToStorageAccountAsync(IStorageAccount storageAccount, string managedIdentityName)
            => Execute(
                $"Writing {ContainersToMountFileName} file to '{ConfigurationContainerName}' storage container...",
                async () =>
                {
                    await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, ContainersToMountFileName, Utility.PersonalizeContent(new Utility.ConfigReplaceTextItem[]
                    {
                        new("{DefaultStorageAccountName}", configuration.StorageAccountName),
                        new("{ManagedIdentityName}", managedIdentityName)
                    }, "scripts", ContainersToMountFileName));

                    // Configure Cromwell config file for Docker Mysql or PostgreSQL on Azure.
                    //if (configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault())
                    //{
                    //    await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, CromwellConfigurationFileName, Utility.PersonalizeContent(new Utility.ConfigReplaceTextItem[]
                    //    {
                    //        new("{DatabaseUrl}", $"\"jdbc:postgresql://{configuration.PostgreSqlServerName}.postgres.database.azure.com/{configuration.PostgreSqlCromwellDatabaseName}?sslmode=require\""),
                    //        new("{DatabaseUser}", configuration.UsePostgreSqlSingleServer ? $"\"{configuration.PostgreSqlCromwellUserLogin}@{configuration.PostgreSqlServerName}\"": $"\"{configuration.PostgreSqlCromwellUserLogin}\""),
                    //        new("{DatabasePassword}", $"\"{configuration.PostgreSqlCromwellUserPassword}\""),
                    //        new("{DatabaseDriver}", $"\"org.postgresql.Driver\""),
                    //        new("{DatabaseProfile}", "\"slick.jdbc.PostgresProfile$\""),
                    //    }, "scripts", CromwellConfigurationFileName));
                    //}
                    //else
                    //{
                    //    await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, CromwellConfigurationFileName, Utility.PersonalizeContent(new Utility.ConfigReplaceTextItem[]
                    //    {
                    //        new("{DatabaseUrl}", $"\"jdbc:mysql://mysqldb/cromwell_db?useSSL=false&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true\""),
                    //        new("{DatabaseUser}", $"\"cromwell\""),
                    //        new("{DatabasePassword}", $"\"cromwell\""),
                    //        new("{DatabaseDriver}", $"\"com.mysql.cj.jdbc.Driver\""),
                    //        new("{DatabaseProfile}", "\"slick.jdbc.MySQLProfile$\""),
                    //    }, "scripts", CromwellConfigurationFileName));
                    //}

                    await UploadTextToStorageAccountAsync(storageAccount, ConfigurationContainerName, AllowedVmSizesFileName, Utility.GetFileContent("scripts", AllowedVmSizesFileName));
                });

        private Task AssignVmAsContributorToBatchAccountAsync(IIdentity managedIdentity, BatchAccount batchAccount)
            => Execute(
                $"Assigning {BuiltInRole.Contributor} role for user-managed identity to Batch Account resource scope...",
                () => roleAssignmentHashConflictRetryPolicy.ExecuteAsync(
                    () => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithScope(batchAccount.Id)
                        .CreateAsync(cts.Token)));

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

            //await Execute(
            //    $"Creating PostgreSQL cromwell database: {configuration.PostgreSqlCromwellDatabaseName}...",
            //    () => postgresManagementClient.Databases.CreateAsync(
            //        configuration.ResourceGroupName, configuration.PostgreSqlServerName, configuration.PostgreSqlCromwellDatabaseName,
            //        new()));

            await Execute(
                $"Creating PostgreSQL tes database: {configuration.PostgreSqlTesDatabaseName}...",
                () => postgresManagementClient.Databases.CreateAsync(
                    configuration.ResourceGroupName, configuration.PostgreSqlServerName, configuration.PostgreSqlTesDatabaseName,
                    new()));

            return server;
        }

        private async Task<SingleServerModel.Server> CreateSinglePostgreSqlServerAndDatabaseAsync(SingleServer.IPostgreSQLManagementClient postgresManagementClient, ISubnet subnet, IPrivateDnsZone postgreSqlDnsZone)
        {
            SingleServerModel.Server server = null;

            await Execute(
                $"Creating Azure Single Server for PostgreSQL: {configuration.PostgreSqlServerName}...",
                async () =>
                {
                    server = await postgresManagementClient.Servers.CreateAsync(
                        configuration.ResourceGroupName, configuration.PostgreSqlServerName,
                        new(
                           new SingleServerModel.ServerPropertiesForDefaultCreate(
                               administratorLogin: configuration.PostgreSqlAdministratorLogin,
                               administratorLoginPassword: configuration.PostgreSqlAdministratorPassword,
                               version: configuration.PostgreSqlVersion,
                               publicNetworkAccess: "Disabled",
                               storageProfile: new(
                                   storageMB: configuration.PostgreSqlStorageSize * 1000)),
                           configuration.RegionName,
                           sku: new("GP_Gen5_4")
                       ));

                    var privateEndpoint = await networkManagementClient.PrivateEndpoints.CreateOrUpdateAsync(configuration.ResourceGroupName, "pe-postgres1",
                        new(
                            name: "pe-coa-postgresql",
                            location: configuration.RegionName,
                            privateLinkServiceConnections: new List<Microsoft.Azure.Management.Network.Models.PrivateLinkServiceConnection>()
                            { new(name: "pe-coa-postgresql", privateLinkServiceId: server.Id, groupIds: new List<string>(){"postgresqlServer"}) },
                            subnet: new(subnet.Inner.Id)));
                    var networkInterfaceName = privateEndpoint.NetworkInterfaces.First().Id.Split("/").Last();
                    var networkInterface = await networkManagementClient.NetworkInterfaces.GetAsync(configuration.ResourceGroupName, networkInterfaceName);

                    await postgreSqlDnsZone
                        .Update()
                        .DefineARecordSet(server.Name)
                        .WithIPv4Address(networkInterface.IpConfigurations.First().PrivateIPAddress)
                        .Attach()
                        .ApplyAsync();
                });

            //await Execute(
            //    $"Creating PostgreSQL cromwell database: {configuration.PostgreSqlCromwellDatabaseName}...",
            //    async () => await postgresManagementClient.Databases.CreateOrUpdateAsync(
            //        configuration.ResourceGroupName, configuration.PostgreSqlServerName, configuration.PostgreSqlCromwellDatabaseName,
            //        new()));

            await Execute(
                $"Creating PostgreSQL tes database: {configuration.PostgreSqlTesDatabaseName}...",
                () => postgresManagementClient.Databases.CreateOrUpdateAsync(
                    configuration.ResourceGroupName, configuration.PostgreSqlServerName, configuration.PostgreSqlTesDatabaseName,
                    new()));
            return server;
        }

        private string GetFormattedPostgresqlUser()
        {
            if (!configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault())
            {
                return string.Empty;
            }

            if (configuration.UsePostgreSqlSingleServer)
            {
                return $"{configuration.PostgreSqlTesUserLogin}@{configuration.PostgreSqlServerName}";
            }
            else
            {
                return configuration.PostgreSqlTesUserLogin;
            }
        }

        private string GetCreateTesUserString()
        {
            return $"CREATE USER {configuration.PostgreSqlTesUserLogin} WITH PASSWORD '{configuration.PostgreSqlTesUserPassword}'; GRANT ALL PRIVILEGES ON DATABASE {configuration.PostgreSqlTesDatabaseName} TO {configuration.PostgreSqlTesUserLogin};";
        }

        private string GetPostgreSQLCreateUserCommand(bool useSingleServer, string dbName, string sqlCommand)
        {
            if (useSingleServer)
            {
                return $"PGPASSWORD={configuration.PostgreSqlAdministratorPassword} psql -U {configuration.PostgreSqlAdministratorLogin}@{configuration.PostgreSqlServerName} -h {configuration.PostgreSqlServerName}.postgres.database.azure.com -d {dbName}  -v sslmode=true -c \"{sqlCommand}\"";
            }
            else
            {
                return $"psql postgresql://{configuration.PostgreSqlAdministratorLogin}:{configuration.PostgreSqlAdministratorPassword}@{configuration.PostgreSqlServerName}.postgres.database.azure.com/{dbName} -c \"{sqlCommand}\"";
            }
        }

        private Task ExecuteQueriesOnAzurePostgreSQLDbFromK8(IKubernetes kubernetesClient, string podName, string aksNamespace)
            => Execute(
                $"Executing scripts on postgresql...",
                async () =>
                {
                    var tesScript = GetCreateTesUserString();
                    var serverPath = $"{configuration.PostgreSqlServerName}.postgres.database.azure.com";
                    var adminUser = configuration.PostgreSqlAdministratorLogin;

                    if (configuration.UsePostgreSqlSingleServer)
                    {
                        adminUser = $"{configuration.PostgreSqlAdministratorLogin}@{configuration.PostgreSqlServerName}";
                    }

                    var commands = new List<string[]> {
                        new string[] { "apt", "update" },
                        new string[] { "apt", "install", "-y", "postgresql-client" },
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
                    () => azureSubscriptionClient.AccessManagement.RoleAssignments
                        .Define(Guid.NewGuid().ToString())
                        .ForObjectId(managedIdentity.PrincipalId)
                        .WithBuiltInRole(BuiltInRole.Contributor)
                        .WithResourceScope(appInsights)
                        .CreateAsync(cts.Token)));

        private Task<(INetwork virtualNetwork, ISubnet vmSubnet, ISubnet postgreSqlSubnet)> CreateVnetAndSubnetsAsync(IResourceGroup resourceGroup)
          => Execute(
                $"Creating virtual network and subnets: {configuration.VnetName}...",
                async () =>
                {
                    var vnetDefinition = azureSubscriptionClient.Networks
                        .Define(configuration.VnetName)
                        .WithRegion(configuration.RegionName)
                        .WithExistingResourceGroup(resourceGroup)
                        .WithAddressSpace(configuration.VnetAddressSpace)
                        .DefineSubnet(configuration.VmSubnetName).WithAddressPrefix(configuration.VmSubnetAddressSpace).Attach();

                    vnetDefinition = configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault()
                        ? vnetDefinition.DefineSubnet(configuration.PostgreSqlSubnetName).WithAddressPrefix(configuration.PostgreSqlSubnetAddressSpace).WithDelegation("Microsoft.DBforPostgreSQL/flexibleServers").Attach()
                        : vnetDefinition;

                    var vnet = await vnetDefinition.CreateAsync();

                    return (vnet, vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.VmSubnetName, StringComparison.OrdinalIgnoreCase)).Value, vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.PostgreSqlSubnetName, StringComparison.OrdinalIgnoreCase)).Value);
                });

        //private Task<INetworkSecurityGroup> CreateNetworkSecurityGroupAsync(IResourceGroup resourceGroup, string networkSecurityGroupName)
        //{
        //    const int SshAllowedPort = 22;
        //    const int SshDefaultPriority = 300;

        //    return Execute(
        //        $"Creating Network Security Group: {networkSecurityGroupName}...",
        //        () => azureSubscriptionClient.NetworkSecurityGroups.Define(networkSecurityGroupName)
        //            .WithRegion(configuration.RegionName)
        //            .WithExistingResourceGroup(resourceGroup)
        //            .DefineRule(SshNsgRuleName)
        //            .AllowInbound()
        //            .FromAnyAddress()
        //            .FromAnyPort()
        //            .ToAnyAddress()
        //            .ToPort(SshAllowedPort)
        //            .WithProtocol(Microsoft.Azure.Management.Network.Fluent.Models.SecurityRuleProtocol.Tcp)
        //            .WithPriority(SshDefaultPriority)
        //            .Attach()
        //            .CreateAsync(cts.Token)
        //    );
        //}

        //private Task<INetworkInterface> AssociateNicWithNetworkSecurityGroupAsync(INetworkInterface networkInterface, INetworkSecurityGroup networkSecurityGroup)
        //    => Execute(
        //        $"Associating VM NIC with Network Security Group {networkSecurityGroup.Name}...",
        //        () => networkInterface.Update().WithExistingNetworkSecurityGroup(networkSecurityGroup).ApplyAsync()
        //    );

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
                        .CreateAsync();
                    return dnsZone;
                });

        private static async Task SetStorageKeySecret(string vaultUrl, string secretName, string secretValue)
        {
            var client = new SecretClient(new(vaultUrl), new DefaultAzureCredential());
            await client.SetSecretAsync(secretName, secretValue);
        }

        private Task<Vault> GetKeyVaultAsync(string vaultName)
        {
            var keyVaultManagementClient = new KeyVaultManagementClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId };
            return keyVaultManagementClient.Vaults.GetAsync(configuration.ResourceGroupName, vaultName);
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

                    var vault = await keyVaultManagementClient.Vaults.CreateOrUpdateAsync(configuration.ResourceGroupName, vaultName, properties);

                    if (configuration.PrivateNetworking.GetValueOrDefault())
                    {
                        var privateEndpoint = await networkManagementClient.PrivateEndpoints.CreateOrUpdateAsync(configuration.ResourceGroupName, "pe-keyvault",
                            new(
                                name: "pe-coa-keyvault",
                                location: configuration.RegionName,
                                privateLinkServiceConnections: new List<Microsoft.Azure.Management.Network.Models.PrivateLinkServiceConnection>()
                                { new(name: "pe-coa-keyvault", privateLinkServiceId: vault.Id, groupIds: new List<string>(){"vault"}) },
                                subnet: new(subnet.Inner.Id)));

                        var networkInterfaceName = privateEndpoint.NetworkInterfaces.First().Id.Split("/").Last();
                        var networkInterface = await networkManagementClient.NetworkInterfaces.GetAsync(configuration.ResourceGroupName, networkInterfaceName);

                        var dnsZone = await CreatePrivateDnsZoneAsync(subnet.Parent, "privatelink.vaultcore.azure.net", "KeyVault");
                        await dnsZone
                            .Update()
                            .DefineARecordSet(vault.Name)
                            .WithIPv4Address(networkInterface.IpConfigurations.First().PrivateIPAddress)
                            .Attach()
                            .ApplyAsync();
                    }

                    return vault;

                    async ValueTask<string> GetUserObjectId()
                    {
                        const string graphUri = "https://graph.windows.net";
                        var credentials = new AzureCredentials(default, new TokenCredentials(new RefreshableAzureServiceTokenProvider(graphUri)), tenantId, AzureEnvironment.AzureGlobalCloud);
                        using GraphRbacManagementClient rbacClient = new(Configure().WithEnvironment(AzureEnvironment.AzureGlobalCloud).WithCredentials(credentials).WithBaseUri(graphUri).Build()) { TenantID = tenantId };
                        credentials.InitializeServiceClient(rbacClient);
                        return (await rbacClient.SignedInUser.GetAsync()).ObjectId;
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
                () => resourceGroupDefinition.CreateAsync());
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
                        .CreateAsync());
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
            var validRegionNames = azureSubscriptionClient.GetCurrentSubscription().ListLocations().Select(loc => loc.Region.Name);

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

            var subscriptionExists = (await azure.Subscriptions.ListAsync()).Any(sub => sub.SubscriptionId.Equals(configuration.SubscriptionId, StringComparison.OrdinalIgnoreCase));

            if (!subscriptionExists)
            {
                throw new ValidationException($"Invalid or inaccessible subcription id '{configuration.SubscriptionId}'. Make sure that subscription exists and that you are either an Owner or have Contributor and User Access Administrator roles on the subscription.", displayExample: false);
            }

            var rgExists = !string.IsNullOrEmpty(configuration.ResourceGroupName) && await azureSubscriptionClient.ResourceGroups.ContainAsync(configuration.ResourceGroupName);

            if (!string.IsNullOrEmpty(configuration.ResourceGroupName) && !rgExists)
            {
                throw new ValidationException($"If ResourceGroupName is provided, the resource group must already exist.", displayExample: false);
            }

            var token = (await tokenProvider.GetAuthenticationHeaderAsync(CancellationToken.None)).Parameter;
            var currentPrincipalObjectId = new JwtSecurityTokenHandler().ReadJwtToken(token).Claims.FirstOrDefault(c => c.Type == "oid").Value;

            var currentPrincipalSubscriptionRoleIds = (await azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeWithHttpMessagesAsync($"/subscriptions/{configuration.SubscriptionId}", new($"atScope() and assignedTo('{currentPrincipalObjectId}')")))
                .Body.AsContinuousCollection(link => Extensions.Synchronize(() => azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeNextWithHttpMessagesAsync(link)).Body)
                .Select(b => b.RoleDefinitionId.Split(new[] { '/' }).Last());

            if (!currentPrincipalSubscriptionRoleIds.Contains(ownerRoleId) && !(currentPrincipalSubscriptionRoleIds.Contains(contributorRoleId)))
            {
                if (!rgExists)
                {
                    throw new ValidationException($"Insufficient access to deploy. You must be: 1) Owner of the subscription, or 2) Contributor and User Access Administrator of the subscription, or 3) Owner of the resource group", displayExample: false);
                }

                var currentPrincipalRgRoleIds = (await azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeWithHttpMessagesAsync($"/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}", new($"atScope() and assignedTo('{currentPrincipalObjectId}')")))
                    .Body.AsContinuousCollection(link => Extensions.Synchronize(() => azureSubscriptionClient.AccessManagement.RoleAssignments.Inner.ListForScopeNextWithHttpMessagesAsync(link)).Body)
                    .Select(b => b.RoleDefinitionId.Split(new[] { '/' }).Last());

                if (!currentPrincipalRgRoleIds.Contains(ownerRoleId))
                {
                    throw new ValidationException($"Insufficient access to deploy. You must be: 1) Owner of the subscription, or 2) Contributor and User Access Administrator of the subscription, or 3) Owner of the resource group", displayExample: false);
                }
            }

            if (configuration.UsePostgreSqlSingleServer && int.Parse(configuration.PostgreSqlVersion) > 11)
            {
                // https://learn.microsoft.com/en-us/azure/postgresql/single-server/concepts-version-policy
                ConsoleEx.WriteLine($"Warning: as of 2/7/2023, Azure Database for PostgreSQL Single Server only supports up to PostgreSQL version 11", ConsoleColor.Yellow);
                ConsoleEx.WriteLine($"{nameof(configuration.PostgreSqlVersion)} is currently set to {configuration.PostgreSqlVersion}", ConsoleColor.Yellow);
                ConsoleEx.WriteLine($"Deployment will continue but could fail; please consider including '--{nameof(configuration.PostgreSqlVersion)} 11'", ConsoleColor.Yellow);
                ConsoleEx.WriteLine("More info: https://learn.microsoft.com/en-us/azure/postgresql/single-server/concepts-version-policy");
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

        private async Task<(INetwork virtualNetwork, ISubnet vmSubnet, ISubnet postgreSqlSubnet)?> ValidateAndGetExistingVirtualNetworkAsync()
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

            if (configuration.ProvisionPostgreSqlOnAzure == true && !AllOrNoneSet(configuration.VnetResourceGroupName, configuration.VnetName, configuration.VmSubnetName, configuration.PostgreSqlSubnetName))
            {
                throw new ValidationException($"{nameof(configuration.VnetResourceGroupName)}, {nameof(configuration.VnetName)}, {nameof(configuration.VmSubnetName)} and {nameof(configuration.PostgreSqlSubnetName)} are required when using an existing virtual network and {nameof(configuration.ProvisionPostgreSqlOnAzure)} is set.");
            }

            if (!AllOrNoneSet(configuration.VnetResourceGroupName, configuration.VnetName, configuration.VmSubnetName))
            {
                throw new ValidationException($"{nameof(configuration.VnetResourceGroupName)}, {nameof(configuration.VnetName)} and {nameof(configuration.VmSubnetName)} are required when using an existing virtual network.");
            }

            if (!(await azureSubscriptionClient.ResourceGroups.ListAsync(true)).Any(rg => rg.Name.Equals(configuration.VnetResourceGroupName, StringComparison.OrdinalIgnoreCase)))
            {
                throw new ValidationException($"Resource group '{configuration.VnetResourceGroupName}' does not exist.");
            }

            var vnet = await azureSubscriptionClient.Networks.GetByResourceGroupAsync(configuration.VnetResourceGroupName, configuration.VnetName);

            if (vnet is null)
            {
                throw new ValidationException($"Virtual network '{configuration.VnetName}' does not exist in resource group '{configuration.VnetResourceGroupName}'.");
            }

            if (!vnet.RegionName.Equals(configuration.RegionName, StringComparison.OrdinalIgnoreCase))
            {
                throw new ValidationException($"Virtual network '{configuration.VnetName}' must be in the same region that you are deploying to ({configuration.RegionName}).");
            }

            var vmSubnet = vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.VmSubnetName, StringComparison.OrdinalIgnoreCase)).Value;

            if (vmSubnet is null)
            {
                throw new ValidationException($"Virtual network '{configuration.VnetName}' does not contain subnet '{configuration.VmSubnetName}'");
            }

            var resourceGraphClient = new ResourceGraphClient(tokenCredentials);
            var postgreSqlSubnet = vnet.Subnets.FirstOrDefault(s => s.Key.Equals(configuration.PostgreSqlSubnetName, StringComparison.OrdinalIgnoreCase)).Value;

            if (configuration.ProvisionPostgreSqlOnAzure == true)
            {
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
                var resourcesExist = (await resourceGraphClient.ResourcesAsync(new(new[] { configuration.SubscriptionId }, resourcesInPostgreSqlSubnetQuery))).TotalRecords > 0;

                if (hasNoDelegations && resourcesExist)
                {
                    throw new ValidationException($"Subnet '{configuration.PostgreSqlSubnetName}' must be either empty or have 'Microsoft.DBforPostgreSQL/flexibleServers' delegation.");
                }
            }

            return (vnet, vmSubnet, postgreSqlSubnet);
        }

        private async Task ValidateBatchAccountQuotaAsync()
        {
            var accountQuota = (await new BatchManagementClient(tokenCredentials) { SubscriptionId = configuration.SubscriptionId }.Location.GetQuotasAsync(configuration.RegionName)).AccountQuota;
            var existingBatchAccountCount = (await new BatchManagementClient(tokenCredentials) { SubscriptionId = configuration.SubscriptionId }.BatchAccount.ListAsync()).AsEnumerable().Count(b => b.Location.Equals(configuration.RegionName));

            if (existingBatchAccountCount >= accountQuota)
            {
                throw new ValidationException($"The regional Batch account quota ({accountQuota} account(s) per region) for the specified subscription has been reached. Submit a support request to increase the quota or choose another region.", displayExample: false);
            }
        }

        private async Task ValidateVmAsync()
        {
            var computeSkus = (await generalRetryPolicy.ExecuteAsync(() =>
                azureSubscriptionClient.ComputeSkus.ListbyRegionAndResourceTypeAsync(
                    Region.Create(configuration.RegionName),
                    ComputeResourceType.VirtualMachines)))
                .Where(s => !s.Restrictions.Any())
                .Select(s => s.Name.Value)
                .ToList();

            if (!computeSkus.Any())
            {
                throw new ValidationException($"Your subscription doesn't support virtual machine creation in {configuration.RegionName}.  Please create an Azure Support case: https://docs.microsoft.com/en-us/azure/azure-portal/supportability/how-to-create-azure-support-request", displayExample: false);
            }
            else if (!computeSkus.Any(s => s.Equals(configuration.VmSize, StringComparison.OrdinalIgnoreCase)))
            {
                throw new ValidationException($"The VmSize {configuration.VmSize} is not available or does not exist in {configuration.RegionName}.  You can use 'az vm list-skus --location {configuration.RegionName} --output table' to find an available VM.", displayExample: false);
            }
        }

        private static async Task<BlobServiceClient> GetBlobClientAsync(IStorageAccount storageAccount)
            => new(
                new($"https://{storageAccount.Name}.blob.core.windows.net"),
                new StorageSharedKeyCredential(
                    storageAccount.Name,
                    (await storageAccount.GetKeysAsync())[0].Value));

        private async Task ValidateTokenProviderAsync()
        {
            try
            {
                _ = await Execute("Retrieving Azure management token...", () => new AzureServiceTokenProvider("RunAs=Developer; DeveloperTool=AzureCli").GetAccessTokenAsync("https://management.azure.com/"));
            }
            catch (AzureServiceTokenProviderException ex)
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

            ThrowIfNotProvided(configuration.SubscriptionId, nameof(configuration.SubscriptionId));

            ThrowIfNotProvidedForInstall(configuration.RegionName, nameof(configuration.RegionName));

            ThrowIfNotProvidedForUpdate(configuration.ResourceGroupName, nameof(configuration.ResourceGroupName));

            ThrowIfProvidedForUpdate(configuration.BatchPrefix, nameof(configuration.BatchPrefix));
            ThrowIfProvidedForUpdate(configuration.RegionName, nameof(configuration.RegionName));
            ThrowIfProvidedForUpdate(configuration.BatchAccountName, nameof(configuration.BatchAccountName));
            ThrowIfProvidedForUpdate(configuration.CrossSubscriptionAKSDeployment, nameof(configuration.CrossSubscriptionAKSDeployment));
            ThrowIfProvidedForUpdate(configuration.ProvisionPostgreSqlOnAzure, nameof(configuration.ProvisionPostgreSqlOnAzure));
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

            ValidateDependantFeature(configuration.EnableIngress.GetValueOrDefault(), nameof(configuration.EnableIngress), !string.IsNullOrEmpty(configuration.LetsEncryptEmail), nameof(configuration.LetsEncryptEmail));

            //if (configuration.ProvisionPostgreSqlOnAzure is null)
            //{
            //    configuration.ProvisionPostgreSqlOnAzure = true;
            //}
            //else if (!configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault())
            //{
            //    ValidateDependantFeature(configuration.UseAks, nameof(configuration.UseAks), configuration.ProvisionPostgreSqlOnAzure.GetValueOrDefault(), nameof(configuration.ProvisionPostgreSqlOnAzure));
            //}

            if (!configuration.Update)
            {
                if (configuration.BatchPrefix?.Length > 11 || (configuration.BatchPrefix?.Any(c => !char.IsAsciiLetterOrDigit(c)) ?? false))
                {
                    throw new ValidationException("BatchPrefix must not be longer than 11 chars and may contain only ASCII letters or digits", false);
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
                await DeleteResourceGroupAsync();
            }
        }

        private static void WriteGeneralRetryMessageToConsole()
            => ConsoleEx.WriteLine("Please try deployment again, and create an issue if this continues to fail: https://github.com/microsoft/ga4gh-tes/issues");

        public Task Execute(string message, Func<Task> func)
            => Execute(message, async () => { await func(); return false; });

        private async Task<T> Execute<T>(string message, Func<Task<T>> func)
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
                    cts.Cancel();
                    throw;
                }
            }

            line.Write($" Failed", ConsoleColor.Red);
            cts.Cancel();
            throw new Exception($"Failed after {retryCount} attempts");
        }

        private static void WriteExecutionTime(ConsoleEx.Line line, DateTime startTime)
            => line.Write($" Completed in {DateTime.UtcNow.Subtract(startTime).TotalSeconds:n0}s", ConsoleColor.Green);

        public static async Task<string> DownloadTextFromStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName, CancellationTokenSource cts)
        {
            var blobClient = await GetBlobClientAsync(storageAccount);
            var container = blobClient.GetBlobContainerClient(containerName);

            return (await container.GetBlobClient(blobName).DownloadContentAsync(cts.Token)).Value.Content.ToString();
        }

        private async Task UploadTextToStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName, string content)
            => await UploadTextToStorageAccountAsync(storageAccount, containerName, blobName, content, cts.Token);

        public static async Task UploadTextToStorageAccountAsync(IStorageAccount storageAccount, string containerName, string blobName, string content, CancellationToken token)
        {
            var blobClient = await GetBlobClientAsync(storageAccount);
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
