// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.ResourceManager.ContainerService;
using Azure.ResourceManager.ManagedServiceIdentities;
using Azure.Storage.Blobs;
using CommonUtilities.AzureCloud;
using k8s;
using k8s.Models;
using Polly;
using Polly.Retry;

namespace TesDeployer
{
    /// <summary>
    /// Class to hold all the kubernetes specific deployer logic.
    /// </summary>
    public class KubernetesManager
    {
        private static readonly AsyncRetryPolicy WorkloadReadyRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(80, retryAttempt => TimeSpan.FromSeconds(15));

        private static readonly AsyncRetryPolicy KubeExecRetryPolicy = Policy
                    .Handle<WebSocketException>(ex => ex.WebSocketErrorCode == WebSocketError.NotAWebSocket)
                    .WaitAndRetryAsync(200, retryAttempt => TimeSpan.FromSeconds(5));

        private const string NginxIngressRepo = "https://kubernetes.github.io/ingress-nginx";
        private const string NginxIngressVersion = "4.7.1";
        private const string CertManagerRepo = "https://charts.jetstack.io";
        private const string CertManagerVersion = "v1.12.3";

        private Configuration configuration { get; }
        private AzureCloudConfig azureEndpoints { get; }
        private CancellationToken cancellationToken { get; }
        private string workingDirectoryTemp { get; set; }
        private string kubeConfigPath { get; set; }
        private string valuesTemplatePath { get; set; }
        public string helmScriptsRootDirectory { get; private set; }
        public string TempHelmValuesYamlPath { get; private set; }
        public string TesCname { get; private set; }
        public string TesHostname { get; set; }
        public string AzureDnsLabelName { get; private set; }

        public delegate BlobClient GetBlobClient(Azure.ResourceManager.Storage.StorageAccountData storageAccount, string containerName, string blobName);
        private readonly GetBlobClient getBlobClient;

        public KubernetesManager(Configuration config, AzureCloudConfig azureCloudConfig, GetBlobClient getBlobClient, CancellationToken cancellationToken)
        {
            this.azureEndpoints = azureCloudConfig;
            this.cancellationToken = cancellationToken;
            configuration = config;
            this.getBlobClient = getBlobClient;

            CreateAndInitializeWorkingDirectoriesAsync().Wait(cancellationToken);
        }

        public void SetTesIngressNetworkingConfiguration(string prefix)
        {
            const int maxCnLength = 64;
            var suffix = $".{configuration.RegionName}.cloudapp.{azureEndpoints.Domain}";
            var prefixMaxLength = maxCnLength - suffix.Length;
            TesCname = GetTesCname(prefix, prefixMaxLength);
            TesHostname = $"{TesCname}{suffix}";
            AzureDnsLabelName = TesCname;
        }

        public async Task<IKubernetes> GetKubernetesClientAsync(ContainerServiceManagedClusterResource managedCluster)
        {
            using MemoryStream kubeconfig = new((await managedCluster.GetClusterAdminCredentialsAsync(cancellationToken: cancellationToken)).Value.Kubeconfigs[0].Value, writable: false);

            // Write kubeconfig in the working directory as helm & kubctl need it.
            var kubeConfigFile = new FileInfo(kubeConfigPath);
            await File.WriteAllTextAsync(kubeConfigFile.FullName, Encoding.Default.GetString(kubeconfig.ToArray()), cancellationToken);
            kubeConfigFile.Refresh();

            if (!OperatingSystem.IsWindows())
            {
                kubeConfigFile.UnixFileMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
            }

            var k8sClientConfiguration = KubernetesClientConfiguration.BuildConfigFromConfigFile(kubeconfig);
            return new Kubernetes(k8sClientConfiguration);
        }

        public static (string, V1Deployment) GetUbuntuDeploymentTemplate(string ubuntuImage)
            => ("ubuntu", KubernetesYaml.Deserialize<V1Deployment>(
                $"""
                apiVersion: apps/v1
                kind: Deployment
                metadata:
                  labels:
                    io.kompose.service: ubuntu
                  name: ubuntu
                spec:
                  replicas: 1
                  selector:
                    matchLabels:
                      io.kompose.service: ubuntu
                  template:
                    metadata:
                      labels:
                        io.kompose.service: ubuntu
                    spec:
                      containers:
                      - name: ubuntu
                        image: {ubuntuImage}
                        command: ["/bin/bash", "-c", "--"]
                        args: ["while true; do sleep 30; done;"]
                """));

        /// <summary>
        /// Enable ingress for TES
        /// See: https://cert-manager.io/docs/tutorials/acme/nginx-ingress/
        /// </summary>
        /// <param name="tesUsername"></param>
        /// <param name="tesPassword"></param>
        /// <param name="client"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<IKubernetes> EnableIngress(string tesUsername, string tesPassword, IKubernetes client)
        {
            var certManagerRegistry = "quay.io";
            var certImageController = $"{certManagerRegistry}/jetstack/cert-manager-controller";
            var certImageWebhook = $"{certManagerRegistry}/jetstack/cert-manager-webhook";
            var certImageCainjector = $"{certManagerRegistry}/jetstack/cert-manager-cainjector";

            V1Namespace coaNamespace = null;
            try
            {
                coaNamespace = await client.CoreV1.ReadNamespaceAsync(configuration.AksCoANamespace, cancellationToken: cancellationToken);
            }
            catch { }

            var coaNamespaceBody = new V1Namespace()
            {
                Metadata = new V1ObjectMeta
                {
                    Name = configuration.AksCoANamespace,
                    Labels = new Dictionary<string, string>()
                    {
                        { "cert-manager.io/disable-validation", "true" }
                    }
                },
            };

            if (coaNamespace == null)
            {
                await client.CoreV1.CreateNamespaceAsync(coaNamespaceBody, cancellationToken: cancellationToken);
            }
            else
            {
                await client.CoreV1.PatchNamespaceAsync(new V1Patch(coaNamespaceBody, V1Patch.PatchType.MergePatch), configuration.AksCoANamespace, cancellationToken: cancellationToken);
            }

            // Encryption options: https://httpd.apache.org/docs/2.4/misc/password_encryptions.html
            // APR1 is would be better,but need to find a c# library for it. http://svn.apache.org/viewvc/apr/apr/trunk/crypto/apr_md5.c?view=markup
            var format = "{SHA}";
            var hash = SHA1.HashData(Encoding.UTF8.GetBytes(tesPassword));
            var data = Encoding.UTF8.GetBytes($"{tesUsername}:{format}{Convert.ToBase64String(hash)}");

            await client.CoreV1.CreateNamespacedSecretAsync(new V1Secret()
            {
                Metadata = new V1ObjectMeta()
                {
                    Name = "tes-basic-auth"
                },
                Data = new Dictionary<string, byte[]>()
                    {
                        { "auth", data}
                    },
                Type = "Opaque"
            }, configuration.AksCoANamespace, cancellationToken: cancellationToken);

            var helmRepoList = await ExecHelmProcessAsync($"repo list", workingDirectory: null, throwOnNonZeroExitCode: false);

            foreach (var (helmCmd, throwOnNonZeroExitCode) in EnsureUpdateHelmRepo(helmRepoList, "ingress-nginx", NginxIngressRepo).Concat(EnsureUpdateHelmRepo(helmRepoList, "jetstack", CertManagerRepo)))
            {
                await ExecHelmProcessAsync(helmCmd, throwOnNonZeroExitCode: throwOnNonZeroExitCode);
            }

            await ExecHelmProcessAsync($"repo update");

            var dnsAnnotation = $"--set controller.service.annotations.\"service\\.beta\\.kubernetes\\.io/azure-dns-label-name\"={AzureDnsLabelName}";
            var healthProbeAnnotation = "--set controller.service.annotations.\"service\\.beta\\.kubernetes\\.io/azure-load-balancer-health-probe-request-path\"=/healthz";
            await ExecHelmProcessAsync($"install ingress-nginx ingress-nginx/ingress-nginx --namespace {configuration.AksCoANamespace} --kubeconfig \"{kubeConfigPath}\" --version {NginxIngressVersion} {healthProbeAnnotation} {dnsAnnotation}");
            await ExecHelmProcessAsync("install cert-manager jetstack/cert-manager " +
                    $"--namespace {configuration.AksCoANamespace} --kubeconfig \"{kubeConfigPath}\" " +
                    $"--version {CertManagerVersion} --set installCRDs=true " +
                    "--set nodeSelector.\"kubernetes\\.io/os\"=linux " +
                    $"--set image.repository={certImageController}  " +
                    $"--set image.tag={CertManagerVersion} " +
                    $"--set webhook.image.repository={certImageWebhook} " +
                    $"--set webhook.image.tag={CertManagerVersion} " +
                    $"--set cainjector.image.repository={certImageCainjector} " +
                    $"--set cainjector.image.tag={CertManagerVersion}");

            await WaitForWorkloadAsync(client, "ingress-nginx-controller", configuration.AksCoANamespace, cancellationToken);
            await WaitForWorkloadAsync(client, "cert-manager", configuration.AksCoANamespace, cancellationToken);

            // Wait 10 secs before deploying TES for cert manager to finish starting. 
            await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);

            return client;

            static IEnumerable<(string HelmCmd, bool ThrowOnNonZeroExitCode)> EnsureUpdateHelmRepo(string helmRepoList, string helmRepo, string helmRepoUri)
            {
                if (string.IsNullOrWhiteSpace(helmRepoList) || !helmRepoList.Contains(helmRepo, StringComparison.OrdinalIgnoreCase))
                {
                    return [($"repo add {helmRepo} {helmRepoUri}", true)];
                }
                else if (!helmRepoList.Contains(helmRepoUri, StringComparison.OrdinalIgnoreCase))
                {
                    return
                    [
                        ($"repo remove {helmRepo}", false),
                        ($"repo add {helmRepo} {helmRepoUri}", true)
                    ];
                }
                else
                {
                    return [];
                }
            }
        }

        public async Task DeployHelmChartToClusterAsync(IKubernetes kubernetesClient)
        {
            // https://helm.sh/docs/helm/helm_upgrade/
            // The chart argument can be either: a chart reference('example/mariadb'), a path to a chart directory, a packaged chart, or a fully qualified URL
            await ExecHelmProcessAsync($"upgrade --install tesonazure ./helm --kubeconfig \"{kubeConfigPath}\" --namespace {configuration.AksCoANamespace} --create-namespace",
                workingDirectory: workingDirectoryTemp);
            await WaitForWorkloadAsync(kubernetesClient, "tes", configuration.AksCoANamespace, cancellationToken);
        }

        public async Task RemovePodAadChart()
        {
            await ExecHelmProcessAsync($"uninstall aad-pod-identity --namespace kube-system --kubeconfig \"{kubeConfigPath}\"", throwOnNonZeroExitCode: false);
        }

        public async Task<HelmValues> GetHelmValuesAsync(string valuesTemplatePath)
        {
            var templateText = await File.ReadAllTextAsync(valuesTemplatePath, cancellationToken);
            var values = KubernetesYaml.Deserialize<HelmValues>(templateText);
            return values;
        }

        public async Task UpdateHelmValuesAsync(Azure.ResourceManager.Storage.StorageAccountData storageAccount, Uri keyVaultUrl, string resourceGroupName, Dictionary<string, string> settings, UserAssignedIdentityData managedId)
        {
            var values = await GetHelmValuesAsync(valuesTemplatePath);
            UpdateValuesFromSettings(values, settings);
            values.Config["resourceGroup"] = resourceGroupName;
            values.Identity["name"] = managedId.Name;
            values.Identity["resourceId"] = managedId.Id.ToString();
            values.Identity["clientId"] = managedId.ClientId?.ToString("D");

            if (configuration.CrossSubscriptionAKSDeployment.GetValueOrDefault())
            {
                values.InternalContainersKeyVaultAuth = [];

                foreach (var container in values.DefaultContainers)
                {
                    var containerConfig = new Dictionary<string, string>()
                    {
                        { "accountName",  storageAccount.Name },
                        { "containerName", container },
                        { "keyVaultURL", keyVaultUrl.AbsoluteUri },
                        { "keyVaultSecretName", Deployer.StorageAccountKeySecretName}
                    };

                    values.InternalContainersKeyVaultAuth.Add(containerConfig);
                }
            }
            else
            {
                values.InternalContainersMIAuth = [];

                foreach (var container in values.DefaultContainers)
                {
                    var containerConfig = new Dictionary<string, string>()
                    {
                        { "accountName",  storageAccount.Name },
                        { "containerName", container },
                        { "resourceGroup", resourceGroupName },
                    };

                    values.InternalContainersMIAuth.Add(containerConfig);
                }
            }

            var valuesString = KubernetesYaml.Serialize(values);
            await File.WriteAllTextAsync(TempHelmValuesYamlPath, valuesString, cancellationToken);
            await Deployer.UploadTextToStorageAccountAsync(getBlobClient(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml"), valuesString, cancellationToken);
        }

        public async Task UpgradeValuesYamlAsync(Azure.ResourceManager.Storage.StorageAccountData storageAccount, Dictionary<string, string> settings, Version previousVersion)
        {
            var blobClient = getBlobClient(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml");
            var values = KubernetesYaml.Deserialize<HelmValues>(await Deployer.DownloadTextFromStorageAccountAsync(blobClient, cancellationToken));
            UpdateValuesFromSettings(values, settings);
            ProcessHelmValuesUpdates(values, previousVersion);
            var valuesString = KubernetesYaml.Serialize(values);
            await File.WriteAllTextAsync(TempHelmValuesYamlPath, valuesString, cancellationToken);
            await Deployer.UploadTextToStorageAccountAsync(blobClient, valuesString, cancellationToken);
        }

        private static void ProcessHelmValuesUpdates(HelmValues values, Version previousVersion)
        {
            if (previousVersion is null || previousVersion < new Version(5, 5, 2))
            {
                var datasettestinputs = values.ExternalSasContainers?.SingleOrDefault(container => container.TryGetValue("accountName", out var name) && "datasettestinputs".Equals(name, StringComparison.OrdinalIgnoreCase));

                if (datasettestinputs is not null)
                {
                    _ = values.ExternalSasContainers.Remove(datasettestinputs);

                    if (values.ExternalSasContainers.Count == 0)
                    {
                        values.ExternalSasContainers = null;
                    }
                }
            }
        }

        public async Task<FileInfo> ConfigureAltLocalValuesYamlAsync(string altName, Action<HelmValues> configure)
        {
            FileInfo altValues = new(Path.Combine(Path.GetDirectoryName(TempHelmValuesYamlPath), altName));
            var values = KubernetesYaml.Deserialize<HelmValues>(await File.ReadAllTextAsync(TempHelmValuesYamlPath, cancellationToken));
            configure(values);
            await File.WriteAllTextAsync(altValues.FullName, KubernetesYaml.Serialize(values), cancellationToken);
            return altValues;
        }

        public FileInfo SwapLocalValuesYaml(FileInfo file)
        {
            FileInfo backup = new(Path.Combine(file.DirectoryName, "backup.bak"));
            File.Replace(file.FullName, TempHelmValuesYamlPath, backup.FullName);
            return backup;
        }

        public void RestoreLocalValuesYaml(FileInfo backup)
        {
            File.Replace(backup.FullName, TempHelmValuesYamlPath, default);
        }

        public async Task<Dictionary<string, string>> GetAKSSettingsAsync(Azure.ResourceManager.Storage.StorageAccountData storageAccount)
        {
            var values = KubernetesYaml.Deserialize<HelmValues>(await Deployer.DownloadTextFromStorageAccountAsync(getBlobClient(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml"), cancellationToken));
            return ValuesToSettings(values);
        }

        public async Task ExecuteCommandsOnPodAsync(IKubernetes client, string podName, IEnumerable<string[]> commands, string podNamespace)
        {
            async Task StreamHandler(Stream stream)
            {
                using var reader = new StreamReader(stream);
                var line = await reader.ReadLineAsync(CancellationToken.None);

                while (line is not null)
                {
                    if (configuration.DebugLogging)
                    {
                        ConsoleEx.WriteLine(podName + ": " + line);
                    }
                    line = await reader.ReadLineAsync(CancellationToken.None);
                }
            }

            if (!await WaitForWorkloadAsync(client, podName, podNamespace, cancellationToken))
            {
                throw new Exception($"Timed out waiting for {podName} to start.");
            }

            var pods = await client.CoreV1.ListNamespacedPodAsync(podNamespace, cancellationToken: cancellationToken);
            var workloadPod = pods.Items.Where(x => x.Metadata.Name.Contains(podName)).FirstOrDefault();

            // Pod Exec can fail even after the pod is marked ready.
            // Retry on WebSocketExceptions for up to 40 secs.
            var result = await KubeExecRetryPolicy.ExecuteAndCaptureAsync(async token =>
            {
                foreach (var command in commands)
                {
                    // Debug: ConsoleEx.WriteLine($"Executing: {string.Join(' ', command)}");
                    _ = await client.NamespacedPodExecAsync(workloadPod.Metadata.Name, podNamespace, podName, command, true,
                        (stdIn, stdOut, stdError) => Task.WhenAll(StreamHandler(stdOut), StreamHandler(stdError)), CancellationToken.None);
                }
            }, cancellationToken);

            if (result.Outcome != OutcomeType.Successful && result.FinalException is not null)
            {
                throw result.FinalException;
            }
        }

        public void DeleteTempFiles()
        {
            if (Directory.Exists(workingDirectoryTemp))
            {
                Directory.Delete(workingDirectoryTemp, true);
            }
        }

        private async Task CreateAndInitializeWorkingDirectoriesAsync()
        {
            try
            {
                var workingDirectory = Directory.GetCurrentDirectory();
                workingDirectoryTemp = Path.Join(workingDirectory, "cromwell-on-azure");
                helmScriptsRootDirectory = Path.Join(workingDirectoryTemp, "helm");
                kubeConfigPath = Path.Join(workingDirectoryTemp, "aks", "kubeconfig.txt");
                TempHelmValuesYamlPath = Path.Join(helmScriptsRootDirectory, "values.yaml");
                valuesTemplatePath = Path.Join(helmScriptsRootDirectory, "values-template.yaml");
                Directory.CreateDirectory(helmScriptsRootDirectory);
                Directory.CreateDirectory(Path.GetDirectoryName(kubeConfigPath));
                await Utility.WriteEmbeddedFilesAsync(helmScriptsRootDirectory, cancellationToken, "scripts", "helm");
            }
            catch (Exception exc)
            {
                ConsoleEx.WriteLine(exc.ToString());
                throw;
            }
        }

        private static void UpdateValuesFromSettings(HelmValues values, Dictionary<string, string> settings)
        {
            var batchAccount = GetObjectFromConfig(values, "batchAccount") ?? new Dictionary<string, string>();
            var batchNodes = GetObjectFromConfig(values, "batchNodes") ?? new Dictionary<string, string>();
            var batchScheduling = GetObjectFromConfig(values, "batchScheduling") ?? new Dictionary<string, string>();
            var batchImageGen2 = GetObjectFromConfig(values, "batchImageGen2") ?? new Dictionary<string, string>();
            var batchImageGen1 = GetObjectFromConfig(values, "batchImageGen1") ?? new Dictionary<string, string>();
            var drsHub = GetObjectFromConfig(values, "drsHub") ?? new Dictionary<string, string>();
            var deployment = GetObjectFromConfig(values, "deployment") ?? new Dictionary<string, string>();

            values.Config["acrId"] = GetValueOrDefault(settings, "AcrId");
            values.Config["tesImage"] = GetValueOrDefault(settings, "TesImageName");
            values.Config["azureCloudName"] = GetValueOrDefault(settings, "AzureCloudName");
            values.Config["tesOnAzureVersion"] = GetValueOrDefault(settings, "TesOnAzureVersion");
            values.Config["azureServicesAuthConnectionString"] = GetValueOrDefault(settings, "AzureServicesAuthConnectionString");
            values.Config["applicationInsightsAccountName"] = GetValueOrDefault(settings, "ApplicationInsightsAccountName");
            batchAccount["accountName"] = GetValueOrDefault(settings, "BatchAccountName");
            batchNodes["subnetId"] = GetValueOrDefault(settings, "BatchNodesSubnetId");
            values.Config["coaNamespace"] = GetValueOrDefault(settings, "AksCoANamespace");
            batchNodes["disablePublicIpAddress"] = GetValueOrDefault(settings, "DisableBatchNodesPublicIpAddress");
            batchScheduling["usePreemptibleVmsOnly"] = GetValueOrDefault(settings, "UsePreemptibleVmsOnly");
            batchImageGen2["offer"] = GetValueOrDefault(settings, "Gen2BatchImageOffer");
            batchImageGen2["publisher"] = GetValueOrDefault(settings, "Gen2BatchImagePublisher");
            batchImageGen2["sku"] = GetValueOrDefault(settings, "Gen2BatchImageSku");
            batchImageGen2["version"] = GetValueOrDefault(settings, "Gen2BatchImageVersion");
            batchImageGen2["nodeAgentSkuId"] = GetValueOrDefault(settings, "Gen2BatchNodeAgentSkuId");
            batchImageGen1["offer"] = GetValueOrDefault(settings, "Gen1BatchImageOffer");
            batchImageGen1["publisher"] = GetValueOrDefault(settings, "Gen1BatchImagePublisher");
            batchImageGen1["sku"] = GetValueOrDefault(settings, "Gen1BatchImageSku");
            batchImageGen1["version"] = GetValueOrDefault(settings, "Gen1BatchImageVersion");
            batchImageGen1["nodeAgentSkuId"] = GetValueOrDefault(settings, "Gen1BatchNodeAgentSkuId");
            drsHub["url"] = GetValueOrDefault(settings, "DrsHubUrl");
            batchScheduling["prefix"] = GetValueOrDefault(settings, "BatchPrefix");
            values.Config["crossSubscriptionAKSDeployment"] = GetValueOrDefault(settings, "CrossSubscriptionAKSDeployment");
            values.Images["tes"] = GetValueOrDefault(settings, "ActualTesImageName");
            values.Service["tesHostname"] = GetValueOrDefault(settings, "TesHostname");
            values.Service["enableIngress"] = GetValueOrDefault(settings, "EnableIngress");
            values.Config["letsEncryptEmail"] = GetValueOrDefault(settings, "LetsEncryptEmail");
            values.Persistence["storageAccount"] = GetValueOrDefault(settings, "DefaultStorageAccountName");
            values.Persistence["executionsContainerName"] = GetValueOrDefault(settings, "ExecutionsContainerName");
            values.TesDatabase["serverName"] = GetValueOrDefault(settings, "PostgreSqlServerName");
            values.TesDatabase["serverNameSuffix"] = GetValueOrDefault(settings, "PostgreSqlServerNameSuffix");
            values.TesDatabase["serverPort"] = GetValueOrDefault(settings, "PostgreSqlServerPort");
            values.TesDatabase["serverSslMode"] = GetValueOrDefault(settings, "PostgreSqlServerSslMode");
            // Note: Notice "Tes" is omitted from the property name since it's now in the TesDatabase section
            values.TesDatabase["databaseName"] = GetValueOrDefault(settings, "PostgreSqlTesDatabaseName");
            values.TesDatabase["databaseUserLogin"] = GetValueOrDefault(settings, "PostgreSqlTesDatabaseUserLogin");
            values.TesDatabase["databaseUserPassword"] = GetValueOrDefault(settings, "PostgreSqlTesDatabaseUserPassword");
            deployment["organizationName"] = GetValueOrDefault(settings, "DeploymentOrganizationName");
            deployment["organizationUrl"] = GetValueOrDefault(settings, "DeploymentOrganizationUrl");
            deployment["contactUri"] = GetValueOrDefault(settings, "DeploymentContactUri");
            deployment["environment"] = GetValueOrDefault(settings, "DeploymentEnvironment");
            deployment["created"] = GetValueOrDefault(settings, "DeploymentCreated");
            deployment["updated"] = GetValueOrDefault(settings, "DeploymentUpdated");

            // ensure entries have values
            _ = batchNodes.TryAdd("contentMD5", "false");
            _ = batchScheduling.TryAdd("poolRotationForcedDays", "7");
            _ = batchScheduling.TryAdd("taskMaxWallClockTimeDays", "7");

            values.Config["batchAccount"] = batchAccount;
            values.Config["batchNodes"] = batchNodes;
            values.Config["batchScheduling"] = batchScheduling;
            values.Config["batchImageGen2"] = batchImageGen2;
            values.Config["batchImageGen1"] = batchImageGen1;
            values.Config["drsHub"] = drsHub;
            values.Config["deployment"] = deployment;
        }

        private static IDictionary<string, string> GetObjectFromConfig(HelmValues values, string key)
            => (values?.Config.TryGetValue(key, out var config) ?? false ? (config as IDictionary<object, object>) : null)?.ToDictionary(p => p.Key as string, p => p.Value as string);

        private static T GetValueOrDefault<T>(IDictionary<string, T> propertyBag, string key)
            => propertyBag.TryGetValue(key, out var value) ? value : default;

        private static Dictionary<string, string> ValuesToSettings(HelmValues values)
        {
            var batchAccount = GetObjectFromConfig(values, "batchAccount") ?? new Dictionary<string, string>();
            var batchNodes = GetObjectFromConfig(values, "batchNodes") ?? new Dictionary<string, string>();
            var batchScheduling = GetObjectFromConfig(values, "batchScheduling") ?? new Dictionary<string, string>();
            var batchImageGen2 = GetObjectFromConfig(values, "batchImageGen2") ?? new Dictionary<string, string>();
            var batchImageGen1 = GetObjectFromConfig(values, "batchImageGen1") ?? new Dictionary<string, string>();
            var drsHub = GetObjectFromConfig(values, "drsHub") ?? new Dictionary<string, string>();
            var deployment = GetObjectFromConfig(values, "deployment") ?? new Dictionary<string, string>();

            return new()
            {
                ["AcrId"] = GetValueOrDefault(values.Config, "acrId") as string,
                ["TesImageName"] = GetValueOrDefault(values.Config, "tesImage") as string ?? GetValueOrDefault(values.Images, "tes"),
                ["AzureCloudName"] = GetValueOrDefault(values.Config, "azureCloudName") as string,
                ["TesOnAzureVersion"] = GetValueOrDefault(values.Config, "tesOnAzureVersion") as string,
                ["AzureServicesAuthConnectionString"] = GetValueOrDefault(values.Config, "azureServicesAuthConnectionString") as string,
                ["ApplicationInsightsAccountName"] = GetValueOrDefault(values.Config, "applicationInsightsAccountName") as string,
                ["BatchAccountName"] = GetValueOrDefault(batchAccount, "accountName"),
                ["BatchNodesSubnetId"] = GetValueOrDefault(batchNodes, "subnetId"),
                ["AksCoANamespace"] = GetValueOrDefault(values.Config, "coaNamespace") as string,
                ["DisableBatchNodesPublicIpAddress"] = GetValueOrDefault(batchNodes, "disablePublicIpAddress"),
                ["UsePreemptibleVmsOnly"] = GetValueOrDefault(batchScheduling, "usePreemptibleVmsOnly"),
                ["Gen2BatchImageOffer"] = GetValueOrDefault(batchImageGen2, "offer"),
                ["Gen2BatchImagePublisher"] = GetValueOrDefault(batchImageGen2, "publisher"),
                ["Gen2BatchImageSku"] = GetValueOrDefault(batchImageGen2, "sku"),
                ["Gen2BatchImageVersion"] = GetValueOrDefault(batchImageGen2, "version"),
                ["Gen2BatchNodeAgentSkuId"] = GetValueOrDefault(batchImageGen2, "nodeAgentSkuId"),
                ["Gen1BatchImageOffer"] = GetValueOrDefault(batchImageGen1, "offer"),
                ["Gen1BatchImagePublisher"] = GetValueOrDefault(batchImageGen1, "publisher"),
                ["Gen1BatchImageSku"] = GetValueOrDefault(batchImageGen1, "sku"),
                ["Gen1BatchImageVersion"] = GetValueOrDefault(batchImageGen1, "version"),
                ["Gen1BatchNodeAgentSkuId"] = GetValueOrDefault(batchImageGen1, "nodeAgentSkuId"),
                ["DrsHubUrl"] = GetValueOrDefault(drsHub, "url"),
                ["BatchPrefix"] = GetValueOrDefault(batchScheduling, "prefix"),
                ["CrossSubscriptionAKSDeployment"] = GetValueOrDefault(values.Config, "crossSubscriptionAKSDeployment") as string,
                ["UsePostgreSqlSingleServer"] = GetValueOrDefault(values.Config, "usePostgreSqlSingleServer") as string,
                ["ManagedIdentityClientId"] = GetValueOrDefault(values.Identity, "clientId"),
                ["ActualTesImageName"] = string.IsNullOrEmpty(GetValueOrDefault(values.Config, "tesImage") as string) ? null : GetValueOrDefault(values.Images, "tes"),
                ["TesHostname"] = GetValueOrDefault(values.Service, "tesHostname"),
                ["EnableIngress"] = GetValueOrDefault(values.Service, "enableIngress"),
                ["LetsEncryptEmail"] = GetValueOrDefault(values.Config, "letsEncryptEmail") as string,
                ["DefaultStorageAccountName"] = GetValueOrDefault(values.Persistence, "storageAccount"),
                ["ExecutionsContainerName"] = GetValueOrDefault(values.Persistence, "executionsContainerName"),
                ["PostgreSqlServerName"] = GetValueOrDefault(values.TesDatabase, "serverName"),
                ["PostgreSqlServerNameSuffix"] = GetValueOrDefault(values.TesDatabase, "serverNameSuffix"),
                ["PostgreSqlServerPort"] = GetValueOrDefault(values.TesDatabase, "serverPort"),
                ["PostgreSqlServerSslMode"] = GetValueOrDefault(values.TesDatabase, "serverSslMode"),
                // Note: Notice "Tes" is added to the property name since it's coming from the TesDatabase section
                ["PostgreSqlTesDatabaseName"] = GetValueOrDefault(values.TesDatabase, "databaseName"),
                ["PostgreSqlTesDatabaseUserLogin"] = GetValueOrDefault(values.TesDatabase, "databaseUserLogin"),
                ["PostgreSqlTesDatabaseUserPassword"] = GetValueOrDefault(values.TesDatabase, "databaseUserPassword"),
                ["DeploymentOrganizationName"] = GetValueOrDefault(deployment, "organizationName"),
                ["DeploymentOrganizationUrl"] = GetValueOrDefault(deployment, "organizationUrl"),
                ["DeploymentContactUri"] = GetValueOrDefault(deployment, "contactUri"),
                ["DeploymentEnvironment"] = GetValueOrDefault(deployment, "environment"),
                ["DeploymentCreated"] = GetValueOrDefault(deployment, "created"),
                ["DeploymentUpdated"] = GetValueOrDefault(deployment, "updated"),
            };
        }

        /// <summary>
        /// Return a cname derived from the resource group name
        /// </summary>
        /// <param name="maxLength">Max length of the cname</param>
        /// <returns></returns>
        private static string GetTesCname(string prefix, int maxLength = 40)
        {
            var tempCname = Utility.RandomResourceName($"{prefix.Replace(".", "")}-", maxLength);

            if (tempCname.Length > maxLength)
            {
                tempCname = tempCname[..maxLength];
            }

            return tempCname.TrimEnd('-').ToLowerInvariant();
        }

        public Task<string> ExecKubectlProcessAsync(string command, CancellationToken cancellationToken, string workingDirectory = null, bool throwOnNonZeroExitCode = true, bool appendKubeconfig = false)
        {
            if (appendKubeconfig)
            {
                command = $"{command} --kubeconfig \"{kubeConfigPath}\"";
            }

            return ExecProcessAsync(configuration.KubectlBinaryPath, "KUBE", command, cancellationToken, workingDirectory, throwOnNonZeroExitCode);
        }

        private Task<string> ExecHelmProcessAsync(string command, string workingDirectory = null, bool throwOnNonZeroExitCode = true)
        {
            return ExecProcessAsync(configuration.HelmBinaryPath, "HELM", command, cancellationToken, workingDirectory, throwOnNonZeroExitCode);
        }

        private async Task<string> ExecProcessAsync(string binaryFullPath, string tag, string command, CancellationToken cancellationToken, string workingDirectory = null, bool throwOnNonZeroExitCode = true)
        {
            var outputStringBuilder = new StringBuilder();

            void OutputHandler(object sendingProcess, DataReceivedEventArgs outLine)
            {
                if (configuration.DebugLogging)
                {
                    ConsoleEx.WriteLine($"{tag}: {outLine.Data}");
                }

                outputStringBuilder.AppendLine(outLine.Data);
            }

            var process = new Process();

            try
            {
                process.StartInfo.UseShellExecute = false;
                process.StartInfo.RedirectStandardOutput = true;
                process.StartInfo.RedirectStandardError = true;
                process.StartInfo.FileName = binaryFullPath;
                process.StartInfo.Arguments = command;
                process.OutputDataReceived += OutputHandler;
                process.ErrorDataReceived += OutputHandler;

                if (!string.IsNullOrWhiteSpace(workingDirectory))
                {
                    process.StartInfo.WorkingDirectory = workingDirectory;
                }

                process.Start();
                process.BeginOutputReadLine();
                process.BeginErrorReadLine();
                await process.WaitForExitAsync(cancellationToken);
            }
            finally
            {
                if (cancellationToken.IsCancellationRequested && !process.HasExited)
                {
                    process.Kill();
                }
            }

            var output = outputStringBuilder.ToString();

            if (throwOnNonZeroExitCode && process.ExitCode != 0)
            {
                if (!configuration.DebugLogging) // already written to console
                {
                    foreach (var line in output.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries))
                    {
                        ConsoleEx.WriteLine($"{tag}: {line}");
                    }
                }

                Debugger.Break();
                throw new Exception($"{tag} ExitCode = {process.ExitCode}");
            }

            return output;
        }

        private static async Task<bool> WaitForWorkloadAsync(IKubernetes client, string deploymentName, string aksNamespace, CancellationToken cancellationToken)
        {
            var result = await WorkloadReadyRetryPolicy.ExecuteAndCaptureAsync(async token =>
            {
                var deployments = await client.AppsV1.ListNamespacedDeploymentAsync(aksNamespace, cancellationToken: token);
                var deployment = deployments.Items.Where(x => x.Metadata.Name.Equals(deploymentName, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();

                if ((deployment?.Status?.ReadyReplicas ?? 0) < 1)
                {
                    throw new Exception("Workload not ready.");
                }
            }, cancellationToken);

            return result.Outcome == OutcomeType.Successful;
        }

        public class HelmValues
        {
            public Dictionary<string, string> Service { get; set; }
            public Dictionary<string, object> Config { get; set; }
            public Dictionary<string, string> TesDatabase { get; set; }
            public Dictionary<string, string> Images { get; set; }
            public List<string> DefaultContainers { get; set; }
            public List<Dictionary<string, string>> InternalContainersMIAuth { get; set; }
            public List<Dictionary<string, string>> InternalContainersKeyVaultAuth { get; set; }
            public List<Dictionary<string, string>> ExternalContainers { get; set; }
            public List<Dictionary<string, string>> ExternalSasContainers { get; set; }
            public Dictionary<string, string> Persistence { get; set; }
            public Dictionary<string, string> Identity { get; set; }
            public Dictionary<string, string> Db { get; set; }
        }
    }
}
