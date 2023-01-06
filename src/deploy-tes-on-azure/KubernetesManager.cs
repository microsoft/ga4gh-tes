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

namespace TesDeployer
{
    /// <summary>
    /// Class to hold all the kubernetes specific deployer logic.
    /// </summary>
    internal class KubernetesManager
    {
        private static readonly AsyncRetryPolicy WorkloadReadyRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(12, retryAttempt => TimeSpan.FromSeconds(15));

        private static readonly AsyncRetryPolicy KubeExecRetryPolicy = Policy
            .Handle<WebSocketException>(ex => ex.WebSocketErrorCode == WebSocketError.NotAWebSocket)
            .WaitAndRetryAsync(8, retryAttempt => TimeSpan.FromSeconds(5));

        // "master" is used despite not being a best practice: https://github.com/kubernetes-sigs/blob-csi-driver/issues/783
        private const string NginxIngressRepo = "https://kubernetes.github.io/ingress-nginx";
        private const string NginxIngressVersion = "4.4.2";
        private const string CertManagerRepo = "https://charts.jetstack.io";
        private const string CertManagerVersion = "v1.8.0";
        private const string AadPluginGithubReleaseVersion = "v1.8.13";
        private const string AadPluginRepo = $"https://raw.githubusercontent.com/Azure/aad-pod-identity/{AadPluginGithubReleaseVersion}/charts";
        private const string AadPluginVersion = "4.1.14";

        private Configuration configuration { get; set; }
        private AzureCredentials azureCredentials { get; set; }
        private CancellationTokenSource cts { get; set; }
        private string workingDirectoryTemp { get; set; }
        private string kubeConfigPath { get; set; }
        private string valuesTemplatePath { get; set; }
        private string helmScriptsRootDirectory { get; set; }
        public string TempHelmValuesYamlPath { get; set; }
        public string TesCname { get; set; }
        public string TesHostname { get; set; }
        public string AzureDnsLabelName { get; set; }

        public KubernetesManager(Configuration config, AzureCredentials credentials, CancellationTokenSource cts)
        {
            this.cts = cts;
            configuration = config;
            azureCredentials = credentials;

            CreateAndInitializeWorkingDirectoriesAsync().Wait();
        }

        public void SetTesIngressNetworkingConfiguration(string prefix)
        {
            const int maxCnLength = 64;
            var suffix = $".{configuration.RegionName}.cloudapp.azure.com";
            var prefixMaxLength = maxCnLength - suffix.Length;
            TesCname = GetTesCname(prefix, prefixMaxLength);
            TesHostname = $"{TesCname}{suffix}";
            AzureDnsLabelName = TesCname;
        }

        public async Task<IKubernetes> GetKubernetesClientAsync()
        {
            var containerServiceClient = new ContainerServiceClient(azureCredentials) { SubscriptionId = configuration.SubscriptionId };

            // Write kubeconfig in the working directory, because KubernetesClientConfiguration needs to read from a file, TODO figure out how to pass this directly. 
            var creds = await containerServiceClient.ManagedClusters.ListClusterAdminCredentialsAsync(configuration.ResourceGroupName, configuration.AksClusterName);
            var kubeConfigFile = new FileInfo(kubeConfigPath);
            await File.WriteAllTextAsync(kubeConfigFile.FullName, Encoding.Default.GetString(creds.Kubeconfigs.First().Value));
            kubeConfigFile.Refresh();

            var k8sConfiguration = KubernetesClientConfiguration.LoadKubeConfig(kubeConfigFile, false);
            var k8sClientConfiguration = KubernetesClientConfiguration.BuildConfigFromConfigObject(k8sConfiguration);
            return new Kubernetes(k8sClientConfiguration);
        }

        public async Task<IKubernetes> DeployCoADependenciesAsync(IResourceGroup _/*resourceGroup*/, IKubernetes client = default) //TODO: use or remove resourceGroup
        {
            client ??= await GetKubernetesClientAsync();

            var helmRepoList = await ExecHelmProcessAsync($"repo list", workingDirectory: null, throwOnNonZeroExitCode: false);

            if (string.IsNullOrWhiteSpace(helmRepoList) || !helmRepoList.Contains("aad-pod-identity", StringComparison.OrdinalIgnoreCase))
            {
                await ExecHelmProcessAsync($"repo add aad-pod-identity {AadPluginRepo}");
            }

            await ExecHelmProcessAsync($"repo update");
            await ExecHelmProcessAsync($"install aad-pod-identity aad-pod-identity/aad-pod-identity --namespace kube-system --version {AadPluginVersion} --kubeconfig {kubeConfigPath}");

            return client;
        }

        public async Task<IKubernetes> EnableIngress(string tesUsername, string tesPassword, IKubernetes client = default)
        {
            var certManagerRegistry = "quay.io";
            var certImageController = $"{certManagerRegistry}/jetstack/cert-manager-controller";
            var certImageWebhook = $"{certManagerRegistry}/jetstack/cert-manager-webhook";
            var certImageCainjector = $"{certManagerRegistry}/jetstack/cert-manager-cainjector";

            client ??= await GetKubernetesClientAsync();

            await client.CoreV1.CreateNamespaceAsync(new V1Namespace()
            {
                Metadata = new V1ObjectMeta
                {
                    Name = configuration.AksCoANamespace
                },
            });

            V1Namespace coaNamespace = null;
            try
            {
                coaNamespace = await client.CoreV1.ReadNamespaceAsync(configuration.AksCoANamespace);
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
                await client.CoreV1.CreateNamespaceAsync(coaNamespaceBody);
            }
            else
            {
                await client.CoreV1.PatchNamespaceAsync(new V1Patch(coaNamespaceBody, V1Patch.PatchType.MergePatch), configuration.AksCoANamespace);
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
            }, configuration.AksCoANamespace);

            var helmRepoList = await ExecHelmProcessAsync($"repo list", workingDirectory: null, throwOnNonZeroExitCode: false);

            if (string.IsNullOrWhiteSpace(helmRepoList) || !helmRepoList.Contains("ingress-nginx", StringComparison.OrdinalIgnoreCase))
            {
                await ExecHelmProcessAsync($"repo add ingress-nginx {NginxIngressRepo}");
            }

            if (string.IsNullOrWhiteSpace(helmRepoList) || !helmRepoList.Contains("jetstack", StringComparison.OrdinalIgnoreCase))
            {
                await ExecHelmProcessAsync($"repo add jetstack {CertManagerRepo}");
            }

            await ExecHelmProcessAsync($"repo update");

            var dnsAnnotation = $"--set controller.service.annotations.\"service\\.beta\\.kubernetes\\.io/azure-dns-label-name\"={AzureDnsLabelName}";
            var healthProbeAnnotation = "--set controller.service.annotations.\"service\\.beta\\.kubernetes\\.io/azure-load-balancer-health-probe-request-path\"=/healthz";
            await ExecHelmProcessAsync($"install ingress-nginx ingress-nginx/ingress-nginx --namespace {configuration.AksCoANamespace} --kubeconfig {kubeConfigPath} --version {NginxIngressVersion} {healthProbeAnnotation} {dnsAnnotation}");
            await ExecHelmProcessAsync("install cert-manager jetstack/cert-manager " +
                    $"--namespace {configuration.AksCoANamespace} --kubeconfig {kubeConfigPath} " +
                    $"--version {CertManagerVersion} --set installCRDs=true " +
                    "--set nodeSelector.\"kubernetes\\.io/os\"=linux " +
                    $"--set image.repository={certImageController}  " +
                    $"--set image.tag={CertManagerVersion} " +
                    $"--set webhook.image.repository={certImageWebhook} " +
                    $"--set webhook.image.tag={CertManagerVersion} " +
                    $"--set cainjector.image.repository={certImageCainjector} " +
                    $"--set cainjector.image.tag={CertManagerVersion}");

            await WaitForWorkloadAsync(client, "ingress-nginx-controller", configuration.AksCoANamespace, cts.Token);

            return client;
        }

        public async Task<IKubernetes> DeployHelmChartToClusterAsync(IKubernetes client = default)
        {
            client ??= await GetKubernetesClientAsync();

            // https://helm.sh/docs/helm/helm_upgrade/
            // The chart argument can be either: a chart reference('example/mariadb'), a path to a chart directory, a packaged chart, or a fully qualified URL
            await ExecHelmProcessAsync($"upgrade --install tesonazure ./helm --kubeconfig {kubeConfigPath} --namespace {configuration.AksCoANamespace} --create-namespace",
                workingDirectory: workingDirectoryTemp);
            await WaitForWorkloadAsync(client, "tes", configuration.AksCoANamespace, cts.Token);

            return client;
        }


        public async Task UpdateHelmValuesAsync(IStorageAccount storageAccount, string keyVaultUrl, string resourceGroupName, Dictionary<string, string> settings, IIdentity managedId)
        {
            var values = KubernetesYaml.Deserialize<HelmValues>(await File.ReadAllTextAsync(valuesTemplatePath));
            UpdateValuesFromSettings(values, settings);
            values.Config["resourceGroup"] = resourceGroupName;
            values.Identity["name"] = managedId.Name;
            values.Identity["resourceId"] = managedId.Id;
            values.Identity["clientId"] = managedId.ClientId;

            if (configuration.CrossSubscriptionAKSDeployment.GetValueOrDefault())
            {
                values.InternalContainersKeyVaultAuth = new List<Dictionary<string, string>>();

                foreach (var container in values.DefaultContainers)
                {
                    var containerConfig = new Dictionary<string, string>()
                    {
                        { "accountName",  storageAccount.Name },
                        { "containerName", container },
                        { "keyVaultURL", keyVaultUrl },
                        { "keyVaultSecretName", Deployer.StorageAccountKeySecretName}
                    };

                    values.InternalContainersKeyVaultAuth.Add(containerConfig);
                }
            }
            else
            {
                values.InternalContainersMIAuth = new List<Dictionary<string, string>>();

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
            await File.WriteAllTextAsync(TempHelmValuesYamlPath, valuesString);
            await Deployer.UploadTextToStorageAccountAsync(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml", valuesString, cts.Token);
        }

        public async Task UpgradeValuesYamlAsync(IStorageAccount storageAccount, Dictionary<string, string> settings)
        {
            var values = KubernetesYaml.Deserialize<HelmValues>(await Deployer.DownloadTextFromStorageAccountAsync(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml", cts));
            UpdateValuesFromSettings(values, settings);
            var valuesString = KubernetesYaml.Serialize(values);
            await File.WriteAllTextAsync(TempHelmValuesYamlPath, valuesString);
            await Deployer.UploadTextToStorageAccountAsync(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml", valuesString, cts.Token);
        }

        public async Task<Dictionary<string, string>> GetAKSSettingsAsync(IStorageAccount storageAccount)
        {
            var values = KubernetesYaml.Deserialize<HelmValues>(await Deployer.DownloadTextFromStorageAccountAsync(storageAccount, Deployer.ConfigurationContainerName, "aksValues.yaml", cts));
            return ValuesToSettings(values);
        }

        public async Task ExecuteCommandsOnPodAsync(IKubernetes client, string podName, IEnumerable<string[]> commands, string podNamespace)
        {
            var printHandler = new ExecAsyncCallback(async (stdIn, stdOut, stdError) =>
            {
                using (var reader = new StreamReader(stdOut))
                {
                    var line = await reader.ReadLineAsync();

                    while (line is not null)
                    {
                        if (configuration.DebugLogging)
                        {
                            ConsoleEx.WriteLine(podName + ": " + line);
                        }
                        line = await reader.ReadLineAsync();
                    }
                }

                using (var reader = new StreamReader(stdError))
                {
                    var line = await reader.ReadLineAsync();

                    while (line is not null)
                    {
                        if (configuration.DebugLogging)
                        {
                            ConsoleEx.WriteLine(podName + ": " + line);
                        }
                        line = await reader.ReadLineAsync();
                    }
                }
            });

            var pods = await client.CoreV1.ListNamespacedPodAsync(podNamespace);
            var workloadPod = pods.Items.Where(x => x.Metadata.Name.Contains(podName)).FirstOrDefault();

            if (!await WaitForWorkloadAsync(client, podName, podNamespace, cts.Token))
            {
                throw new Exception($"Timed out waiting for {podName} to start.");
            }

            // Pod Exec can fail even after the pod is marked ready.
            // Retry on WebSocketExceptions for up to 40 secs.
            var result = await KubeExecRetryPolicy.ExecuteAndCaptureAsync(async () =>
            {
                foreach (var command in commands)
                {
                    await client.NamespacedPodExecAsync(workloadPod.Metadata.Name, podNamespace, podName, command, true, printHandler, CancellationToken.None);
                }
            });

            if (result.Outcome != OutcomeType.Successful && result.FinalException is not null)
            {
                throw result.FinalException;
            }
        }

        public async Task UpgradeAKSDeploymentAsync(Dictionary<string, string> settings, IStorageAccount storageAccount)
        {
            await UpgradeValuesYamlAsync(storageAccount, settings);
            _ = await DeployHelmChartToClusterAsync();
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
                await Utility.WriteEmbeddedFilesAsync(helmScriptsRootDirectory, "scripts", "helm");
            }
            catch (Exception exc)
            {
                ConsoleEx.WriteLine(exc.ToString());
                throw;
            }
        }

        private static void UpdateValuesFromSettings(HelmValues values, Dictionary<string, string> settings)
        {
            values.Config["tesOnAzureVersion"] = settings["TesOnAzureVersion"];
            values.Config["azureServicesAuthConnectionString"] = settings["AzureServicesAuthConnectionString"];
            values.Config["applicationInsightsAccountName"] = settings["ApplicationInsightsAccountName"];
            values.Config["cosmosDb__accountName"] = settings["CosmosDbAccountName"];
            values.Config["batchAccount__accountName"] = settings["BatchAccountName"];
            values.Config["batchNodesSubnetId"] = settings["BatchNodesSubnetId"];
            values.Config["coaNamespace"] = settings["AksCoANamespace"];
            values.Config["disableBatchNodesPublicIpAddress"] = settings["DisableBatchNodesPublicIpAddress"];
            values.Config["disableBatchScheduling"] = settings["DisableBatchScheduling"];
            values.Config["usePreemptibleVmsOnly"] = settings["UsePreemptibleVmsOnly"];
            values.Config["blobxferImageName"] = settings["BlobxferImageName"];
            values.Config["dockerInDockerImageName"] = settings["DockerInDockerImageName"];
            values.Config["batchImageOffer"] = settings["BatchImageOffer"];
            values.Config["batchImagePublisher"] = settings["BatchImagePublisher"];
            values.Config["batchImageSku"] = settings["BatchImageSku"];
            values.Config["batchImageVersion"] = settings["BatchImageVersion"];
            values.Config["batchNodeAgentSkuId"] = settings["BatchNodeAgentSkuId"];
            values.Config["marthaUrl"] = settings["MarthaUrl"];
            values.Config["marthaKeyVaultName"] = settings["MarthaKeyVaultName"];
            values.Config["marthaSecretName"] = settings["MarthaSecretName"];
            values.Config["crossSubscriptionAKSDeployment"] = settings["CrossSubscriptionAKSDeployment"];
            //values.Config["postgreSqlServerName"] = settings["PostgreSqlServerName"];
            //values.Config["postgreSqlDatabaseName"] = settings["PostgreSqlDatabaseName"];
            //values.Config["postgreSqlUserLogin"] = settings["PostgreSqlUserLogin"];
            //values.Config["postgreSqlUserPassword"] = settings["PostgreSqlUserPassword"];
            //values.Config["usePostgreSqlSingleServer"] = settings["UsePostgreSqlSingleServer"];
            values.Images["tes"] = settings["TesImageName"];
            values.Service["tesHostname"] = settings["TesHostname"];
            values.Service["enableIngress"] = settings["EnableIngress"];
            values.Config["letsEncryptEmail"] = settings["LetsEncryptEmail"];
            values.Persistence["storageAccount"] = settings["DefaultStorageAccountName"];
        }

        private static Dictionary<string, string> ValuesToSettings(HelmValues values)
            => new()
            {
                ["TesOnAzureVersion"] = values.Config["tesOnAzureVersion"],
                ["AzureServicesAuthConnectionString"] = values.Config["azureServicesAuthConnectionString"],
                ["ApplicationInsightsAccountName"] = values.Config["applicationInsightsAccountName"],
                ["CosmosDbAccountName"] = values.Config["cosmosDbAccountName"],
                ["BatchAccountName"] = values.Config["batchAccountName"],
                ["BatchNodesSubnetId"] = values.Config["batchNodesSubnetId"],
                ["AksCoANamespace"] = values.Config["coaNamespace"],
                ["DisableBatchNodesPublicIpAddress"] = values.Config["disableBatchNodesPublicIpAddress"],
                ["DisableBatchScheduling"] = values.Config["disableBatchScheduling"],
                ["UsePreemptibleVmsOnly"] = values.Config["usePreemptibleVmsOnly"],
                ["BlobxferImageName"] = values.Config["blobxferImageName"],
                ["DockerInDockerImageName"] = values.Config["dockerInDockerImageName"],
                ["BatchImageOffer"] = values.Config["batchImageOffer"],
                ["BatchImagePublisher"] = values.Config["batchImagePublisher"],
                ["BatchImageSku"] = values.Config["batchImageSku"],
                ["BatchImageVersion"] = values.Config["batchImageVersion"],
                ["BatchNodeAgentSkuId"] = values.Config["batchNodeAgentSkuId"],
                ["MarthaUrl"] = values.Config["marthaUrl"],
                ["MarthaKeyVaultName"] = values.Config["marthaKeyVaultName"],
                ["MarthaSecretName"] = values.Config["marthaSecretName"],
                ["CrossSubscriptionAKSDeployment"] = values.Config["crossSubscriptionAKSDeployment"],
                //["PostgreSqlServerName"] = values.Config["postgreSqlServerName"],
                //["PostgreSqlDatabaseName"] = values.Config["postgreSqlDatabaseName"],
                //["PostgreSqlUserLogin"] = values.Config["postgreSqlUserLogin"],
                //["PostgreSqlUserPassword"] = values.Config["postgreSqlUserPassword"],
                //["UsePostgreSqlSingleServer"] = values.Config["usePostgreSqlSingleServer"],
                ["ManagedIdentityClientId"] = values.Identity["clientId"],
                ["TesImageName"] = values.Images["tes"],
                ["TesHostname"] = values.Service["tesHostname"],
                ["EnableIngress"] = values.Service["enableIngress"],
                ["LetsEncryptEmail"] = values.Config["letsEncryptEmail"],
                ["DefaultStorageAccountName"] = values.Persistence["storageAccount"],
            };

        /// <summary>
        /// Return a cname derived from the resource group name
        /// </summary>
        /// <param name="maxLength">Max length of the cname</param>
        /// <returns></returns>
        private string GetTesCname(string prefix, int maxLength = 40)
        {
            var tempCname = SdkContext.RandomResourceName($"{prefix.Replace(".", "")}-", maxLength);

            if (tempCname.Length > maxLength)
            {
                tempCname = tempCname.Substring(0, maxLength);
            }

            return tempCname.TrimEnd('-').ToLowerInvariant();
        }

        private async Task<string> ExecHelmProcessAsync(string command, string workingDirectory = null, bool throwOnNonZeroExitCode = true)
        {
            var process = new Process();
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.StartInfo.FileName = configuration.HelmBinaryPath;
            process.StartInfo.Arguments = command;

            if (!string.IsNullOrWhiteSpace(workingDirectory))
            {
                process.StartInfo.WorkingDirectory = workingDirectory;
            }

            process.Start();

            var outputStringBuilder = new StringBuilder();

            _ = Task.Run(async () =>
            {
                var line = (await process.StandardOutput.ReadLineAsync())?.Trim();

                while (line is not null)
                {
                    if (configuration.DebugLogging)
                    {
                        ConsoleEx.WriteLine($"HELM: {line}");
                    }

                    outputStringBuilder.AppendLine(line);
                    line = await process.StandardOutput.ReadLineAsync().WaitAsync(cts.Token);
                }
            });

            _ = Task.Run(async () =>
            {
                var line = (await process.StandardError.ReadLineAsync())?.Trim();

                while (line is not null)
                {
                    if (configuration.DebugLogging)
                    {
                        ConsoleEx.WriteLine($"HELM: {line}");
                    }

                    outputStringBuilder.AppendLine(line);
                    line = await process.StandardError.ReadLineAsync().WaitAsync(cts.Token);
                }
            });

            await process.WaitForExitAsync();
            var output = outputStringBuilder.ToString();

            if (throwOnNonZeroExitCode && process.ExitCode != 0)
            {
                foreach (var line in output.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries))
                {
                    ConsoleEx.WriteLine($"HELM: {line}");
                }

                Debugger.Break();
                throw new Exception($"HELM ExitCode = {process.ExitCode}");
            }

            return output;
        }

        private static async Task<bool> WaitForWorkloadAsync(IKubernetes client, string deploymentName, string deploymentNamespace, CancellationToken cancellationToken)
        {
            var deployments = await client.AppsV1.ListNamespacedDeploymentAsync(deploymentNamespace, cancellationToken: cancellationToken);
            var deployment = deployments.Items.Where(x => x.Metadata.Name.Equals(deploymentName, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();

            var result = await WorkloadReadyRetryPolicy.ExecuteAndCaptureAsync(async () =>
            {
                deployments = await client.AppsV1.ListNamespacedDeploymentAsync(deploymentNamespace, cancellationToken: cancellationToken);
                deployment = deployments.Items.Where(x => x.Metadata.Name.Equals(deploymentName, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();

                if ((deployment?.Status?.ReadyReplicas ?? 0) < 1)
                {
                    throw new Exception("Workload not ready.");
                }
            });

            return result.Outcome == OutcomeType.Successful;
        }

        private class HelmValues
        {
            public Dictionary<string, string> Service { get; set; }
            public Dictionary<string, string> Config { get; set; }
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
