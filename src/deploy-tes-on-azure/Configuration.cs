﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Reflection;
using CommonUtilities.AzureCloud;
using Microsoft.Extensions.Configuration;

namespace TesDeployer
{
    public class Configuration : UserAccessibleConfiguration
    {
        public string PostgreSqlAdministratorLogin { get; set; } = "tes_admin";
        public string PostgreSqlAdministratorPassword { get; set; }
        public string PostgreSqlTesDatabaseName { get; set; } = "tes_db";
        public string PostgreSqlTesUserLogin { get; set; } = "tes";
        public string PostgreSqlTesUserPassword { get; set; }
        public string PostgreSqlSkuName { get; set; } = "Standard_B2s";
        public string PostgreSqlTier { get; set; } = "Burstable";
        public string DefaultVmSubnetName { get; set; } = "vmsubnet";
        public string PostgreSqlVersion { get; set; } = "14";
        public string DefaultPostgreSqlSubnetName { get; set; } = "sqlsubnet";
        public string DefaultBatchSubnetName { get; set; } = "batchsubnet";
        public int PostgreSqlStorageSize { get; set; } = 128;  // GiB
    }

    public abstract class UserAccessibleConfiguration
    {
        public string AzureCloudName { get; set; } = AzureCloudConfig.DefaultAzureCloudName;
        public string BatchPrefix { get; set; }
        public string SubscriptionId { get; set; }
        public string RegionName { get; set; }
        public string MainIdentifierPrefix { get; set; } = "tes";
        public string VmSize { get; set; } = "Standard_D4s_v3";
        public string VnetAddressSpace { get; set; } = "10.1.0.0/16"; // 10.1.0.0 - 10.1.255.255, 65536 IPs
        // Address space for TES services.
        public string VmSubnetAddressSpace { get; set; } = "10.1.0.0/24"; // 10.1.0.0 - 10.1.0.255, 256 IPs
        public string PostgreSqlSubnetAddressSpace { get; set; } = "10.1.1.0/24"; // 10.1.1.0 - 10.1.1.255, 256 IPs
        // Address space for kubernetes system services, must not overlap with any subnet.
        public string KubernetesServiceCidr = "10.1.4.0/22"; // 10.1.4.0 -> 10.1.7.255, 1024 IPs
        public string KubernetesDnsServiceIP = "10.1.4.10";
        public string KubernetesDockerBridgeCidr = "172.17.0.1/16"; // 172.17.0.0 - 172.17.255.255, 65536 IPs
        public string BatchNodesSubnetAddressSpace { get; set; } = "10.1.128.0/17"; // 10.1.128.0 - 10.1.255.255, 32768 IPs

        public string ResourceGroupName { get; set; }
        public string BatchAccountName { get; set; }
        public string StorageAccountName { get; set; }
        //public string NetworkSecurityGroupName { get; set; }
        public string LogAnalyticsArmId { get; set; }
        public string ApplicationInsightsAccountName { get; set; }
        public string AksClusterName { get; set; }
        public string AksCoANamespace { get; set; } = "tes";
        public bool ManualHelmDeployment { get; set; }
        public string HelmBinaryPath { get; set; } = OperatingSystem.IsWindows() ? @"C:\ProgramData\chocolatey\bin\helm.exe" : "/usr/local/bin/helm";
        public string KubectlBinaryPath { get; set; } = OperatingSystem.IsWindows() ? @"C:\ProgramData\chocolatey\bin\kubectl.exe" : "/usr/local/bin/kubectl";
        public int AksPoolSize { get; set; } = 2;
        public bool? CrossSubscriptionAKSDeployment { get; set; } = null;
        public bool Silent { get; set; }
        public bool RunIntTests { get; set; }
        public bool DeleteResourceGroupOnFailure { get; set; }
        public string AcrId { get; set; }
        public string GitHubCommit { get; set; }
        public string SolutionDir { get; set; }
        public string TesImageName { get; set; }
        public bool SkipTestWorkflow { get; set; } = false;
        public bool Update { get; set; } = false;
        public string VnetResourceGroupName { get; set; }
        public string VnetName { get; set; }
        public string SubnetName { get; set; }
        public string VmSubnetName { get; set; }
        public string PostgreSqlSubnetName { get; set; }
        public string BatchSubnetName { get; set; }
        public bool? PrivateNetworking { get; set; } = null;
        public string Tags { get; set; } = null;
        public string BatchNodesSubnetId { get; set; } = null;
        public bool? DisableBatchNodesPublicIpAddress { get; set; } = null;
        public bool DebugLogging { get; set; } = false;
        public string PostgreSqlServerName { get; set; }
        public string PostgreSqlServerNameSuffix { get; set; } = null;
        public int PostgreSqlServerPort { get; set; } = 5432;
        public string PostgreSqlServerSslMode { get; set; } = "VerifyFull";
        public string KeyVaultName { get; set; }
        public bool? EnableIngress { get; set; } = null;
        public bool? OutputTesCredentialsJson { get; set; } = true;
        public string LetsEncryptEmail { get; set; } = null;
        public string TesUsername { get; set; } = "tes";
        public string TesPassword { get; set; }
        public string AadGroupIds { get; set; }
        public string DeploymentOrganizationName { get; set; }
        public string DeploymentOrganizationUrl { get; set; }
        public string DeploymentContactUri { get; set; }
        public string DeploymentEnvironment { get; set; }
        public string PrivateTestUbuntuImage { get; set; } = "mcr.microsoft.com/mirror/docker/library/ubuntu:24.04"; // mcr's docker mirror does not host "latest"
        public string PrivatePSQLUbuntuImage { get; set; } = "mcr.microsoft.com/mirror/docker/library/ubuntu:24.04"; // mcr's docker mirror does not host "latest"
        public string ServicePrincipalId { get; set; }

        public static Configuration BuildConfiguration(string[] args)
        {
            const string configFilename = "config.json";

            var configBuilder = new ConfigurationBuilder();

            if (System.IO.File.Exists(configFilename))
            {
                configBuilder.AddJsonFile(configFilename);
            }

            var configurationSource = configBuilder.AddCommandLine(args).Build();
            var configurationProperties = typeof(UserAccessibleConfiguration).GetTypeInfo().DeclaredProperties.Select(p => p.Name).ToList();

            var invalidArguments = configurationSource.Providers
                .SelectMany(p => p.GetChildKeys(Enumerable.Empty<string>(), null))
                .Where(k => !configurationProperties.Contains(k, StringComparer.OrdinalIgnoreCase));

            if (invalidArguments.Any())
            {
                throw new ArgumentException($"Invalid argument(s): {string.Join(", ", invalidArguments)}");
            }

            var configuration = new Configuration();
            configurationSource.Bind(configuration);

            return configuration;
        }
    }
}
