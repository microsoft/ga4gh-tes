// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Tes.Runner.Transfer;

namespace Tes.Runner.Docker
{
    public class NetworkUtility
    {
        private const string defaultRuleChain = "DOCKER-USER";
        private readonly ILogger logger = PipelineLoggerFactory.Create<NetworkUtility>();

        /// <summary>
        /// Blocks or unblocks Docker container access to a specific IP address on linux
        /// </summary>
        /// <param name="ipAddress">The IP address to block</param>
        /// <param name="callerMemberName">The caller of the function</param>
        /// <returns></returns>
        public async Task BlockIpAddressAsync(string ipAddress, string ruleChain = defaultRuleChain)
        {
            if (!OperatingSystem.IsLinux())
            {
                // Not implemented; TES only supports Linux VMs
                return;
            }

            bool isBlocked = await CheckIfIpAddressIsBlockedAsync(ipAddress, ruleChain);

            if (!isBlocked)
            {
                await AddBlockRuleAsync(ipAddress, ruleChain);
            }
        }

        public async Task UnblockIpAddressAsync(string ipAddress, string ruleChain = defaultRuleChain)
        {
            if (!OperatingSystem.IsLinux())
            {
                // Not implemented; TES only supports Linux VMs
                return;
            }

            bool isBlocked = await CheckIfIpAddressIsBlockedAsync(ipAddress, ruleChain);

            if (isBlocked)
            {
                await RemoveBlockRuleAsync(ipAddress, ruleChain);
            }
        }

        private async Task<bool> CheckIfIpAddressIsBlockedAsync(string ipAddress, string ruleChain = defaultRuleChain)
        {
            string listRulesCommand = $"-S {ruleChain}";
            var outputAndError = await RunIptablesCommandAsync(listRulesCommand);
            return outputAndError.Output.Contains(ipAddress, StringComparison.OrdinalIgnoreCase);
        }

        private async Task AddBlockRuleAsync(string ipAddress, string ruleChain = defaultRuleChain)
        {
            string addRuleCommand = $"-A {ruleChain} -i eth0 -o eth0 -m conntrack --ctorigdst {ipAddress} -j DROP";
            _ = await RunIptablesCommandAsync(addRuleCommand);
        }

        private async Task RemoveBlockRuleAsync(string ipAddress, string ruleChain = defaultRuleChain)
        {
            string removeRuleCommand = $"-D {ruleChain} -i eth0 -o eth0 -m conntrack --ctorigdst {ipAddress} -j DROP";
            _ = await RunIptablesCommandAsync(removeRuleCommand);
        }

        /// <summary>
        /// Executes the "iptables" command in Linux
        /// </summary>
        /// <param name="arguments">Arguments to pass to iptables</param>
        /// <returns>A tuple with the output and error</returns>
        /// <exception cref="UnauthorizedAccessException"></exception>
        private async Task<(string Output, string Error)> RunIptablesCommandAsync(string arguments)
        {
            var process = new Process
            {
                StartInfo =
                {
                    FileName = "iptables",
                    Arguments = arguments,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                }
            };

            process.Start();
            await process.WaitForExitAsync();

            string output = await process.StandardOutput.ReadToEndAsync();
            string error = await process.StandardError.ReadToEndAsync();

            switch (process.ExitCode)
            {
                case 0:
                    return (output, error);
                case 4:
                    // iptables v1.8.7 (nf_tables): Could not fetch rule set generation id: Permission denied (you must be root)
                    var unauthorizedException = new UnauthorizedAccessException($"TES Runner and Tests must be run with 'sudo' or as a user with root privileges in order to execute 'iptables' to manage network access.\nError: {error}");
                    logger.LogError(unauthorizedException, unauthorizedException.Message);
                    throw unauthorizedException;
                default:
                    var exc = new Exception($"'iptables {arguments}' failed. Exit code: {process.ExitCode}\nOutput: {output}\nError: {error}");
                    logger.LogError(exc, exc.Message);
                    throw exc;
            }
        }
    }
}
