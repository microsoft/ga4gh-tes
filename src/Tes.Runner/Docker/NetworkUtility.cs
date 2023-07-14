// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Tes.Runner.Transfer;

namespace Tes.Runner.Docker
{
    public class NetworkUtility
    {
        private readonly ILogger logger = PipelineLoggerFactory.Create<NetworkUtility>();

        /// <summary>
        /// Blocks or unblocks Docker container access to a specific IP address on linux
        /// </summary>
        /// <param name="ipAddress">The IP address to block</param>
        /// <param name="callerMemberName">The caller of the function</param>
        /// <returns></returns>
        public async Task BlockIpAddressAsync(string ipAddress, [CallerMemberName] string callerMemberName = "")
        {
            if (!OperatingSystem.IsLinux())
            {
                // Not implemented; TES only supports Linux VMs
                return;
            }

            bool isBlocked = await CheckIfIpAddressIsBlockedAsync(ipAddress, callerMemberName);

            if (!isBlocked)
            {
                await AddBlockRuleAsync(ipAddress, callerMemberName);
            }
        }

        public async Task UnblockIpAddressAsync(string ipAddress, [CallerMemberName] string callerMemberName = "")
        {
            if (!OperatingSystem.IsLinux())
            {
                // Not implemented; TES only supports Linux VMs
                return;
            }

            bool isBlocked = await CheckIfIpAddressIsBlockedAsync(ipAddress, callerMemberName);

            if (isBlocked)
            {
                await RemoveBlockRuleAsync(ipAddress, callerMemberName);
            }
        }

        private async Task<bool> CheckIfIpAddressIsBlockedAsync(string ipAddress, string callerMemberName)
        {
            var output = await RunIptablesCommandAsync($"-S DOCKER-USER | grep {ipAddress} 2>&1", ipAddress, "checking", callerMemberName);
            return !string.IsNullOrWhiteSpace(output);
        }

        private async Task AddBlockRuleAsync(string ipAddress, string callerMemberName)
        {
            string addCommand = $"-A DOCKER-USER -i eth0 -o eth0 -m conntrack --ctorigdstaddr {ipAddress} -j DROP";
            await RunIptablesCommandAsync(addCommand, ipAddress, "blocking", callerMemberName);
        }

        private async Task RemoveBlockRuleAsync(string ipAddress, string callerMemberName)
        {
            string removeCommand = $"-D DOCKER-USER -i eth0 -o eth0 -m conntrack --ctorigdstaddr {ipAddress} -j DROP";
            await RunIptablesCommandAsync(removeCommand, ipAddress, "unblocking", callerMemberName);
        }

        private async Task<string> RunIptablesCommandAsync(string arguments, string ipAddress, string action, string callerMemberName)
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

            if (process.ExitCode != 0)
            {
                var exc = new Exception($"IP address {ipAddress} {action} failed in {callerMemberName}. Exit code: {process.ExitCode}\nOutput: {output}\nError: {error}");
                logger.LogError(exc, exc.Message);
                throw exc;
            }

            return output;
        }
    }
}
