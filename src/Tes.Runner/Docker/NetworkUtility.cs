// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
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
        public async Task BlockIpAddressAsync(string ipAddress)
        {
            if (!OperatingSystem.IsLinux())
            {
                // Not implemented; TES only supports Linux VMs
                return;
            }

            bool isBlocked = await CheckIfIpAddressIsBlockedAsync(ipAddress);

            if (!isBlocked)
            {
                await AddBlockRuleAsync(ipAddress);
            }
        }

        public async Task UnblockIpAddressAsync(string ipAddress)
        {
            if (!OperatingSystem.IsLinux())
            {
                // Not implemented; TES only supports Linux VMs
                return;
            }

            bool isBlocked = await CheckIfIpAddressIsBlockedAsync(ipAddress);

            if (isBlocked)
            {
                await RemoveBlockRuleAsync(ipAddress);
            }
        }

        private async Task<bool> CheckIfIpAddressIsBlockedAsync(string ipAddress)
        {
            var output = await RunIptablesCommandAsync($"-S DOCKER-USER");
            return output.Contains(ipAddress, StringComparison.OrdinalIgnoreCase);
        }

        private async Task AddBlockRuleAsync(string ipAddress)
        {
            string addCommand = $"-A DOCKER-USER -i eth0 -o eth0 -m conntrack --ctorigdst {ipAddress} -j DROP";
            _ = await RunIptablesCommandAsync(addCommand);
        }

        private async Task RemoveBlockRuleAsync(string ipAddress)
        {
            string removeCommand = $"-D DOCKER-USER -i eth0 -o eth0 -m conntrack --ctorigdst {ipAddress} -j DROP";
            _ = await RunIptablesCommandAsync(removeCommand);
        }

        private async Task<string> RunIptablesCommandAsync(string arguments)
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
                var exc = new Exception($"'iptables {arguments}' failed. Exit code: {process.ExitCode}\nOutput: {output}\nError: {error}");
                logger.LogError(exc, exc.Message);
                throw exc;
            }

            return output;
        }
    }
}
