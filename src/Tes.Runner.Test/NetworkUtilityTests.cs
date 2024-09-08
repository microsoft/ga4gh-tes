// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net;
using Tes.Runner.Docker;

namespace Tes.Runner.Test
{
    [TestClass]
    [TestCategory("Unit")]
    public class NetworkUtilityTests
    {
        private const string TestUrl = "https://github.com";
        private const string RuleChain = "OUTPUT";
        private readonly NetworkUtility utility = new(Microsoft.Extensions.Logging.Abstractions.NullLogger<NetworkUtility>.Instance);

        [TestMethod]
        [TestCategory("Unit")]
        [TestCategory("RequiresRoot")]
        public async Task BlockAndUnblockAnIpAddressOnLinux()
        {
            if (!OperatingSystem.IsLinux())
            {
                // Not implemented; TES only supports Linux VMs
                Console.WriteLine("Test did not run because host is not Linux.");
                Assert.Inconclusive();
                return;
            }

            var uri = new Uri(TestUrl);
            var ipAddress = Dns.GetHostAddresses(uri.Host).First().ToString();

            using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
            await client.GetStringAsync(uri);

            await utility.BlockIpAddressAsync(ipAddress, RuleChain);

            await Assert.ThrowsExceptionAsync<TaskCanceledException>(async () =>
            {
                // The request was canceled due to the configured HttpClient.Timeout of 10 seconds elapsing.
                await client.GetStringAsync(uri);
            }, "IP address was not blocked");

            Console.WriteLine($"Successfully blocked {ipAddress} ({TestUrl})");

            await utility.UnblockIpAddressAsync(ipAddress, RuleChain);
            await client.GetStringAsync(uri);
        }

        [TestMethod]
        [TestCategory("Unit")]
        [TestCategory("RequiresRoot")]
        public async Task BlockAndUnblockFunctionsAreIdempotentOnLinux()
        {
            if (!OperatingSystem.IsLinux())
            {
                // Not implemented; TES only supports Linux VMs
                Console.WriteLine("Test did not run because host is not Linux.");
                Assert.Inconclusive();
                return;
            }

            var uri = new Uri(TestUrl);
            var ipAddress = Dns.GetHostAddresses(uri.Host).First().ToString();

            using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
            await client.GetStringAsync(uri);

            await utility.BlockIpAddressAsync(ipAddress, RuleChain);
            await utility.BlockIpAddressAsync(ipAddress, RuleChain);

            await Assert.ThrowsExceptionAsync<TaskCanceledException>(async () =>
            {
                // The request was canceled due to the configured HttpClient.Timeout of 10 seconds elapsing.
                await client.GetStringAsync(uri);
            }, "IP address was not blocked");

            Console.WriteLine($"Successfully blocked {ipAddress} ({TestUrl})");

            await utility.UnblockIpAddressAsync(ipAddress, RuleChain);
            await utility.UnblockIpAddressAsync(ipAddress, RuleChain);
            await client.GetStringAsync(uri);
        }
    }
}
