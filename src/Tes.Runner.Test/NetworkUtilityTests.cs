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
        private readonly NetworkUtility utility = new NetworkUtility();

        [TestMethod]
        [TestCategory("Unit")]
        public async Task BlockAndUnblockAnIpAddressOnLinux()
        {
            if (!OperatingSystem.IsLinux())
            {
                // Not implemented; TES only supports Linux VMs
                return;
            }

            var uri = new Uri("https://www.microsoft.com");
            var ipAddress = Dns.GetHostAddresses(uri.Host).First().ToString();

            using var client = new HttpClient();
            await client.GetStringAsync(uri);

            await utility.BlockIpAddressOnLinuxAsync(ipAddress);

            try
            {
                await client.GetStringAsync(uri);
            }
            catch (HttpRequestException ex) when (ex.InnerException is WebException webException && webException.Status == WebExceptionStatus.ConnectFailure)
            {
                Console.WriteLine("Successfully blocked");
            }

            await utility.UnblockIpAddressOnLinuxAsync(ipAddress);
            await client.GetStringAsync(uri);
        }

        [TestMethod]
        [TestCategory("Unit")]
        public async Task BlockAndUnblockFunctionsAreIdempotentOnLinux()
        {
            if (!OperatingSystem.IsLinux())
            {
                // Not implemented; TES only supports Linux VMs
                return;
            }

            var uri = new Uri("https://www.microsoft.com");
            var ipAddress = Dns.GetHostAddresses(uri.Host).First().ToString();

            using var client = new HttpClient();
            await client.GetStringAsync(uri);

            await utility.BlockIpAddressOnLinuxAsync(ipAddress);
            await utility.BlockIpAddressOnLinuxAsync(ipAddress);

            try
            {
                await client.GetStringAsync(uri);
            }
            catch (Exception)
            {
                Console.WriteLine("Successfully blocked");
            }

            await utility.UnblockIpAddressOnLinuxAsync(ipAddress);
            await utility.UnblockIpAddressOnLinuxAsync(ipAddress);
            await client.GetStringAsync(uri);
        }
    }
}
