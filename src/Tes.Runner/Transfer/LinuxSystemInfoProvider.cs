// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Runtime.InteropServices;

namespace Tes.Runner.Transfer
{
    public class LinuxSystemInfoProvider : ISystemInfoProvider
    {
        const string ProcMemInfo = "/proc/meminfo";
        const string ProcCpuInfo = "/proc/cpuinfo";

        public LinuxSystemInfoProvider()
        {
            if (!IsLinuxSystem())
            {
                throw new InvalidOperationException("Invalid OS.");
            }
        }

        public static bool IsLinuxSystem()
        {
            return RuntimeInformation.IsOSPlatform(OSPlatform.Linux);
        }

        public int ProcessorCount { get; } = File.ReadLines(ProcCpuInfo).Count(line => line.StartsWith("processor"));

        public long TotalMemory { get; } = Convert.ToInt64(File.ReadLines(ProcMemInfo).First(line => line.StartsWith("MemTotal")).Split(' ', StringSplitOptions.RemoveEmptyEntries)[1]) * 1024;
    }
}
