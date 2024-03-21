// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Tes.Models;

namespace TesApi.Tests
{
    [TestClass]
    [Ignore]
    public class BatchSupportVmSizeInformationConverter
    {
        [TestMethod]
        [Ignore]
        public async Task ConvertAsync()
        {
            string sourceCloud = "AZURECLOUD";
            string destinationCloud = "AZUREUSGOVERNMENT";
            string sourcePath = $"../../../../TesApi.Web/BatchSupportedVmSizeInformation_{sourceCloud}.json";
            string destPath = $"../../../../TesApi.Web/BatchSupportedVmSizeInformation_{destinationCloud}.json";

            var vms = JsonConvert.DeserializeObject<List<VirtualMachineInformation>>(await File.ReadAllTextAsync(sourcePath));

            foreach (var vm in vms)
            {
                vm.RegionsAvailable = new List<string> { "usgovvirginia" };
            }

            var json = JsonConvert.SerializeObject(vms, Formatting.Indented);
            await File.WriteAllTextAsync(destPath, json);
        }
    }
}
