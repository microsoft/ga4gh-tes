using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace TesApi.Web.Management.Models.Terra;

public class QuotaValuesApiResponse
{
    [JsonPropertyName("poolQuota")]
    public int PoolQuota { get; set; }

    [JsonPropertyName("dedicatedCoreQuotaPerVMFamily")]
    public Dictionary<string, int> DedicatedCoreQuotaPerVMFamily { get; set; }

    [JsonPropertyName("dedicatedCoreQuotaPerVMFamilyEnforced")]
    public bool DedicatedCoreQuotaPerVMFamilyEnforced { get; set; }

    [JsonPropertyName("activeJobAndJobScheduleQuota")]
    public int ActiveJobAndJobScheduleQuota { get; set; }

    [JsonPropertyName("dedicatedCoreQuota")]
    public int DedicatedCoreQuota { get; set; }

    [JsonPropertyName("lowPriorityCoreQuota")]
    public int LowPriorityCoreQuota { get; set; }

}
