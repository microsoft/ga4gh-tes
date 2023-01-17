using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace TesApi.Web.Management.Models.Terra;

/// <summary>
/// Landing zone resource quota api response
/// </summary>
public class QuotaValuesApiResponse
{
    /// <summary>
    /// Pool quota
    /// </summary>
    [JsonPropertyName("poolQuota")]
    public int PoolQuota { get; set; }

    /// <summary>
    /// Dedicated core quota per vm family
    /// </summary>
    [JsonPropertyName("dedicatedCoreQuotaPerVMFamily")]
    public Dictionary<string, int> DedicatedCoreQuotaPerVmFamily { get; set; }

    /// <summary>
    /// Flag indicating whether dedicated core quota is enforced
    /// </summary>
    [JsonPropertyName("dedicatedCoreQuotaPerVMFamilyEnforced")]
    public bool DedicatedCoreQuotaPerVmFamilyEnforced { get; set; }

    /// <summary>
    /// Active and scheduled job quota
    /// </summary>
    [JsonPropertyName("activeJobAndJobScheduleQuota")]
    public int ActiveJobAndJobScheduleQuota { get; set; }

    /// <summary>
    /// Dedicated core quota
    /// </summary>
    [JsonPropertyName("dedicatedCoreQuota")]
    public int DedicatedCoreQuota { get; set; }

    /// <summary>
    /// Low priority core quota
    /// </summary>
    [JsonPropertyName("lowPriorityCoreQuota")]
    public int LowPriorityCoreQuota { get; set; }

}
