namespace TesApi.Web.Management.Models.Quotas;

/// <summary>
/// Account quota information
/// </summary>
/// <param name="ActiveJobAndJobScheduleQuota"></param>
/// <param name="PoolQuota"></param>
/// <param name="DedicatedCoreQuota"></param>
/// <param name="LowPriorityCoreQuota"></param>
public record AccountQuota(
    int ActiveJobAndJobScheduleQuota,
    int PoolQuota,
    int DedicatedCoreQuota,
    int LowPriorityCoreQuota);
