namespace TesApi.Web.Management.Models.Quotas;

/// <summary>
/// Cores of a vm family.
/// </summary>
/// <param name="VmFamilyName"></param>
/// <param name="CoreQuota"></param>
public record BatchVmCoresPerFamily(string VmFamilyName, int CoreQuota);
