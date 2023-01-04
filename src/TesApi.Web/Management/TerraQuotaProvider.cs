using System.Threading.Tasks;
using TesApi.Web.Management.Models.Quotas;

namespace TesApi.Web.Management
{
    public class TerraQuotaProvider : IBatchQuotaProvider
    {
        public Task<BatchVmFamilyQuotas> GetBatchAccountQuotaForRequirementAsync(string vmFamily, bool lowPriority, int? coresRequirement)
        {
            throw new System.NotImplementedException();
        }

        public Task<BatchVmCoreQuota> GetVmCoresPerFamilyAsync(bool lowPriority)
        {
            throw new System.NotImplementedException();
        }
    }
}
