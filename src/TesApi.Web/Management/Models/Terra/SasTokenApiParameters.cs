namespace TesApi.Web.Management.Models.Terra
{
    /// <summary>
    /// Sas Token Api Parameters
    /// </summary>
    /// <param name="SasIpRange"></param>
    /// <param name="SasExpirationInSeconds"></param>
    /// <param name="SasPermission"></param>
    /// <param name="SasBlobName"></param>
    public record SasTokenApiParameters(string SasIpRange,
        int SasExpirationInSeconds, string SasPermission, string SasBlobName);
}
