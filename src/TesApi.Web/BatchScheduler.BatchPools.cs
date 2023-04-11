using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Logging;
using Tes.Extensions;
using Tes.Models;
using static TesApi.Web.BatchScheduler.BatchPools;
using BatchModels = Microsoft.Azure.Management.Batch.Models;

namespace TesApi.Web
{
    public partial class BatchScheduler
    {
        [GeneratedRegex("^[a-zA-Z0-9_-]+$")]
        private static partial Regex PoolNameRegex();

        internal delegate ValueTask<BatchModels.Pool> ModelPoolFactory(string poolId, CancellationToken cancellationToken);

        private readonly BatchPools batchPools = new();
        private readonly ConcurrentHashSet<string> neededPools = new();
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, Nito.AsyncEx.PauseTokenSource> poolWaiters = new(StringComparer.OrdinalIgnoreCase);

        private async Task<(string PoolKey, string DisplayName)> GetPoolKey(TesTask tesTask, VirtualMachineInformation virtualMachineInformation, CancellationToken cancellationToken)
        {
            var identityResourceId = tesTask.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) == true ? tesTask.Resources?.GetBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) : default;
            var executorImage = tesTask.Executors.First().Image;
            string registryServer = null;
            
            if (!containerRegistryProvider.IsImagePublic(executorImage))
            {
                registryServer = (await containerRegistryProvider.GetContainerRegistryInfoAsync(executorImage, cancellationToken))?.RegistryServer;
            }
            
            var label = string.IsNullOrWhiteSpace(batchPrefix) ? "<none>" : batchPrefix;
            var vmSize = virtualMachineInformation.VmSize ?? "<none>";
            var isPreemptable = virtualMachineInformation.LowPriority;
            registryServer ??= "<none>";
            identityResourceId ??= "<none>";

            // Generate hash of everything that differentiates this group of pools
            var displayName = $"{label}:{vmSize}:{isPreemptable}:{registryServer}:{identityResourceId}";
            var hash = CommonUtilities.Base32.ConvertToBase32(SHA1.HashData(Encoding.UTF8.GetBytes(displayName))).TrimEnd('=').ToLowerInvariant(); // This becomes 32 chars

            // Build a PoolName that is of legal length, while exposing the most important metadata without requiring user to find DisplayName
            // Note that the hash covers all necessary parts to make name unique, so limiting the size of the other parts is not expected to appreciably change the risk of collisions. Those other parts are for convenience
            var remainingLength = PoolKeyLength - hash.Length - 6; // 55 is max name length, 6 is number of inserted chars (prefix and '-'s). This will always be 21 if we use an entire SHA1
            var visibleVmSize = LimitVmSize(vmSize, Math.Max(remainingLength - label.Length, 6)); // At least 6 chars from the VmSize will be visible in the name
            var visibleLabel = label[0..Math.Min(label.Length, remainingLength - visibleVmSize.Length)]; // Fill up to the max length if needed with the "branchPrefix", truncating as needed
            var name = FlattenChars($"TES-{visibleLabel}-{visibleVmSize}-{hash}");

            // Trim DisplayName if needed
            if (displayName.Length > 1024)
            {
                // Remove "path" of identityResourceId
                displayName = displayName[..^identityResourceId.Length] + identityResourceId[(identityResourceId.LastIndexOf('/') + 1)..];
                if (displayName.Length > 1024)
                {
                    // Trim end, leaving fake elipsys as marker
                    displayName = displayName[..1021] + "...";
                }
            }

            return (name, displayName);

            static string LimitVmSize(string vmSize, int limit)
            {
                // First try optimizing by removing "Standard_" prefix.
                // Then remove chars from the front until it fits
                var standard = "Standard_";
                return vmSize.Length <= limit
                    ? vmSize
                    : vmSize.StartsWith(standard, StringComparison.OrdinalIgnoreCase)
                    ? LimitVmSize(vmSize[standard.Length..], limit)
                    : vmSize[^limit..];
            }

            static string FlattenChars(string text) // ^[a-zA-Z0-9_-]+$
            {
                return new(text.AsEnumerable().Select(Flatten).ToArray());

                static char Flatten(char ch)
                    => ch switch
                    {
                        var x when char.IsAsciiLetterOrDigit(x) => x,
                        '_' => ch,
                        '-' => ch,
                        _ => '_',
                    };
            }
        }

        /// <inheritdoc/>
        public bool NeedPoolFlush
            => 0 != neededPools.Count;

        internal bool TryGetPool(string poolId, out IBatchPool batchPool)
        {
            batchPool = batchPools.GetPoolOrDefault(poolId);
            return batchPool is not null;
        }

        internal bool IsPoolAvailable(string key)
            => batchPools.TryGetValue(key, out var pools) && ((IEnumerable<IBatchPool>)pools).Any(p => p.IsAvailable);

        // returns true if did wait, false if first in line.
        private async Task<bool> WaitForTurn(string key, CancellationToken cancellationToken)
        {
            var trial = new Nito.AsyncEx.PauseTokenSource { IsPaused = true };
            var source = poolWaiters.GetOrAdd(key, trial);

            if (source == trial)
            {
                return false;
            }

            await source.Token.WaitWhilePausedAsync(cancellationToken);
            return true;
        }

        private void ReleaseWaiters(string key)
        {
            if (poolWaiters.TryRemove(key, out var source))
            {
                source.IsPaused = false;
            }
        }

        internal async Task<IBatchPool> GetOrAddPoolAsync(string key, bool isPreemptable, ModelPoolFactory modelPoolFactory, CancellationToken cancellationToken)
        {
            if (enableBatchAutopool)
            {
                return default;
            }

            ArgumentNullException.ThrowIfNull(modelPoolFactory);
            var keyLength = key?.Length ?? 0;
            if (keyLength > PoolKeyLength || keyLength < 1)
            {
                throw new ArgumentException("Key must be between 1-50 chars in length", nameof(key));
            }

            if (!PoolNameRegex().IsMatch(key))
            {
                throw new ArgumentException("Key contains unsupported characters", nameof(key));
            }

            var pool = batchPools.TryGetValue(key, out var set) ? ((IEnumerable<IBatchPool>)set).LastOrDefault(Available) : default;

            if (pool is null)
            {
                if (await WaitForTurn(key, cancellationToken))
                {
                    return await GetOrAddPoolAsync(key, isPreemptable, modelPoolFactory, cancellationToken);
                }

                try
                {
                    var quota = (await quotaVerifier.GetBatchQuotaProvider().GetVmCoreQuotaAsync(isPreemptable, cancellationToken)).AccountQuota;
                    var poolQuota = quota?.PoolQuota;
                    var jobQuota = quota?.ActiveJobAndJobScheduleQuota;
                    var activePoolsCount = azureProxy.GetBatchActivePoolCount();
                    var activeJobsCount = azureProxy.GetBatchActiveJobCount();

                    if (poolQuota is null || activePoolsCount + 1 > poolQuota)
                    {
                        throw new AzureBatchQuotaMaxedOutException($"No remaining pool quota available. There are {activePoolsCount} pools in use out of {poolQuota}.");
                    }

                    if (jobQuota is null || activeJobsCount + 1 > jobQuota)
                    {
                        throw new AzureBatchQuotaMaxedOutException($"No remaining active jobs quota available. There are {activeJobsCount} active jobs out of {jobQuota}.");
                    }

                    var uniquifier = new byte[5]; // This always becomes 8 chars when converted to base32
                    RandomNumberGenerator.Fill(uniquifier);
                    var poolId = $"{key}-{CommonUtilities.Base32.ConvertToBase32(uniquifier).TrimEnd('=').ToLowerInvariant()}"; // embedded '-' is required by GetKeyFromPoolId()
                    var modelPool = await modelPoolFactory(poolId, cancellationToken);
                    modelPool.Metadata ??= new List<BatchModels.MetadataItem>();
                    modelPool.Metadata.Add(new(PoolHostName, this.batchPrefix));
                    modelPool.Metadata.Add(new(PoolIsDedicated, (!isPreemptable).ToString()));
                    var batchPool = _batchPoolFactory.CreateNew();
                    await batchPool.CreatePoolAndJobAsync(modelPool, isPreemptable, cancellationToken);
                    pool = batchPool;
                }
                finally
                {
                    ReleaseWaiters(key);
                }
            }

            return pool;

            static bool Available(IBatchPool pool)
                => pool.IsAvailable;
        }

        private async ValueTask<List<IBatchPool>> GetEmptyPools(CancellationToken cancellationToken)
            => await batchPools.GetAllPools()
                .ToAsyncEnumerable()
                .WhereAwait(async p => await p.CanBeDeleted(cancellationToken))
                .ToListAsync(cancellationToken);

        /// <inheritdoc/>
        public IEnumerable<IBatchPool> GetPools()
            => batchPools.GetAllPools();

        /// <inheritdoc/>
        public bool RemovePoolFromList(IBatchPool pool)
            => batchPools.Remove(pool);

        /// <inheritdoc/>
        public async ValueTask FlushPoolsAsync(IEnumerable<string> assignedPools, CancellationToken cancellationToken)
        {
            assignedPools = assignedPools.ToList();

            try
            {
                if (!this.enableBatchAutopool)
                {
                    var pools = (await GetEmptyPools(cancellationToken))
                        .Where(p => !assignedPools.Contains(p.Pool.PoolId))
                        .OrderBy(p => p.GetAllocationStateTransitionTime(cancellationToken))
                        .Take(neededPools.Count)
                        .ToList();

                    foreach (var pool in pools)
                    {
                        await DeletePoolAsync(pool, cancellationToken);
                        _ = RemovePoolFromList(pool);
                    }
                }
            }
            finally
            {
                neededPools.Clear();
            }
        }

        /// <inheritdoc/>
        public Task DeletePoolAsync(IBatchPool pool, CancellationToken cancellationToken)
        {
            logger.LogDebug(@"Deleting pool and job {PoolId}", pool.Pool.PoolId);

            return Task.WhenAll(
                AllowIfNotFound(azureProxy.DeleteBatchPoolAsync(pool.Pool.PoolId, cancellationToken)),
                AllowIfNotFound(azureProxy.DeleteBatchJobAsync(pool.Pool, cancellationToken)));

            static async Task AllowIfNotFound(Task task)
            {
                try { await task; }
                catch (BatchException ex) when (ex.InnerException is Microsoft.Azure.Batch.Protocol.Models.BatchErrorException e && e.Response.StatusCode == System.Net.HttpStatusCode.NotFound) { }
                catch { throw; }
            }
        }

        /// <inheritdoc/>
        public bool AddPool(IBatchPool pool)
            => batchPools.Add(pool);

        private static string GetKeyFromPoolId(string poolId)
            => poolId is null || !poolId.Contains('-') ? string.Empty : poolId[..poolId.LastIndexOf('-')];

        private class BatchPoolEqualityComparer : IEqualityComparer<IBatchPool>
        {
            bool IEqualityComparer<IBatchPool>.Equals(IBatchPool x, IBatchPool y)
                => x.Pool.PoolId?.Equals(y.Pool.PoolId) ?? false;

            int IEqualityComparer<IBatchPool>.GetHashCode(IBatchPool obj)
                => obj.Pool.PoolId?.GetHashCode() ?? 0;
        }

        #region Used for unit/module testing
        internal IEnumerable<string> GetPoolGroupKeys()
            => batchPools.GetPoolKeys();
        #endregion

        internal sealed class BatchPools : System.Collections.Concurrent.ConcurrentDictionary<string, PoolSet>
        {
            private readonly object SyncRoot = new();

            public BatchPools()
                : base(StringComparer.OrdinalIgnoreCase)
            { }

            private static string GetKeyForItem(IBatchPool pool)
                => pool is null ? default : GetKeyFromPoolId(pool.Pool.PoolId);

            public IEnumerable<IBatchPool> GetAllPools()
                => this.Values.Select<PoolSet, IEnumerable<IBatchPool>>(s => s).SelectMany(s => s);

            public IBatchPool GetPoolOrDefault(string poolId)
                => TryGetValue(GetKeyFromPoolId(poolId), out var poolSet) ? ((IEnumerable<IBatchPool>)poolSet).FirstOrDefault(p => p.Pool.PoolId.Equals(poolId, StringComparison.OrdinalIgnoreCase)) : default;

            public bool Add(IBatchPool pool)
            {
                lock (SyncRoot)
                {
                    return GetOrAdd(GetKeyForItem(pool), k => new PoolSet(GetKeyForItem(pool))).Add(pool);
                }
            }

            public bool Remove(IBatchPool pool)
            {
                lock (SyncRoot)
                {
                    if (TryGetValue(GetKeyForItem(pool), out var poolSet) && poolSet.Remove(pool))
                    {
                        if (poolSet.IsEmpty)
                        {
                            _ = TryRemove(GetKeyForItem(pool), out _);
                        }

                        return true;
                    }

                    return false;
                }
            }

            internal IEnumerable<string> GetPoolKeys()
                => Keys;

            internal sealed class PoolSet : ConcurrentHashSet<IBatchPool>
            {
                public string Key { get; }
                public PoolSet(string key)
                    : base(new BatchPoolEqualityComparer())
                {
                    ArgumentException.ThrowIfNullOrEmpty(key);
                    Key = key;
                }
            }
        }
    }
}
