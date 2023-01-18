using Azure;
using static Azure.Core.HttpHeader;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Threading.Tasks;
using System.Threading;
using System;
using System.Text;
using Tes.Models;
using Tes.Extensions;
using BatchModels = Microsoft.Azure.Management.Batch.Models;
using Microsoft.Azure.Batch;

namespace TesApi.Web
{
    public partial class BatchScheduler
    {
        internal delegate ValueTask<BatchModels.Pool> ModelPoolFactory(string poolId);

        private async Task<(string PoolName, string DisplayName)> GetPoolName(TesTask tesTask, VirtualMachineInformation virtualMachineInformation)
        {
            var identityResourceId = tesTask.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) == true ? tesTask.Resources?.GetBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) : default;
            var containerInfo = await azureProxy.GetContainerRegistryInfoAsync(tesTask.Executors.FirstOrDefault()?.Image);
            var registryServer = containerInfo is null ? default : containerInfo.RegistryServer;

            var vmName = string.IsNullOrWhiteSpace(hostname) ? "<none>" : hostname;
            var vmSize = virtualMachineInformation.VmSize ?? "<none>";
            var isPreemptable = virtualMachineInformation.LowPriority;
            registryServer ??= "<none>";
            identityResourceId ??= "<none>";

            // Generate hash of everything that differentiates this group of pools
            var displayName = $"{vmName}:{vmSize}:{isPreemptable}:{registryServer}:{identityResourceId}";
            var hash = BatchUtils.ConvertToBase32(SHA1.HashData(Encoding.UTF8.GetBytes(displayName))).TrimEnd('='); // This becomes 32 chars

            // Build a PoolName that is of legal length, while exposing the most important metadata without requiring user to find DisplayName
            // Note that the hash covers all necessary parts to make name unique, so limiting the size of the other parts is not expected to appreciably change the risk of collisions. Those other parts are for convenience
            var remainingLength = PoolKeyLength - hash.Length - 2; // 50 is max name length, 2 is number of inserted chars. This will always be 16 if we use an entire SHA1
            var visibleVmSize = LimitVmSize(vmSize, Math.Max(remainingLength - vmName.Length, 6));
            var visibleHostName = vmName[0..Math.Min(vmName.Length, remainingLength - visibleVmSize.Length)];
            var name = LimitChars($"{visibleHostName}-{visibleVmSize}-{hash}");

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
                var standard = "Standard_";
                return vmSize.Length <= limit
                    ? vmSize
                    : vmSize.StartsWith(standard, StringComparison.OrdinalIgnoreCase)
                    ? LimitVmSize(vmSize[standard.Length..], limit)
                    : vmSize[^limit..];
            }

            static string LimitChars(string text) // ^[a-zA-Z0-9_-]+$
            {
                return new(text.AsEnumerable().Select(Limit).ToArray());

                static char Limit(char ch)
                    => ch switch
                    {
                        var x when x >= '0' || x <= '9' => x,
                        var x when x >= 'A' || x <= 'Z' => x,
                        var x when x >= 'a' || x <= 'z' => x,
                        '_' => ch,
                        '-' => ch,
                        _ => '_',
                    };
            }
        }

        private readonly BatchPools batchPools = new();
        private readonly HashSet<string> neededPools = new();

        /// <inheritdoc/>
        public bool NeedPoolFlush
            => 0 != neededPools.Count;

        internal bool TryGetPool(string poolId, out IBatchPool batchPool)
        {
            batchPool = batchPools.FirstOrDefault(p => p.Pool.PoolId.Equals(poolId, StringComparison.Ordinal));
            return batchPool is not null;
        }

        internal bool IsPoolAvailable(string key)
            => batchPools.TryGetValue(key, out var pools) && pools.Any(p => p.IsAvailable);

        internal async Task<IBatchPool> GetOrAddPoolAsync(string key, bool isPreemptable, ModelPoolFactory modelPoolFactory)
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
            // TODO: Make sure key doesn't contain any unsupported chars

            var pool = batchPools.TryGetValue(key, out var set) ? set.LastOrDefault(Available) : default;
            if (pool is null)
            {
                await quotaVerifier.CheckBatchPoolAvailabilityQuotaAsync();
                var uniquifier = new byte[8]; // This always becomes 13 chars when converted to base32 after removing the three '='s at the end. We won't ever decode this, so we don't need the '='s
                RandomNumberGenerator.Fill(uniquifier);
                var poolId = $"{key}-{BatchUtils.ConvertToBase32(uniquifier).TrimEnd('=')}"; // embedded '-' is required by GetKeyFromPoolId()

                try
                {
                    var modelPool = await modelPoolFactory(poolId);
                    modelPool.Metadata ??= new List<BatchModels.MetadataItem>();
                    modelPool.Metadata.Add(new(PoolHostName, this.hostname));
                    modelPool.Metadata.Add(new(PoolIsDedicated, (!isPreemptable).ToString()));
                    var poolInfo = await azureProxy.CreateBatchPoolAsync(modelPool, isPreemptable);

                    try
                    {
                        await azureProxy.CreateBatchJobAsync(poolInfo);
                    }
                    catch (Exception ex)
                    {
                        HandleTimeout(poolId, ex);
                    }

                    pool = _batchPoolFactory.CreateNew(poolInfo);
                }
                catch (OperationCanceledException)
                {
                    HandleTimeout(poolId);
                }
                catch (RequestFailedException ex) when (ex.Status == 0 && ex.InnerException is WebException webException && webException.Status == WebExceptionStatus.Timeout)
                {
                    HandleTimeout(poolId);
                }
                catch (Exception ex) when (IsInnermostExceptionSocketException125(ex))
                {
                    HandleTimeout(poolId);
                }

                _ = AddPool(pool);
            }
            return pool;

            static bool Available(IBatchPool pool)
                => pool.IsAvailable;

            void HandleTimeout(string poolId, Exception ex = default)
            {
                // When the batch management API creating the pool times out, it may or may not have created the pool. Add an inactive record to delete it if it did get created and try again later. That record will be removed later whether or not the pool was created.
                _ = AddPool(_batchPoolFactory.CreateNew(poolId));
                throw ex switch
                {
                    null => new AzureBatchQuotaMaxedOutException("Pool creation timed out"),
                    OperationCanceledException => ex,
                    var x when x is RequestFailedException rfe && rfe.Status == 0 && rfe.InnerException is WebException webException && webException.Status == WebExceptionStatus.Timeout => new AzureBatchQuotaMaxedOutException("Pool creation timed out", ex),
                    var x when IsInnermostExceptionSocketException125(x) => new AzureBatchQuotaMaxedOutException("Pool creation timed out", ex),
                    _ => new Exception(ex.Message, ex),
                };
            }

            static bool IsInnermostExceptionSocketException125(Exception ex)
            {
                // errno: ECANCELED 125 Operation canceled
                for (var e = ex; e is System.Net.Sockets.SocketException /*se && se.ErrorCode == 125*/; e = e.InnerException)
                {
                    if (e.InnerException is null) { return false; }
                }
                return true;
            }
        }

        private async ValueTask<List<IBatchPool>> GetEmptyPools(CancellationToken cancellationToken)
            => await batchPools
                .ToAsyncEnumerable()
                .WhereAwait(async p => await p.CanBeDeleted(cancellationToken))
                .ToListAsync(cancellationToken);

        /// <inheritdoc/>
        public async ValueTask<IEnumerable<Task>> GetShutdownCandidatePools(CancellationToken cancellationToken)
            => (await GetEmptyPools(cancellationToken))
                .Select(async t => await DeletePoolAsync(t.Pool));

        /// <inheritdoc/>
        public IEnumerable<IBatchPool> GetPools()
            => batchPools;

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

                    if (pools.Any())
                    {
                        await Task.WhenAll(pools.Select(async p =>
                        {
                            await DeletePoolAsync(p.Pool);
                            _ = RemovePoolFromList(p);
                        }).ToArray());
                    }
                }
            }
            finally
            {
                neededPools.Clear();
            }
        }

        private async ValueTask DeletePoolAsync(PoolInformation pool, CancellationToken cancellationToken = default)
        {
            // TODO: deal with missing pool and/or job
            await azureProxy.DeleteBatchPoolAsync(pool.PoolId, cancellationToken);
            await azureProxy.DeleteBatchJobAsync(pool, cancellationToken);
        }

        private bool AddPool(IBatchPool pool)
            => batchPools.Add(pool);

        private static string GetKeyFromPoolId(string poolId)
            => poolId[..poolId.LastIndexOf('-')];

        private class BatchPoolEqualityComparer : IEqualityComparer<IBatchPool>
        {
            bool IEqualityComparer<IBatchPool>.Equals(IBatchPool x, IBatchPool y)
                => x.Pool.PoolId?.Equals(y.Pool.PoolId) ?? false;

            int IEqualityComparer<IBatchPool>.GetHashCode(IBatchPool obj)
                => obj.Pool.PoolId?.GetHashCode() ?? 0;
        }

        #region Used for unit/module testing
        internal IEnumerable<string> GetPoolGroupKeys()
            => batchPools.Keys;
        #endregion

        private class BatchPools : KeyedGroup<IBatchPool, GroupableSet<IBatchPool>>
        {
            public BatchPools()
                : base(p => p is null ? default : GetKeyFromPoolId(p.Pool.PoolId), StringComparer.Ordinal)
            { }

            protected override Func<IEnumerable<IBatchPool>, GroupableSet<IBatchPool>> CreateSetFunc
                => e => new(e, new BatchPoolEqualityComparer());

            public IBatchPool GetPoolOrDefault(string poolId)
                => TryGetValue(GetKeyFromPoolId(poolId), out var batchPools) ? batchPools.FirstOrDefault(p => p.Pool.PoolId.Equals(poolId, StringComparison.Ordinal)) : default;
        }
    }
}
