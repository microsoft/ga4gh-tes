﻿using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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

        internal delegate ValueTask<BatchModels.Pool> ModelPoolFactory(string poolId);

        private async Task<(string PoolName, string DisplayName)> GetPoolName(TesTask tesTask, VirtualMachineInformation virtualMachineInformation)
        {
            var identityResourceId = tesTask.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) == true ? tesTask.Resources?.GetBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) : default;
            var containerInfo = await containerRegistryProvider.GetContainerRegistryInfoAsync(tesTask.Executors.FirstOrDefault()?.Image);
            var registryServer = containerInfo is null ? default : containerInfo.RegistryServer;

            var vmName = string.IsNullOrWhiteSpace(hostname) ? "<none>" : hostname;
            var vmSize = virtualMachineInformation.VmSize ?? "<none>";
            var isPreemptable = virtualMachineInformation.LowPriority;
            registryServer ??= "<none>";
            identityResourceId ??= "<none>";

            // Generate hash of everything that differentiates this group of pools
            var displayName = $"{vmName}:{vmSize}:{isPreemptable}:{registryServer}:{identityResourceId}";
            var hash = CommonUtilities.Base32.ConvertToBase32(SHA1.HashData(Encoding.UTF8.GetBytes(displayName))).TrimEnd('='); // This becomes 32 chars

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
                        var x when char.IsAsciiLetterOrDigit(x) => x,
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
            batchPool = batchPools.GetPoolOrDefault(poolId);
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

            if (!PoolNameRegex().IsMatch(key))
            {
                throw new ArgumentException("Key contains unsupported characters", nameof(key));
            }

            var pool = batchPools.TryGetValue(key, out var set) ? set.LastOrDefault(Available) : default;

            if (pool is null)
            {
                var activePoolsCount = azureProxy.GetBatchActivePoolCount();
                var poolQuota = (await quotaVerifier.GetBatchQuotaProvider().GetVmCoreQuotaAsync(isPreemptable)).AccountQuota?.PoolQuota;

                if (poolQuota is null || activePoolsCount + 1 > poolQuota)
                {
                    throw new AzureBatchQuotaMaxedOutException($"No remaining pool quota available. There are {activePoolsCount} pools in use out of {poolQuota}.");
                }

                var uniquifier = new byte[8]; // This always becomes 13 chars when converted to base32 after removing the three '='s at the end. We won't ever decode this, so we don't need the '='s
                RandomNumberGenerator.Fill(uniquifier);
                var poolId = $"{key}-{CommonUtilities.Base32.ConvertToBase32(uniquifier).TrimEnd('=')}"; // embedded '-' is required by GetKeyFromPoolId()
                var modelPool = await modelPoolFactory(poolId);
                modelPool.Metadata ??= new List<BatchModels.MetadataItem>();
                modelPool.Metadata.Add(new(PoolHostName, this.hostname));
                modelPool.Metadata.Add(new(PoolIsDedicated, (!isPreemptable).ToString()));
                var batchPool = _batchPoolFactory.CreateNew();
                await batchPool.CreatePoolAndJobAsync(modelPool, isPreemptable, CancellationToken.None);
                pool = batchPool;
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
        public async ValueTask<IEnumerable<Task>> GetShutdownCandidatePools(CancellationToken cancellationToken)
            => (await GetEmptyPools(cancellationToken))
                .Select(DeletePoolAsyncWrapper);

        private Task DeletePoolAsyncWrapper(IBatchPool pool)
            => DeletePoolAsync(pool, CancellationToken.None);

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
        public async Task DeletePoolAsync(IBatchPool pool, CancellationToken cancellationToken)
        {
            logger.LogDebug(@"Deleting pool and job {PoolId}", pool.Pool.PoolId);
            try
            {
                await Task.WhenAll(
                    AllowIfNotFound(azureProxy.DeleteBatchPoolAsync(pool.Pool.PoolId, cancellationToken)),
                    AllowIfNotFound(azureProxy.DeleteBatchJobAsync(pool.Pool, cancellationToken)));
            }
            catch { }

            static async Task AllowIfNotFound(Task task)
            {
                try
                {
                    await task;
                }
                catch (BatchException ex) when (ex.InnerException is Microsoft.Azure.Batch.Protocol.Models.BatchErrorException e && e.Response.StatusCode == System.Net.HttpStatusCode.NotFound)
                { }
                catch
                {
                    throw;
                }
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

        internal sealed class BatchPools : KeyedCollection<string, PoolSet>
        {
            public BatchPools()
                : base(StringComparer.OrdinalIgnoreCase)
            { }

            protected override string GetKeyForItem(PoolSet item)
                => item.Key;

            private static string GetKeyForItem(IBatchPool pool)
                => pool is null ? default : GetKeyFromPoolId(pool.Pool.PoolId);

            public IEnumerable<IBatchPool> GetAllPools()
                => this.SelectMany(s => s);

            public IBatchPool GetPoolOrDefault(string poolId)
                => TryGetValue(GetKeyFromPoolId(poolId), out var poolSet) ? poolSet.FirstOrDefault(p => p.Pool.PoolId.Equals(poolId, StringComparison.OrdinalIgnoreCase)) : default;

            public bool Add(IBatchPool pool)
            {
                return TryGetValue(GetKeyForItem(pool), out var poolSet)
                    ? poolSet.Add(pool)
                    : AddSet();

                bool AddSet()
                {
                    Add(new PoolSet(pool));
                    return true;
                }
            }

            public bool Remove(IBatchPool pool)
            {
                if (TryGetValue(GetKeyForItem(pool), out var poolSet))
                {
                    if (poolSet.Remove(pool))
                    {
                        if (0 == poolSet.Count)
                        {
                            if (!Remove(poolSet))
                            {
                                throw new InvalidOperationException();
                            }
                        }

                        return true;
                    }
                }

                return false;
            }

            internal IEnumerable<string> GetPoolKeys()
                => this.Select(GetKeyForItem);

            internal sealed class PoolSet : HashSet<IBatchPool>
            {
                public string Key { get; }
                public PoolSet(IBatchPool pool)
                    : base(new BatchPoolEqualityComparer())
                {
                    Key = GetKeyForItem(pool);

                    if (string.IsNullOrWhiteSpace(Key))
                    {
                        throw new ArgumentException(default, nameof(pool));
                    }

                    if (!Add(pool))
                    {
                        throw new InvalidOperationException();
                    }
                }
            }
        }
    }
}
