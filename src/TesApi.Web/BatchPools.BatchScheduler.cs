// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
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

        internal delegate ValueTask<BatchModels.Pool> ModelPoolFactory(string poolId, CancellationToken cancellationToken);

        private (string PoolKey, string DisplayName) GetPoolKey(TesTask tesTask, VirtualMachineInformation virtualMachineInformation, BatchModels.ContainerConfiguration containerConfiguration, CancellationToken cancellationToken)
        {
            var identityResourceId = tesTask.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) == true ? tesTask.Resources?.GetBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) : default;
            var executorImage = tesTask.Executors.First().Image;
            string containerImageNames = null;

            if (containerConfiguration?.ContainerImageNames?.Any() ?? false)
            {
                containerImageNames = string.Join(';', containerConfiguration.ContainerImageNames);
            }

            var label = string.IsNullOrWhiteSpace(batchPrefix) ? "<none>" : batchPrefix;
            var vmSize = virtualMachineInformation.VmSize ?? "<none>";
            var isPreemptable = virtualMachineInformation.LowPriority;
            containerImageNames ??= "<none>";
            identityResourceId ??= "<none>";

            // Generate hash of everything that differentiates this group of pools
            var displayName = $"{label}:{vmSize}:{isPreemptable}:{containerImageNames}:{identityResourceId}";
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

        internal async Task<IBatchPool> GetOrAddPoolAsync(string key, bool isPreemptable, ModelPoolFactory modelPoolFactory, CancellationToken cancellationToken)
        {
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

            return pool;

            static bool Available(IBatchPool pool)
                => pool.IsAvailable;
        }

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
                var pools = (await batchPools.GetAllPools()
                        .ToAsyncEnumerable()
                        .WhereAwait(async p => await p.CanBeDeleted(cancellationToken))
                        .ToListAsync(cancellationToken))
                    .Where(p => !assignedPools.Contains(p.Id))
                    .OrderBy(p => p.GetAllocationStateTransitionTime(cancellationToken))
                    .Take(neededPools.Count)
                    .ToList();

                foreach (var pool in pools)
                {
                    await DeletePoolAsync(pool, cancellationToken);
                    _ = RemovePoolFromList(pool);
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
            // TODO: Consider moving any remaining tasks to another pool, or failing tasks explicitly
            logger.LogDebug(@"Deleting pool and job {PoolId}", pool.Id);

            return Task.WhenAll(
                AllowIfNotFound(azureProxy.DeleteBatchPoolAsync(pool.Id, cancellationToken)),
                AllowIfNotFound(azureProxy.DeleteBatchJobAsync(pool.Id, cancellationToken)));

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
                => x.Id?.Equals(y.Id) ?? false;

            int IEqualityComparer<IBatchPool>.GetHashCode(IBatchPool obj)
                => obj.Id?.GetHashCode() ?? 0;
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
                => pool is null ? default : GetKeyFromPoolId(pool.Id);

            public IEnumerable<IBatchPool> GetAllPools()
                => this.SelectMany(s => s);

            public IBatchPool GetPoolOrDefault(string poolId)
                => TryGetValue(GetKeyFromPoolId(poolId), out var poolSet) ? poolSet.FirstOrDefault(p => p.Id.Equals(poolId, StringComparison.OrdinalIgnoreCase)) : default;

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
