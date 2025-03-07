﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine.Rendering;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using Azure.Core;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Primitives;
using Polly;
using Polly.Retry;
using Tes.Models;
using static GenerateBatchVmSkus.Program;

/*
 * TODO considerations:
 *   Currently, we consider a compute node in LeavingPool or Preempted states as a success. We may want to instead actually run a task on the node before calling it a success.
 *   Whenever a resize error of 'OverconstrainedAllocationRequestError' is received for any SKU in a VmFamily, it is returned for all members of that family. Consider grouping by VmFamily and preemptively skipping all SKUs of same family.
 */

namespace GenerateBatchVmSkus
{
    internal static partial class AzureBatchSkuValidator
    {
        private static ImageReference V1ImageReference
            => new("ubuntu-server-container", "microsoft-azure-batch", "20-04-lts", "latest");

        private static ImageReference V2ImageReference
            => new("ubuntu-hpc", "microsoft-dsvm", "2004", "latest");

        private static VirtualMachineConfiguration GetMachineConfiguration(bool useV2, bool encryptionAtHostSupported)
        {
            return new(useV2 ? V2ImageReference : V1ImageReference, "batch.node.ubuntu 20.04")
            {
                DiskEncryptionConfiguration = encryptionAtHostSupported
                    ? new DiskEncryptionConfiguration(
                        targets:
                        [
                            DiskEncryptionTarget.OsDisk,
                            DiskEncryptionTarget.TemporaryDisk
                        ])
                    : null
            };
        }

        private static TimeSpan RetryWaitTime => TimeSpan.FromMinutes(8);

        private static int UnknownVCpuCores;

        internal static bool WriteLogs { get; set; } = false;
        internal static bool ShowCounts { get; set; } = false;

        public record struct ValidationResults(IEnumerable<VirtualMachineInformation> Verified, IEnumerable<VmSku> Unverifyable);

        public record struct BatchSkuInfo(string VmFamily, int? VCpusAvailable);

        private record class WrappedVmSku(VmSku VmSku)
        {
            public bool Validated { get; set; } = false;

            public static WrappedVmSku Create(VmSku sku) => new(sku);
        }

        [GeneratedRegex("^(sort|process)\\-\\S+\\.txt$")]
        private static partial Regex ValidationLogFileRegex();

        public static async ValueTask<ValidationResults> ValidateSkus(IEnumerable<VmSku> skus, IAsyncEnumerable<BatchAccountInfo> batchAccounts, IDictionary<string, BatchSkuInfo> batchSkus, CancellationToken cancellationToken)
        {
            if (WriteLogs)
            {
                var regex = ValidationLogFileRegex(); // "sort-*.txt", "process-*.txt"
                Directory.EnumerateFiles(Environment.CurrentDirectory)
                    .Where(path => regex.IsMatch(Path.GetFileName(path)))
                    .ForEach(File.Delete);
            }

            var startTime = DateTimeOffset.UtcNow;
            ArgumentNullException.ThrowIfNull(skus);
            ArgumentNullException.ThrowIfNull(batchAccounts);
            ArgumentNullException.ThrowIfNull(batchSkus);

            AzureBatchSkuValidator.batchSkus = batchSkus;
            List<Validator> validators = [];
            skus = skus.ToList();
            UnknownVCpuCores = skus.Max(vm => vm.Sku.VCpusAvailable ?? 1);
            var asyncSkus = skus.ToAsyncEnumerable().Select(WrappedVmSku.Create);

            try
            {
                await foreach (var account in batchAccounts)
                {
                    validators.Add(new Validator(account));
                }

                validators.ForEach(validator => asyncSkus = validator.ValidateSkus(asyncSkus, cancellationToken));
                return await GetResults(asyncSkus, cancellationToken);

                async ValueTask<ValidationResults> GetResults(IAsyncEnumerable<WrappedVmSku> results, CancellationToken cancellationToken)
                {
                    var skus = await results.ToListAsync(cancellationToken);

                    if (ShowCounts)
                    {
                        ConsoleHelper.WriteLine("Tally", ForegroundColorSpan.Blue(), $"Qty-in: {skus.Where(sku => sku.Validated).Count()} validated / {skus.Where(sku => !sku.Validated).Count()} nonvalidated. strt/cmpt/cnt {started}/{completed}/{count}");
                    }

                    ConsoleHelper.WriteLine("SKU validation in Batch", ForegroundColorSpan.Green(), $"Completed in {DateTimeOffset.UtcNow - startTime:g}.");

                    return new(
                        skus.Where(sku => sku.Validated).SelectMany(sku => sku.VmSku.Skus),
                        skus.Where(sku => !sku.Validated).Select(sku => sku.VmSku));
                }
            }
            finally
            {
                validators.ForEach(validator => ((IDisposable)validator).Dispose());
            }
        }

        private static readonly AsyncRetryPolicy asyncRetryPolicy = Policy
            .Handle<IOException>()
            .WaitAndRetryForeverAsync(i => TimeSpan.FromSeconds(0.05));

        private static IDictionary<string, BatchSkuInfo>? batchSkus;

        private static long count = 0;
        private static long started = 0;
        private static long completed = 0;

        private static readonly Func<string> progressLineFunc = static () => $"SKU validations: Started: {started / (double)count:P2} / Completed: {completed / (double)count:P2}";

        private sealed class Validator : IDisposable
        {
            private readonly Channel<WrappedVmSku> resultSkus = Channel.CreateUnbounded<WrappedVmSku>(new() { SingleReader = true, SingleWriter = true });
            private readonly Channel<WrappedVmSku> candidateSkus = Channel.CreateUnbounded<WrappedVmSku>(new() { SingleReader = true, SingleWriter = true });
            private readonly BatchAccountInfo accountInfo;

            public Validator(BatchAccountInfo batchAccount)
            {
                ArgumentNullException.ThrowIfNull(batchAccount);
                accountInfo = batchAccount;
            }

            internal AzureLocation Location => accountInfo.Location;
            internal Task? ValidationTask = null;

            private Func<CancellationToken, Task> WriteLog(string log, string action, WrappedVmSku sku)
                => WriteLogs
                    ? new(token => File.AppendAllTextAsync($"{log}-{accountInfo.Name}.txt", $"{action}\t{sku.VmSku.Name}{Environment.NewLine}", token))
                    : token => Task.CompletedTask;

            private async ValueTask<IEnumerable<WrappedVmSku>> GetVmSkusAsync(TestContext context, CancellationToken cancellationToken)
            {
                var result = Enumerable.Empty<WrappedVmSku>();

                while (candidateSkus.Reader.TryRead(out var vm))
                {
                    if (CanBatchAccountValidateSku(vm, context))
                    {
                        result = result.Append(vm);
                        await asyncRetryPolicy.ExecuteAsync(WriteLog("sort", "process", vm), cancellationToken);
                    }
                    else
                    {
                        await resultSkus.Writer.WriteAsync(vm, cancellationToken);
                        await asyncRetryPolicy.ExecuteAsync(WriteLog("sort", "forward", vm), cancellationToken);
                    }
                }

                result = result.ToList();
                _ = Interlocked.Add(ref count, result.Count());
                return result;
            }

            internal async IAsyncEnumerable<WrappedVmSku> ValidateSkus(IAsyncEnumerable<WrappedVmSku> skus, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
            {
                var loadValuesTask = StartLoadValues();

                await foreach (var sku in resultSkus.Reader.ReadAllAsync(cancellationToken).WithCancellation(cancellationToken))
                {
                    yield return sku;
                }

                await loadValuesTask;

                Task StartLoadValues()
                {
                    var task = new Task(async () => await LoadValues(), cancellationToken, TaskCreationOptions.LongRunning);
                    task.Start();
                    return task;
                }

                Task StartValidation()
                {
                    var task = new Task(async () => await ValidateSkus(cancellationToken), cancellationToken, TaskCreationOptions.LongRunning);
                    task.Start();
                    return task;
                }

                async ValueTask LoadValues()
                {
                    var perFamilyQuota = accountInfo.DedicatedCoreQuotaPerVmFamily.ToDictionary(quota => quota.Name, quota => quota.CoreQuota ?? 0, StringComparer.OrdinalIgnoreCase);
                    var validatedCount = 0;
                    var nonvalidatedCount = 0;
                    var forwarded = 0;
                    var considered = 0;

                    await foreach (var sku in skus.WithCancellation(cancellationToken))
                    {
                        if (sku.Validated) { ++validatedCount; } else { ++nonvalidatedCount; }

                        if (sku.Validated ||
                            !sku.VmSku.Sku.RegionsAvailable.Contains(Location.Name) ||
                            ((!sku.VmSku.Sku.LowPriority) && perFamilyQuota.TryGetValue(sku.VmSku.Sku.VmFamily, out var quota) && quota < (sku.VmSku.Sku.VCpusAvailable ?? UnknownVCpuCores)))
                        {
                            ++forwarded;
                            await resultSkus.Writer.WriteAsync(sku, cancellationToken);
                        }
                        else
                        {
                            ++considered;
                            await candidateSkus.Writer.WriteAsync(sku, cancellationToken);
                            ValidationTask ??= StartValidation();
                        }
                    }

                    if (ShowCounts)
                    {
                        ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.Blue(), $"Qty-in: {validatedCount} validated / {nonvalidatedCount} nonvalidated.{Environment.NewLine}Qty-sorted: {forwarded} forwarded / {considered} considered.");
                    }

                    candidateSkus.Writer.Complete();

                    if (ValidationTask is null)
                    {
                        resultSkus.Writer.Complete();
                    }
                }
            }

            private async ValueTask ValidateSkus(CancellationToken cancellationToken)
            {
                var processed = 0;
                var processedDeferred = 0;
                var processedRetried = 0;

                try
                {
                    var context = await GetTestQuotaContext(accountInfo, cancellationToken);

                    var StartLoadedTest = new Func<WrappedVmSku, Task<(WrappedVmSku vmSize, VerifyVMIResult result)>>(async vmSize =>
                    {
                        _ = Interlocked.Increment(ref started);
                        await asyncRetryPolicy.ExecuteAsync(WriteLog("process", "post", vmSize), cancellationToken);
                        return (vmSize, result: await TestVMSizeInBatchAsync(vmSize, cancellationToken));
                    });

                    Dictionary<string, (bool Ignore, int RetryCount, DateTime NextAttempt, WrappedVmSku VmSize)> retries = new(StringComparer.OrdinalIgnoreCase);
                    List<Task<(WrappedVmSku vmSize, VerifyVMIResult result)>> tests = [];

                    List<Task> tasks = [];
                    Task? retryReadyTask = default;
                    tasks.Add(candidateSkus.Reader.Completion);
                    var moreInputTask = candidateSkus.Reader.WaitToReadAsync(cancellationToken).AsTask();
                    tasks.Add(moreInputTask);

                    if (!cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            List<WrappedVmSku> skusToTest = [.. await GetVmSkusAsync(context, cancellationToken)];
                            await skusToTest.ToAsyncEnumerable()
                                .ForEachAwaitWithCancellationAsync(async (sku, token) => await asyncRetryPolicy.ExecuteAsync(WriteLog("process", "queue", sku), token), cancellationToken);
                            var loadedTests = skusToTest.Where(CanTestNow).ToList();

                            for (tests = [.. loadedTests.Select(StartLoadedTest)];
                                (tests.Any() || tasks.Any()) && !cancellationToken.IsCancellationRequested;
                                tests.AddRange(loadedTests.Select(StartLoadedTest)))
                            {
                                loadedTests.ForEach(loadedTest => _ = skusToTest.Remove(loadedTest));
                                loadedTests.Clear();
                                ConsoleHelper.WriteTemporaryLine(progressLineFunc);
                                var task = await Task.WhenAny(tasks.Concat(tests.Cast<Task>()));
                                _ = tasks.Remove(task);

                                switch (task)
                                {
                                    case Task<(WrappedVmSku vmSize, VerifyVMIResult result)> validationTask:
                                        {
                                            _ = tests.Remove(validationTask);
                                            _ = Interlocked.Increment(ref completed);

                                            try
                                            {
                                                var (vmSize, result) = await validationTask;
                                                RemoveSkuMetadataFromQuota(vmSize, context!);

                                                switch (result)
                                                {
                                                    case VerifyVMIResult.Use:
                                                        ++processed;
                                                        _ = retries.Remove(vmSize.VmSku.Name);
                                                        vmSize.Validated = true;
                                                        await resultSkus.Writer.WriteAsync(vmSize, cancellationToken);
                                                        await asyncRetryPolicy.ExecuteAsync(WriteLog("process", "use", vmSize), cancellationToken);
                                                        break;

                                                    case VerifyVMIResult.Skip:
                                                        ++processed;
                                                        _ = retries.Remove(vmSize.VmSku.Name);
                                                        await asyncRetryPolicy.ExecuteAsync(WriteLog("process", "skip", vmSize), cancellationToken);
                                                        break;

                                                    case VerifyVMIResult.NextRegion:
                                                        ++processedDeferred;
                                                        _ = retries.Remove(vmSize.VmSku.Name);
                                                        await resultSkus.Writer.WriteAsync(vmSize, cancellationToken);
                                                        await asyncRetryPolicy.ExecuteAsync(WriteLog("process", "forward", vmSize), cancellationToken);
                                                        break;

                                                    case VerifyVMIResult.Retry:
                                                        if (!retries.TryGetValue(vmSize.VmSku.Name, out var lastRetry) || lastRetry.RetryCount < 3)
                                                        {
                                                            ++processedRetried;
                                                            retries[vmSize.VmSku.Name] = (false, lastRetry.RetryCount + 1, DateTime.UtcNow + AzureBatchSkuValidator.RetryWaitTime, vmSize);
                                                            _ = Interlocked.Decrement(ref started);
                                                            _ = Interlocked.Decrement(ref completed);
                                                            await asyncRetryPolicy.ExecuteAsync(WriteLog("process", $"wait{lastRetry.RetryCount}", vmSize), cancellationToken);
                                                        }
                                                        else
                                                        {
                                                            ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.LightYellow(), $"Deferring {vmSize.VmSku.Name} because retry attempts were exhausted.");
                                                            ++processed;
                                                            _ = retries.Remove(vmSize.VmSku.Name);
                                                            await resultSkus.Writer.WriteAsync(vmSize, cancellationToken);
                                                            await asyncRetryPolicy.ExecuteAsync(WriteLog("process", "forwardRT", vmSize), cancellationToken);
                                                        }

                                                        break;
                                                }
                                            }
                                            catch (Exception e) when (e is not System.OperationCanceledException)
                                            {
                                                ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.Red(),
                                                    $"Due to the following failure, it is unknown which SKU this failure report is related to. This SKU will not be included in the final results.{Environment.NewLine}{e}");
                                            }
                                        }
                                        break;

                                    case Task<bool> boolReturnTask:
                                        if (moreInputTask == boolReturnTask)
                                        {
                                            if (moreInputTask.Result)
                                            {
                                                skusToTest.AddRange(await (await GetVmSkusAsync(context, cancellationToken)).ToAsyncEnumerable()
                                                    .WhereAwaitWithCancellation(async (vmSize, token) =>
                                                    {
                                                        await asyncRetryPolicy.ExecuteAsync(WriteLog("process", "queue", vmSize), cancellationToken);
                                                        return true;
                                                    })
                                                    .ToListAsync(cancellationToken));

                                                if (!candidateSkus.Reader.Completion.IsCompleted)
                                                {
                                                    moreInputTask = candidateSkus.Reader.WaitToReadAsync(cancellationToken).AsTask();
                                                    tasks.Add(moreInputTask);
                                                }
                                            }
                                        }
                                        else
                                        {
                                            ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.Red(), "Unexpected Task<bool> completion. Contact developer.");
                                        }
                                        break;

                                    case Task voidReturnTask:
                                        if (retryReadyTask == voidReturnTask)
                                        {
                                            var now = DateTime.UtcNow;
                                            var readyRetries = retries.Values.Where(retry => !retry.Ignore && now >= retry.NextAttempt).ToList();
                                            skusToTest.AddRange(await readyRetries.Select(retry => retry.VmSize)
                                                .ToAsyncEnumerable()
                                                .WhereAwaitWithCancellation(async (vmSize, token) =>
                                                {
                                                    await asyncRetryPolicy.ExecuteAsync(WriteLog("process", "queueRT", vmSize), cancellationToken);
                                                    return true;
                                                })
                                                .ToListAsync(cancellationToken));
                                            readyRetries.ForEach(retry => retries[retry.VmSize.VmSku.Name] = (true, retry.RetryCount, retry.NextAttempt, retry.VmSize));

                                            retryReadyTask = null;
                                        }
                                        else if (candidateSkus.Reader.Completion == voidReturnTask)
                                        {
                                            _ = tasks.Remove(moreInputTask);
                                        }
                                        else
                                        {
                                            ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.Red(), "Unexpected Task completion. Contact developer.");
                                        }
                                        break;

                                    default:
                                        ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.Red(), "Unexpected task (unknown type) completion. Contact developer.");
                                        break;
                                }

                                if (retryReadyTask is null)
                                {
                                    var oldestNextRetryAttempt = retries.Values.Any(retry => !retry.Ignore) ? retries.Values.Where(retry => !retry.Ignore).Min(retryCount => retryCount.NextAttempt) : default;
                                    var now = DateTime.UtcNow;

                                    if (oldestNextRetryAttempt != default)
                                    {
                                        retryReadyTask = Task.Delay(TimeSpan.Zero.MaxOfThisOr(oldestNextRetryAttempt - now), cancellationToken);
                                        tasks.Add(retryReadyTask);
                                    }
                                }

                                loadedTests = [.. skusToTest.Where(CanTestNow)];
                            }

                            await skusToTest.ToAsyncEnumerable().ForEachAwaitWithCancellationAsync(async (vmSize, token) =>
                            {
                                await resultSkus.Writer.WriteAsync(vmSize, token);
                                await asyncRetryPolicy.ExecuteAsync(WriteLog("process", "dump", vmSize), cancellationToken);
                                ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.LightYellow(), $"Deferring '{vmSize.VmSku.Name}' due to quota (end of processing).");
                            },
                            cancellationToken);

                            _ = Interlocked.Add(ref count, -skusToTest.Count);
                        }
                        // TODO: Flag somewhere to prevent any results from being produced.
                        catch (Exception e) when (e is not System.OperationCanceledException)
                        {
                            ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.Red(),
                                $"{e}{Environment.NewLine}This failure caused this batch account to not be used again in this session. This will probably produce incorrect results.");
                        }
                        finally
                        {
                            ConsoleHelper.WriteTemporaryLine(progressLineFunc);
                            // Keep the batch account around long enough to allow the batch pools to be deleted.
                            _ = await Task.WhenAll(tests);
                        }
                    }

                    resultSkus.Writer.Complete();

                    if (ShowCounts)
                    {
                        ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.Green(), $"Validated a net of {processed} SKUs out of {processed + processedDeferred + processedRetried} attempts.");
                    }

                    bool CanTestNow(WrappedVmSku sku) => AddSkuMetadataToQuotaIfQuotaPermits(sku, context);
                }
                catch (OperationCanceledException)
                { }
                catch (Exception e)
                {
                    ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.Red(), $"'{accountInfo.Name}' has completed processing its inputs due to an error.");

                    resultSkus.Writer.Complete(e);
                }
                finally
                {
                    ConsoleHelper.WriteTemporaryLine(progressLineFunc);
                }
            }

            private enum VerifyVMIResult
            {
                Use,
                Skip,
                NextRegion,
                Retry
            }

            private async Task<VerifyVMIResult> TestVMSizeInBatchAsync(WrappedVmSku vmSize, CancellationToken cancellationToken)
            {
                ArgumentNullException.ThrowIfNull(vmSize);

                // When a pool is first created and the autoscale is first evaluated, the metrics have no samples (so $<metric>.Count() will return 0). All subsequent reevaluations should return positive integer values, causing the nodes to be removed while the developer is working on the reason the pool wasn't deleted.
                // https://learn.microsoft.com/azure/batch/batch-automatic-scaling#methods
                const string DedicatedAutoScaleFormula = "$NodeDeallocationOption = retaineddata; $TargetDedicatedNodes = ($PendingTasks.Count() ? 0 : 1);";
                const string LowPriorityAutoScaleFormula = "$NodeDeallocationOption = retaineddata; $TargetLowPriorityNodes = ($PendingTasks.Count() ? 0 : 1);";

                var vm = vmSize.VmSku.Sku;
                var name = vm.VmSize;
                var generation = vm.HyperVGenerations.Contains("V2", StringComparer.OrdinalIgnoreCase) ? "V2" : "V1";

                var pool = accountInfo.Client.PoolOperations.CreatePool(
                    poolId: name,
                    virtualMachineSize: name,
                    virtualMachineConfiguration: GetMachineConfiguration("V2" == generation, vm.EncryptionAtHostSupported ?? false));

                pool.DisplayName = $"{System.Reflection.Assembly.GetEntryAssembly()!.GetName().Name}: VmSize: {name} VmFamily: {vm.VmFamily} HyperVGeneration: {generation} IsDedicated: {!vm.LowPriority}";

                pool.TargetNodeCommunicationMode = NodeCommunicationMode.Simplified;
                pool.NetworkConfiguration = new() { PublicIPAddressConfiguration = new(IPAddressProvisioningType.BatchManaged), SubnetId = accountInfo.SubnetId };

                //pool.TargetDedicatedComputeNodes = vm.LowPriority ? 0 : 1;
                //pool.TargetLowPriorityComputeNodes = vm.LowPriority ? 1 : 0;
                //pool.ResizeTimeout = TimeSpan.FromMinutes(15);
                //pool.AutoScaleEnabled = false;
                pool.AutoScaleEvaluationInterval = TimeSpan.FromMinutes(5);
                pool.AutoScaleFormula = vm.LowPriority ? LowPriorityAutoScaleFormula : DedicatedAutoScaleFormula;
                pool.AutoScaleEnabled = true;

                try
                {
                    await pool.CommitAsync(cancellationToken: cancellationToken);

                    do
                    {
                        await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
                        await pool.RefreshAsync(detailLevel: new ODATADetailLevel(selectClause: "allocationState,id,resizeErrors"), cancellationToken: cancellationToken);
                    }
                    while (AllocationState.Steady != pool.AllocationState);

                    if (pool.ResizeErrors?.Any() ?? false)
                    {
                        if (pool.ResizeErrors.Any(e => PoolResizeErrorCodes.UnsupportedVMSize.Equals(e.Code, StringComparison.OrdinalIgnoreCase)))
                        {
                            ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.LightYellow(), $"Deferring {name} due to 'UnsupportedVMSize'");
                            return VerifyVMIResult.NextRegion;
                        }
                        else if (pool.ResizeErrors.Any(e =>
                            PoolResizeErrorCodes.AccountCoreQuotaReached.Equals(e.Code, StringComparison.OrdinalIgnoreCase) ||
                            PoolResizeErrorCodes.AccountLowPriorityCoreQuotaReached.Equals(e.Code, StringComparison.OrdinalIgnoreCase) ||
                            PoolResizeErrorCodes.AccountSpotCoreQuotaReached.Equals(e.Code, StringComparison.OrdinalIgnoreCase) ||
                            (!vm.LowPriority && @"AccountVMSeriesCoreQuotaReached".Equals(e.Code, StringComparison.OrdinalIgnoreCase))))
                        {
                            if (!vm.LowPriority)
                            {
                                ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.Yellow(), $"Retrying {name} ({vm.VmFamily}) for '~CoreQuotaReached'.");
                            }

                            return VerifyVMIResult.Retry; // Either a timing race condition or other concurrent use of the batch account. Try again.
                        }
                        else
                        {
                            var errorCodes = pool.ResizeErrors.Select(e => e.Code).Distinct().ToList();
                            var isAllocationFailed = errorCodes.Count == 1 && errorCodes.Contains(PoolResizeErrorCodes.AllocationFailed, StringComparer.OrdinalIgnoreCase);
                            var isAllocationTimedOut = errorCodes.Count == 1 && errorCodes.Contains(PoolResizeErrorCodes.AllocationTimedOut, StringComparer.OrdinalIgnoreCase);
                            var isOverconstrainedAllocationRequest = errorCodes.Count == 1 && errorCodes.Contains(PoolResizeErrorCodes.OverconstrainedAllocationRequestError, StringComparer.OrdinalIgnoreCase);
                            var isBatchOutOfCapacity = errorCodes.Count == 1 && errorCodes.Contains("BatchOutOfCapacity", StringComparer.OrdinalIgnoreCase);
                            ProviderError? providerError = default;
                            string? additionalReport = default;
                            VerifyVMIResult? result = default;

                            if (isAllocationTimedOut)
                            {
                                ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.Yellow(), $"Retrying {name} ({vm.VmFamily}) for '{PoolResizeErrorCodes.AllocationTimedOut}'.");
                                return VerifyVMIResult.Retry;
                            }

                            if (isBatchOutOfCapacity)
                            {
                                ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.LightYellow(), $"Deferring {name} due to 'BatchOutOfCapacity'");
                                return VerifyVMIResult.NextRegion;
                            }

                            if (isAllocationFailed)
                            {
                                var values = pool.ResizeErrors
                                    .SelectMany(e => e.Values ?? [])
                                    .ToDictionary(pair => pair.Name, pair => pair.Value, StringComparer.OrdinalIgnoreCase);

                                if (values.TryGetValue("Reason", out var reason))
                                {
                                    if ("The server encountered an internal error.".Equals(reason, StringComparison.OrdinalIgnoreCase))
                                    {
                                        // https://learn.microsoft.com/troubleshoot/azure/general/azure-batch-pool-resizing-failure#symptom-for-scenario-4
                                        ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.LightYellow(), $"Retrying {name} ({vm.VmFamily}) for 'internal error'.");
                                        return VerifyVMIResult.Retry;
                                    }
                                    else if ("Allocation failed as subnet has delegation to external resources.".Equals(reason, StringComparison.OrdinalIgnoreCase))
                                    {
                                        // https://learn.microsoft.com/troubleshoot/azure/general/azure-batch-pool-resizing-failure#symptom-for-scenario-1
                                        result = VerifyVMIResult.Skip;
                                    }
                                    else if ("Deployment allocation failed due to insufficient capacity for the requested VM size in the region.".Equals(reason, StringComparison.OrdinalIgnoreCase))
                                    {
                                        // https://learn.microsoft.com/en-us/troubleshoot/azure/hpc/batch/azure-batch-pool-resizing-failure#scenario-5-insufficient-capacity-for-the-requested-vm-size-in-the-current-region
                                        ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.LightYellow(), $"Deferring {name} due to 'InsufficientCapacity'");
                                        return VerifyVMIResult.NextRegion;
                                    }
                                }

                                if (values.TryGetValue("Provider Error Json Truncated", out var isTruncatedString) && bool.TryParse(isTruncatedString, out var isTruncated))
                                {
                                    // Based on https://learn.microsoft.com/troubleshoot/azure/general/azure-batch-pool-resizing-failure#symptom-for-scenario-2

                                    if (!isTruncated && values.TryGetValue("Provider Error Json", out var errorJsonString))
                                    {
                                        providerError = JsonSerializer.Deserialize<ProviderError>(errorJsonString);

                                        (var now, additionalReport, result) = providerError?.Error.Code switch
                                        {
                                            var x when StringComparison.OrdinalIgnoreCase.Equals("InternalServerError", x) => (true, default, VerifyVMIResult.Retry),
                                            var x when StringComparison.OrdinalIgnoreCase.Equals("NetworkingInternalOperationError", x) => (true, default, VerifyVMIResult.Retry),
                                            var x when StringComparison.OrdinalIgnoreCase.Equals("BadRequest", x) => (false, $"Note: HyperVGenerations: '{string.Join("', '", vm.HyperVGenerations)}'", VerifyVMIResult.Skip),
                                            var x when (x?.Contains("Internal", StringComparison.OrdinalIgnoreCase) ?? false) || (providerError?.Error.Message?.Contains(" retry later", StringComparison.OrdinalIgnoreCase) ?? false) => (true, default, VerifyVMIResult.Retry),
                                            _ => (false, default, VerifyVMIResult.Skip),
                                        };

                                        if (now)
                                        {
                                            if (!result.HasValue)
                                            {
                                                System.Diagnostics.Debugger.Break();
                                            }

                                            ConsoleHelper.WriteLine($"{VerbFromResult(result)} {name} ({vm.VmFamily}) for '{providerError?.Error.Code}'.");
                                            return result.Value;
                                        }
                                    }
                                }
                            }

                            result ??= VerifyVMIResult.Skip;

                            StringBuilder sb = new($"{VerbFromResult(result)} {name} ({vm.VmFamily}) due to allocation failure(s): '{string.Join("', '", pool.ResizeErrors.Select(e => e.Code))}'.");

                            if (providerError is null && !isOverconstrainedAllocationRequest)
                            {
                                sb.AppendLine($"Additional information: Message: '{string.Join($"',{Environment.NewLine}'", pool.ResizeErrors.Select(e => e.Message))}'");
                                pool.ResizeErrors!.SelectMany(e => e.Values ?? [])
                                    .ForEach(detail => sb.Append($"'{detail.Name}': '{detail.Value}'."));
                            }

                            if (providerError is not null)
                            {
                                sb.AppendLine($"Code: {providerError.Value.Error.Code}{Environment.NewLine}Message: '{providerError.Value.Error.Message}'.");
                            }

                            if (additionalReport is not null)
                            {
                                sb.AppendLine(additionalReport);
                            }

                            ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.LightYellow(), sb.ToString());
                            return result.Value;

                            static string VerbFromResult(VerifyVMIResult? value) => value switch
                            {
                                VerifyVMIResult.Skip => "Skipping",
                                VerifyVMIResult.NextRegion => "Deferring",
                                VerifyVMIResult.Retry => "Retrying",
                                _ => throw new System.Diagnostics.UnreachableException($"Invalid value of VerifyVMIResult: {value?.ToString() ?? "<null>"}."),
                            };
                        }
                    }

                    List<ComputeNode> nodes;

                    do
                    {
                        await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                        nodes = await pool.ListComputeNodes(detailLevel: new ODATADetailLevel(selectClause: "id,state,errors")).ToAsyncEnumerable().ToListAsync(cancellationToken);

                        if (nodes.Any(n => n.Errors?.Any() ?? false))
                        {
                            StringBuilder sb = new($"Skipping {name} ({vm.VmFamily}) due to node startup failure(s): '{string.Join("', '", nodes.SelectMany(n => n.Errors).Select(e => e.Code))}'.");
                            sb.AppendLine($"Additional information: Message: '{string.Join("', '", nodes.SelectMany(n => n.Errors).Select(e => e.Message))}'");
                            nodes.SelectMany(n => n.Errors).SelectMany(e => e.ErrorDetails ?? Enumerable.Empty<NameValuePair>())
                                .ForEach(detail => sb.AppendLine($"'{detail.Name}': '{detail.Value}'."));
                            ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.LightYellow(), sb.ToString());
                            return VerifyVMIResult.Skip;
                        }

                        if (nodes.Any(n => ComputeNodeState.Unusable == n.State
                            || ComputeNodeState.Unknown == n.State))
                        {
                            ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.LightYellow(), $"Skipping {name} ({vm.VmFamily}) due to node startup-to-unusable transition without errors (likely batch node agent startup failure).");
                            return VerifyVMIResult.Skip;
                        }
                    }
                    while (nodes.Any(n => ComputeNodeState.Idle != n.State
                        && ComputeNodeState.Preempted != n.State
                        && ComputeNodeState.LeavingPool != n.State));
                }
                catch (BatchException exception)
                {
                    if (exception.StackTrace?.Contains(" at Microsoft.Azure.Batch.CloudPool.CommitAsync(IEnumerable`1 additionalBehaviors, CancellationToken cancellationToken)") ?? false)
                    {
                        if (exception.RequestInformation.HttpStatusCode is null)
                        {
                            return VerifyVMIResult.Retry;
                        }
                        else if (System.Net.HttpStatusCode.InternalServerError == exception.RequestInformation.HttpStatusCode)
                        {
                            var error = exception.RequestInformation.BatchError;
                            var isTimedOut = error is null || error.Code == BatchErrorCodeStrings.OperationTimedOut;

                            if (!isTimedOut)
                            {
                                pool = default;
                            }

                            StringBuilder sb = new(isTimedOut
                                ? $"Retrying {name} due to pool creation failure(s): '{error?.Code ?? "[unknown]"}'."
                                : $"Deferring {name} due to pool creation failure(s): '{error?.Code ?? "[unknown]"}'.");

                            if (error is not null)
                            {
                                sb.AppendLine($"'{error.Code}'({error.Message.Language}): '{error.Message.Value}'");
                                (error.Values ?? Enumerable.Empty<BatchErrorDetail>())
                                    .ForEach(value => sb.AppendLine($"'{value.Key}': '{value.Value}'"));
                            }

                            sb.AppendLine("Please check and delete pool manually.");
                            ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.LightYellow(), sb.ToString());
                            return isTimedOut ? VerifyVMIResult.Retry : VerifyVMIResult.NextRegion;
                        }
                        else if (exception.RequestInformation.HttpStatusCode == System.Net.HttpStatusCode.Conflict)
                        {
                            var error = exception.RequestInformation.BatchError;
                            var isQuota = BatchErrorCodeStrings.PoolQuotaReached.Equals(error?.Code, StringComparison.OrdinalIgnoreCase);
                            var isPoolBeingDeleted = BatchErrorCodeStrings.PoolBeingDeleted.Equals(error?.Code, StringComparison.OrdinalIgnoreCase);
                            pool = default;

                            if (isPoolBeingDeleted || isQuota)
                            {
                                return VerifyVMIResult.Retry;
                            }

                            StringBuilder sb = new($"Deferring {name} due to due to pool creation failure(s):'{error?.Code ?? "[unknown]"}'.");

                            if (error is not null)
                            {
                                sb.AppendLine($"'{error.Code}'({error.Message.Language}): '{error.Message.Value}'");
                                (error.Values ?? Enumerable.Empty<BatchErrorDetail>())
                                    .ForEach(value => sb.AppendLine($"'{value.Key}': '{value.Value}'"));
                            }

                            ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.LightYellow(), sb.ToString());
                            return VerifyVMIResult.NextRegion;
                        }
                        else
                        {
                            ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.LightYellow(), $"Deferring {name} due to pool creation '{exception.RequestInformation.HttpStatusCode}' failure ('{exception.RequestInformation?.BatchError?.Code}' error).{Environment.NewLine}Please check and delete pool manually.");
                            pool = default;
                            return VerifyVMIResult.NextRegion;
                        }
                    }
                }
                finally
                {
                    System.Net.HttpStatusCode? firstStatusCode = null;

                    while (pool is not null)
                    {
                        try
                        {
                            await ResetOnAccess(ref pool).DeleteAsync(cancellationToken: CancellationToken.None);
                        }
                        catch (BatchException exception)
                        {
                            firstStatusCode ??= exception.RequestInformation.HttpStatusCode;

                            switch (exception.RequestInformation.HttpStatusCode)
                            {
                                case System.Net.HttpStatusCode.Conflict:
                                    break;

                                default:
                                    ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.Red(), $"Failed to delete pool '{name}' ('{exception.RequestInformation.HttpStatusCode?.ToString("G") ?? "[no response]"}', initial attempt '{firstStatusCode}') {exception.GetType().FullName}: {exception.Message}{Environment.NewLine}Please check and delete pool manually.{Environment.NewLine}");
                                    break;
                            }
                        }
                        catch (InvalidOperationException exception)
                        {
                            if ("This operation is forbidden on unbound objects.".Equals(exception.Message, StringComparison.OrdinalIgnoreCase))
                            {
                                try
                                {
                                    pool = await accountInfo.Client.PoolOperations.GetPoolAsync(name, new ODATADetailLevel(selectClause: "id"), cancellationToken: CancellationToken.None);
                                }
                                catch (Exception ex) when (ex is not System.OperationCanceledException)
                                {
                                    ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.Red(), $"Failed to delete pool '{name}' on '{accountInfo.Name}' due to '{firstStatusCode}'. Attempting to locate the pool resulted in {ex.GetType().FullName}: {ex.Message}{Environment.NewLine}Please check and delete pool manually.");
                                }
                            }
                            else
                            {
                                ConsoleHelper.WriteLine(accountInfo.Name, ForegroundColorSpan.Red(), $"Failed to delete pool '{name}' on '{accountInfo.Name}' due to '{firstStatusCode}' {exception.GetType().FullName}: {exception.Message}{Environment.NewLine}Please check and delete pool manually.");
                            }
                        }
                    }
                }

                return VerifyVMIResult.Use;
            }

            private static async ValueTask<TestContext> GetTestQuotaContext(BatchAccountInfo batchAccount, CancellationToken cancellationToken)
            {
                var count = 0;
                var lowPriorityCoresInUse = 0;
                var dedicatedCoresInUse = 0;
                Dictionary<string, int> dedicatedCoresInUseByVmFamily = new(StringComparer.OrdinalIgnoreCase);

                await foreach (var (vmsize, dedicated, lowPriority) in batchAccount.Client.PoolOperations
                    .ListPools(detailLevel: new ODATADetailLevel(selectClause: "id,vmSize")).ToAsyncEnumerable()
                    .Join(batchAccount.Client.PoolOperations.ListPoolNodeCounts().ToAsyncEnumerable(),
                        pool => pool.Id,
                        counts => counts.PoolId,
                        (pool, counts) => (pool.VirtualMachineSize, counts.Dedicated, counts.LowPriority),
                        StringComparer.OrdinalIgnoreCase)
                    .WithCancellation(cancellationToken))
                {
                    ++count;
                    var info = batchSkus![vmsize];
                    var vmFamily = info.VmFamily;
                    var coresPerNode = info.VCpusAvailable ?? UnknownVCpuCores;

                    lowPriorityCoresInUse += ((lowPriority?.Total ?? 0) - (lowPriority?.Preempted ?? 0)) * coresPerNode;
                    var dedicatedCores = (dedicated?.Total ?? 0) * coresPerNode;
                    dedicatedCoresInUse += dedicatedCores;
                    if (!dedicatedCoresInUseByVmFamily.TryAdd(vmFamily, dedicatedCores))
                    {
                        dedicatedCoresInUseByVmFamily[vmFamily] += dedicatedCores;
                    }
                }

                return new(
                    batchAccount.PoolQuota ?? 0,
                    batchAccount.LowPriorityCoreQuota ?? 0,
                    batchAccount.DedicatedCoreQuota ?? 0,
                    batchAccount.IsDedicatedCoreQuotaPerVmFamilyEnforced ?? false,
                    batchAccount.DedicatedCoreQuotaPerVmFamily.ToDictionary(quota => quota.Name, quota => quota.CoreQuota ?? 0, StringComparer.OrdinalIgnoreCase),
                    dedicatedCoresInUseByVmFamily)
                {
                    PoolCount = count,
                    LowPriorityCoresInUse = lowPriorityCoresInUse,
                    DedicatedCoresInUse = dedicatedCoresInUse,
                };
            }

            private static bool CanBatchAccountValidateSku(WrappedVmSku vmSize, TestContext context)
            {
                var info = vmSize.VmSku.Sku;
                var family = info.VmFamily;
                var coresPerNode = info.VCpusAvailable ?? UnknownVCpuCores;
                var isLowPriority = info.LowPriority;

                if (coresPerNode == 0) { coresPerNode = UnknownVCpuCores; }

                return !(0 > context.PoolQuota
                    || isLowPriority && coresPerNode > context.LowPriorityCoreQuota
                    || !isLowPriority && coresPerNode > context.DedicatedCoreQuota
                    || !isLowPriority && context.IsDedicatedCoreQuotaPerVmFamilyEnforced && coresPerNode > (context.DedicatedCoreQuotaPerVmFamily.TryGetValue(family, out var quota) ? quota : 0));
            }

            private static bool AddSkuMetadataToQuotaIfQuotaPermits(WrappedVmSku vmSize, TestContext context)
            {
                var info = vmSize.VmSku.Sku;
                var family = info.VmFamily;
                var coresPerNode = info.VCpusAvailable ?? UnknownVCpuCores;
                var isLowPriority = info.LowPriority;

                if (context.PoolCount + 1 > context.PoolQuota ||
                    isLowPriority && context.LowPriorityCoresInUse + coresPerNode > context.LowPriorityCoreQuota ||
                    !isLowPriority && context.DedicatedCoresInUse + coresPerNode > context.DedicatedCoreQuota ||
                    !isLowPriority && context.IsDedicatedCoreQuotaPerVmFamilyEnforced && (context.DedicatedCoresInUseByVmFamily.TryGetValue(family, out var inUse) ? inUse : 0) > (context.DedicatedCoreQuotaPerVmFamily.TryGetValue(family, out var quota) ? quota : 0))
                {
                    return false;
                }

                context.PoolCount += 1;

                if (isLowPriority)
                {
                    context.LowPriorityCoresInUse += coresPerNode;
                }
                else
                {
                    context.DedicatedCoresInUse += coresPerNode;

                    if (context.IsDedicatedCoreQuotaPerVmFamilyEnforced)
                    {
                        if (!context.DedicatedCoresInUseByVmFamily.TryAdd(family, coresPerNode))
                        {
                            context.DedicatedCoresInUseByVmFamily[family] += coresPerNode;
                        }
                    }
                }

                return true;
            }

            private static void RemoveSkuMetadataFromQuota(WrappedVmSku vmSize, TestContext context)
            {
                var info = vmSize.VmSku.Sku;
                var family = info.VmFamily;
                var coresPerNode = info.VCpusAvailable ?? UnknownVCpuCores;
                var isLowPriority = info.LowPriority;

                context.PoolCount -= 1;

                if (isLowPriority)
                {
                    context.LowPriorityCoresInUse -= coresPerNode;
                }
                else
                {
                    context.DedicatedCoresInUse -= coresPerNode;

                    if (context.IsDedicatedCoreQuotaPerVmFamilyEnforced)
                    {
                        context.DedicatedCoresInUseByVmFamily[family] -= coresPerNode;
                    }
                }
            }

            private static T ResetOnAccess<T>(ref T? value)
            {
                ArgumentNullException.ThrowIfNull(value);

                var result = value;
                value = default;
                return result;
            }

            void IDisposable.Dispose()
            {
                ((IDisposable)accountInfo).Dispose();
            }

            private struct ProviderError
            {
                [JsonPropertyName("error")]
                public ErrorRecord Error { get; set; }

                public struct ErrorRecord
                {
                    [JsonPropertyName("code")]
                    public string Code { get; set; }

                    [JsonPropertyName("message")]
                    public string Message { get; set; }
                }
            }

            // Quota stays the same for the entire session
            private sealed record class TestContext(int PoolQuota, int LowPriorityCoreQuota, int DedicatedCoreQuota, bool IsDedicatedCoreQuotaPerVmFamilyEnforced, Dictionary<string, int> DedicatedCoreQuotaPerVmFamily, Dictionary<string, int> DedicatedCoresInUseByVmFamily)
            {
                // Calculated current values are adjusted for each SKU to help prevent exceeding quota
                public int PoolCount { get; set; }
                public int LowPriorityCoresInUse { get; set; }
                public int DedicatedCoresInUse { get; set; }
            }
        }
    }
}
