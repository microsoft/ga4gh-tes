// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Azure.Core;
using Tes.Models;
using static GenerateBatchVmSkus.Program;

namespace GenerateBatchVmSkus
{
    internal static class AzureBatchSkuValidator
    {
        private static Microsoft.Azure.Batch.ImageReference V1ImageReference
            => new("ubuntu-server-container", "microsoft-azure-batch", "20-04-lts", "latest");

        private static Microsoft.Azure.Batch.ImageReference V2ImageReference
            => new("ubuntu-hpc", "microsoft-dsvm", "2004", "latest");

        private static Microsoft.Azure.Batch.VirtualMachineConfiguration GetMachineConfiguration(bool useV2, bool encryptionAtHostSupported)
        {
            return new(useV2 ? V2ImageReference : V1ImageReference, "batch.node.ubuntu 20.04")
            {
                DiskEncryptionConfiguration = encryptionAtHostSupported
                    ? new Microsoft.Azure.Batch.DiskEncryptionConfiguration(
                        targets: new List<Microsoft.Azure.Batch.Common.DiskEncryptionTarget>()
                        {
                            Microsoft.Azure.Batch.Common.DiskEncryptionTarget.OsDisk,
                            Microsoft.Azure.Batch.Common.DiskEncryptionTarget.TemporaryDisk
                        })
                    : null
            };
        }

        public record struct ValidationResults(IEnumerable<VirtualMachineInformation> Verified, IEnumerable<VmSku> Unverifyable);

        private record class WrappedVmSku(VmSku VmSku)
        {
            public bool Validated { get; set; } = false;
        }

        public static async ValueTask<ValidationResults> ValidateSkus(IEnumerable<VmSku> skus, IAsyncEnumerable<BatchAccountInfo> batchAccounts, List<VirtualMachineInformation> batchSkus, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(skus);
            ArgumentNullException.ThrowIfNull(batchAccounts);
            ArgumentNullException.ThrowIfNull(batchSkus);

            AzureBatchSkuValidator.batchSkus = batchSkus.ToDictionary(sku => sku.VmSize, StringComparer.OrdinalIgnoreCase);
            var batchAccountEnumerator = batchAccounts.GetAsyncEnumerator(cancellationToken);
            List<Validator> validators = new();
            List<Task> validatorTasks = new();
            skus = skus.ToList();

            try
            {
                var skusChannel = Channel.CreateBounded<WrappedVmSku>(new BoundedChannelOptions(skus.Count()) { SingleReader = true, SingleWriter = true, FullMode = BoundedChannelFullMode.Wait });

                await skus.ToAsyncEnumerable().Select(sku => new WrappedVmSku(sku)).ForEachAwaitWithCancellationAsync(async (sku, token) => await skusChannel.Writer.WriteAsync(sku, token), cancellationToken);
                skusChannel.Writer.Complete();

                for (var validator = await GetNextValidator(); validator is not null; validator = await GetNextValidator())
                {
                    validatorTasks.Add(validator.ValidateSkus(skusChannel, cancellationToken));
                    skusChannel = validator.ResultSkus;
                }

                return 0 == validators.Count
                    ? new(Enumerable.Empty<VirtualMachineInformation>(), Enumerable.Empty<VmSku>())
                    : await GetResults(skusChannel.Reader.ReadAllAsync(cancellationToken), cancellationToken);

                async ValueTask<ValidationResults> GetResults(IAsyncEnumerable<WrappedVmSku> results, CancellationToken cancellationToken)
                {
                    var skus = await results.ToListAsync(cancellationToken);
                    // Keep the batch accounts around long enough to allow the batch pools to be deleted.
                    await Task.WhenAll(validatorTasks.Concat(validators.Select(v => v.ValidationTask!).Where(t => t is not null)).ToArray());
                    Console.Write(new string(Enumerable.Repeat(' ', linePrefix.Length - 1).Append('\r').ToArray()));

                    return new(
                        skus.Where(sku => sku.Validated).SelectMany(sku => sku.VmSku.Skus),
                        skus.Where(sku => !sku.Validated).Select(sku => sku.VmSku));
                }
            }
            finally
            {
                validators.ForEach(validator => ((IDisposable)validator).Dispose());
                await batchAccountEnumerator.DisposeAsync();
            }

            async ValueTask<Validator?> GetNextValidator()
            {
                if (await batchAccountEnumerator!.MoveNextAsync())
                {
                    var validator = new Validator(batchAccountEnumerator.Current);
                    validators.Add(validator);
                    return validator;
                }

                return default;
            }
        }

        private static IDictionary<string, VirtualMachineInformation>? batchSkus;

        private static long count = 0;
        private static long started = 0;
        private static long completed = 0;

        private static readonly object consoleLock = new();
        private static string linePrefix = string.Empty;

        private static void ConsoleWriteTempLine()
        {
            if (!Console.IsOutputRedirected)
            {
                lock (consoleLock)
                {
                    var line = $"SKU validations: Started: {started / (double)count:P2} / Completed: {completed / (double)count:P2}";
                    var sb = new StringBuilder();
                    sb.Append(linePrefix);
                    sb.Append(line);
                    sb.Append('\r');
                    Console.Write(sb.ToString());

                    sb.Clear();
                    Enumerable.Repeat(' ', line.Length).ForEach(space => sb.Append(space));
                    sb.Append('\r');
                    linePrefix = sb.ToString();
                }
            }
        }

        private static void ConsoleWriteLine(string name, ConsoleColor foreground, string? content = null)
        {
            var result = string.Empty;

            if (!string.IsNullOrEmpty(content))
            {
                var lines = content
                    .ReplaceLineEndings(Environment.NewLine)
                    .Split(Environment.NewLine);

                if (string.IsNullOrEmpty(lines.Last()))
                {
                    lines = lines.SkipLast(1).ToArray();
                }

                result = string.Join(Environment.NewLine,
                    lines.Select((line, i) => i == 0 ? $"[{name}]: {line}" : "    " + line));
            }
            else
            {
                result = Environment.NewLine;
            }

            lock (consoleLock)
            {
                Console.ForegroundColor = foreground;
                Console.WriteLine(linePrefix + result);
                linePrefix = string.Empty;
                Console.ResetColor();
            }
        }

        private sealed class Validator : IDisposable
        {
            private readonly BatchAccountInfo accountInfo;
            private readonly object consoleLock = new();

            public Validator(BatchAccountInfo batchAccount)
            {
                ArgumentNullException.ThrowIfNull(batchAccount);
                accountInfo = batchAccount;
            }

            internal AzureLocation Location => accountInfo.Location;
            internal readonly Channel<WrappedVmSku> ResultSkus = Channel.CreateUnbounded<WrappedVmSku>(new() { SingleReader = true, SingleWriter = true });
            private readonly Channel<WrappedVmSku> candidateSkus = Channel.CreateUnbounded<WrappedVmSku>(new() { SingleReader = true, SingleWriter = true });
            internal Task? ValidationTask = null;

            private async ValueTask<IEnumerable<WrappedVmSku>> GetVmSkusAsync(TestContext context, CancellationToken cancellationToken)
            {
                var result = Enumerable.Empty<WrappedVmSku>();

                while (candidateSkus.Reader.TryRead(out var vm))
                {
                    if (CanBatchAccountValidateSku(vm, context))
                    {
                        result = result.Append(vm);
                    }
                    else
                    {
                        await ResultSkus.Writer.WriteAsync(vm, cancellationToken);
                        //lock(consoleLock)
                        //{
                        //    SetForegroundColor(ConsoleColor.Yellow);
                        //    ConsoleWriteLine($"Deferring '{vm.VmSku.Name}' due to quota (start of processing).");
                        //    ResetColor();
                        //}
                    }
                }

                result = result.ToList();
                _ = Interlocked.Add(ref count, result.Count());
                return result;
            }

            internal async Task ValidateSkus(Channel<WrappedVmSku> skus, CancellationToken cancellationToken)
            {
                await foreach (var sku in skus.Reader.ReadAllAsync(cancellationToken).WithCancellation(cancellationToken))
                {
                    if (sku.Validated || !sku.VmSku.Sku.RegionsAvailable.Contains(Location.Name))
                    {
                        await ResultSkus.Writer.WriteAsync(sku, cancellationToken);
                    }
                    else
                    {
                        await candidateSkus.Writer.WriteAsync(sku, cancellationToken);
                        ValidationTask ??= StartValidation();
                    }
                }

                candidateSkus.Writer.Complete();

                Task StartValidation()
                {
                    var task = new Task(async () => await ValidateSkus(cancellationToken), cancellationToken, TaskCreationOptions.LongRunning);
                    task.Start();
                    return task;
                }
            }

            private async ValueTask ValidateSkus(CancellationToken cancellationToken)
            {
                var processed = 0;
                var processedDeferred = 0;

                try
                {
                    var context = await GetTestQuotaContext(accountInfo, cancellationToken);

                    var StartLoadedTest = new Func<WrappedVmSku, Task<(WrappedVmSku vmSize, VerifyVMIResult result)>>(async vmSize =>
                    {
                        _ = Interlocked.Increment(ref started);
                        return (vmSize, result: await TestVMSizeInBatchAsync(vmSize, cancellationToken));
                    });

                    Dictionary<string, (bool Ignore, int RetryCount, DateTime NextAttempt, WrappedVmSku VmSize)> retries = new(StringComparer.OrdinalIgnoreCase);
                    List<Task<(WrappedVmSku vmSize, VerifyVMIResult result)>> tests = new();

                    List<Task> tasks = new();
                    Task? retryReadyTask = default;
                    tasks.Add(candidateSkus.Reader.Completion);
                    var moreInputTask = candidateSkus.Reader.WaitToReadAsync(cancellationToken).AsTask();
                    tasks.Add(moreInputTask);

                    if (!cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            List<WrappedVmSku> skusToTest = new(await GetVmSkusAsync(context, cancellationToken));
                            var loadedTests = skusToTest.Where(CanTestNow).ToList();

                            for (tests = loadedTests.Select(StartLoadedTest).ToList();
                                (tests.Any() || tasks.Any()) && !cancellationToken.IsCancellationRequested;
                                tests.AddRange(loadedTests.Select(StartLoadedTest)))
                            {
                                loadedTests.ForEach(loadedTest => _ = skusToTest.Remove(loadedTest));
                                loadedTests.Clear();
                                ConsoleWriteTempLine();
                                var now = DateTime.UtcNow;
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
                                                        await ResultSkus.Writer.WriteAsync(vmSize, cancellationToken);
                                                        break;

                                                    case VerifyVMIResult.Skip:
                                                        ++processed;
                                                        _ = retries.Remove(vmSize.VmSku.Name);
                                                        break;

                                                    case VerifyVMIResult.NextRegion:
                                                        ++processed;
                                                        ++processedDeferred;
                                                        _ = retries.Remove(vmSize.VmSku.Name);
                                                        await ResultSkus.Writer.WriteAsync(vmSize, cancellationToken);
                                                        break;

                                                    case VerifyVMIResult.Retry:
                                                        ++processedDeferred;

                                                        if (!retries.TryGetValue(vmSize.VmSku.Name, out var lastRetry) || lastRetry.RetryCount < 3)
                                                        {
                                                            retries[vmSize.VmSku.Name] = (false, lastRetry.RetryCount + 1, DateTime.UtcNow + TimeSpan.FromMinutes(4), vmSize);
                                                            _ = Interlocked.Decrement(ref started);
                                                            _ = Interlocked.Decrement(ref completed);
                                                        }
                                                        else
                                                        {
                                                            lock (consoleLock)
                                                            {
                                                                SetForegroundColor(ConsoleColor.Yellow);
                                                                ConsoleWriteLine($"Skipping {vmSize.VmSku.Name} because retry attempts were exhausted.");
                                                                ResetColor();
                                                            }

                                                            ++processed;
                                                            _ = retries.Remove(vmSize.VmSku.Name);
                                                        }

                                                        break;
                                                }
                                            }
                                            catch (Exception e)
                                            {
                                                lock (consoleLock)
                                                {
                                                    SetForegroundColor(ConsoleColor.Red);
                                                    ConsoleWriteLine("Due to the following failure, it is unknown which SKU this failure report is related to. This SKU will not be included in the final results.");
                                                    ConsoleWriteLine(e.ToString());
                                                    ResetColor();
                                                }
                                            }
                                        }
                                        break;

                                    case Task<bool> boolReturnTask:
                                        if (moreInputTask == boolReturnTask)
                                        {
                                            skusToTest.AddRange(await GetVmSkusAsync(context, cancellationToken));

                                            if (!candidateSkus.Reader.Completion.IsCompleted)
                                            {
                                                moreInputTask = candidateSkus.Reader.WaitToReadAsync(cancellationToken).AsTask();
                                                tasks.Add(moreInputTask);
                                            }
                                        }
                                        else
                                        {
                                            lock (consoleLock)
                                            {
                                                SetForegroundColor(ConsoleColor.Yellow);
                                                ConsoleWriteLine("Unexpected task completion. Contact developer.");
                                                ResetColor();
                                            }
                                        }
                                        break;

                                    case Task voidReturnTask:
                                        if (retryReadyTask == voidReturnTask)
                                        {
                                            now = DateTime.UtcNow;
                                            var readyRetries = retries.Values.Where(retry => !retry.Ignore && now >= retry.NextAttempt).ToList();
                                            skusToTest.AddRange(readyRetries.Select(retry => retry.VmSize));
                                            readyRetries.ForEach(retry => retries[retry.VmSize.VmSku.Name] = (true, retry.RetryCount, retry.NextAttempt, retry.VmSize));

                                            var oldestNextRetryAttempt = retries.Values.Any(retry => !retry.Ignore) ? retries.Values.Where(retry => !retry.Ignore).Min(retryCount => retryCount.NextAttempt) : default;

                                            if (oldestNextRetryAttempt != default)
                                            {
                                                retryReadyTask = Task.Delay(TimeSpan.Zero.Max(oldestNextRetryAttempt - now), cancellationToken);
                                                tasks.Add(retryReadyTask);
                                            }
                                        }
                                        else if (candidateSkus.Reader.Completion == voidReturnTask)
                                        {
                                            _ = tasks.Remove(moreInputTask);
                                        }
                                        else
                                        {
                                            lock (consoleLock)
                                            {
                                                SetForegroundColor(ConsoleColor.Yellow);
                                                ConsoleWriteLine("Unexpected task completion. Contact developer.");
                                                ResetColor();
                                            }
                                        }
                                        break;

                                    default:
                                        lock (consoleLock)
                                        {
                                            SetForegroundColor(ConsoleColor.Yellow);
                                            ConsoleWriteLine("Unexpected task completion. Contact developer.");
                                            ResetColor();
                                        }
                                        break;
                                }

                                loadedTests = skusToTest.Where(CanTestNow).ToList();
                            }

                            await skusToTest.ToAsyncEnumerable().ForEachAwaitWithCancellationAsync(async (vmSize, token) =>
                            {
                                await ResultSkus.Writer.WriteAsync(vmSize, token);
                                lock (consoleLock)
                                {
                                    SetForegroundColor(ConsoleColor.Yellow);
                                    ConsoleWriteLine($"Deferring '{vmSize.VmSku.Name}' due to quota (end of processing).");
                                    ResetColor();
                                }
                            },
                            cancellationToken);

                            _ = Interlocked.Add(ref count, -skusToTest.Count);
                        }
                        catch (Exception e) // TODO: Flag somewhere to prevent any results from being produced.
                        {
                            lock (consoleLock)
                            {
                                SetForegroundColor(ConsoleColor.Red);
                                ConsoleWriteLine(e.ToString());
                                ConsoleWriteLine(Environment.NewLine + "This failure caused this batch account to not be used again in this session. This will probably cause incorrect results.");
                                ResetColor();
                            }
                        }
                        finally
                        {
                            ConsoleWriteTempLine();
                            // Keep the batch account around long enough to allow the batch pools to be deleted.
                            _ = await Task.WhenAll(tests);
                        }
                    }

                    ResultSkus.Writer.Complete();

                    lock (consoleLock)
                    {
                        SetForegroundColor(ConsoleColor.Green);
                        ConsoleWriteLine($"Validated a net of {processed - processedDeferred} SKUs out of {processed} attempts.");
                        ResetColor();
                    }

                    bool CanTestNow(WrappedVmSku sku) => AddSkuMetadataToQuotaIfQuotaPermits(sku, context);
                }
                catch (OperationCanceledException)
                { }
                catch (Exception e)
                {
                    lock (consoleLock)
                    {
                        SetForegroundColor(ConsoleColor.Red);
                        ConsoleWriteLine($"'{accountInfo.Name}' is done processing its inputs due to an error.");
                        ResetColor();
                    }
                    ResultSkus.Writer.Complete(e);
                }
                finally
                {
                    ConsoleWriteTempLine();
                }
            }

            private enum VerifyVMIResult
            {
                Use,
                Skip,
                NextRegion,
                Retry
            }

            private readonly StringBuilder consoleLines = new();
            private ConsoleColor? consoleColor = null;

            private void SetForegroundColor(ConsoleColor color)
            {
                consoleLines.Clear();
                consoleColor = color;
            }

            private void ConsoleWriteLine(string? content = null)
            {
                consoleLines.AppendLine(content);
            }

            private void ResetColor()
            {
                AzureBatchSkuValidator.ConsoleWriteLine($"{accountInfo.Name} ({accountInfo.Location.DisplayName})", consoleColor!.Value, consoleLines.ToString());
                consoleColor = null;
            }

            private async Task<VerifyVMIResult> TestVMSizeInBatchAsync(WrappedVmSku vmSize, CancellationToken cancellationToken)
            {
                ArgumentNullException.ThrowIfNull(vmSize);

                // When a pool is first created and the autoscale is first evaluated, the metrics have no samples (so $<metric>.Count() will return 0). All subsequent reevaluations should return positive integer values, causing the nodes to be removed while the developer is working on the reason the pool wasn't deleted.
                // https://learn.microsoft.com/azure/batch/batch-automatic-scaling#methods
                const string DedicatedAutoScaleFormula = "$NodeDeallocationOption = retaineddata; $TargetDedicatedNodes = ($PendingTasks.Count() ? 0 : 1);";
                const string LowPriorityAutoScaleFormula = "$NodeDeallocationOption = retaineddata; $TargetLowPriorityNodes = ($PendingTasks.Count() ? 0 : 1);";

                var name = vmSize.VmSku.Name;
                var vm = vmSize.VmSku.Sku;

                var pool = accountInfo.Client.PoolOperations.CreatePool(poolId: name, virtualMachineSize: name, virtualMachineConfiguration: GetMachineConfiguration(vm.HyperVGenerations.Contains("V2", StringComparer.OrdinalIgnoreCase), vm.EncryptionAtHostSupported ?? false));

                pool.TargetNodeCommunicationMode = Microsoft.Azure.Batch.Common.NodeCommunicationMode.Simplified;
                pool.NetworkConfiguration = new() { PublicIPAddressConfiguration = new(Microsoft.Azure.Batch.Common.IPAddressProvisioningType.BatchManaged), SubnetId = accountInfo.SubnetId };

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
                        await pool.RefreshAsync(detailLevel: new Microsoft.Azure.Batch.ODATADetailLevel(selectClause: "allocationState,id,resizeErrors"), cancellationToken: cancellationToken);
                    }
                    while (Microsoft.Azure.Batch.Common.AllocationState.Steady != pool.AllocationState);

                    if (pool.ResizeErrors?.Any() ?? false)
                    {
                        if (pool.ResizeErrors.Any(e =>
                            Microsoft.Azure.Batch.Common.PoolResizeErrorCodes.UnsupportedVMSize.Equals(e.Code, StringComparison.OrdinalIgnoreCase)
                            || (!vm.LowPriority && @"AccountVMSeriesCoreQuotaReached".Equals(e.Code, StringComparison.OrdinalIgnoreCase))))
                        {
                            return VerifyVMIResult.NextRegion;  // Dedicated vm family quota. Try another batch account.
                        }
                        else if (pool.ResizeErrors.Any(e =>
                            Microsoft.Azure.Batch.Common.PoolResizeErrorCodes.AccountCoreQuotaReached.Equals(e.Code, StringComparison.OrdinalIgnoreCase) ||
                            Microsoft.Azure.Batch.Common.PoolResizeErrorCodes.AccountLowPriorityCoreQuotaReached.Equals(e.Code, StringComparison.OrdinalIgnoreCase) ||
                            Microsoft.Azure.Batch.Common.PoolResizeErrorCodes.AccountSpotCoreQuotaReached.Equals(e.Code, StringComparison.OrdinalIgnoreCase)))
                        {
                            return VerifyVMIResult.Retry; // Either a timing issue or other concurrent use of the batch account. Try again.
                        }
                        else
                        {
                            var errorCodes = pool.ResizeErrors!.Select(e => e.Code).Distinct().ToList();
                            var isAllocationFailed = errorCodes.Count == 1 && errorCodes.Contains(Microsoft.Azure.Batch.Common.PoolResizeErrorCodes.AllocationFailed, StringComparer.OrdinalIgnoreCase);
                            var isAllocationTimedOut = errorCodes.Count == 1 && errorCodes.Contains(Microsoft.Azure.Batch.Common.PoolResizeErrorCodes.AllocationTimedOut, StringComparer.OrdinalIgnoreCase);
                            var isOverconstrainedAllocationRequest = errorCodes.Count == 1 && errorCodes.Contains(Microsoft.Azure.Batch.Common.PoolResizeErrorCodes.OverconstrainedAllocationRequestError, StringComparer.OrdinalIgnoreCase);
                            ProviderError? providerError = default;
                            //bool? isRetryCandidate = default;
                            string? additionalReport = default;
                            VerifyVMIResult? result = default;

                            if (isAllocationTimedOut)
                            {
                                return VerifyVMIResult.Retry;
                            }

                            if (isAllocationFailed)
                            {
                                var values = pool.ResizeErrors!
                                    .SelectMany(e => e.Values ?? new List<Microsoft.Azure.Batch.NameValuePair>())
                                    .ToDictionary(pair => pair.Name, pair => pair.Value, StringComparer.OrdinalIgnoreCase);

                                if (values.TryGetValue("Reason", out var reason))
                                {
                                    if ("The server encountered an internal error.".Equals(reason, StringComparison.OrdinalIgnoreCase))
                                    {
                                        // https://learn.microsoft.com/troubleshoot/azure/general/azure-batch-pool-resizing-failure#symptom-for-scenario-4
                                        return VerifyVMIResult.Retry;
                                    }
                                    else if ("Allocation failed as subnet has delegation to external resources.".Equals(reason, StringComparison.OrdinalIgnoreCase))
                                    {
                                        // https://learn.microsoft.com/troubleshoot/azure/general/azure-batch-pool-resizing-failure#symptom-for-scenario-1
                                        result = VerifyVMIResult.Skip;
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
                                            _ => (false, default, default),
                                        };

                                        //isRetryCandidate = (providerError?.Error.Code.Contains("Internal", StringComparison.OrdinalIgnoreCase) ?? false) || (providerError?.Error.Message.Contains(" retry later", StringComparison.OrdinalIgnoreCase) ?? false);

                                        if (!now)
                                        {
                                            return result.Value;
                                        }
                                    }
                                }

                                //if (isRetryCandidate == true)
                                //{
                                //    return VerifyVMIResult.Retry;
                                //}

                            }

                            result ??= VerifyVMIResult.Skip;

                            lock (consoleLock)
                            {
                                SetForegroundColor(ConsoleColor.Yellow);
                                ConsoleWriteLine($"{VerbFromResult(result)} {name} due to allocation failure(s): '{string.Join("', '", pool.ResizeErrors!.Select(e => e.Code))}'.");

                                if (!isOverconstrainedAllocationRequest)
                                {
                                    ConsoleWriteLine($"Additional information: Message: '{string.Join("', '", pool.ResizeErrors!.Select(e => e.Message))}'");
                                    pool.ResizeErrors!.SelectMany(e => e.Values ?? Enumerable.Empty<Microsoft.Azure.Batch.NameValuePair>())
                                        .ForEach(detail => ConsoleWriteLine($"'{detail.Name}': '{detail.Value}'."));
                                }

                                if (additionalReport is not null)
                                {
                                    ConsoleWriteLine(string.Join(Environment.NewLine, additionalReport.Split(Environment.NewLine)));
                                }

                                ResetColor();

                                static string VerbFromResult(VerifyVMIResult? value) => value switch
                                {
                                    VerifyVMIResult.Skip => "Skipping",
                                    VerifyVMIResult.NextRegion => "Deferring",
                                    VerifyVMIResult.Retry => "Retrying",
                                    _ => throw new System.Diagnostics.UnreachableException($"Invalid value of VerifyVMIResult: {value?.ToString() ?? "<null>"}."),
                                };
                            }

                            return result.Value;
                        }
                    }

                    List<Microsoft.Azure.Batch.ComputeNode> nodes;

                    do
                    {
                        await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                        nodes = await pool.ListComputeNodes(detailLevel: new Microsoft.Azure.Batch.ODATADetailLevel(selectClause: "id,state,errors")).ToAsyncEnumerable().ToListAsync(cancellationToken);

                        if (nodes.Any(n => n.Errors?.Any() ?? false))
                        {
                            lock (consoleLock)
                            {
                                SetForegroundColor(ConsoleColor.Yellow);
                                ConsoleWriteLine($"Skipping {name} due to node startup failure(s): '{string.Join("', '", nodes.SelectMany(n => n.Errors).Select(e => e.Code))}'.");
                                ConsoleWriteLine($"Additional information: Message: '{string.Join("', '", nodes.SelectMany(n => n.Errors).Select(e => e.Message))}'");
                                nodes.SelectMany(n => n.Errors).SelectMany(e => e.ErrorDetails ?? Enumerable.Empty<Microsoft.Azure.Batch.NameValuePair>())
                                    .ForEach(detail => ConsoleWriteLine($"'{detail.Name}': '{detail.Value}'."));
                                ResetColor();
                            }

                            return VerifyVMIResult.Skip;
                        }

                        if (nodes.Any(n => Microsoft.Azure.Batch.Common.ComputeNodeState.Unusable == n.State
                            || Microsoft.Azure.Batch.Common.ComputeNodeState.Unknown == n.State))
                        {
                            lock (consoleLock)
                            {
                                SetForegroundColor(ConsoleColor.Yellow);
                                ConsoleWriteLine($"Skipping {name} due to node startup-to-unusable transition without errors (likely batch node agent startup failure).");
                                ResetColor();
                            }

                            return VerifyVMIResult.Skip;
                        }
                    }
                    while (nodes.Any(n => Microsoft.Azure.Batch.Common.ComputeNodeState.Idle != n.State
                        && Microsoft.Azure.Batch.Common.ComputeNodeState.Preempted != n.State
                        && Microsoft.Azure.Batch.Common.ComputeNodeState.LeavingPool != n.State));
                }
                catch (Microsoft.Azure.Batch.Common.BatchException exception)
                {
                    if (exception.StackTrace?.Contains(" at Microsoft.Azure.Batch.CloudPool.CommitAsync(IEnumerable`1 additionalBehaviors, CancellationToken cancellationToken)") ?? false)
                    {
                        if (System.Net.HttpStatusCode.InternalServerError == exception.RequestInformation.HttpStatusCode)
                        {
                            var error = exception.RequestInformation.BatchError;
                            var isTimedOut = error is null || error.Code == Microsoft.Azure.Batch.Common.BatchErrorCodeStrings.OperationTimedOut;

                            if (!isTimedOut)
                            {
                                pool = default;
                            }

                            lock (consoleLock)
                            {
                                SetForegroundColor(ConsoleColor.Yellow);

                                if (isTimedOut)
                                {
                                    ConsoleWriteLine($"Retrying {name} due to pool creation failure(s): '{error?.Code ?? "[unknown]"}'.");
                                }
                                else
                                {
                                    ConsoleWriteLine($"Deferring {name} due to pool creation failure(s): '{error?.Code ?? "[unknown]"}'.");
                                }

                                if (error is not null)
                                {
                                    ConsoleWriteLine($"'{error.Code}'({error.Message.Language}): '{error.Message.Value}'");
                                    (error.Values ?? Enumerable.Empty<Microsoft.Azure.Batch.BatchErrorDetail>())
                                        .ForEach(value => ConsoleWriteLine($"'{value.Key}': '{value.Value}'"));
                                }

                                ResetColor();
                            }

                            return isTimedOut ? VerifyVMIResult.Retry : VerifyVMIResult.NextRegion;
                        }
                        else if (exception.RequestInformation.HttpStatusCode is null)
                        {
                            return VerifyVMIResult.Retry;
                        }
                        else if (exception.RequestInformation.HttpStatusCode == System.Net.HttpStatusCode.Conflict)
                        {
                            var error = exception.RequestInformation.BatchError;
                            var isQuota = Microsoft.Azure.Batch.Common.BatchErrorCodeStrings.PoolQuotaReached.Equals(error?.Code, StringComparison.OrdinalIgnoreCase);
                            var isPoolBeingDeleted = Microsoft.Azure.Batch.Common.BatchErrorCodeStrings.PoolBeingDeleted.Equals(error?.Code, StringComparison.OrdinalIgnoreCase);
                            pool = default;

                            if (isPoolBeingDeleted || isQuota)
                            {
                                return VerifyVMIResult.Retry;
                            }

                            lock (consoleLock)
                            {
                                SetForegroundColor(ConsoleColor.Yellow);
                                ConsoleWriteLine($"Deferring {name} due to due to pool creation failure(s):'{error?.Code ?? "[unknown]"}'.");

                                if (error is not null)
                                {
                                    ConsoleWriteLine($"'{error.Code}'({error.Message.Language}): '{error.Message.Value}'");
                                    (error.Values ?? Enumerable.Empty<Microsoft.Azure.Batch.BatchErrorDetail>())
                                        .ForEach(value => ConsoleWriteLine($"'{value.Key}': '{value.Value}'"));
                                }

                                ResetColor();
                            }

                            return VerifyVMIResult.NextRegion;
                        }
                        else
                        {
                            lock (consoleLock)
                            {
                                SetForegroundColor(ConsoleColor.Yellow);
                                ConsoleWriteLine($"Deferring {name} due to pool creation '{exception.RequestInformation.HttpStatusCode}' failure ('{exception.RequestInformation?.BatchError?.Code}' error).");
                                ConsoleWriteLine("Please check and delete pool manually.");
                                ResetColor();
                            }

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
                        catch (Microsoft.Azure.Batch.Common.BatchException exception)
                        {
                            firstStatusCode ??= exception.RequestInformation.HttpStatusCode;

                            switch (exception.RequestInformation.HttpStatusCode)
                            {
                                case System.Net.HttpStatusCode.Conflict:
                                    break;

                                default:
                                    lock (consoleLock)
                                    {
                                        SetForegroundColor(ConsoleColor.Red);
                                        ConsoleWriteLine($"Failed to delete pool '{name}' on '{accountInfo.Name}' ('{exception.RequestInformation.HttpStatusCode?.ToString("G") ?? "[no response]"}', initially '{firstStatusCode}') {exception.GetType().FullName}: {exception.Message}");
                                        ConsoleWriteLine("Please check and delete pool manually.");
                                        ConsoleWriteLine();
                                        ResetColor();
                                    }
                                    break;
                            }
                        }
                        catch (InvalidOperationException exception)
                        {
                            if ("This operation is forbidden on unbound objects.".Equals(exception.Message, StringComparison.OrdinalIgnoreCase))
                            {
                                try
                                {
                                    pool = await accountInfo.Client.PoolOperations.GetPoolAsync(name, new Microsoft.Azure.Batch.ODATADetailLevel(selectClause: "id"), cancellationToken: CancellationToken.None);
                                }
                                catch (Exception ex)
                                {
                                    lock (consoleLock)
                                    {
                                        SetForegroundColor(ConsoleColor.Red);
                                        ConsoleWriteLine($"Failed to delete pool '{name}' on '{accountInfo.Name}' due to '{firstStatusCode}'. Attempting to locate the pool resulted in {ex.GetType().FullName}: {ex.Message}");
                                        ConsoleWriteLine("Please check and delete pool manually.");
                                        ConsoleWriteLine();
                                        ResetColor();
                                    }
                                }
                            }
                            else
                            {
                                lock (consoleLock)
                                {
                                    SetForegroundColor(ConsoleColor.Red);
                                    ConsoleWriteLine($"Failed to delete pool '{name}' on '{accountInfo.Name}' due to '{firstStatusCode}' {exception.GetType().FullName}: {exception.Message}");
                                    ConsoleWriteLine("Please check and delete pool manually.");
                                    ConsoleWriteLine();
                                    ResetColor();
                                }
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
                    .ListPools(detailLevel: new Microsoft.Azure.Batch.ODATADetailLevel(selectClause: "id,vmSize")).ToAsyncEnumerable()
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
                    var coresPerNode = info.VCpusAvailable ?? 200;

                    lowPriorityCoresInUse += ((lowPriority?.Total ?? 0) - (lowPriority?.Preempted ?? 0)) * coresPerNode;
                    var dedicatedCores = (dedicated?.Total ?? 0) * coresPerNode;
                    dedicatedCoresInUse += dedicatedCores;
                    if (!dedicatedCoresInUseByVmFamily.TryAdd(vmFamily, dedicatedCores))
                    {
                        dedicatedCoresInUseByVmFamily[vmFamily] += dedicatedCores;
                    }
                }

                return new(
                    batchAccount.PoolQuota ?? int.MaxValue,
                    batchAccount.LowPriorityCoreQuota ?? int.MaxValue,
                    batchAccount.DedicatedCoreQuota ?? int.MaxValue,
                    batchAccount.IsDedicatedCoreQuotaPerVmFamilyEnforced ?? false,
                    batchAccount.DedicatedCoreQuotaPerVmFamily.ToDictionary(quota => quota.Name, quota => quota.CoreQuota ?? int.MaxValue, StringComparer.OrdinalIgnoreCase),
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
                var coresPerNode = info.VCpusAvailable ?? 200;
                var isLowPriority = info.LowPriority;

                if (coresPerNode == 0) { coresPerNode = 200; }

                return !(0 > context.PoolQuota
                    || (isLowPriority && coresPerNode > context.LowPriorityCoreQuota)
                    || (!isLowPriority && coresPerNode > context.DedicatedCoreQuota)
                    || (!isLowPriority && context.IsDedicatedCoreQuotaPerVmFamilyEnforced && coresPerNode > (context.DedicatedCoreQuotaPerVmFamily.TryGetValue(family, out var quota) ? quota : 0)));
            }

            private static bool AddSkuMetadataToQuotaIfQuotaPermits(WrappedVmSku vmSize, TestContext context)
            {
                var info = vmSize.VmSku.Sku;
                var family = info.VmFamily;
                var coresPerNode = info.VCpusAvailable ?? 200;
                var isLowPriority = info.LowPriority;

                if (context.PoolCount + 1 > context.PoolQuota ||
                    (isLowPriority && context.LowPriorityCoresInUse + coresPerNode > context.LowPriorityCoreQuota) ||
                    (!isLowPriority && context.DedicatedCoresInUse + coresPerNode > context.DedicatedCoreQuota) ||
                    (!isLowPriority && context.IsDedicatedCoreQuotaPerVmFamilyEnforced && (context.DedicatedCoresInUseByVmFamily.TryGetValue(family, out var inUse) ? inUse : 0) > (context.DedicatedCoreQuotaPerVmFamily.TryGetValue(family, out var quota) ? quota : 0)))
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
                var coresPerNode = info.VCpusAvailable ?? 200;
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
