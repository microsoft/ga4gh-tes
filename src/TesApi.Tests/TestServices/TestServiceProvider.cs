﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CommonUtilities;
using CommonUtilities.Options;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Swashbuckle.AspNetCore.SwaggerGen;
using Tes.ApiClients;
using Tes.Models;
using Tes.Repository;
using TesApi.Web;
using TesApi.Web.Management;
using TesApi.Web.Management.Batch;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Options;
using TesApi.Web.Runner;
using TesApi.Web.Storage;

namespace TesApi.Tests.TestServices
{
    internal sealed class TestServiceProvider<T> : IDisposable, IAsyncDisposable
    {
        private readonly IServiceProvider provider;

        internal TestServiceProvider(
            bool mockStorageAccessProvider = false,
            bool wrapAzureProxy = false,
            IEnumerable<(string Key, string Value)> configuration = default,
            BatchAccountResourceInformation accountResourceInformation = default,
            Action<Mock<IAzureProxy>> azureProxy = default,
            Action<Mock<IBatchPoolManager>> batchPoolManager = default,
            Action<Mock<IRepository<TesTask>>> tesTaskRepository = default,
            Action<Mock<IStorageAccessProvider>> storageAccessProvider = default,
            Action<Mock<IBatchSkuInformationProvider>> batchSkuInformationProvider = default,
            Action<Mock<IBatchQuotaProvider>> batchQuotaProvider = default,
            (Func<IServiceProvider, System.Linq.Expressions.Expression<Func<ArmBatchQuotaProvider>>> expression, Action<Mock<ArmBatchQuotaProvider>> action) armBatchQuotaProvider = default, //added so config utils gets the arm implementation, to be removed once config utils is refactored.
            Action<Mock<IAllowedVmSizesService>> allowedVmSizesServiceSetup = default,
            Action<IServiceCollection> additionalActions = default)
        {
            Configuration = GetConfiguration(configuration);
            var azureCloudConfig = ExpensiveObjectTestUtility.AzureCloudConfig;
            provider = new ServiceCollection()
                .AddSingleton(_ => new TesServiceInfo { CreatedAt = DateTimeOffset.UtcNow, Environment = "unittest", Id = "unit-test-id", Organization = new() { Name = "unit-test-org", Url = "http://localhost/" }, Storage = [], UpdatedAt = DateTimeOffset.UtcNow })
                .AddSingleton(azureCloudConfig)
                .AddSingleton(azureCloudConfig.AzureEnvironmentConfig)
                .AddSingleton<ConfigurationUtils>()
                .AddSingleton(_ => GetAllowedVmSizesServiceProviderProvider(allowedVmSizesServiceSetup).Object)
                .AddSingleton(Configuration)
                .AddSingleton(BindHelper<BatchAccountOptions>(BatchAccountOptions.SectionName))
                .AddSingleton(BindHelper<RetryPolicyOptions>(RetryPolicyOptions.SectionName))
                .AddSingleton(BindHelper<TerraOptions>(TerraOptions.SectionName))
                .AddSingleton(BindHelper<BatchImageGeneration1Options>(BatchImageGeneration1Options.SectionName))
                .AddSingleton(BindHelper<BatchImageGeneration2Options>(BatchImageGeneration2Options.SectionName))
                .AddSingleton(BindHelper<BatchNodesOptions>(BatchNodesOptions.SectionName))
                .AddSingleton(BindHelper<BatchSchedulingOptions>(BatchSchedulingOptions.SectionName))
                .AddSingleton(BindHelper<StorageOptions>(StorageOptions.SectionName))
                .AddSingleton(BindHelper<DrsHubOptions>(DrsHubOptions.SectionName))
                .AddSingleton(s => wrapAzureProxy ? ActivatorUtilities.CreateInstance<CachingWithRetriesAzureProxy>(s, GetAzureProxy(azureProxy).Object) : GetAzureProxy(azureProxy).Object)
                .AddSingleton(_ => GetTesTaskRepository(tesTaskRepository).Object)
                .AddSingleton(s => mockStorageAccessProvider ? GetStorageAccessProvider(storageAccessProvider).Object : ActivatorUtilities.CreateInstance<DefaultStorageAccessProvider>(s))
                .AddMemoryCache()
                .IfThenElse(accountResourceInformation is null, s => s, s => s.AddSingleton(accountResourceInformation))
                .AddTransient<ILogger<T>>(_ => NullLogger<T>.Instance)
                .IfThenElse(mockStorageAccessProvider, s => s, s => s.AddTransient<ILogger<DefaultStorageAccessProvider>>(_ => NullLogger<DefaultStorageAccessProvider>.Instance))
                .IfThenElse(batchSkuInformationProvider is null,
                    s => s.AddSingleton<IBatchSkuInformationProvider>(sp => ActivatorUtilities.CreateInstance<PriceApiBatchSkuInformationProvider>(sp))
                        .AddSingleton(sp => new PriceApiBatchSkuInformationProvider(sp.GetRequiredService<PriceApiClient>(), azureCloudConfig, sp.GetRequiredService<ILogger<PriceApiBatchSkuInformationProvider>>())),
                    s => s.AddSingleton(_ => GetBatchSkuInformationProvider(batchSkuInformationProvider).Object))
                .AddSingleton(_ => GetBatchQuotaProvider(batchQuotaProvider).Object)
                .AddTransient<ILogger<BatchScheduler>>(_ => NullLogger<BatchScheduler>.Instance)
                .AddTransient<ILogger<BatchPool>>(_ => NullLogger<BatchPool>.Instance)
                .AddTransient<ILogger<ArmBatchQuotaProvider>>(_ => NullLogger<ArmBatchQuotaProvider>.Instance)
                .AddTransient<ILogger<BatchQuotaVerifier>>(_ => NullLogger<BatchQuotaVerifier>.Instance)
                .AddTransient<ILogger<ConfigurationUtils>>(_ => NullLogger<ConfigurationUtils>.Instance)
                .AddTransient<ILogger<PriceApiBatchSkuInformationProvider>>(_ => NullLogger<PriceApiBatchSkuInformationProvider>.Instance)
                .AddTransient<ILogger<TaskToNodeTaskConverter>>(_ => NullLogger<TaskToNodeTaskConverter>.Instance)
                .AddTransient<ILogger<TaskExecutionScriptingManager>>(_ => NullLogger<TaskExecutionScriptingManager>.Instance)
                .AddTransient<ILogger<CachingWithRetriesAzureProxy>>(_ => NullLogger<CachingWithRetriesAzureProxy>.Instance)
                .AddSingleton<CachingRetryPolicyBuilder>()
                .AddTransient<IActionIdentityProvider, DefaultActionIdentityProvider>()
                .AddSingleton<PriceApiClient>()
                .AddSingleton<IBatchPoolFactory, BatchPoolFactory>()
                .AddTransient<BatchPool>()
                .AddSingleton<IBatchScheduler, BatchScheduler>()
                .AddSingleton(s => GetArmBatchQuotaProvider(s, armBatchQuotaProvider)) //added so config utils gets the arm implementation, to be removed once config utils is refactored.
                .AddSingleton<IBatchQuotaVerifier, BatchQuotaVerifier>()
                .AddSingleton<TaskToNodeTaskConverter>()
                .AddSingleton<TaskExecutionScriptingManager>()
                .AddSingleton(GetBatchPoolManager(batchPoolManager).Object)
                .IfThenElse(additionalActions is null, s => { }, s => additionalActions(s))
            .BuildServiceProvider();

            IOptions<TOption> BindHelper<TOption>(string key) where TOption : class, new()
                => Options.Create<TOption>(Configuration.GetSection(key).Get<TOption>() ?? new TOption());
        }

        internal IConfiguration Configuration { get; private set; }
        internal Mock<IAzureProxy> AzureProxy { get; private set; }
        internal Mock<IBatchSkuInformationProvider> BatchSkuInformationProvider { get; private set; }
        internal Mock<IBatchQuotaProvider> BatchQuotaProvider { get; private set; }
        internal Mock<ArmBatchQuotaProvider> ArmBatchQuotaProvider { get; private set; } //added so config utils gets the arm implementation, to be removed once config utils is refactored.
        internal Mock<IRepository<TesTask>> TesTaskRepository { get; private set; }
        internal Mock<IStorageAccessProvider> StorageAccessProvider { get; private set; }
        internal Mock<IAllowedVmSizesService> AllowedVmSizesServiceProvider { get; private set; }
        internal Mock<IBatchPoolManager> BatchPoolManager { get; private set; }

        internal T GetT()
            => GetT([], []);

        internal T GetT<T1>(T1 t1)
            => GetT([typeof(T1)], [t1]);

        internal T GetT<T1, T2>(T1 t1, T2 t2)
            => GetT([typeof(T1), typeof(T2)], [t1, t2]);

        internal T GetT<T1, T2, T3>(T1 t1, T2 t2, T3 t3)
            => GetT([typeof(T1), typeof(T2), typeof(T3)], [t1, t2, t3]);

        internal T GetT<T1, T2, T3, T4>(T1 t1, T2 t2, T3 t3, T4 t4)
            => GetT([typeof(T1), typeof(T2), typeof(T3), typeof(T4)], [t1, t2, t3, t4]);

        internal T GetT<T1, T2, T3, T4, T5>(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5)
            => GetT([typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5)], [t1, t2, t3, t4, t5]);

        internal T GetT<T1, T2, T3, T4, T5, T6>(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6)
            => GetT([typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6)], [t1, t2, t3, t4, t5, t6]);

        internal T GetT(Type[] types, object[] args)
        {
            types ??= [];
            args ??= [];
            if (types.Length != args.Length) throw new ArgumentException("The quantity of argument types and arguments does not match.", nameof(types));
            foreach (var (type, arg) in types.Zip(args))
            {
                if (type is null) throw new ArgumentException("One or more of the types is null.", nameof(types));
                if (!type.IsInstanceOfType(arg) && type.GetDefaultValue() != arg)
                {
                    throw new ArgumentException("One or more arguments are not of the corresponding type.", nameof(args));
                }
            }

            return args.Length == 0 ? ActivatorUtilities.GetServiceOrCreateInstance<T>(provider) : (T)ActivatorUtilities.CreateFactory(typeof(T), types)(provider, args);
        }

        internal TService GetService<TService>()
            => provider.GetService<TService>();

        internal TInstance GetServiceOrCreateInstance<TInstance>()
            => ActivatorUtilities.GetServiceOrCreateInstance<TInstance>(provider);

        private static IConfiguration GetConfiguration(IEnumerable<(string Key, string Value)> configuration)
            => new ConfigurationBuilder()
                .AddInMemoryCollection(
                [ // defaults
                    new($"{RetryPolicyOptions.SectionName}:{nameof(RetryPolicyOptions.MaxRetryCount)}", "3"),
                    new($"{RetryPolicyOptions.SectionName}:{nameof(RetryPolicyOptions.ExponentialBackOffExponent)}", "2")
                ])
                .AddInMemoryCollection(configuration?.Select(t => new KeyValuePair<string, string>(t.Key, t.Value)) ?? [])
                .Build();

        private Mock<IAzureProxy> GetAzureProxy(Action<Mock<IAzureProxy>> action)
        {
            var proxy = new Mock<IAzureProxy>();
            action?.Invoke(proxy);
            return AzureProxy = proxy;
        }

        private Mock<IBatchPoolManager> GetBatchPoolManager(Action<Mock<IBatchPoolManager>> action)
        {
            var proxy = new Mock<IBatchPoolManager>();
            action?.Invoke(proxy);
            return BatchPoolManager = proxy;
        }

        private Mock<IAllowedVmSizesService> GetAllowedVmSizesServiceProviderProvider(Action<Mock<IAllowedVmSizesService>> action)
        {
            var proxy = new Mock<IAllowedVmSizesService>();
            action?.Invoke(proxy);
            return AllowedVmSizesServiceProvider = proxy;
        }

        private Mock<IBatchSkuInformationProvider> GetBatchSkuInformationProvider(Action<Mock<IBatchSkuInformationProvider>> action)
        {
            var proxy = new Mock<IBatchSkuInformationProvider>();
            action?.Invoke(proxy);
            return BatchSkuInformationProvider = proxy;
        }

        private Mock<IBatchQuotaProvider> GetBatchQuotaProvider(Action<Mock<IBatchQuotaProvider>> action)
        {
            var proxy = new Mock<IBatchQuotaProvider>();
            action?.Invoke(proxy);
            return BatchQuotaProvider = proxy;
        }

        private ArmBatchQuotaProvider GetArmBatchQuotaProvider(IServiceProvider provider, (Func<IServiceProvider, System.Linq.Expressions.Expression<Func<ArmBatchQuotaProvider>>> expression, Action<Mock<ArmBatchQuotaProvider>> action) configure) //added so config utils gets the arm implementation, to be removed once config utils is refactored.
        {
            var constructor = configure.expression is null ? null : configure.expression(provider);
            var proxy = constructor is null ? new Mock<ArmBatchQuotaProvider>() : new Mock<ArmBatchQuotaProvider>(constructor);
            configure.action?.Invoke(proxy);
            ArmBatchQuotaProvider = proxy;
            return proxy.Object;
        }

        private Mock<IRepository<TesTask>> GetTesTaskRepository(Action<Mock<IRepository<TesTask>>> action)
        {
            var proxy = new Mock<IRepository<TesTask>>();
            action?.Invoke(proxy);
            return TesTaskRepository = proxy;
        }

        private Mock<IStorageAccessProvider> GetStorageAccessProvider(Action<Mock<IStorageAccessProvider>> action)
        {
            var proxy = new Mock<IStorageAccessProvider>();
            action?.Invoke(proxy);
            return StorageAccessProvider = proxy;
        }

        public void Dispose()
            => (provider as IDisposable)?.Dispose();

        public ValueTask DisposeAsync()
        {
            if (provider is IAsyncDisposable asyncDisposable)
                return asyncDisposable.DisposeAsync();
            else
                Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
