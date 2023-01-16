// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using LazyCache;
using LazyCache.Providers;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Swashbuckle.AspNetCore.SwaggerGen;
using Tes.Models;
using Tes.Repository;
using TesApi.Web;
using TesApi.Web.Management;
using TesApi.Web.Management.Clients;

namespace TesApi.Tests.TestServices
{
    internal sealed class TestServiceProvider<T> : IDisposable, IAsyncDisposable
    {
        private readonly IServiceProvider provider;

        internal TestServiceProvider(
            bool mockStorageAccessProvider = false,
            bool wrapAzureProxy = false,
            //bool wrapBatchSkuInformationProvider = true,
            IEnumerable<(string Key, string Value)> configuration = default,
            BatchAccountResourceInformation accountResourceInformation = default,
            Action<Mock<IAzureProxy>> azureProxy = default,
            Action<Mock<IRepository<TesTask>>> tesTaskRepository = default,
            (string endpoint, string key, string databaseId, string containerId, string partitionKeyValue) batchPoolRepositoryArgs = default,
            Action<Mock<IStorageAccessProvider>> storageAccessProvider = default,
            Action<Mock<IBatchSkuInformationProvider>> batchSkuInformationProvider = default,
            Action<Mock<IBatchQuotaProvider>> batchQuotaProvider = default,
            (Func<IServiceProvider, System.Linq.Expressions.Expression<Func<ArmBatchQuotaProvider>>> expression, Action<Mock<ArmBatchQuotaProvider>> action) armBatchQuotaProvider = default, //added so config utils gets the arm implementation, to be removed once config utils is refactored.
            Action<IServiceCollection> additionalActions = default)
            => provider = new ServiceCollection()
                .AddSingleton(_ => GetConfiguration(configuration))
                .AddSingleton(s => wrapAzureProxy ? ActivatorUtilities.CreateInstance<CachingWithRetriesAzureProxy>(s, GetAzureProxy(azureProxy).Object) : GetAzureProxy(azureProxy).Object)
                .AddSingleton(_ => GetTesTaskRepository(tesTaskRepository).Object)
                //.AddSingleton(_ => new CosmosCredentials(batchPoolRepositoryArgs.endpoint ?? "endpoint", batchPoolRepositoryArgs.key ?? "key"))
                .AddSingleton(s => mockStorageAccessProvider ? GetStorageAccessProvider(storageAccessProvider).Object : ActivatorUtilities.CreateInstance<StorageAccessProvider>(s))
                .AddSingleton<IAppCache>(_ => new CachingService(new MemoryCacheProvider(new MemoryCache(new MemoryCacheOptions()))))
                .IfThenElse(accountResourceInformation is null, s => s, s => s.AddSingleton(accountResourceInformation))
                .AddTransient<ILogger<T>>(_ => NullLogger<T>.Instance)
                .IfThenElse(mockStorageAccessProvider, s => s, s => s.AddTransient<ILogger<StorageAccessProvider>>(_ => NullLogger<StorageAccessProvider>.Instance))
                .IfThenElse(batchSkuInformationProvider is null,
                    s => s.AddSingleton<IBatchSkuInformationProvider>(sp => ActivatorUtilities.CreateInstance<PriceApiBatchSkuInformationProvider>(sp))
                        .AddSingleton(sp => new PriceApiBatchSkuInformationProvider(sp.GetRequiredService<PriceApiClient>(), sp.GetRequiredService<ILogger<PriceApiBatchSkuInformationProvider>>())),
                    s => s.AddSingleton(_ => GetBatchSkuInformationProvider(batchSkuInformationProvider).Object))
                .AddSingleton(_ => GetBatchQuotaProvider(batchQuotaProvider).Object)
                .AddTransient<ILogger<BatchScheduler>>(_ => NullLogger<BatchScheduler>.Instance)
                .AddTransient<ILogger<ArmBatchQuotaProvider>>(_ => NullLogger<ArmBatchQuotaProvider>.Instance)
                .AddTransient<ILogger<BatchQuotaVerifier>>(_ => NullLogger<BatchQuotaVerifier>.Instance)
                .AddTransient<ILogger<ConfigurationUtils>>(_ => NullLogger<ConfigurationUtils>.Instance)
                .AddTransient<ILogger<PriceApiBatchSkuInformationProvider>>(_ => NullLogger<PriceApiBatchSkuInformationProvider>.Instance)
                .AddSingleton<TestRepositoryStorage>()
                .AddSingleton<PriceApiClient>()
                .AddSingleton<IBatchScheduler, BatchScheduler>()
                .AddSingleton(s => GetArmBatchQuotaProvider(s, armBatchQuotaProvider)) //added so config utils gets the arm implementation, to be removed once config utils is refactored.
                                                                                       //.AddSingleton<ArmBatchQuotaProvider, ArmBatchQuotaProvider>() //added so config utils gets the arm implementation, to be removed once config utils is refactored.
                .AddSingleton<IBatchQuotaVerifier, BatchQuotaVerifier>()
                .IfThenElse(additionalActions is null, s => s, s => { additionalActions(s); return s; })
            .BuildServiceProvider();

        internal IConfiguration Configuration { get; private set; }
        internal Mock<IAzureProxy> AzureProxy { get; private set; }
        internal Mock<IBatchSkuInformationProvider> BatchSkuInformationProvider { get; private set; }
        internal Mock<IBatchQuotaProvider> BatchQuotaProvider { get; private set; }
        internal Mock<ArmBatchQuotaProvider> ArmBatchQuotaProvider { get; private set; } //added so config utils gets the arm implementation, to be removed once config utils is refactored.
        internal Mock<IRepository<TesTask>> TesTaskRepository { get; private set; }
        internal Mock<IStorageAccessProvider> StorageAccessProvider { get; private set; }

        internal T GetT()
            => GetT(Array.Empty<Type>(), Array.Empty<object>());

        internal T GetT<T1>(T1 t1)
            => GetT(new Type[] { typeof(T1) }, new object[] { t1 });

        internal T GetT<T1, T2>(T1 t1, T2 t2)
            => GetT(new Type[] { typeof(T1), typeof(T2) }, new object[] { t1, t2 });

        internal T GetT<T1, T2, T3>(T1 t1, T2 t2, T3 t3)
            => GetT(new Type[] { typeof(T1), typeof(T2), typeof(T3) }, new object[] { t1, t2, t3 });

        internal T GetT<T1, T2, T3, T4>(T1 t1, T2 t2, T3 t3, T4 t4)
            => GetT(new Type[] { typeof(T1), typeof(T2), typeof(T3), typeof(T4) }, new object[] { t1, t2, t3, t4 });

        internal T GetT<T1, T2, T3, T4, T5>(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5)
            => GetT(new Type[] { typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5) }, new object[] { t1, t2, t3, t4, t5 });

        internal T GetT<T1, T2, T3, T4, T5, T6>(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6)
            => GetT(new Type[] { typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(T6) }, new object[] { t1, t2, t3, t4, t5, t6 });

        internal T GetT(Type[] types, object[] args)
        {
            types ??= Array.Empty<Type>();
            args ??= Array.Empty<object>();
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

        private IConfiguration GetConfiguration(IEnumerable<(string Key, string Value)> configuration)
            => Configuration = new ConfigurationBuilder().AddInMemoryCollection(configuration?.Select(t => new KeyValuePair<string, string>(t.Key, t.Value)) ?? Enumerable.Empty<KeyValuePair<string, string>>()).Build();

        private Mock<IAzureProxy> GetAzureProxy(Action<Mock<IAzureProxy>> action)
        {
            var proxy = new Mock<IAzureProxy>();
            action?.Invoke(proxy);
            return AzureProxy = proxy;
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

    internal sealed class TestRepository<T> : BaseRepository<T> where T : RepositoryItem<T>
    {
        public TestRepository(string endpoint, string key, string databaseId, string containerId, string partitionKeyValue, TestRepositoryStorage storage)
            : base(endpoint, key, databaseId, containerId, partitionKeyValue, storage) { }
    }
}
