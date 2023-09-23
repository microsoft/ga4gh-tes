using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.Models;
using Tes.Repository;
using TesApi.Web;
using TesApi.Web.Storage;

namespace TesApi.Tests
{
    [TestClass]
    public class SchedulerTests
    {
        private Mock<IRepository<TesTask>> mockRepository;
        private Mock<IBatchScheduler> mockBatchScheduler;
        private Mock<ILogger<Scheduler>> mockLogger;
        private IRetryPolicyProvider retryPolicyProvider;
        private Mock<IHostApplicationLifetime> mockApplicationLifetime;
        private Mock<IStorageAccessProvider> mockStorageAccessProvider;
        private CancellationTokenSource cts;
        private Scheduler scheduler;

        [TestInitialize]
        public void SetUp()
        {
            mockRepository = new Mock<IRepository<TesTask>>();
            mockBatchScheduler = new Mock<IBatchScheduler>();
            mockLogger = new Mock<ILogger<Scheduler>>();
            retryPolicyProvider = new RetryPolicyProvider();
            mockApplicationLifetime = new Mock<IHostApplicationLifetime>();
            mockStorageAccessProvider = new Mock<IStorageAccessProvider>();
            
            cts = new CancellationTokenSource();
            scheduler = new Scheduler(mockRepository.Object, mockBatchScheduler.Object, mockLogger.Object, mockStorageAccessProvider.Object, retryPolicyProvider, mockApplicationLifetime.Object);
        }

        [TestMethod]
        public async Task TestProcessTesTasksCalled()
        {
            var tesTaskList = new List<TesTask>
            {
                new TesTask { State = TesState.QUEUEDEnum, CreationTime = DateTime.UtcNow }
            };
            mockRepository.Setup(r => r.GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>(), It.IsAny<CancellationToken>()))
                          .Returns(Task.FromResult(tesTaskList as IEnumerable<TesTask>));
            var manualResetEvent = new ManualResetEvent(false);
            mockBatchScheduler.Setup(bs => bs.ProcessTesTaskAsync(It.IsAny<TesTask>(), It.IsAny<CancellationToken>()))
                              .Callback(() => manualResetEvent.WaitOne())
                              .ReturnsAsync(true);
#pragma warning disable CS0618 // Type or member is obsolete
            var task = scheduler.ExecuteForTestAsync(cts.Token);
#pragma warning restore CS0618 // Type or member is obsolete
            await Task.Delay(500);
            manualResetEvent.Set();
            await Task.Delay(500);
            cts.Cancel();
            try { await task; }
            catch (OperationCanceledException) { }
            mockBatchScheduler.Verify(bs => bs.ProcessTesTaskAsync(It.IsAny<TesTask>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        }

        [TestMethod]
        public async Task TestProcessTesTasksCalledWithAreExistingTesTasksFalse()
        {
            var tesTaskList = new List<TesTask>
            {
                new TesTask { State = TesState.QUEUEDEnum, CreationTime = DateTime.UtcNow }
            };
            mockRepository.Setup(r => r.GetItemsAsync(It.IsAny<Expression<Func<TesTask, bool>>>(), It.IsAny<CancellationToken>()))
                          .Returns(Task.FromResult(tesTaskList as IEnumerable<TesTask>));
            mockBatchScheduler.SetupGet(bs => bs.NeedPoolFlush).Returns(true);
            mockBatchScheduler.Setup(bs => bs.FlushPoolsAsync(It.IsAny<HashSet<string>>(), It.IsAny<CancellationToken>()))
                              .Throws(new InvalidOperationException("FlushPoolsAsync should not be called with areExistingTesTasks=false"));
#pragma warning disable CS0618 // Type or member is obsolete
            var task = scheduler.ExecuteForTestAsync(cts.Token);
#pragma warning restore CS0618 // Type or member is obsolete
            await Task.Delay(500);
            cts.Cancel();
            try { await task; }
            catch (OperationCanceledException) { }
        }
    }
}
