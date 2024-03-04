// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Transfer;

namespace Tes.Runner.Test.Transfer
{
    [TestClass]
    [TestCategory("Unit")]
    public class MaxProcessingTimeScalingStrategyTests
    {
        private MaxProcessingTimeScalingStrategy strategy = null!;

        [TestInitialize]
        public void SetUp()
        {
            strategy = new MaxProcessingTimeScalingStrategy();
        }


        [DataTestMethod]
        [DataRow(1, 2)] // 2 ^ 1
        [DataRow(2, 4)] // 2 ^ 2
        [DataRow(3, 8)] // 2 ^ 3
        [DataRow(8, MaxProcessingTimeScalingStrategy.DefaultMaxScalingDelayInSec)] //max of 1 minute (default) should be reached
        [DataRow(9, MaxProcessingTimeScalingStrategy.DefaultMaxScalingDelayInSec)] //max of 1 minute (default) should be reached
        [DataRow(10, MaxProcessingTimeScalingStrategy.DefaultMaxScalingDelayInSec)] //max of 1 minute (default) should be reached
        public void GetScalingDelay_NumberOfProcessorsIsProvided_ExponentialScalingDelayWithCap(int processors, int expectedDelayInSec)
        {
            Assert.AreEqual(TimeSpan.FromSeconds(expectedDelayInSec), strategy.GetScalingDelay(processors));
        }

        [DataTestMethod]
        [DataRow(1000 * 9, true)] // 10s, the default max processing time is 10 seconds
        [DataRow(1000 * 10, false)] // 10s, the default max processing time is 10 seconds
        [DataRow(1000 * 11, false)] // 11s, the default max processing time is 10 seconds
        public void IsScalingRequired_PartProcessingTimeIsProvided_ExpectedResult(int currentMaxPartProcessingTimeInMs, bool expectedResults)
        {
            Assert.AreEqual(expectedResults, strategy.IsScalingAllowed(10, TimeSpan.FromMilliseconds(currentMaxPartProcessingTimeInMs)));
        }
    }
}
