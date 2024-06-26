// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer
{
    /// <summary>
    /// Enables scaling considering the maximum processing time of a part.
    /// The scaling delay is exponential and is calculated based on the current number of processors.
    /// </summary>
    public class MaxProcessingTimeScalingStrategy : IScalingStrategy
    {
        private readonly TimeSpan maxPartProcessingTime;
        private readonly TimeSpan maxScalingDelay;
        public const int DefaultMaxPartProcessingInSec = 10;
        public const int DefaultMaxScalingDelayInSec = 60;
        public const int ScaleBase = 2;

        public MaxProcessingTimeScalingStrategy(int maxPartProcessingInSec = DefaultMaxPartProcessingInSec, int maxScalingDelayInSec = DefaultMaxScalingDelayInSec)
        {
            if (maxPartProcessingInSec <= 0)
            {
                throw new ArgumentException("The maximum part processing time must be greater than 0.");
            }

            if (maxScalingDelayInSec <= 0)
            {
                throw new ArgumentException("The maximum scaling delay must be greater than 0.");
            }

            maxPartProcessingTime = TimeSpan.FromSeconds(maxPartProcessingInSec);
            maxScalingDelay = TimeSpan.FromSeconds(maxScalingDelayInSec);
        }

        public TimeSpan GetScalingDelay(int currentProcessorsCount)
        {

            var delay = Math.Pow(ScaleBase, currentProcessorsCount);

            if (delay > maxScalingDelay.TotalSeconds)
            {
                return TimeSpan.FromSeconds(maxScalingDelay.TotalSeconds);
            }

            return TimeSpan.FromSeconds(delay);
        }
        public bool IsScalingAllowed(int currentProcessorsCount, TimeSpan currentMaxPartProcessingTime)
        {
            return currentMaxPartProcessingTime.TotalMilliseconds < this.maxPartProcessingTime.TotalMilliseconds;
        }
    }
}
