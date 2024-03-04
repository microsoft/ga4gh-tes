// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer
{
    public class SimpleScalingStrategy : IScalingStrategy
    {
        public bool IsScalingAllowed(int currentProcessorsCount, TimeSpan currentMaxPartProcessingTime)
        {
            //scale in all cases
            return true;
        }
        public TimeSpan GetScalingDelay(int currentProcessorsCount)
        {
            //technically, no delay
            return TimeSpan.FromMilliseconds(10);
        }
    }
}
