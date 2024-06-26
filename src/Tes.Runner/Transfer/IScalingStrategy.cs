// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer
{
    /// <summary>
    /// Defines a strategy for scaling the number of tasks processors for a blob operation. 
    /// </summary>
    public interface IScalingStrategy
    {
        /// <summary>
        /// Determines if scaling is possible based on the current number of processors and the maximum processing time.
        /// </summary>
        /// <param name="currentProcessorsCount">Current number of tasks processors</param>
        /// <param name="currentMaxPartProcessingTime">The maximum duration of processing a part in the current pipeline</param>
        /// <returns></returns>
        bool IsScalingAllowed(int currentProcessorsCount, TimeSpan currentMaxPartProcessingTime);


        /// <summary>
        /// Defines the delay before attempting to add a new task processor.
        /// </summary>
        /// <returns></returns>
        TimeSpan GetScalingDelay(int currentProcessorsCount);
    }
}
