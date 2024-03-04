// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
