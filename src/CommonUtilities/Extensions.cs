// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace CommonUtilities
{
    /// <summary>
    /// Common extension methods
    /// </summary>
    public static class Extensions
    {
        /// <summary>
        /// Tries to release a semaphore if it is not already released.
        /// </summary>
        /// <param name="semaphore">The SemaphoreSlim</param>
        /// <returns>True if successful, false otherwise</returns>
        public static bool TryRelease(this SemaphoreSlim semaphore)
        {
            if (semaphore.CurrentCount == 0)
            {
                semaphore.Release();
                return true;
            }

            return false;
        }
    }
}
