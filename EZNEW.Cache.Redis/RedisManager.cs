using System;
using System.Collections.Generic;
using System.Text;

namespace EZNEW.Cache.Redis
{
    /// <summary>
    /// Redis manager
    /// </summary>
    public static class RedisManager
    {
        #region Sliding expiration

        internal static bool SlidingExpiration = true;

        /// <summary>
        /// Enable sliding expiration
        /// </summary>
        public static void EnableSlidingExpiration()
        {
            SlidingExpiration = true;
        }

        /// <summary>
        /// Disable sliding expiration
        /// </summary>
        public static void DisableSlidingExpiration()
        {
            SlidingExpiration = false;
        }

        /// <summary>
        /// Gets whether allow sliding expiration
        /// </summary>
        /// <returns></returns>
        public static bool AllowSlidingExpiration()
        {
            return SlidingExpiration;
        }

        #endregion
    }
}
