using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;

namespace Sixnet.Cache.Redis
{
    public class SixnetRedisDatabase : SixnetCacheDatabase
    {
        /// <summary>
        /// Gets or sets the redis database
        /// </summary>
       public IDatabase RemoteDatabase { get; set; }
    }
}
