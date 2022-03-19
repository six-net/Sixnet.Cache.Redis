using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;

namespace EZNEW.Cache.Redis
{
    public class RedisDatabase : CacheDatabase
    {
        /// <summary>
        /// Gets or sets the redis database
        /// </summary>
       public IDatabase RemoteDatabase { get; set; }
    }
}
