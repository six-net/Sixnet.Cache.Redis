using System;
using System.Collections.Generic;
using System.Text;
using StackExchange.Redis;

namespace Sixnet.Cache.Redis
{
    public struct RedisStatement
    {
        public string Script { get; set; }

        public RedisKey[] Keys { get; set; }

        public RedisValue[] Parameters { get; set; }

        public CommandFlags Flags { get; set; }
    }
}
