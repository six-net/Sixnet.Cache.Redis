using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using Sixnet.Logging;
using Sixnet.Exceptions;

namespace Sixnet.Cache.Redis
{
    /// <summary>
    /// Redis manager
    /// </summary>
    public static class RedisManager
    {
        #region Expiration

        /// <summary>
        /// Expiration key suffix
        /// </summary>
        internal const string ExpirationKeySuffix = ":ex";

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

        /// <summary>
        /// Get key expiration
        /// </summary>
        /// <param name="expiration">Cache expiration</param>
        /// <returns></returns>
        internal static Tuple<bool, TimeSpan?> GetExpiration(CacheExpiration expiration)
        {
            if (expiration == null)
            {
                return new Tuple<bool, TimeSpan?>(false, null);
            }
            if (expiration.SlidingExpiration)
            {
                return new Tuple<bool, TimeSpan?>(true, expiration.AbsoluteExpirationRelativeToNow);
            }
            else if (expiration.AbsoluteExpiration.HasValue)
            {
                var nowDate = DateTimeOffset.Now;
                if (expiration.AbsoluteExpiration.Value <= nowDate)
                {
                    return new Tuple<bool, TimeSpan?>(false, TimeSpan.Zero);
                }
                return new Tuple<bool, TimeSpan?>(false, expiration.AbsoluteExpiration.Value - nowDate);
            }
            return new Tuple<bool, TimeSpan?>(false, null);
        }

        #endregion

        #region Connection

        /// <summary>
        /// key=>The CacheServer's name
        /// value=>ConnectionMultiplexer instance
        /// </summary>
        static readonly Dictionary<string, ConnectionMultiplexer> ConnectionMultiplexers = new Dictionary<string, ConnectionMultiplexer>();

        /// <summary>
        /// key=>"{CacheServer.Name}_{DatabaseIndex}"
        /// </summary>
        static readonly Dictionary<string, RedisDatabase> Databases = new Dictionary<string, RedisDatabase>();

        /// <summary>
        /// Get config connection
        /// </summary>
        /// <param name="cacheServer">Cache server</param>
        /// <returns></returns>
        public static ConnectionMultiplexer GetConfigConnection(CacheServer cacheServer)
        {
            ConnectionMultiplexers.TryGetValue(cacheServer?.Name, out var conn);
            return conn;
        }

        /// <summary>
        /// Get connection
        /// </summary>
        /// <param name="cacheServer">Cache server</param>
        /// <param name="endPoints">End points</param>
        /// <returns></returns>
        public static ConnectionMultiplexer GetConnection(CacheServer cacheServer, IEnumerable<CacheEndPoint> endPoints)
        {
            if (cacheServer == null || endPoints == null)
            {
                return null;
            }
            return CreateConnection(cacheServer, endPoints);
        }

        /// <summary>
        /// Get databases
        /// </summary>
        /// <param name="cacheServer">Cache server</param>
        /// <returns></returns>
        public static RedisDatabase GetDatabase(CacheServer cacheServer)
        {
            if (string.IsNullOrEmpty(cacheServer?.Name))
            {
                throw new ArgumentNullException(nameof(CacheServer.Name));
            }
            var database = cacheServer.Database;
            if (string.IsNullOrWhiteSpace(database))
            {
                database = "0";
            }
            var dbName = $"{cacheServer.Name}_{cacheServer.Database}";
            if (Databases.TryGetValue(dbName, out var nowDatabase))
            {
                return nowDatabase;
            }
            else if (int.TryParse(database, out var dbIndex) && dbIndex >= 0 && ConnectionMultiplexers.TryGetValue(cacheServer.Name, out var conn) && conn != null)
            {
                lock (Databases)
                {
                    if (nowDatabase != null)
                    {
                        return nowDatabase;
                    }
                    nowDatabase = new RedisDatabase()
                    {
                        Index = dbIndex,
                        Name = dbName,
                        RemoteDatabase = conn.GetDatabase(dbIndex)
                    };
                    Databases[dbName] = nowDatabase;
                    return nowDatabase;
                }
            }
            throw new SixnetException($"Redis database {database} is invalid");
        }

        #endregion

        #region Server

        /// <summary>
        /// Create redis connection
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="endPoints">EndPoints</param>
        /// <param name="ignoreConnectionException">Ignore connection exception</param>
        /// <returns></returns>
        internal static ConnectionMultiplexer CreateConnection(CacheServer server, IEnumerable<CacheEndPoint> endPoints, bool ignoreConnectionException = true)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(server?.Name))
                {
                    throw new ArgumentNullException("Server's name is null or empty");
                }
                if (endPoints.IsNullOrEmpty())
                {
                    throw new ArgumentNullException("No endpoint specified");
                }
                var configOptions = new ConfigurationOptions()
                {
                    AllowAdmin = server.AllowAdmin,
                    ResolveDns = server.ResolveDns,
                    Ssl = server.SSL,
                    SyncTimeout = 10000
                };
                foreach (var endPoint in endPoints)
                {
                    configOptions.EndPoints.Add(endPoint.Host, endPoint.Port);
                }
                if (server.ConnectTimeout > 0)
                {
                    configOptions.ConnectTimeout = server.ConnectTimeout;
                }
                if (!string.IsNullOrWhiteSpace(server.Password))
                {
                    configOptions.Password = server.Password;
                }
                if (!string.IsNullOrWhiteSpace(server.ClientName))
                {
                    configOptions.ClientName = server.ClientName;
                }
                if (!string.IsNullOrWhiteSpace(server.SSLHost))
                {
                    configOptions.SslHost = server.SSLHost;
                }
                if (server.SyncTimeout > 0)
                {
                    configOptions.SyncTimeout = server.SyncTimeout;
                }
                if (!string.IsNullOrWhiteSpace(server.TieBreaker))
                {
                    configOptions.TieBreaker = server.TieBreaker;
                }
                return ConnectionMultiplexer.Connect(configOptions);
            }
            catch (Exception ex)
            {
                SixnetLogger.LogError<RedisProvider>(SixnetLogEvents.Cache.ConnectionCacheServerError, ex, ex.Message);
                if (!ignoreConnectionException)
                {
                    throw ex;
                }
            }
            return null;
        }

        /// <summary>
        /// Register server
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="endPoints">End points</param>
        /// <param name="ignoreConnectionException">Ignore connection exception</param>
        public static void RegisterServer(CacheServer server, IEnumerable<CacheEndPoint> endPoints, bool ignoreConnectionException = true)
        {
            var conn = CreateConnection(server, endPoints, ignoreConnectionException);
            if (conn == null)
            {
                return;
            }
            var database = server.Database;
            if (string.IsNullOrWhiteSpace(database))
            {
                database = "0";
            }
            if (int.TryParse(database, out var dbIndex) && dbIndex >= 0)
            {
                string databaseName = $"{server.Name}_{database}";
                Databases[databaseName] = new RedisDatabase()
                {
                    Name = databaseName,
                    Index = dbIndex,
                    RemoteDatabase = conn.GetDatabase(dbIndex)
                };
            }
            ConnectionMultiplexers[server.Name] = conn;
        }

        /// <summary>
        /// Register server
        /// </summary>
        /// <param name="serverName">Server name</param>
        /// <param name="endPoint">End point</param>
        /// <param name="ignoreConnectionException">Ignore connection exception</param>
        public static void RegisterServer(string serverName, CacheEndPoint endPoint, bool ignoreConnectionException = true)
        {
            var server = new CacheServer() 
            {
                Name = serverName,
                Type = CacheServerType.Redis
            };
            RegisterServer(server, new CacheEndPoint[1] { endPoint }, ignoreConnectionException);
        }

        /// <summary>
        /// Register server
        /// </summary>
        /// <param name="serverName">Server name</param>
        /// <param name="host">Host</param>
        /// <param name="port">Port</param>
        /// <param name="ignoreConnectionException">Ignore connection exception</param>
        public static void RegisterServer(string serverName, string host, int port, bool ignoreConnectionException = true)
        {
            RegisterServer(serverName, new CacheEndPoint() { Host = host, Port = port }, ignoreConnectionException);
        }

        /// <summary>
        /// Register
        /// </summary>
        /// <param name="serverName">Server name</param>
        /// <param name="host">Host</param>
        /// <param name="ignoreConnectionException">Ignore connection exception</param>
        public static void RegisterServer(string serverName, string host, bool ignoreConnectionException = true)
        {
            RegisterServer(serverName, host, 6379, ignoreConnectionException);
        }

        #endregion

        /// <summary>
        /// get options flags
        /// </summary>
        /// <param name="cacheCommandFlags">cache options flags</param>
        /// <returns>options flags</returns>
        internal static CommandFlags GetCommandFlags(CacheCommandFlags cacheCommandFlags)
        {
            CommandFlags cmdFlags = cacheCommandFlags switch
            {
                CacheCommandFlags.DemandMaster => CommandFlags.DemandMaster,
                CacheCommandFlags.DemandReplica => CommandFlags.DemandReplica,
                CacheCommandFlags.FireAndForget => CommandFlags.FireAndForget,
                CacheCommandFlags.NoRedirect => CommandFlags.NoRedirect,
                CacheCommandFlags.NoScriptCache => CommandFlags.NoScriptCache,
                CacheCommandFlags.PreferReplica => CommandFlags.PreferReplica,
                _ => CommandFlags.None,
            };
            return cmdFlags;
        }

        /// <summary>
        /// Get set value command
        /// </summary>
        /// <param name="setWhen"></param>
        /// <returns></returns>
        internal static string GetSetWhenCommand(CacheSetWhen setWhen)
        {
            return setWhen switch
            {
                CacheSetWhen.Exists => "XX",
                CacheSetWhen.NotExists => "NX",
                _ => "",
            };
        }

        /// <summary>
        /// Get the bit operator
        /// </summary>
        /// <param name="bitwise">bitwise</param>
        /// <returns>Return the bit operator</returns>
        internal static string GetBitOperator(CacheBitwise bitwise)
        {
            string bitOperator = "AND";
            switch (bitwise)
            {
                case CacheBitwise.And:
                default:
                    break;
                case CacheBitwise.Not:
                    bitOperator = "NOT";
                    break;
                case CacheBitwise.Or:
                    bitOperator = "OR";
                    break;
                case CacheBitwise.Xor:
                    bitOperator = "XOR";
                    break;
            }
            return bitOperator;
        }

        /// <summary>
        /// Get set combine command
        /// </summary>
        /// <param name="setOperationType"></param>
        /// <returns></returns>
        internal static string GetSetCombineCommand(CombineOperation operationType)
        {
            return operationType switch
            {
                CombineOperation.Difference => "SDIFF",
                CombineOperation.Intersect => "SUNION",
                _ => "SUNION",
            };
        }

        /// <summary>
        /// Get sorted set combine command
        /// </summary>
        /// <param name="operationType">Set operation type</param>
        /// <returns></returns>
        internal static string GetSortedSetCombineCommand(CombineOperation operationType)
        {
            return operationType switch
            {
                CombineOperation.Difference => throw new InvalidOperationException(nameof(CombineOperation.Difference)),
                CombineOperation.Intersect => "ZINTERSTORE",
                _ => "ZUNIONSTORE",
            };
        }

        /// <summary>
        /// Get sorted set aggregate name
        /// </summary>
        /// <param name="aggregate">Aggregate type</param>
        /// <returns></returns>
        internal static string GetSortedSetAggregateName(SetAggregate aggregate)
        {
            return aggregate switch
            {
                SetAggregate.Min => "MIN",
                SetAggregate.Max => "MAX",
                _ => "SUM",
            };
        }

        /// <summary>
        /// Get cache key type
        /// </summary>
        /// <param name="typeName">Redis type name</param>
        /// <returns></returns>
        internal static CacheKeyType GetCacheKeyType(string typeName)
        {
            CacheKeyType keyType = CacheKeyType.Unknown;
            typeName = typeName?.ToLower() ?? string.Empty;
            switch (typeName)
            {
                case "string":
                    keyType = CacheKeyType.String;
                    break;
                case "list":
                    keyType = CacheKeyType.List;
                    break;
                case "hash":
                    keyType = CacheKeyType.Hash;
                    break;
                case "set":
                    keyType = CacheKeyType.Set;
                    break;
                case "zset":
                    keyType = CacheKeyType.SortedSet;
                    break;
            }
            return keyType;
        }

        /// <summary>
        /// Get total seconds
        /// </summary>
        /// <param name="timeSpan">Time span</param>
        /// <returns></returns>
        internal static long GetTotalSeconds(TimeSpan? timeSpan)
        {
            return (long)(timeSpan?.TotalSeconds ?? 0);
        }

        /// <summary>
        /// Get match pattern
        /// </summary>
        /// <param name="key">Match key</param>
        /// <param name="patternType">Pattern type</param>
        /// <returns></returns>
        internal static string GetMatchPattern(string key, KeyMatchPattern patternType)
        {
            if (string.IsNullOrEmpty(key))
            {
                return "*";
            }
            return patternType switch
            {
                KeyMatchPattern.StartWith => $"{key}*",
                KeyMatchPattern.EndWith => $"*{key}",
                _ => $"*{key}*",
            };
        }
    }
}
