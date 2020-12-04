using System;
using System.Collections.Generic;
using System.Text;
using EZNEW.Cache.Server;

namespace EZNEW.Cache.Redis
{
    /// <summary>
    /// Redis server configuration
    /// </summary>
    public class RedisServerConfiguration : CacheServerConfiguration
    {
        /// <summary>
        /// Gets or sets whether daemonize
        /// </summary>
        public bool Daemonize
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the pid file
        /// </summary>
        public string PidFile
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the port
        /// </summary>
        public int Port
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the host
        /// </summary>
        public string Host
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the time out
        /// </summary>
        public long TimeOut
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the log level
        /// </summary>
        public LogLevel LogLevel
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the log file
        /// </summary>
        public string LogFile
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the data base count
        /// </summary>
        public int DatabaseCount
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets save configuration
        /// </summary>
        public List<DataChangeSaveOptions> SaveConfiguration
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets whether compression rdb
        /// </summary>
        public bool RdbCompression
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the database file name
        /// </summary>
        public string DatabaseFileName
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the database directory
        /// </summary>
        public string DatabaseDirectory
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the master host
        /// </summary>
        public string MasterHost
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the master port
        /// </summary>
        public int MasterPort
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the master pwd
        /// </summary>
        public string MasterPassword
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the password
        /// </summary>
        public string Password
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the max client
        /// </summary>
        public int MaxClient
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the max memory
        /// </summary>
        public long MaxMemory
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets whether append only
        /// </summary>
        public bool AppendOnly
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the append file name
        /// </summary>
        public string AppendFileName
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the appendf sync
        /// </summary>
        public AppendfSync AppendfSync
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets whether enabled virtual memory
        /// </summary>
        public bool EnabledVirtualMemory
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the virtual memory swap file
        /// </summary>
        public string VirtualMemorySwapFile
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the max virtual memory
        /// </summary>
        public long MaxVirtualMemory
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the virtual memory page size(byte)
        /// </summary>
        public long VirtualMemoryPageSize
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the virtual memory pages
        /// </summary>
        public long VirtualMemoryPages
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the virtual memory max threads
        /// </summary>
        public int VirtualMemoryMaxThreads
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets whether glueoutputbuf
        /// </summary>
        public bool Glueoutputbuf
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets hash max zip map
        /// </summary>
        public Dictionary<string, long> HashMaxZipMap
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets whether activere hashing
        /// </summary>
        public bool ActivereHashing
        {
            get; set;
        }

        /// <summary>
        /// Gets or sets the include configuration file
        /// </summary>
        public string IncludeConfigurationFile
        {
            get; set;
        }
    }
}
