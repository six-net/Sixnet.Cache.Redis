using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using StackExchange.Redis;
using EZNEW.Cache.SortedSet;
using EZNEW.Cache.Constant;
using EZNEW.Cache.Server;
using EZNEW.ValueType;
using EZNEW.Cache.Keys;
using EZNEW.Cache.Hash;
using EZNEW.Cache.List;
using EZNEW.Cache.String;
using EZNEW.Cache.Set;
using EZNEW.Fault;

namespace EZNEW.Cache.Redis
{
    /// <summary>
    /// Implements ICacheProvider by Redis
    /// </summary>
    public class RedisProvider : ICacheProvider
    {
        #region String

        #region StringSetRange

        /// <summary>
        /// Overwrites part of the string stored at key, starting at the specified offset,
        /// for the entire length of value. If the offset is larger than the current length
        /// of the string at key, the string is padded with zero-bytes to make offset fit.
        /// Non-existing keys are considered as empty strings, so this options will make
        /// sure it holds a string large enough to be able to set value at offset.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">String set range options</param>
        /// <returns>Return string set range response</returns>
        public async Task<IEnumerable<StringSetRangeResponse>> StringSetRangeAsync(CacheServer server, StringSetRangeOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringSetRangeOptions)}.{nameof(StringSetRangeOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<StringSetRangeResponse>(server);
            }
            string script = $@"local len=redis.call('SETRANGE',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return len";
            var expire = RedisManager.GetExpiration(options.Expiration);
            var keys = new RedisKey[] { options.Key.GetActualKey() };
            var parameters = new RedisValue[]
            {
                options.Offset,
                options.Value,
                options.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds
            };
            var commandFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<StringSetRangeResponse> responses = new List<StringSetRangeResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, commandFlags).ConfigureAwait(false);
                responses.Add(new StringSetRangeResponse()
                {
                    Success = true,
                    CacheServer = server,
                    Database = db,
                    NewValueLength = (long)result
                });
            }
            return responses;
        }

        #endregion

        #region StringSetBit

        /// <summary>
        /// Sets or clears the bit at offset in the string value stored at key. The bit is
        /// either set or cleared depending on value, which can be either 0 or 1. When key
        /// does not exist, a new string value is created.The string is grown to make sure
        /// it can hold a bit at offset.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">String set bit options</param>
        /// <returns>Return string set bit response</returns>
        public async Task<IEnumerable<StringSetBitResponse>> StringSetBitAsync(CacheServer server, StringSetBitOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringSetBitOptions)}.{nameof(StringSetBitOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<StringSetBitResponse>(server);
            }
            string script = $@"local obv=redis.call('SETBIT',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return obv";
            var expire = RedisManager.GetExpiration(options.Expiration);
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Offset,
                options.Bit,
                options.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<StringSetBitResponse> responses = new List<StringSetBitResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new StringSetBitResponse()
                {
                    Success = true,
                    OldBitValue = (bool)result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region StringSet

        /// <summary>
        /// Set key to hold the string value. If key already holds a value, it is overwritten,
        /// regardless of its type.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">String set options</param>
        /// <returns>Return string set response</returns>
        public async Task<IEnumerable<StringSetResponse>> StringSetAsync(CacheServer server, StringSetOptions options)
        {
            if (options?.Items.IsNullOrEmpty() ?? true)
            {
                return GetNoValueResponse<StringSetResponse>(server);
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<StringSetResponse>(server);
            }
            var itemCount = options.Items.Count;
            var valueCount = itemCount * 5;
            var allowSlidingExpire = RedisManager.AllowSlidingExpiration();
            RedisKey[] setKeys = new RedisKey[itemCount];
            RedisValue[] setValues = new RedisValue[valueCount];
            for (var i = 0; i < itemCount; i++)
            {
                var nowItem = options.Items[i];
                var nowExpire = RedisManager.GetExpiration(nowItem.Expiration);
                setKeys[i] = nowItem.Key.GetActualKey();

                var argIndex = i * 5;
                var allowSliding = nowExpire.Item1 && allowSlidingExpire;
                setValues[argIndex] = nowItem.Value.ToNullableString();
                setValues[argIndex + 1] = nowItem.Expiration == null;
                setValues[argIndex + 2] = allowSliding;
                setValues[argIndex + 3] = nowExpire.Item2.HasValue ? RedisManager.GetTotalSeconds(nowExpire.Item2) : (allowSliding ? 0 : -1);
                setValues[argIndex + 4] = RedisManager.GetSetWhenCommand(nowItem.When);
            }
            string script = $@"local skeys={{}}
local ckey=''
local exkey=''
local argBi=1
local sr=true
for ki=1,{itemCount}
do
    argBi=(ki-1)*5+1
    ckey=KEYS[ki]
    exkey=ckey..'{RedisManager.ExpirationKeySuffix}'
    local setCmd=ARGV[argBi+4]
    if(setCmd=='')
    then
        local res=redis.call('SET',ckey,ARGV[argBi])
        sr=res and string.lower(res['ok'])=='ok'
    else
        local res=redis.call('SET',ckey,ARGV[argBi],setCmd);
        sr=res and string.lower(res['ok'])=='ok'
    end
    if sr
    then
        skeys[ki]=ckey
        if ARGV[argBi+1] == '1'
        then
            local ct=redis.call('GET',exkey)
            if ct
            then
                local rs=redis.call('EXPIRE',ckey,ct)
                if rs==1
                then
                    redis.call('SET',exkey,ct,'EX',ct)
                end
            end
        else
            local nt=tonumber(ARGV[argBi+3])
            if nt>0
            then
                local rs=redis.call('EXPIRE',ckey,nt)
                if rs==1 and ARGV[argBi+2]=='1'
                then
                    redis.call('SET',exkey,nt,'EX',nt)
                end
            elseif nt<0
            then
                redis.call('PERSIST',ckey)
            end
        end
    end
end
return skeys";
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<StringSetResponse> responses = new List<StringSetResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = await db.RemoteDatabase.ScriptEvaluateAsync(script, setKeys, setValues, cmdFlags).ConfigureAwait(false);
                responses.Add(new StringSetResponse()
                {
                    CacheServer = server,
                    Database = db,
                    Success = true,
                    Results = ((RedisValue[])result)?.Select(c => new StringEntrySetResult() { SetSuccess = true, Key = c }).ToList()
                });
            }
            return responses;
        }

        #endregion

        #region StringLength

        /// <summary>
        /// Returns the length of the string value stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">String length options</param>
        /// <returns>Return string length response</returns>
        public async Task<IEnumerable<StringLengthResponse>> StringLengthAsync(CacheServer server, StringLengthOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringLengthOptions)}.{nameof(StringLengthOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<StringLengthResponse>(server);
            }
            string script = $@"local obv=redis.call('STRLEN',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return obv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<StringLengthResponse> responses = new List<StringLengthResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new StringLengthResponse()
                {
                    Success = true,
                    Length = (long)result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region StringIncrement

        /// <summary>
        /// Increments the string representing a floating point number stored at key by the
        /// specified increment. If the key does not exist, it is set to 0 before performing
        /// the operation. The precision of the output is fixed at 17 digits after the decimal
        /// point regardless of the actual internal precision of the computation.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">String increment options</param>
        /// <returns>Return string increment response</returns>
        public async Task<IEnumerable<StringIncrementResponse>> StringIncrementAsync(CacheServer server, StringIncrementOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringIncrementOptions)}.{nameof(StringIncrementOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<StringIncrementResponse>(server);
            }
            string script = $@"local obv=redis.call('INCRBY',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return obv";
            var expire = RedisManager.GetExpiration(options.Expiration);
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Value,
                options.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<StringIncrementResponse> responses = new List<StringIncrementResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new StringIncrementResponse()
                {
                    Success = true,
                    NewValue = (long)result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region StringGetWithExpiry

        /// <summary>
        /// Get the value of key. If the key does not exist the special value nil is returned.
        /// An error is returned if the value stored at key is not a string, because GET
        /// only handles string values.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">String get with expiry ption</param>
        /// <returns>Return string get with expiry response</returns>
        public async Task<IEnumerable<StringGetWithExpiryResponse>> StringGetWithExpiryAsync(CacheServer server, StringGetWithExpiryOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetWithExpiryOptions)}.{nameof(StringGetWithExpiryOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<StringGetWithExpiryResponse>(server);
            }
            string script = $@"local obv=redis.call('GET',{Keys(1)})
local exts=0
local res={{}}
if obv
then
{GetRefreshExpirationScript(-2)}
exts=redis.call('TTL',{Keys(1)})
end
res[1]=obv
res[2]=exts
return res";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<StringGetWithExpiryResponse> responses = new List<StringGetWithExpiryResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (RedisValue[])(await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false));
                responses.Add(new StringGetWithExpiryResponse()
                {
                    Success = true,
                    Value = result[0],
                    Expiry = TimeSpan.FromSeconds((long)result[1]),
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region StringGetSet

        /// <summary>
        /// Atomically sets key to value and returns the old value stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">String get set options</param>
        /// <returns>Return string get set response</returns>
        public async Task<IEnumerable<StringGetSetResponse>> StringGetSetAsync(CacheServer server, StringGetSetOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetSetOptions)}.{nameof(StringGetSetOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<StringGetSetResponse>(server);
            }
            string script = $@"local ov=redis.call('GETSET',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return ov";
            var expire = RedisManager.GetExpiration(options.Expiration);
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.NewValue,
                options.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<StringGetSetResponse> responses = new List<StringGetSetResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new StringGetSetResponse()
                {
                    Success = true,
                    OldValue = (string)result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region StringGetRange

        /// <summary>
        /// Returns the substring of the string value stored at key, determined by the offsets
        /// start and end (both are inclusive). Negative offsets can be used in order to
        /// provide an offset starting from the end of the string. So -1 means the last character,
        /// -2 the penultimate and so forth.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">String get range options</param>
        /// <returns>Return string get range response</returns>
        public async Task<IEnumerable<StringGetRangeResponse>> StringGetRangeAsync(CacheServer server, StringGetRangeOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetRangeOptions)}.{nameof(StringGetRangeOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<StringGetRangeResponse>(server);
            }
            string script = $@"local ov=redis.call('GETRANGE',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return ov";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Start,
                options.End,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<StringGetRangeResponse> responses = new List<StringGetRangeResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new StringGetRangeResponse()
                {
                    Success = true,
                    Value = (string)result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region StringGetBit

        /// <summary>
        /// Returns the bit value at offset in the string value stored at key. When offset
        /// is beyond the string length, the string is assumed to be a contiguous space with
        /// 0 bits
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">String get bit options</param>
        /// <returns>Return string get bit response</returns>
        public async Task<IEnumerable<StringGetBitResponse>> StringGetBitAsync(CacheServer server, StringGetBitOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetBitOptions)}.{nameof(StringGetBitOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<StringGetBitResponse>(server);
            }
            string script = $@"local ov=redis.call('GETBIT',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return ov";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Offset,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<StringGetBitResponse> responses = new List<StringGetBitResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new StringGetBitResponse()
                {
                    Success = true,
                    Bit = (bool)result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region StringGet

        /// <summary>
        /// Returns the values of all specified keys. For every key that does not hold a
        /// string value or does not exist, the special value nil is returned.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">String get options</param>
        /// <returns>Return string get response</returns>
        public async Task<IEnumerable<StringGetResponse>> StringGetAsync(CacheServer server, StringGetOptions options)
        {
            if (options?.Keys.IsNullOrEmpty() ?? true)
            {
                return GetNoKeyResponse<StringGetResponse>(server);
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<StringGetResponse>(server);
            }
            var keys = options.Keys.Select(c => { RedisKey rv = c.GetActualKey(); return rv; }).ToArray();
            string script = $@"local vals={{}}
local ri=1
for ki=1,{keys.Length}
do
    local cv=redis.call('GET',KEYS[ki])
    if cv
    then
        vals[ri]=KEYS[ki]..'$::$'..cv
        ri=ri+1
    end
end
{GetRefreshExpirationScript(-2, keyCount: keys.Length)}
return vals";
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<StringGetResponse> responses = new List<StringGetResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (RedisValue[])(await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false));
                responses.Add(new StringGetResponse()
                {
                    Success = true,
                    Values = result.Select(c =>
                    {
                        string stringValue = c;
                        var valueArray = stringValue.LSplit("$::$");
                        return new CacheEntry()
                        {
                            Key = valueArray[0],
                            Value = valueArray.Length > 1 ? valueArray[1] : null
                        };
                    }).ToList(),
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region StringDecrement

        /// <summary>
        /// Decrements the number stored at key by decrement. If the key does not exist,
        /// it is set to 0 before performing the operation. An error is returned if the key
        /// contains a value of the wrong type or contains a string that is not representable
        /// as integer. This operation is limited to 64 bit signed integers.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">String decrement options</param>
        /// <returns>Return string decrement response</returns>
        public async Task<IEnumerable<StringDecrementResponse>> StringDecrementAsync(CacheServer server, StringDecrementOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringDecrementOptions)}.{nameof(StringDecrementOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<StringDecrementResponse>(server);
            }
            string script = $@"local obv=redis.call('DECRBY',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return obv";
            var expire = RedisManager.GetExpiration(options.Expiration);
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Value,
                options.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<StringDecrementResponse> responses = new List<StringDecrementResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new StringDecrementResponse()
                {
                    Success = true,
                    NewValue = (long)result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region StringBitPosition

        /// <summary>
        /// Return the position of the first bit set to 1 or 0 in a string. The position
        /// is returned thinking at the string as an array of bits from left to right where
        /// the first byte most significant bit is at position 0, the second byte most significant
        /// bit is at position 8 and so forth. An start and end may be specified; these are
        /// in bytes, not bits; start and end can contain negative values in order to index
        /// bytes starting from the end of the string, where -1 is the last byte, -2 is the
        /// penultimate, and so forth.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">String bit position options</param>
        /// <returns>Return string bit position response</returns>
        public async Task<IEnumerable<StringBitPositionResponse>> StringBitPositionAsync(CacheServer server, StringBitPositionOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringBitPositionOptions)}.{nameof(StringBitPositionOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<StringBitPositionResponse>(server);
            }
            string script = $@"local obv=redis.call('BITPOS',{Keys(1)},{Arg(1)},{Arg(2)},{Arg(3)})
{GetRefreshExpirationScript(1)}
return obv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Bit,
                options.Start,
                options.End,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<StringBitPositionResponse> responses = new List<StringBitPositionResponse>();
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new StringBitPositionResponse()
                {
                    Success = true,
                    Position = result,
                    HasValue = result >= 0,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region StringBitOperation

        /// <summary>
        /// Perform a bitwise operation between multiple keys (containing string values)
        ///  and store the result in the destination key. The BITOP options supports four
        ///  bitwise operations; note that NOT is a unary operator: the second key should
        ///  be omitted in this case and only the first key will be considered. The result
        /// of the operation is always stored at destkey.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">String bit operation options</param>
        /// <returns>Return string bit operation response</returns>
        public async Task<IEnumerable<StringBitOperationResponse>> StringBitOperationAsync(CacheServer server, StringBitOperationOptions options)
        {
            if (options?.Keys.IsNullOrEmpty() ?? true)
            {
                return GetNoKeyResponse<StringBitOperationResponse>(server);
            }
            if (string.IsNullOrWhiteSpace(options?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(StringBitOperationOptions)}.{nameof(StringBitOperationOptions.DestinationKey)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<StringBitOperationResponse>(server);
            }
            var keys = new RedisKey[options.Keys.Count + 1];
            var keyParameters = new string[options.Keys.Count + 1];
            keys[0] = options.DestinationKey.GetActualKey();
            keyParameters[0] = "KEYS[1]";
            for (var i = 0; i < options.Keys.Count; i++)
            {
                keys[i + 1] = options.Keys[i].GetActualKey();
                keyParameters[i + 1] = $"KEYS[{2 + i}]";
            }
            string script = $@"local obv=redis.call('BITOP',{Arg(1)},{string.Join(",", keyParameters)})
{GetRefreshExpirationScript(-1)}
{GetRefreshExpirationScript(2, 1, options.Keys.Count)}
return obv";
            var expire = RedisManager.GetExpiration(options.Expiration);
            bool allowSlidingExpiration = RedisManager.AllowSlidingExpiration();
            var parameters = new RedisValue[]
            {
                RedisManager.GetBitOperator(options.Bitwise),
                options.Expiration==null,//refresh current time
                expire.Item1&&allowSlidingExpiration,//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds,
                true,//refresh current time-source key
                allowSlidingExpiration,//whether allow set refresh time-source key,
                0//expire time seconds-source key
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<StringBitOperationResponse> responses = new List<StringBitOperationResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new StringBitOperationResponse()
                {
                    Success = true,
                    DestinationValueLength = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region StringBitCount

        /// <summary>
        /// Count the number of set bits (population counting) in a string. By default all
        /// the bytes contained in the string are examined.It is possible to specify the
        /// counting operation only in an interval passing the additional arguments start
        /// and end. Like for the GETRANGE options start and end can contain negative values
        /// in order to index bytes starting from the end of the string, where -1 is the
        /// last byte, -2 is the penultimate, and so forth.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">String bit count options</param>
        /// <returns>Return string bit count response</returns>
        public async Task<IEnumerable<StringBitCountResponse>> StringBitCountAsync(CacheServer server, StringBitCountOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringBitCountOptions)}.{nameof(StringBitCountOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<StringBitCountResponse>(server);
            }
            string script = $@"local obv=redis.call('BITCOUNT',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return obv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Start,
                options.End,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<StringBitCountResponse> responses = new List<StringBitCountResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new StringBitCountResponse()
                {
                    Success = true,
                    BitNum = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region StringAppend

        /// <summary>
        /// If key already exists and is a string, this options appends the value at the
        /// end of the string. If key does not exist it is created and set as an empty string,
        /// so APPEND will be similar to SET in this special case.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">String append options</param>
        /// <returns>Return string append response</returns>
        public async Task<IEnumerable<StringAppendResponse>> StringAppendAsync(CacheServer server, StringAppendOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringAppendOptions)}.{nameof(StringAppendOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<StringAppendResponse>(server);
            }
            string script = $@"local obv=redis.call('APPEND',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return obv";
            var expire = RedisManager.GetExpiration(options.Expiration);
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Value,
                options.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<StringAppendResponse> responses = new List<StringAppendResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new StringAppendResponse()
                {
                    Success = true,
                    NewValueLength = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #endregion

        #region List

        #region ListTrim

        /// <summary>
        /// Trim an existing list so that it will contain only the specified range of elements
        /// specified. Both start and stop are zero-based indexes, where 0 is the first element
        /// of the list (the head), 1 the next element and so on. For example: LTRIM foobar
        /// 0 2 will modify the list stored at foobar so that only the first three elements
        /// of the list will remain. start and end can also be negative numbers indicating
        /// offsets from the end of the list, where -1 is the last element of the list, -2
        /// the penultimate element and so on.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">List trim options</param>
        /// <returns>Return list trim response</returns>
        public async Task<IEnumerable<ListTrimResponse>> ListTrimAsync(CacheServer server, ListTrimOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListTrimOptions)}.{nameof(ListTrimOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<ListTrimResponse>(server);
            }
            string script = $@"redis.call('LTRIM',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Start,
                options.Stop,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<ListTrimResponse> responses = new List<ListTrimResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new ListTrimResponse()
                {
                    Success = true,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region ListSetByIndex

        /// <summary>
        /// Sets the list element at index to value. For more information on the index argument,
        ///  see ListGetByIndex. An error is returned for out of range indexes.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">List set by index options</param>
        /// <returns>Return list set by index response</returns>
        public async Task<IEnumerable<ListSetByIndexResponse>> ListSetByIndexAsync(CacheServer server, ListSetByIndexOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListSetByIndexOptions)}.{nameof(ListSetByIndexOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<ListSetByIndexResponse>(server);
            }
            string script = $@"local obv=redis.call('LSET',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return obv['ok']";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Index,
                options.Value,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<ListSetByIndexResponse> responses = new List<ListSetByIndexResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (string)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new ListSetByIndexResponse()
                {
                    Success = string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase),
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region ListRightPush

        /// <summary>
        /// Insert all the specified values at the tail of the list stored at key. If key
        /// does not exist, it is created as empty list before performing the push operation.
        /// Elements are inserted one after the other to the tail of the list, from the leftmost
        /// element to the rightmost element. So for instance the options RPUSH mylist a
        /// b c will result into a list containing a as first element, b as second element
        /// and c as third element.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">List right push options</param>
        /// <returns>Return list right push</returns>
        public async Task<IEnumerable<ListRightPushResponse>> ListRightPushAsync(CacheServer server, ListRightPushOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRightPushOptions)}.{nameof(ListRightPushOptions.Key)}");
            }
            if (options?.Values.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentException($"{nameof(ListRightPushOptions)}.{nameof(ListRightPushOptions.Values)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<ListRightPushResponse>(server);
            }
            var values = new RedisValue[options.Values.Count + 3];
            var valueParameters = new string[options.Values.Count];
            for (var i = 0; i < options.Values.Count; i++)
            {
                values[i] = options.Values[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            string script = $@"local obv=redis.call('RPUSH',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(options.Values.Count - 2)}
return obv";
            var expire = RedisManager.GetExpiration(options.Expiration);
            values[values.Length - 3] = options.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = expire.Item2?.TotalSeconds ?? 0;//expire time seconds
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<ListRightPushResponse> responses = new List<ListRightPushResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, values, cmdFlags).ConfigureAwait(false);
                responses.Add(new ListRightPushResponse()
                {
                    Success = true,
                    NewListLength = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region ListRightPopLeftPush

        /// <summary>
        /// Atomically returns and removes the last element (tail) of the list stored at
        /// source, and pushes the element at the first element (head) of the list stored
        /// at destination.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">List right pop left push options</param>
        /// <returns>Return list right pop left response</returns>
        public async Task<IEnumerable<ListRightPopLeftPushResponse>> ListRightPopLeftPushAsync(CacheServer server, ListRightPopLeftPushOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.SourceKey))
            {
                throw new ArgumentNullException($"{nameof(ListRightPopLeftPushOptions)}.{nameof(ListRightPopLeftPushOptions.SourceKey)}");
            }
            if (string.IsNullOrWhiteSpace(options?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(ListRightPopLeftPushOptions)}.{nameof(ListRightPopLeftPushOptions.DestinationKey)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<ListRightPopLeftPushResponse>(server);
            }
            string script = $@"local pv=redis.call('RPOPLPUSH',{Keys(1)},{Keys(2)})
{GetRefreshExpirationScript(-2)}
{GetRefreshExpirationScript(1, 1)}
return pv";
            var expire = RedisManager.GetExpiration(options.Expiration);
            bool allowSlidingExpiration = RedisManager.AllowSlidingExpiration();
            var keys = new RedisKey[]
            {
                options.SourceKey.GetActualKey(),
                options.DestinationKey.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time-source key
                allowSlidingExpiration,//whether allow set refresh time-source key
                0,//expire time seconds-source key

                options.Expiration==null,//refresh current time
                expire.Item1&&allowSlidingExpiration,//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2)//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<ListRightPopLeftPushResponse> responses = new List<ListRightPopLeftPushResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (string)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new ListRightPopLeftPushResponse()
                {
                    Success = true,
                    PopValue = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region ListRightPop

        /// <summary>
        /// Removes and returns the last element of the list stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">List right pop options</param>
        /// <returns>Return list right pop response</returns>
        public async Task<IEnumerable<ListRightPopResponse>> ListRightPopAsync(CacheServer server, ListRightPopOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRightPopOptions)}.{nameof(ListRightPopOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<ListRightPopResponse>(server);
            }
            string script = $@"local pv=redis.call('RPOP',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<ListRightPopResponse> responses = new List<ListRightPopResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (string)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new ListRightPopResponse()
                {
                    Success = true,
                    PopValue = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region ListRemove

        /// <summary>
        /// Removes the first count occurrences of elements equal to value from the list
        /// stored at key. The count argument influences the operation in the following way
        /// count > 0: Remove elements equal to value moving from head to tail. count less 0:
        /// Remove elements equal to value moving from tail to head. count = 0: Remove all
        /// elements equal to value.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">List remove options</param>
        /// <returns>Return list remove response</returns>
        public async Task<IEnumerable<ListRemoveResponse>> ListRemoveAsync(CacheServer server, ListRemoveOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRemoveOptions)}.{nameof(ListRemoveOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<ListRemoveResponse>(server);
            }
            string script = $@"local rc=redis.call('LREM',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return rc";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Count,
                options.Value,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<ListRemoveResponse> responses = new List<ListRemoveResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new ListRemoveResponse()
                {
                    Success = true,
                    RemoveCount = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region ListRange

        /// <summary>
        /// Returns the specified elements of the list stored at key. The offsets start and
        /// stop are zero-based indexes, with 0 being the first element of the list (the
        /// head of the list), 1 being the next element and so on. These offsets can also
        /// be negative numbers indicating offsets starting at the end of the list.For example,
        /// -1 is the last element of the list, -2 the penultimate, and so on. Note that
        /// if you have a list of numbers from 0 to 100, LRANGE list 0 10 will return 11
        /// elements, that is, the rightmost item is included.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>list range response</returns>
        public async Task<IEnumerable<ListRangeResponse>> ListRangeAsync(CacheServer server, ListRangeOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRangeOptions)}.{nameof(ListRangeOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<ListRangeResponse>(server);
            }
            string script = $@"local rc=redis.call('LRANGE',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return rc";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Start,
                options.Stop,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<ListRangeResponse> responses = new List<ListRangeResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (RedisValue[])await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new ListRangeResponse()
                {
                    Success = true,
                    Values = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region ListLength

        /// <summary>
        /// Returns the length of the list stored at key. If key does not exist, it is interpreted
        ///  as an empty list and 0 is returned.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>list length response</returns>
        public async Task<IEnumerable<ListLengthResponse>> ListLengthAsync(CacheServer server, ListLengthOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListLengthOptions)}.{nameof(ListLengthOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<ListLengthResponse>(server);
            }
            string script = $@"local len=redis.call('LLEN',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return len";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<ListLengthResponse> responses = new List<ListLengthResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new ListLengthResponse()
                {
                    Success = true,
                    Length = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region ListLeftPush

        /// <summary>
        /// Insert the specified value at the head of the list stored at key. If key does
        ///  not exist, it is created as empty list before performing the push operations.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">List left push options</param>
        /// <returns>Return list left push response</returns>
        public async Task<IEnumerable<ListLeftPushResponse>> ListLeftPushAsync(CacheServer server, ListLeftPushOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListLeftPushOptions)}.{nameof(ListLeftPushOptions.Key)}");
            }
            if (options?.Values.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentException($"{nameof(ListRightPushOptions)}.{nameof(ListRightPushOptions.Values)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<ListLeftPushResponse>(server);
            }
            var values = new RedisValue[options.Values.Count + 3];
            var valueParameters = new string[options.Values.Count];
            for (var i = 0; i < options.Values.Count; i++)
            {
                values[i] = options.Values[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            string script = $@"local obv=redis.call('LPUSH',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(options.Values.Count - 2)}
return obv";
            var expire = RedisManager.GetExpiration(options.Expiration);
            values[values.Length - 3] = options.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = expire.Item2?.TotalSeconds ?? 0;//expire time seconds
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<ListLeftPushResponse> responses = new List<ListLeftPushResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, values, cmdFlags).ConfigureAwait(false);
                responses.Add(new ListLeftPushResponse()
                {
                    Success = true,
                    NewListLength = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region ListLeftPop

        /// <summary>
        /// Removes and returns the first element of the list stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">List left pop options</param>
        /// <returns>list left pop response</returns>
        public async Task<IEnumerable<ListLeftPopResponse>> ListLeftPopAsync(CacheServer server, ListLeftPopOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListLeftPopOptions)}.{nameof(ListLeftPopOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<ListLeftPopResponse>(server);
            }
            string script = $@"local pv=redis.call('LPOP',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<ListLeftPopResponse> responses = new List<ListLeftPopResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (string)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new ListLeftPopResponse()
                {
                    Success = true,
                    PopValue = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region ListInsertBefore

        /// <summary>
        /// Inserts value in the list stored at key either before or after the reference
        /// value pivot. When key does not exist, it is considered an empty list and no operation
        /// is performed.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">List insert before options</param>
        /// <returns>Return list insert begore response</returns>
        public async Task<IEnumerable<ListInsertBeforeResponse>> ListInsertBeforeAsync(CacheServer server, ListInsertBeforeOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListInsertBeforeOptions)}.{nameof(ListInsertBeforeOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<ListInsertBeforeResponse>(server);
            }
            string script = $@"local pv=redis.call('LINSERT',{Keys(1)},'BEFORE',{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.PivotValue,
                options.InsertValue,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<ListInsertBeforeResponse> responses = new List<ListInsertBeforeResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new ListInsertBeforeResponse()
                {
                    Success = result > 0,
                    NewListLength = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region ListInsertAfter

        /// <summary>
        /// Inserts value in the list stored at key either before or after the reference
        /// value pivot. When key does not exist, it is considered an empty list and no operation
        /// is performed.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">List insert after options</param>
        /// <returns>Return list insert after response</returns>
        public async Task<IEnumerable<ListInsertAfterResponse>> ListInsertAfterAsync(CacheServer server, ListInsertAfterOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListInsertAfterOptions)}.{nameof(ListInsertAfterOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<ListInsertAfterResponse>(server);
            }
            string script = $@"local pv=redis.call('LINSERT',{Keys(1)},'AFTER',{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.PivotValue,
                options.InsertValue,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<ListInsertAfterResponse> responses = new List<ListInsertAfterResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new ListInsertAfterResponse()
                {
                    Success = result > 0,
                    NewListLength = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region ListGetByIndex

        /// <summary>
        /// Returns the element at index index in the list stored at key. The index is zero-based,
        /// so 0 means the first element, 1 the second element and so on. Negative indices
        /// can be used to designate elements starting at the tail of the list. Here, -1
        /// means the last element, -2 means the penultimate and so forth.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">List get by index options</param>
        /// <returns>Return list get by index response</returns>
        public async Task<IEnumerable<ListGetByIndexResponse>> ListGetByIndexAsync(CacheServer server, ListGetByIndexOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListInsertAfterOptions)}.{nameof(ListInsertAfterOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<ListGetByIndexResponse>(server);
            }
            string script = $@"local pv=redis.call('LINDEX',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Index,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<ListGetByIndexResponse> responses = new List<ListGetByIndexResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (string)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new ListGetByIndexResponse()
                {
                    Success = true,
                    Value = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #endregion

        #region Hash

        #region HashValues

        /// <summary>
        /// Returns all values in the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Hash values options</param>
        /// <returns>Return hash values response</returns>
        public async Task<IEnumerable<HashValuesResponse>> HashValuesAsync(CacheServer server, HashValuesOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashValuesOptions)}.{nameof(HashValuesOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<HashValuesResponse>(server);
            }
            string script = $@"local pv=redis.call('HVALS',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<HashValuesResponse> responses = new List<HashValuesResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (RedisValue[])await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new HashValuesResponse()
                {
                    Success = true,
                    Values = result.Select(c => { dynamic value = c; return value; }).ToList(),
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region HashSet

        /// <summary>
        /// Sets field in the hash stored at key to value. If key does not exist, a new key
        ///  holding a hash is created. If field already exists in the hash, it is overwritten.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Hash set options</param>
        /// <returns>Return hash set response</returns>
        public async Task<IEnumerable<HashSetResponse>> HashSetAsync(CacheServer server, HashSetOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashSetOptions)}.{nameof(HashSetOptions.Key)}");
            }
            if (options?.Items.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(HashSetOptions)}.{nameof(HashSetOptions.Items)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<HashSetResponse>(server);
            }
            var valueCount = options.Items.Count * 2;
            var values = new RedisValue[valueCount + 3];
            var valueParameters = new string[valueCount];
            int valueIndex = 0;
            foreach (var valueItem in options.Items)
            {
                values[valueIndex] = valueItem.Key;
                values[valueIndex + 1] = valueItem.Value;
                valueParameters[valueIndex] = $"{Arg(valueIndex + 1)}";
                valueParameters[valueIndex + 1] = $"{Arg(valueIndex + 2)}";
                valueIndex += 2;
            }
            string script = $@"local obv=redis.call('HMSET',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(valueCount - 2)}
return obv['ok']";
            var expire = RedisManager.GetExpiration(options.Expiration);
            values[values.Length - 3] = options.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = expire.Item2?.TotalSeconds ?? 0;//expire time seconds
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<HashSetResponse> responses = new List<HashSetResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (string)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, values, cmdFlags).ConfigureAwait(false);
                responses.Add(new HashSetResponse()
                {
                    Success = string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase),
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region HashLength

        /// <summary>
        /// Returns the number of fields contained in the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Hash length options</param>
        /// <returns>Return hash length response</returns>
        public async Task<IEnumerable<HashLengthResponse>> HashLengthAsync(CacheServer server, HashLengthOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashLengthOptions)}.{nameof(HashLengthOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<HashLengthResponse>(server);
            }
            string script = $@"local pv=redis.call('HLEN',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<HashLengthResponse> responses = new List<HashLengthResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new HashLengthResponse()
                {
                    Success = true,
                    Length = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region HashKeys

        /// <summary>
        /// Returns all field names in the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Hash key options</param>
        /// <returns>Return hash keys response</returns>
        public async Task<IEnumerable<HashKeysResponse>> HashKeysAsync(CacheServer server, HashKeysOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashKeysOptions)}.{nameof(HashKeysOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<HashKeysResponse>(server);
            }
            string script = $@"local pv=redis.call('HKEYS',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<HashKeysResponse> responses = new List<HashKeysResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (RedisValue[])await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new HashKeysResponse()
                {
                    Success = true,
                    HashKeys = result.Select(c => { string key = c; return key; }).ToList(),
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region HashIncrement

        /// <summary>
        /// Increments the number stored at field in the hash stored at key by increment.
        /// If key does not exist, a new key holding a hash is created. If field does not
        /// exist or holds a string that cannot be interpreted as integer, the value is set
        /// to 0 before the operation is performed.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Hash increment options</param>
        /// <returns>Return hash increment response</returns>
        public async Task<IEnumerable<HashIncrementResponse>> HashIncrementAsync(CacheServer server, HashIncrementOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashIncrementOptions)}.{nameof(HashIncrementOptions.Key)}");
            }
            if (options?.IncrementValue == null)
            {
                throw new ArgumentNullException($"{nameof(HashIncrementOptions)}.{nameof(HashIncrementOptions.IncrementValue)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<HashIncrementResponse>(server);
            }
            var dataType = options.IncrementValue.GetType();
            var typeCode = Type.GetTypeCode(dataType);
            dynamic newValue = options.IncrementValue;
            var cacheKey = options.Key.GetActualKey();
            var keys = new RedisKey[1] { cacheKey };
            var expire = RedisManager.GetExpiration(options.Expiration);
            var values = new RedisValue[]
            {
                options.HashField,
                options.IncrementValue,
                options.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds
            };
            string script = "";
            bool integerValue = false;
            switch (typeCode)
            {
                case TypeCode.Boolean:
                case TypeCode.Byte:
                case TypeCode.Char:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    integerValue = true;
                    break;
            }
            script = @$"local obv=redis.call('{(integerValue ? "HINCRBY" : "HINCRBYFLOAT")}',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript(-2)}
return obv";
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<HashIncrementResponse> responses = new List<HashIncrementResponse>(databases.Count);
            foreach (var db in databases)
            {
                var newCacheValue = await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, values, cmdFlags).ConfigureAwait(false);
                if (integerValue)
                {
                    newValue = DataConverter.Convert((long)newCacheValue, dataType);
                }
                else
                {
                    newValue = DataConverter.Convert((double)newCacheValue, dataType);
                }
                responses.Add(new HashIncrementResponse()
                {
                    Success = true,
                    NewValue = newValue,
                    Key = cacheKey,
                    HashField = options.HashField,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region HashGet

        /// <summary>
        /// Returns the value associated with field in the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Hash get options</param>
        /// <returns>Return hash get response</returns>
        public async Task<IEnumerable<HashGetResponse>> HashGetAsync(CacheServer server, HashGetOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashGetOptions)}.{nameof(HashGetOptions.Key)}");
            }
            if (string.IsNullOrWhiteSpace(options?.HashField))
            {
                throw new ArgumentNullException($"{nameof(HashGetOptions)}.{nameof(HashGetOptions.HashField)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<HashGetResponse>(server);
            }
            string script = $@"local pv=redis.call('HGET',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var keys = new RedisKey[1]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[4]
            {
                options.HashField,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<HashGetResponse> responses = new List<HashGetResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (string)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new HashGetResponse()
                {
                    Success = true,
                    Value = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region HashGetAll

        /// <summary>
        /// Returns all fields and values of the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Hash get all options</param>
        /// <returns>Return hash get all response</returns>
        public async Task<IEnumerable<HashGetAllResponse>> HashGetAllAsync(CacheServer server, HashGetAllOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashGetAllOptions)}.{nameof(HashGetAllOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<HashGetAllResponse>(server);
            }
            string script = $@"local pv=redis.call('HGETALL',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[1]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[3]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<HashGetAllResponse> responses = new List<HashGetAllResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (RedisValue[])await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                Dictionary<string, dynamic> values = new Dictionary<string, dynamic>(result.Length / 2);
                for (var i = 0; i < result.Length; i += 2)
                {
                    values[result[i]] = result[i + 1];
                }
                responses.Add(new HashGetAllResponse()
                {
                    Success = true,
                    HashValues = values,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region HashExists

        /// <summary>
        /// Returns if field is an existing field in the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Options</param>
        /// <returns>hash exists response</returns>
        public async Task<IEnumerable<HashExistsResponse>> HashExistAsync(CacheServer server, HashExistsOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashExistsOptions)}.{nameof(HashExistsOptions.Key)}");
            }
            if (string.IsNullOrWhiteSpace(options?.HashField))
            {
                throw new ArgumentNullException($"{nameof(HashExistsOptions)}.{nameof(HashExistsOptions.HashField)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<HashExistsResponse>(server);
            }
            string script = $@"local pv=redis.call('HEXISTS',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var keys = new RedisKey[1]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[4]
            {
                options.HashField,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<HashExistsResponse> responses = new List<HashExistsResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (int)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new HashExistsResponse()
                {
                    Success = true,
                    HasField = result == 1,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region HashDelete

        /// <summary>
        /// Removes the specified fields from the hash stored at key. Non-existing fields
        /// are ignored. Non-existing keys are treated as empty hashes and this options returns 0
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Hash delete options</param>
        /// <returns>Return hash delete response</returns>
        public async Task<IEnumerable<HashDeleteResponse>> HashDeleteAsync(CacheServer server, HashDeleteOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashDeleteOptions)}.{nameof(HashDeleteOptions.Key)}");
            }
            if (options.HashFields.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(HashDeleteOptions)}.{nameof(HashDeleteOptions.HashFields)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<HashDeleteResponse>(server);
            }
            var values = new RedisValue[options.HashFields.Count + 3];
            var valueParameters = new string[options.HashFields.Count];
            for (var i = 0; i < options.HashFields.Count; i++)
            {
                values[i] = options.HashFields[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            values[values.Length - 3] = true;//refresh current time
            values[values.Length - 2] = RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = 0;//expire time seconds
            string script = $@"local pv=redis.call('HDEL',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(valueParameters.Length - 2)}
return pv";
            var keys = new RedisKey[1]
            {
                options.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<HashDeleteResponse> responses = new List<HashDeleteResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (int)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, values, cmdFlags).ConfigureAwait(false);
                responses.Add(new HashDeleteResponse()
                {
                    Success = result > 0,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region HashDecrement

        /// <summary>
        /// Decrement the specified field of an hash stored at key, and representing a floating
        ///  point number, by the specified decrement. If the field does not exist, it is
        ///  set to 0 before performing the operation.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Hash decrement options</param>
        /// <returns>Return hash decrement response</returns>
        public async Task<IEnumerable<HashDecrementResponse>> HashDecrementAsync(CacheServer server, HashDecrementOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashDecrementOptions)}.{nameof(HashDecrementOptions.Key)}");
            }
            if (options?.DecrementValue == null)
            {
                throw new ArgumentNullException($"{nameof(HashDecrementOptions)}.{nameof(HashDecrementOptions.DecrementValue)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<HashDecrementResponse>(server);
            }
            var dataType = options.DecrementValue.GetType();
            var typeCode = Type.GetTypeCode(dataType);
            dynamic newValue = options.DecrementValue;
            var cacheKey = options.Key.GetActualKey();
            var keys = new RedisKey[1] { cacheKey };
            var expire = RedisManager.GetExpiration(options.Expiration);
            var values = new RedisValue[]
            {
                options.HashField,
                -options.DecrementValue,
                options.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds
            };
            string script = "";
            bool integerValue = false;
            switch (typeCode)
            {
                case TypeCode.Boolean:
                case TypeCode.Byte:
                case TypeCode.Char:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    integerValue = true;
                    break;
            }
            script = @$"local obv=redis.call('{(integerValue ? "HINCRBY" : "HINCRBYFLOAT")}',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript(-2)}
return obv";
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<HashDecrementResponse> responses = new List<HashDecrementResponse>(databases.Count);
            foreach (var db in databases)
            {
                var newCacheValue = await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, values, cmdFlags).ConfigureAwait(false);
                if (integerValue)
                {
                    newValue = DataConverter.Convert((long)newCacheValue, dataType);
                }
                else
                {
                    newValue = DataConverter.Convert((double)newCacheValue, dataType);
                }
                responses.Add(new HashDecrementResponse()
                {
                    Success = true,
                    NewValue = newValue,
                    Key = cacheKey,
                    HashField = options.HashField,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region HashScan

        /// <summary>
        /// The HSCAN options is used to incrementally iterate over a hash
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Hash scan options</param>
        /// <returns>Return hash scan response</returns>
        public async Task<IEnumerable<HashScanResponse>> HashScanAsync(CacheServer server, HashScanOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashScanOptions)}.{nameof(HashScanOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<HashScanResponse>(server);
            }
            string script = $@"{GetRefreshExpirationScript(1)}
local obv={{}}
for i,v in pairs(redis.call('HSCAN',{Keys(1)},{Arg(1)},'MATCH',{Arg(2)},'COUNT',{Arg(3)})) do
    if i==2
    then
        local values={{}}
        for vi,vv in pairs(v) do
            values[vi]=vv
        end
        table.insert(obv,table.concat(values,','))
    else
        table.insert(obv,v)
    end
end
return obv";
            var keys = new RedisKey[1]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[6]
            {
                options.Cursor,
                RedisManager.GetMatchPattern(options.Pattern,options.PatternType),
                options.PageSize,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<HashScanResponse> responses = new List<HashScanResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (RedisValue[])await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                long newCursor = 0;
                Dictionary<string, dynamic> values = null;
                if (result.Length > 0)
                {
                    long.TryParse(result[0], out newCursor);
                }
                if (result.Length > 1)
                {
                    var valueArray = ((string)result[1]).LSplit(",", false);
                    values = new Dictionary<string, dynamic>(valueArray.Length / 2);
                    for (var i = 0; i < valueArray.Length; i += 2)
                    {
                        values[valueArray[i]] = valueArray[i + 1];
                    }
                }
                responses.Add(new HashScanResponse()
                {
                    Success = true,
                    Cursor = newCursor,
                    HashValues = values ?? new Dictionary<string, dynamic>(0),
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #endregion

        #region Set

        #region SetRemove

        /// <summary>
        /// Remove the specified member from the set stored at key. Specified members that
        /// are not a member of this set are ignored.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Set remove options</param>
        /// <returns>Return set remove response</returns>
        public async Task<IEnumerable<SetRemoveResponse>> SetRemoveAsync(CacheServer server, SetRemoveOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetRemoveOptions)}.{nameof(SetRemoveOptions.Key)}");
            }
            if (options.RemoveMembers.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SetRemoveOptions)}.{nameof(SetRemoveOptions.RemoveMembers)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SetRemoveResponse>(server);
            }
            var values = new RedisValue[options.RemoveMembers.Count + 3];
            var valueParameters = new string[options.RemoveMembers.Count];
            for (var i = 0; i < options.RemoveMembers.Count; i++)
            {
                values[i] = options.RemoveMembers[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            values[values.Length - 3] = true;//refresh current time
            values[values.Length - 2] = RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = 0;//expire time seconds
            string script = $@"local obv=redis.call('SREM',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(options.RemoveMembers.Count - 2)}
return obv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SetRemoveResponse> responses = new List<SetRemoveResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, values, cmdFlags).ConfigureAwait(false);
                responses.Add(new SetRemoveResponse()
                {
                    Success = true,
                    RemoveCount = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SetRandomMembers

        /// <summary>
        /// Return an array of count distinct elements if count is positive. If called with
        /// a negative count the behavior changes and the options is allowed to return the
        /// same element multiple times. In this case the numer of returned elements is the
        /// absolute value of the specified count.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Set random members options</param>
        /// <returns>Return set random members response</returns>
        public async Task<IEnumerable<SetRandomMembersResponse>> SetRandomMembersAsync(CacheServer server, SetRandomMembersOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetRandomMembersOptions)}.{nameof(SetRandomMembersOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SetRandomMembersResponse>(server);
            }
            string script = $@"local pv=redis.call('SRANDMEMBER',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var keys = new RedisKey[]
             {
                options.Key.GetActualKey()
             };
            var parameters = new RedisValue[]
            {
                options.Count,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SetRandomMembersResponse> responses = new List<SetRandomMembersResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (RedisValue[])await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SetRandomMembersResponse()
                {
                    Success = true,
                    Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SetRandomMember

        /// <summary>
        /// Return a random element from the set value stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Set random member options</param>
        /// <returns>Return set random member</returns>
        public async Task<IEnumerable<SetRandomMemberResponse>> SetRandomMemberAsync(CacheServer server, SetRandomMemberOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetRandomMemberOptions)}.{nameof(SetRandomMemberOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SetRandomMemberResponse>(server);
            }
            string script = $@"local pv=redis.call('SRANDMEMBER',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SetRandomMemberResponse> responses = new List<SetRandomMemberResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (string)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SetRandomMemberResponse()
                {
                    Success = true,
                    Member = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SetPop

        /// <summary>
        /// Removes and returns a random element from the set value stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Set pop options</param>
        /// <returns>Return set pop response</returns>
        public async Task<IEnumerable<SetPopResponse>> SetPopAsync(CacheServer server, SetPopOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetPopOptions)}.{nameof(SetPopOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SetPopResponse>(server);
            }
            string script = $@"local pv=redis.call('SPOP',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SetPopResponse> responses = new List<SetPopResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (string)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SetPopResponse()
                {
                    Success = true,
                    PopValue = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SetMove

        /// <summary>
        /// Move member from the set at source to the set at destination. This operation
        /// is atomic. In every given moment the element will appear to be a member of source
        /// or destination for other clients. When the specified element already exists in
        /// the destination set, it is only removed from the source set.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Set move options</param>
        /// <returns>Return set move response</returns>
        public async Task<IEnumerable<SetMoveResponse>> SetMoveAsync(CacheServer server, SetMoveOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.SourceKey))
            {
                throw new ArgumentNullException($"{nameof(SetMoveOptions)}.{nameof(SetMoveOptions.SourceKey)}");
            }
            if (string.IsNullOrWhiteSpace(options?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SetMoveOptions)}.{nameof(SetMoveOptions.DestinationKey)}");
            }
            if (string.IsNullOrEmpty(options?.MoveMember))
            {
                throw new ArgumentNullException($"{nameof(SetMoveOptions)}.{nameof(SetMoveOptions.MoveMember)}");
            }
            string script = $@"local pv=redis.call('SMOVE',{Keys(1)},{Keys(2)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
{GetRefreshExpirationScript(2, 1)}
return pv";
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SetMoveResponse>(server);
            }
            var allowSliding = RedisManager.AllowSlidingExpiration();
            var expire = RedisManager.GetExpiration(options.Expiration);
            var keys = new RedisKey[]
            {
                options.SourceKey.GetActualKey(),
                options.DestinationKey.GetActualKey(),
            };
            var parameters = new RedisValue[]
            {
                options.MoveMember,
                true,//refresh current time
                allowSliding,//whether allow set refresh time
                0,//expire time seconds
                options.Expiration==null,//refresh current time-destination key
                expire.Item1&&allowSliding,//whether allow set refresh time-destination key
                RedisManager.GetTotalSeconds(expire.Item2)//expire time seconds-destination key
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SetMoveResponse> responses = new List<SetMoveResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (string)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SetMoveResponse()
                {
                    Success = result == "1",
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SetMembers

        /// <summary>
        /// Returns all the members of the set value stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Set members options</param>
        /// <returns>Return set members response</returns>
        public async Task<IEnumerable<SetMembersResponse>> SetMembersAsync(CacheServer server, SetMembersOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetMembersOptions)}.{nameof(SetMembersOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SetMembersResponse>(server);
            }
            string script = $@"local pv=redis.call('SMEMBERS',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SetMembersResponse> responses = new List<SetMembersResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (RedisValue[])await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SetMembersResponse()
                {
                    Success = true,
                    Members = result?.Select(c => { string member = c; return member; }).ToList() ?? new List<string>(0),
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SetLength

        /// <summary>
        /// Returns the set cardinality (number of elements) of the set stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Set length options</param>
        /// <returns>Return set length response</returns>
        public async Task<IEnumerable<SetLengthResponse>> SetLengthAsync(CacheServer server, SetLengthOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetLengthOptions)}.{nameof(SetLengthOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SetLengthResponse>(server);
            }
            string script = $@"local pv=redis.call('SCARD',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SetLengthResponse> responses = new List<SetLengthResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SetLengthResponse()
                {
                    Success = true,
                    Length = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SetContains

        /// <summary>
        /// Returns if member is a member of the set stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Set contains options</param>
        /// <returns>Return set contains response</returns>
        public async Task<IEnumerable<SetContainsResponse>> SetContainsAsync(CacheServer server, SetContainsOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetContainsOptions)}.{nameof(SetContainsOptions.Key)}");
            }
            if (options.Member == null)
            {
                throw new ArgumentNullException($"{nameof(SetContainsOptions)}.{nameof(SetContainsOptions.Member)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SetContainsResponse>(server);
            }
            string script = $@"local pv=redis.call('SISMEMBER',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Member,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SetContainsResponse> responses = new List<SetContainsResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (string)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SetContainsResponse()
                {
                    Success = true,
                    ContainsValue = result == "1",
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SetCombine

        /// <summary>
        /// Returns the members of the set resulting from the specified operation against
        /// the given sets.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Set combine options</param>
        /// <returns>Return set combine response</returns>
        public async Task<IEnumerable<SetCombineResponse>> SetCombineAsync(CacheServer server, SetCombineOptions options)
        {
            if (options?.Keys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SetCombineOptions)}.{nameof(SetCombineOptions.Keys)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SetCombineResponse>(server);
            }
            var keys = new RedisKey[options.Keys.Count];
            var keyParameters = new List<string>(options.Keys.Count);
            for (var i = 0; i < options.Keys.Count; i++)
            {
                keys[i] = options.Keys[i].GetActualKey();
                keyParameters.Add($"{Keys(i + 1)}");
            }
            string script = $@"local pv=redis.call('{RedisManager.GetSetCombineCommand(options.CombineOperation)}',{string.Join(",", keyParameters)})
{GetRefreshExpirationScript(-2, keyCount: keys.Length)}
return pv";
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SetCombineResponse> responses = new List<SetCombineResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (RedisValue[])await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SetCombineResponse()
                {
                    Success = true,
                    CombineValues = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SetCombineAndStore

        /// <summary>
        /// This options is equal to SetCombine, but instead of returning the resulting set,
        ///  it is stored in destination. If destination already exists, it is overwritten.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Set combine and store options</param>
        /// <returns>Return set combine and store response</returns>
        public async Task<IEnumerable<SetCombineAndStoreResponse>> SetCombineAndStoreAsync(CacheServer server, SetCombineAndStoreOptions options)
        {
            if (options?.SourceKeys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SetCombineAndStoreOptions)}.{nameof(SetCombineAndStoreOptions.SourceKeys)}");
            }
            if (string.IsNullOrWhiteSpace(options.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SetCombineAndStoreOptions)}.{nameof(SetCombineAndStoreOptions.DestinationKey)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SetCombineAndStoreResponse>(server);
            }
            var keys = new RedisKey[options.SourceKeys.Count + 1];
            var keyParameters = new List<string>(options.SourceKeys.Count + 1);
            keys[0] = options.DestinationKey.GetActualKey();
            keyParameters.Add($"{Keys(1)}");
            for (var i = 0; i < options.SourceKeys.Count; i++)
            {
                keys[i + 1] = options.SourceKeys[i].GetActualKey();
                keyParameters.Add($"{Keys(i + 2)}");
            }
            string script = $@"local pv=redis.call('{RedisManager.GetSetCombineCommand(options.CombineOperation)}STORE',{string.Join(",", keyParameters)})
{GetRefreshExpirationScript(-2, 1, keyCount: keys.Length - 1)}
{GetRefreshExpirationScript(1)}
return pv";
            var expire = RedisManager.GetExpiration(options.Expiration);
            bool allowSliding = RedisManager.AllowSlidingExpiration();
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                allowSliding,//whether allow set refresh time
                0,//expire time seconds
                options.Expiration==null,// des key
                expire.Item1&&allowSliding,//des key
                RedisManager.GetTotalSeconds(expire.Item2)
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SetCombineAndStoreResponse> responses = new List<SetCombineAndStoreResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SetCombineAndStoreResponse()
                {
                    Success = true,
                    Count = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SetAdd

        /// <summary>
        /// Add the specified member to the set stored at key. Specified members that are
        /// already a member of this set are ignored. If key does not exist, a new set is
        /// created before adding the specified members.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Set add options</param>
        /// <returns>Return set add response</returns>
        public async Task<IEnumerable<SetAddResponse>> SetAddAsync(CacheServer server, SetAddOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetAddOptions)}.{nameof(SetAddOptions.Key)}");
            }
            if (options.Members.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SetAddOptions)}.{nameof(SetAddOptions.Members)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SetAddResponse>(server);
            }
            var expire = RedisManager.GetExpiration(options.Expiration);
            var values = new RedisValue[options.Members.Count + 3];
            var valueParameters = new string[options.Members.Count];
            for (var i = 0; i < options.Members.Count; i++)
            {
                values[i] = options.Members[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            values[values.Length - 3] = options.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = expire.Item2?.TotalSeconds ?? 0;//expire time seconds
            string script = $@"local obv=redis.call('SADD',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(options.Members.Count - 2)}
return obv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SetAddResponse> responses = new List<SetAddResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, values, cmdFlags).ConfigureAwait(false);
                responses.Add(new SetAddResponse()
                {
                    Success = result > 0,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #endregion

        #region Sorted set

        #region SortedSetScore

        /// <summary>
        /// Returns the score of member in the sorted set at key; If member does not exist
        /// in the sorted set, or key does not exist, nil is returned.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Sorted set score options</param>
        /// <returns>Return sorted set score response</returns>
        public async Task<IEnumerable<SortedSetScoreResponse>> SortedSetScoreAsync(CacheServer server, SortedSetScoreOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetScoreOptions)}.{nameof(SortedSetScoreOptions.Key)}");
            }
            if (options.Member == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetScoreOptions)}.{nameof(SortedSetScoreOptions.Member)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortedSetScoreResponse>(server);
            }
            string script = $@"local pv=redis.call('ZSCORE',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Member,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortedSetScoreResponse> responses = new List<SortedSetScoreResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (double?)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SortedSetScoreResponse()
                {
                    Success = true,
                    Score = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SortedSetRemoveRangeByValue

        /// <summary>
        /// When all the elements in a sorted set are inserted with the same score, in order
        /// to force lexicographical ordering, this options removes all elements in the sorted
        /// set stored at key between the lexicographical range specified by min and max.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Sorted set remove range by value options</param>
        /// <returns>Return sorted set remove range by value response</returns>
        public async Task<IEnumerable<SortedSetRemoveRangeByValueResponse>> SortedSetRemoveRangeByValueAsync(CacheServer server, SortedSetRemoveRangeByValueOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueOptions)}.{nameof(SortedSetRemoveRangeByValueOptions.Key)}");
            }
            if (options.MinValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueOptions)}.{nameof(SortedSetRemoveRangeByValueOptions.MinValue)}");
            }
            if (options.MaxValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueOptions)}.{nameof(SortedSetRemoveRangeByValueOptions.MaxValue)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortedSetRemoveRangeByValueResponse>(server);
            }
            string script = $@"local pv=redis.call('ZREMRANGEBYLEX',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                FormatSortedSetRangeBoundary(options.MinValue,true,options.Exclude),
                FormatSortedSetRangeBoundary(options.MaxValue,false,options.Exclude),
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortedSetRemoveRangeByValueResponse> responses = new List<SortedSetRemoveRangeByValueResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SortedSetRemoveRangeByValueResponse()
                {
                    RemoveCount = result,
                    Success = true,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SortedSetRemoveRangeByScore

        /// <summary>
        /// Removes all elements in the sorted set stored at key with a score between min
        ///  and max (inclusive by default).
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Sorted set remove range by score options</param>
        /// <returns>Return sorted set remove range by score response</returns>
        public async Task<IEnumerable<SortedSetRemoveRangeByScoreResponse>> SortedSetRemoveRangeByScoreAsync(CacheServer server, SortedSetRemoveRangeByScoreOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByScoreOptions)}.{nameof(SortedSetRemoveRangeByScoreOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortedSetRemoveRangeByScoreResponse>(server);
            }
            string script = $@"local pv=redis.call('ZREMRANGEBYSCORE',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                FormatSortedSetScoreRangeBoundary(options.Start,true,options.Exclude),
                FormatSortedSetScoreRangeBoundary(options.Stop,false,options.Exclude),
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortedSetRemoveRangeByScoreResponse> responses = new List<SortedSetRemoveRangeByScoreResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SortedSetRemoveRangeByScoreResponse()
                {
                    RemoveCount = result,
                    Success = true,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SortedSetRemoveRangeByRank

        /// <summary>
        /// Removes all elements in the sorted set stored at key with rank between start
        /// and stop. Both start and stop are 0 -based indexes with 0 being the element with
        /// the lowest score. These indexes can be negative numbers, where they indicate
        /// offsets starting at the element with the highest score. For example: -1 is the
        /// element with the highest score, -2 the element with the second highest score
        /// and so forth.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Sorted set remove range by rank options</param>
        /// <returns>Return sorted set remove range by rank response</returns>
        public async Task<IEnumerable<SortedSetRemoveRangeByRankResponse>> SortedSetRemoveRangeByRankAsync(CacheServer server, SortedSetRemoveRangeByRankOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByRankOptions)}.{nameof(SortedSetRemoveRangeByRankOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortedSetRemoveRangeByRankResponse>(server);
            }
            string script = $@"local pv=redis.call('ZREMRANGEBYRANK',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Start,
                options.Stop,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortedSetRemoveRangeByRankResponse> responses = new List<SortedSetRemoveRangeByRankResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SortedSetRemoveRangeByRankResponse()
                {
                    RemoveCount = result,
                    Success = true,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SortedSetRemove

        /// <summary>
        /// Removes the specified members from the sorted set stored at key. Non existing
        /// members are ignored.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Sorted set remove options</param>
        /// <returns>sorted set remove response</returns>
        public async Task<IEnumerable<SortedSetRemoveResponse>> SortedSetRemoveAsync(CacheServer server, SortedSetRemoveOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveOptions)}.{nameof(SortedSetRemoveOptions.Key)}");
            }
            if (options.RemoveMembers.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SortedSetRemoveOptions)}.{nameof(SortedSetRemoveOptions.RemoveMembers)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortedSetRemoveResponse>(server);
            }
            var values = new RedisValue[options.RemoveMembers.Count + 3];
            var valueParameters = new string[options.RemoveMembers.Count];
            for (var i = 0; i < options.RemoveMembers.Count; i++)
            {
                values[i] = options.RemoveMembers[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            values[values.Length - 3] = true;//refresh current time
            values[values.Length - 2] = RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = 0;//expire time seconds
            string script = $@"local obv=redis.call('ZREM',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(options.RemoveMembers.Count - 2)}
return obv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortedSetRemoveResponse> responses = new List<SortedSetRemoveResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, values, cmdFlags).ConfigureAwait(false);
                responses.Add(new SortedSetRemoveResponse()
                {
                    Success = true,
                    RemoveCount = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SortedSetRank

        /// <summary>
        /// Returns the rank of member in the sorted set stored at key, by default with the
        /// scores ordered from low to high. The rank (or index) is 0-based, which means
        /// that the member with the lowest score has rank 0.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>sorted set rank response</returns>
        public async Task<IEnumerable<SortedSetRankResponse>> SortedSetRankAsync(CacheServer server, SortedSetRankOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRankOptions)}.{nameof(SortedSetRankOptions.Key)}");
            }
            if (options.Member == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetRankOptions)}.{nameof(SortedSetRankOptions.Member)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortedSetRankResponse>(server);
            }
            string script = $@"local pv=redis.call('Z{(options.Order == CacheOrder.Descending ? "REV" : "")}RANK',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Member,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortedSetRankResponse> responses = new List<SortedSetRankResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long?)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SortedSetRankResponse()
                {
                    Success = true,
                    Rank = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SortedSetRangeByValue

        /// <summary>
        /// When all the elements in a sorted set are inserted with the same score, in order
        /// to force lexicographical ordering, this options returns all the elements in the
        /// sorted set at key with a value between min and max.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Sorted set range by value options</param>
        /// <returns>sorted set range by value response</returns>
        public async Task<IEnumerable<SortedSetRangeByValueResponse>> SortedSetRangeByValueAsync(CacheServer server, SortedSetRangeByValueOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueOptions)}.{nameof(SortedSetRemoveRangeByValueOptions.Key)}");
            }
            if (options.MinValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueOptions)}.{nameof(SortedSetRemoveRangeByValueOptions.MinValue)}");
            }
            if (options.MaxValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueOptions)}.{nameof(SortedSetRemoveRangeByValueOptions.MaxValue)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortedSetRangeByValueResponse>(server);
            }
            var command = "ZRANGEBYLEX";
            string beginValue = string.Empty;
            string endValue = string.Empty;
            if (options.Order == CacheOrder.Descending)
            {
                command = "ZREVRANGEBYLEX";
                beginValue = FormatSortedSetRangeBoundary(options.MaxValue, false, options.Exclude);
                endValue = FormatSortedSetRangeBoundary(options.MinValue, true, options.Exclude);
            }
            else
            {
                beginValue = FormatSortedSetRangeBoundary(options.MinValue, true, options.Exclude);
                endValue = FormatSortedSetRangeBoundary(options.MaxValue, false, options.Exclude);
            }
            string script = $@"local pv=redis.call('{command}',{Keys(1)},{Arg(1)},{Arg(2)},'LIMIT',{Arg(3)},{Arg(4)})
{GetRefreshExpirationScript(2)}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                beginValue,
                endValue,
                options.Offset,
                options.Count,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortedSetRangeByValueResponse> responses = new List<SortedSetRangeByValueResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (RedisValue[])await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SortedSetRangeByValueResponse()
                {
                    Success = true,
                    Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SortedSetRangeByScoreWithScores

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key. By default
        /// the elements are considered to be ordered from the lowest to the highest score.
        /// Lexicographical order is used for elements with equal score. Start and stop are
        /// used to specify the min and max range for score values. Similar to other range
        /// methods the values are inclusive.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Option</param>
        /// <returns>Return sorted set range by score with scores response</returns>
        public async Task<IEnumerable<SortedSetRangeByScoreWithScoresResponse>> SortedSetRangeByScoreWithScoresAsync(CacheServer server, SortedSetRangeByScoreWithScoresOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByScoreWithScoresOptions)}.{nameof(SortedSetRangeByScoreWithScoresOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortedSetRangeByScoreWithScoresResponse>(server);
            }
            var command = "ZRANGEBYSCORE";
            string beginValue = "";
            string endValue = "";
            if (options.Order == CacheOrder.Descending)
            {
                command = "ZREVRANGEBYSCORE";
                beginValue = FormatSortedSetScoreRangeBoundary(options.Stop, false, options.Exclude);
                endValue = FormatSortedSetScoreRangeBoundary(options.Start, true, options.Exclude);
            }
            else
            {
                beginValue = FormatSortedSetScoreRangeBoundary(options.Start, true, options.Exclude);
                endValue = FormatSortedSetScoreRangeBoundary(options.Stop, false, options.Exclude);
            }
            string script = $@"local pv=redis.call('{command}',{Keys(1)},{Arg(1)},{Arg(2)},'WITHSCORES','LIMIT',{Arg(3)},{Arg(4)})
{GetRefreshExpirationScript(2)}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                beginValue,
                endValue,
                options.Offset,
                options.Count,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortedSetRangeByScoreWithScoresResponse> responses = new List<SortedSetRangeByScoreWithScoresResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (RedisValue[])await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                List<SortedSetMember> members = new List<SortedSetMember>(result?.Length / 2 ?? 0);
                for (var i = 0; i < result.Length; i += 2)
                {
                    var value = result[i];
                    double.TryParse(result[i + 1], out var score);
                    members.Add(new SortedSetMember
                    {
                        Value = value,
                        Score = score
                    });
                }
                responses.Add(new SortedSetRangeByScoreWithScoresResponse()
                {
                    Success = true,
                    Members = members,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SortedSetRangeByScore

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key. By default
        /// the elements are considered to be ordered from the lowest to the highest score.
        /// Lexicographical order is used for elements with equal score. Start and stop are
        /// used to specify the min and max range for score values. Similar to other range
        /// methods the values are inclusive.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>sorted set range by score response</returns>
        public async Task<IEnumerable<SortedSetRangeByScoreResponse>> SortedSetRangeByScoreAsync(CacheServer server, SortedSetRangeByScoreOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByScoreOptions)}.{nameof(SortedSetRangeByScoreOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortedSetRangeByScoreResponse>(server);
            }
            var command = "ZRANGEBYSCORE";
            string beginValue = "";
            string endValue = "";
            if (options.Order == CacheOrder.Descending)
            {
                command = "ZREVRANGEBYSCORE";
                beginValue = FormatSortedSetScoreRangeBoundary(options.Stop, false, options.Exclude);
                endValue = FormatSortedSetScoreRangeBoundary(options.Start, true, options.Exclude);
            }
            else
            {
                beginValue = FormatSortedSetScoreRangeBoundary(options.Start, true, options.Exclude);
                endValue = FormatSortedSetScoreRangeBoundary(options.Stop, false, options.Exclude);
            }
            string script = $@"local pv=redis.call('{command}',{Keys(1)},{Arg(1)},{Arg(2)},'LIMIT',{Arg(3)},{Arg(4)})
{GetRefreshExpirationScript(2)}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                beginValue,
                endValue,
                options.Offset,
                options.Count,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortedSetRangeByScoreResponse> responses = new List<SortedSetRangeByScoreResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (RedisValue[])await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SortedSetRangeByScoreResponse()
                {
                    Success = true,
                    Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SortedSetRangeByRankWithScores

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key. By default
        /// the elements are considered to be ordered from the lowest to the highest score.
        /// Lexicographical order is used for elements with equal score. Both start and stop
        /// are zero-based indexes, where 0 is the first element, 1 is the next element and
        /// so on. They can also be negative numbers indicating offsets from the end of the
        /// sorted set, with -1 being the last element of the sorted set, -2 the penultimate
        /// element and so on.
        /// </summary>
        /// <param name="server">Cacheserver</param>
        /// <param name="options">Option</param>
        /// <returns>Return sorted set range by rank with scores response</returns>
        public async Task<IEnumerable<SortedSetRangeByRankWithScoresResponse>> SortedSetRangeByRankWithScoresAsync(CacheServer server, SortedSetRangeByRankWithScoresOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByRankWithScoresOptions)}.{nameof(SortedSetRangeByRankWithScoresOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortedSetRangeByRankWithScoresResponse>(server);
            }
            string script = $@"local pv=redis.call('Z{(options.Order == CacheOrder.Descending ? "REV" : "")}RANGE',{Keys(1)},{Arg(1)},{Arg(2)},'WITHSCORES')
{GetRefreshExpirationScript()}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Start,
                options.Stop,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortedSetRangeByRankWithScoresResponse> responses = new List<SortedSetRangeByRankWithScoresResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (RedisValue[])await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                List<SortedSetMember> members = new List<SortedSetMember>(result?.Length / 2 ?? 0);
                for (var i = 0; i < result.Length; i += 2)
                {
                    var value = result[i];
                    double.TryParse(result[i + 1], out var score);
                    members.Add(new SortedSetMember
                    {
                        Value = value,
                        Score = score
                    });
                }
                responses.Add(new SortedSetRangeByRankWithScoresResponse()
                {
                    Success = true,
                    Members = members,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SortedSetRangeByRank

        /// <summary>
        /// Returns the specified range of elements in the sorted set stored at key. By default
        /// the elements are considered to be ordered from the lowest to the highest score.
        /// Lexicographical order is used for elements with equal score. Both start and stop
        /// are zero-based indexes, where 0 is the first element, 1 is the next element and
        /// so on. They can also be negative numbers indicating offsets from the end of the
        /// sorted set, with -1 being the last element of the sorted set, -2 the penultimate
        /// element and so on.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>sorted set range by rank response</returns>
        public async Task<IEnumerable<SortedSetRangeByRankResponse>> SortedSetRangeByRankAsync(CacheServer server, SortedSetRangeByRankOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByRankOptions)}.{nameof(SortedSetRangeByRankOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortedSetRangeByRankResponse>(server);
            }
            string script = $@"local pv=redis.call('Z{(options.Order == CacheOrder.Descending ? "REV" : "")}RANGE',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Start,
                options.Stop,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortedSetRangeByRankResponse> responses = new List<SortedSetRangeByRankResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (RedisValue[])await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SortedSetRangeByRankResponse()
                {
                    Success = true,
                    Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SortedSetLengthByValue

        /// <summary>
        /// When all the elements in a sorted set are inserted with the same score, in order
        /// to force lexicographical ordering, this options returns the number of elements
        /// in the sorted set at key with a value between min and max.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Option</param>
        /// <returns>Return sorted set lenght by value response</returns>
        public async Task<IEnumerable<SortedSetLengthByValueResponse>> SortedSetLengthByValueAsync(CacheServer server, SortedSetLengthByValueOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetLengthByValueOptions)}.{nameof(SortedSetLengthByValueOptions.Key)}");
            }
            if (options.MinValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetLengthByValueOptions)}.{nameof(SortedSetLengthByValueOptions.MinValue)}");
            }
            if (options.MaxValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetLengthByValueOptions)}.{nameof(SortedSetLengthByValueOptions.MaxValue)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortedSetLengthByValueResponse>(server);
            }
            string script = $@"local pv=redis.call('ZLEXCOUNT',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                $"[{options.MinValue}",
                $"[{options.MaxValue}",
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortedSetLengthByValueResponse> responses = new List<SortedSetLengthByValueResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SortedSetLengthByValueResponse()
                {
                    Success = true,
                    Length = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SortedSetLength

        /// <summary>
        /// Returns the sorted set cardinality (number of elements) of the sorted set stored
        /// at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Option</param>
        /// <returns>Return sorted set length response</returns>
        public async Task<IEnumerable<SortedSetLengthResponse>> SortedSetLengthAsync(CacheServer server, SortedSetLengthOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetLengthByValueOptions)}.{nameof(SortedSetLengthByValueOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortedSetLengthResponse>(server);
            }
            string script = $@"local pv=redis.call('ZCARD',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortedSetLengthResponse> responses = new List<SortedSetLengthResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SortedSetLengthResponse()
                {
                    Success = true,
                    Length = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SortedSetIncrement

        /// <summary>
        /// Increments the score of member in the sorted set stored at key by increment.
        /// If member does not exist in the sorted set, it is added with increment as its
        /// score (as if its previous score was 0.0).
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Option</param>
        /// <returns>Return sorted set increment response</returns>
        public async Task<IEnumerable<SortedSetIncrementResponse>> SortedSetIncrementAsync(CacheServer server, SortedSetIncrementOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetIncrementOptions)}.{nameof(SortedSetIncrementOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortedSetIncrementResponse>(server);
            }
            var expire = RedisManager.GetExpiration(options.Expiration);
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
             {
                options.IncrementScore,
                options.Member,
                options.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

             };
            string script = $@"local pv=redis.call('ZINCRBY',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortedSetIncrementResponse> responses = new List<SortedSetIncrementResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (double)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SortedSetIncrementResponse()
                {
                    Success = true,
                    NewScore = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SortedSetDecrement

        /// <summary>
        /// Decrements the score of member in the sorted set stored at key by decrement.
        /// If member does not exist in the sorted set, it is added with -decrement as its
        /// score (as if its previous score was 0.0).
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="options">Option</param>
        /// <returns>Return sorted set decrement response</returns>
        public async Task<IEnumerable<SortedSetDecrementResponse>> SortedSetDecrementAsync(CacheServer server, SortedSetDecrementOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetDecrementOptions)}.{nameof(SortedSetDecrementOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortedSetDecrementResponse>(server);
            }
            string script = $@"local pv=redis.call('ZINCRBY',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var expire = RedisManager.GetExpiration(options.Expiration);
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                -options.DecrementScore,
                options.Member,
                options.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortedSetDecrementResponse> responses = new List<SortedSetDecrementResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (double)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SortedSetDecrementResponse()
                {
                    Success = true,
                    NewScore = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SortedSetCombineAndStore

        /// <summary>
        /// Computes a set operation over multiple sorted sets (optionally using per-set
        /// weights), and stores the result in destination, optionally performing a specific
        /// aggregation (defaults to sum)
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>sorted set combine and store response</returns>
        public async Task<IEnumerable<SortedSetCombineAndStoreResponse>> SortedSetCombineAndStoreAsync(CacheServer server, SortedSetCombineAndStoreOptions options)
        {
            if (options?.SourceKeys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SortedSetCombineAndStoreOptions)}.{nameof(SortedSetCombineAndStoreOptions.SourceKeys)}");
            }
            if (string.IsNullOrWhiteSpace(options.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SortedSetCombineAndStoreOptions)}.{nameof(SortedSetCombineAndStoreOptions.DestinationKey)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortedSetCombineAndStoreResponse>(server);
            }
            var keys = new RedisKey[options.SourceKeys.Count + 1];
            var keyParameters = new List<string>(options.SourceKeys.Count);
            double[] weights = new double[options.SourceKeys.Count];
            keys[0] = options.DestinationKey.GetActualKey();
            for (var i = 0; i < options.SourceKeys.Count; i++)
            {
                keys[i + 1] = options.SourceKeys[i].GetActualKey();
                keyParameters.Add($"{Keys(i + 2)}");
                weights[i] = options.Weights?.ElementAt(i) ?? 1;
            }
            StringBuilder optionScript = new StringBuilder();
            string script = $@"local pv=redis.call('{RedisManager.GetSortedSetCombineCommand(options.CombineOperation)}',{Keys(1)},'{keyParameters.Count}',{string.Join(",", keyParameters)},'WEIGHTS',{string.Join(",", weights)},'AGGREGATE','{RedisManager.GetSortedSetAggregateName(options.Aggregate)}')
{GetRefreshExpirationScript(1)}
{GetRefreshExpirationScript(-2, 1, keyCount: keys.Length - 1)}
return pv";
            var expire = RedisManager.GetExpiration(options.Expiration);
            bool allowSliding = RedisManager.AllowSlidingExpiration();
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                allowSliding,//whether allow set refresh time
                0,//expire time seconds
                options.Expiration==null,// des key
                expire.Item1&&allowSliding,//des key
                RedisManager.GetTotalSeconds(expire.Item2)
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortedSetCombineAndStoreResponse> responses = new List<SortedSetCombineAndStoreResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SortedSetCombineAndStoreResponse()
                {
                    Success = true,
                    NewSetLength = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SortedSetAdd

        /// <summary>
        /// Adds all the specified members with the specified scores to the sorted set stored
        /// at key. If a specified member is already a member of the sorted set, the score
        /// is updated and the element reinserted at the right position to ensure the correct
        /// ordering.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>sorted set add response</returns>
        public async Task<IEnumerable<SortedSetAddResponse>> SortedSetAddAsync(CacheServer server, SortedSetAddOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetAddOptions)}.{nameof(SortedSetAddOptions.Key)}");
            }
            if (options.Members.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SortedSetAddOptions)}.{nameof(SortedSetAddOptions.Members)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortedSetAddResponse>(server);
            }
            var expire = RedisManager.GetExpiration(options.Expiration);
            var valueCount = options.Members.Count * 2;
            var values = new RedisValue[valueCount + 3];
            var valueParameters = new string[valueCount];
            for (var i = 0; i < options.Members.Count; i++)
            {
                var member = options.Members[i];
                var argIndex = i * 2;
                values[argIndex] = member?.Score;
                values[argIndex + 1] = member?.Value;
                valueParameters[argIndex] = $"{Arg(argIndex + 1)}";
                valueParameters[argIndex + 1] = $"{Arg(argIndex + 2)}";
            }
            values[values.Length - 3] = options.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = expire.Item2?.TotalSeconds ?? 0;//expire time seconds
            string script = $@"local obv=redis.call('ZADD',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(valueCount - 2)}
return obv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortedSetAddResponse> responses = new List<SortedSetAddResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, values, cmdFlags).ConfigureAwait(false);
                responses.Add(new SortedSetAddResponse()
                {
                    Success = true,
                    Length = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #endregion

        #region Sort

        #region Sort

        /// <summary>
        /// Sorts a list, set or sorted set (numerically or alphabetically, ascending by
        /// default){await Task.Delay(100);return null;} By default, the elements themselves are compared, but the values can
        /// also be used to perform external key-lookups using the by parameter. By default,
        /// the elements themselves are returned, but external key-lookups (one or many)
        /// can be performed instead by specifying the get parameter (note that # specifies
        /// the element itself, when used in get). Referring to the redis SORT documentation
        /// for examples is recommended. When used in hashes, by and get can be used to specify
        /// fields using -> notation (again, refer to redis documentation).
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>sort response</returns>
        public async Task<IEnumerable<SortResponse>> SortAsync(CacheServer server, SortOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortOptions)}.{nameof(SortOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortResponse>(server);
            }
            string script = $@"local obv=redis.call('SORT',{Keys(1)}{(string.IsNullOrWhiteSpace(options.By) ? string.Empty : $",'BY','{options.By}'")},'LIMIT',{Arg(1)},{Arg(2)}{(options.Gets.IsNullOrEmpty() ? string.Empty : $",{string.Join(",", options.Gets.Select(c => $"'GET','{c}'"))}")},{(options.Order == CacheOrder.Descending ? "'DESC'" : "'ASC'")}{(options.SortType == CacheSortType.Alphabetic ? ",'ALPHA'" : string.Empty)})
{GetRefreshExpirationScript()}
return obv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Offset,
                options.Count,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortResponse> responses = new List<SortResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (RedisValue[])await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SortResponse()
                {
                    Success = true,
                    Values = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region SortAndStore

        /// <summary>
        /// Sorts a list, set or sorted set (numerically or alphabetically, ascending by
        /// default){await Task.Delay(100);return null;} By default, the elements themselves are compared, but the values can
        /// also be used to perform external key-lookups using the by parameter. By default,
        /// the elements themselves are returned, but external key-lookups (one or many)
        /// can be performed instead by specifying the get parameter (note that # specifies
        /// the element itself, when used in get). Referring to the redis SORT documentation
        /// for examples is recommended. When used in hashes, by and get can be used to specify
        /// fields using -> notation (again, refer to redis documentation).
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>sort and store response</returns>
        public async Task<IEnumerable<SortAndStoreResponse>> SortAndStoreAsync(CacheServer server, SortAndStoreOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.SourceKey))
            {
                throw new ArgumentNullException($"{nameof(SortAndStoreOptions)}.{nameof(SortAndStoreOptions.SourceKey)}");
            }
            if (string.IsNullOrWhiteSpace(options?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SortAndStoreOptions)}.{nameof(SortAndStoreOptions.DestinationKey)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<SortAndStoreResponse>(server);
            }
            string script = $@"local obv=redis.call('SORT',{Keys(1)}{(string.IsNullOrWhiteSpace(options.By) ? string.Empty : $",'BY','{options.By}'")},'LIMIT',{Arg(1)},{Arg(2)}{(options.Gets.IsNullOrEmpty() ? string.Empty : $",{string.Join(",", options.Gets.Select(c => $"'GET','{c}'"))}")},{(options.Order == CacheOrder.Descending ? "'DESC'" : "'ASC'")}{(options.SortType == CacheSortType.Alphabetic ? ",'ALPHA'" : string.Empty)},'STORE',{Keys(2)})
{GetRefreshExpirationScript()}
{GetRefreshExpirationScript(3, 1)}
return obv";
            var expire = RedisManager.GetExpiration(options.Expiration);
            bool allowSliding = RedisManager.AllowSlidingExpiration();
            var keys = new RedisKey[]
            {
                options.SourceKey.GetActualKey(),
                options.DestinationKey.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Offset,
                options.Count,
                true,//refresh current time
                allowSliding,//whether allow set refresh time
                0,//expire time seconds
                options.Expiration==null,//refresh current time-des key
                expire.Item1&&allowSliding,//allow set refresh time-deskey
                RedisManager.GetTotalSeconds(expire.Item2)//-deskey
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<SortAndStoreResponse> responses = new List<SortAndStoreResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new SortAndStoreResponse()
                {
                    Success = true,
                    Length = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #endregion

        #region Key

        #region KeyType

        /// <summary>
        /// Returns the string representation of the type of the value stored at key. The
        /// different types that can be returned are: string, list, set, zset and hash.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>key type response</returns>
        public async Task<IEnumerable<TypeResponse>> KeyTypeAsync(CacheServer server, TypeOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(TypeOptions)}.{nameof(TypeOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<TypeResponse>(server);
            }
            string script = $@"local obv=redis.call('TYPE',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return obv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<TypeResponse> responses = new List<TypeResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (string)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new TypeResponse()
                {
                    Success = true,
                    KeyType = RedisManager.GetCacheKeyType(result),
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region KeyTimeToLive

        /// <summary>
        /// Returns the remaining time to live of a key that has a timeout. This introspection
        /// capability allows a Redis client to check how many seconds a given key will continue
        /// to be part of the dataset.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>key time to live response</returns>
        public async Task<IEnumerable<TimeToLiveResponse>> KeyTimeToLiveAsync(CacheServer server, TimeToLiveOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(TimeToLiveOptions)}.{nameof(TimeToLiveOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<TimeToLiveResponse>(server);
            }
            string script = $@"local obv=redis.call('TTL',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return obv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<TimeToLiveResponse> responses = new List<TimeToLiveResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new TimeToLiveResponse()
                {
                    Success = true,
                    TimeToLiveSeconds = result,
                    KeyExist = result != -2,
                    Perpetual = result == -1,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region KeyRestore

        /// <summary>
        /// Create a key associated with a value that is obtained by deserializing the provided
        /// serialized value (obtained via DUMP). If ttl is 0 the key is created without
        /// any expire, otherwise the specified expire time(in milliseconds) is set.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>key restore response</returns>
        public async Task<IEnumerable<RestoreResponse>> KeyRestoreAsync(CacheServer server, RestoreOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(RestoreOptions)}.{nameof(RestoreOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<RestoreResponse>(server);
            }
            string script = $@"local obv= string.lower(tostring(redis.call('RESTORE',{Keys(1)},'0',{Arg(1)})))=='ok'
{GetRefreshExpirationScript(-1)}
return obv";
            var expire = RedisManager.GetExpiration(options.Expiration);
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                options.Value,
                options.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<RestoreResponse> responses = new List<RestoreResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (bool)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new RestoreResponse()
                {
                    Success = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region KeyRename

        /// <summary>
        /// Renames key to newkey. It returns an error when the source and destination names
        /// are the same, or when key does not exist.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>key rename response</returns>
        public async Task<IEnumerable<RenameResponse>> KeyRenameAsync(CacheServer server, RenameOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(RenameOptions)}.{nameof(RenameOptions.Key)}");
            }
            if (string.IsNullOrWhiteSpace(options?.NewKey))
            {
                throw new ArgumentNullException($"{nameof(RenameOptions)}.{nameof(RenameOptions.NewKey)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<RenameResponse>(server);
            }
            string cacheKey = options.Key.GetActualKey();
            string newCacheKey = options.NewKey.GetActualKey();
            string script = $@"{GetRefreshExpirationScript(-2)}
local obv=string.lower(tostring(redis.call('{(options.WhenNewKeyNotExists ? "RENAMENX" : "RENAME")}',{Keys(1)},{Keys(2)})))
if obv=='ok' or obv=='1'
then
    redis.call('RENAME','{GetExpirationKey(cacheKey)}','{GetExpirationKey(newCacheKey)}')
    return true
end
return false";
            var keys = new RedisKey[]
            {
                cacheKey,
                newCacheKey
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<RenameResponse> responses = new List<RenameResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (bool)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new RenameResponse()
                {
                    Success = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region KeyRandom

        /// <summary>
        /// Return a random key from the currently selected database.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>key random response</returns>
        public async Task<IEnumerable<RandomResponse>> KeyRandomAsync(CacheServer server, RandomOptions options)
        {
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<RandomResponse>(server);
            }
            string script = $@"local obv=redis.call('RANDOMKEY')
if obv
then
    local exkey=obv..'{RedisManager.ExpirationKeySuffix}' 
    local ct=redis.call('GET',exkey)
    if ct 
    then
        local rs=redis.call('EXPIRE',ckey,ct)
        if rs 
        then
            redis.call('SET',exkey,ct,'EX',ct)
        end
    end
end
return obv";
            var keys = new RedisKey[0];
            var parameters = new RedisValue[0];
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<RandomResponse> responses = new List<RandomResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (string)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new RandomResponse()
                {
                    Success = true,
                    Key = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region KeyPersist

        /// <summary>
        /// Remove the existing timeout on key, turning the key from volatile (a key with
        /// an expire set) to persistent (a key that will never expire as no timeout is associated).
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>key persist response</returns>
        public async Task<IEnumerable<PersistResponse>> KeyPersistAsync(CacheServer server, PersistOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(PersistOptions)}.{nameof(PersistOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<PersistResponse>(server);
            }
            var cacheKey = options.Key.GetActualKey();
            var keys = new RedisKey[1] { cacheKey };
            var parameters = new RedisValue[0];
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<PersistResponse> responses = new List<PersistResponse>(databases.Count);
            string script = $@"local obv=redis.call('PERSIST',{Keys(1)})==1
if obv
then
    redis.call('DEL','{GetExpirationKey(cacheKey)}')
end
return obv";
            foreach (var db in databases)
            {
                var result = (bool)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new PersistResponse()
                {
                    Success = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region KeyMove

        /// <summary>
        /// Move key from the currently selected database (see SELECT) to the specified destination
        /// database. When key already exists in the destination database, or it does not
        /// exist in the source database, it does nothing. It is possible to use MOVE as
        /// a locking primitive because of this.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>key move response</returns>
        public async Task<IEnumerable<MoveResponse>> KeyMoveAsync(CacheServer server, MoveOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(MoveOptions)}.{nameof(MoveOptions.Key)}");
            }
            if (!int.TryParse(options.DatabaseName, out var dbIndex) || dbIndex < 0)
            {
                throw new ArgumentException($"{nameof(MoveOptions)}.{nameof(MoveOptions.DatabaseName)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<MoveResponse>(server);
            }
            var cacheKey = options.Key.GetActualKey();
            var keys = new RedisKey[1] { cacheKey };
            var parameters = new RedisValue[1]
            {
                options.DatabaseName
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            string script = $@"local obv=redis.call('MOVE',{Keys(1)},{Arg(1)})==1
local exkey='{GetExpirationKey(cacheKey)}' 
local ct=redis.call('GET',exkey)
if ct 
then
    local rs=redis.call('EXPIRE','{cacheKey}',ct)
    if rs 
    then
        redis.call('SET',exkey,ct,'EX',ct)
    end
    if obv
    then
        redis.call('SELECT','{options.DatabaseName}')
        redis.call('EXPIRE','{cacheKey}',ct)
        redis.call('SET',exkey,ct,'EX',ct)
    end
end
return obv";
            List<MoveResponse> responses = new List<MoveResponse>(databases.Count);
            foreach (var db in databases)
            {
                var result = (bool)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new MoveResponse()
                {
                    Success = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region KeyMigrate

        /// <summary>
        /// Atomically transfer a key from a source Redis instance to a destination Redis
        /// instance. On success the key is deleted from the original instance by default,
        /// and is guaranteed to exist in the target instance.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>key migrate response</returns>
        public async Task<IEnumerable<MigrateResponse>> KeyMigrateAsync(CacheServer server, Keys.MigrateOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(Cache.Keys.MigrateOptions)}.{nameof(Cache.Keys.MigrateOptions.Key)}");
            }
            if (options.Destination == null)
            {
                throw new ArgumentNullException($"{nameof(Cache.Keys.MigrateOptions)}.{nameof(Cache.Keys.MigrateOptions.Destination)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<MigrateResponse>(server);
            }
            var cacheKey = options.Key.GetActualKey();
            var keys = new RedisKey[1] { cacheKey };
            var parameters = new RedisValue[1]
            {
                options.CopyCurrent
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<MigrateResponse> responses = new List<MigrateResponse>(databases.Count);
            string script = $@"local obv=string.lower(tostring(redis.call('MIGRATE','{options.Destination.Port}','{options.Destination.Host}',{Keys(1)},'{options.TimeOutMilliseconds}'{(options.CopyCurrent ? ",'COPY'" : string.Empty)}{(options.ReplaceDestination ? ",'REPLACE'" : string.Empty)})))
if {Arg(1)}=='1'
then
    local exkey='{GetExpirationKey(cacheKey)}' 
    local ct=redis.call('GET',exkey)
    if ct
    then
        local rs=redis.call('EXPIRE','{cacheKey}',ct)
        if rs 
        then
            redis.call('SET',exkey,ct,'EX',ct)
        end
    end
end
return obv";
            foreach (var db in databases)
            {
                var result = (bool)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new MigrateResponse()
                {
                    Success = true,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region KeyExpire

        /// <summary>
        /// Set a timeout on key. After the timeout has expired, the key will automatically
        /// be deleted. A key with an associated timeout is said to be volatile in Redis
        /// terminology.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>key expire response</returns>
        public async Task<IEnumerable<ExpireResponse>> KeyExpireAsync(CacheServer server, ExpireOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ExpireOptions)}.{nameof(ExpireOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<ExpireResponse>(server);
            }
            var cacheKey = options.Key.GetActualKey();
            var expire = RedisManager.GetExpiration(options.Expiration);
            var seconds = expire.Item2?.TotalSeconds ?? 0;
            var keys = new RedisKey[0];
            var parameters = new RedisValue[0];
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<ExpireResponse> responses = new List<ExpireResponse>(databases.Count);
            string script = $@"local rs=redis.call('EXPIRE','{cacheKey}','{seconds}')==1
if rs and '{(expire.Item1 && RedisManager.AllowSlidingExpiration() ? "1" : "0")}'=='1'
then
    redis.call('SET','{GetExpirationKey(cacheKey)}','{seconds}','EX','{seconds}')
end
return rs";
            foreach (var db in databases)
            {
                var result = (bool)await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new ExpireResponse()
                {
                    Success = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion;

        #region KeyDump

        /// <summary>
        /// Serialize the value stored at key in a Redis-specific format and return it to
        /// the user. The returned value can be synthesized back into a Redis key using the
        /// RESTORE options.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>key dump response</returns>
        public async Task<IEnumerable<DumpResponse>> KeyDumpAsync(CacheServer server, DumpOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(DumpOptions)}.{nameof(DumpOptions.Key)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<DumpResponse>(server);
            }
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<DumpResponse> responses = new List<DumpResponse>(databases.Count);
            string script = $@"local pv=redis.call('DUMP',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            foreach (var db in databases)
            {
                var result = (byte[])await db.RemoteDatabase.ScriptEvaluateAsync(script, keys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new DumpResponse()
                {
                    Success = true,
                    ByteValues = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region KeyDelete

        /// <summary>
        /// Removes the specified keys. A key is ignored if it does not exist.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>key delete response</returns>
        public async Task<IEnumerable<DeleteResponse>> KeyDeleteAsync(CacheServer server, DeleteOptions options)
        {
            if (options?.Keys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(DeleteOptions)}.{nameof(DeleteOptions.Keys)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<DeleteResponse>(server);
            }
            var keys = options.Keys.Select(c => { RedisKey key = c.GetActualKey(); return key; }).ToArray();
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<DeleteResponse> responses = new List<DeleteResponse>(databases.Count);
            foreach (var db in databases)
            {
                var count = await db.RemoteDatabase.KeyDeleteAsync(keys, cmdFlags).ConfigureAwait(false);
                responses.Add(new DeleteResponse()
                {
                    Success = true,
                    DeleteCount = count,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #region KeyExist

        /// <summary>
        /// key exist
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns></returns>
        public async Task<IEnumerable<ExistResponse>> KeyExistAsync(CacheServer server, ExistOptions options)
        {
            if (options.Keys.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(ExistOptions)}.{nameof(ExistOptions.Keys)}");
            }
            var databases = RedisManager.GetDatabases(server);
            if (databases.IsNullOrEmpty())
            {
                return GetNoDatabaseResponse<ExistResponse>(server);
            }
            var redisKeys = new RedisKey[options.Keys.Count];
            var redisKeyParameters = new List<string>(options.Keys.Count);
            for (var i = 0; i < options.Keys.Count; i++)
            {
                redisKeys[i] = options.Keys[i].GetActualKey();
                redisKeyParameters.Add($"{Keys(i + 1)}");
            }
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            List<ExistResponse> responses = new List<ExistResponse>(databases.Count);
            string script = $@"local pv=redis.call('EXISTS',{string.Join(",", redisKeyParameters)})
{GetRefreshExpirationScript(-2)}
return pv";
            foreach (var db in databases)
            {
                var result = (long)await db.RemoteDatabase.ScriptEvaluateAsync(script, redisKeys, parameters, cmdFlags).ConfigureAwait(false);
                responses.Add(new ExistResponse()
                {
                    Success = true,
                    KeyCount = result,
                    CacheServer = server,
                    Database = db
                });
            }
            return responses;
        }

        #endregion

        #endregion

        #region Server

        #region Get all data base

        /// <summary>
        /// Get all database
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>Return get all database response</returns>
        public async Task<IEnumerable<GetAllDataBaseResponse>> GetAllDataBaseAsync(CacheServer server, GetAllDataBaseOptions options)
        {
            if (server == null)
            {
                throw new ArgumentNullException($"{nameof(server)}");
            }
            if (options?.EndPoints.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(GetAllDataBaseOptions)}.{nameof(GetAllDataBaseOptions.EndPoints)}");
            }
            using (var conn = RedisManager.GetConnection(server, options.EndPoints))
            {
                List<GetAllDataBaseResponse> responses = new List<GetAllDataBaseResponse>(options.EndPoints.Count);
                foreach (var ep in options.EndPoints)
                {
                    var configs = await conn.GetServer(string.Format("{0}:{1}", ep.Host, ep.Port)).ConfigGetAsync("databases").ConfigureAwait(false);
                    if (configs.IsNullOrEmpty())
                    {
                        continue;
                    }
                    var databaseConfig = configs.FirstOrDefault(c => string.Equals(c.Key, "databases", StringComparison.OrdinalIgnoreCase));
                    int dataBaseSize = databaseConfig.Value.ObjToInt32();
                    List<CacheDatabase> databaseList = new List<CacheDatabase>(dataBaseSize);
                    for (var d = 0; d < dataBaseSize; d++)
                    {
                        databaseList.Add(new CacheDatabase()
                        {
                            Index = d,
                            Name = $"{d}"
                        });
                    };
                    responses.Add(new GetAllDataBaseResponse()
                    {
                        Success = true,
                        Databases = databaseList,
                        CacheServer = server,
                        EndPoint = ep
                    });
                }
                return responses;
            }
        }

        #endregion

        #region Query keys

        /// <summary>
        /// Query keys
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>Return get keys response</returns>
        public async Task<IEnumerable<GetKeysResponse>> GetKeysAsync(CacheServer server, GetKeysOptions options)
        {
            if (server == null)
            {
                throw new ArgumentNullException($"{nameof(server)}");
            }
            if (options?.EndPoints.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(GetKeysOptions)}.{nameof(GetKeysOptions.EndPoints)}");
            }

            var query = options.Query;
            string searchString = "*";
            if (query != null && !string.IsNullOrWhiteSpace(query.MateKey))
            {
                switch (query.Type)
                {
                    case KeyMatchPattern.StartWith:
                        searchString = query.MateKey + "*";
                        break;
                    case KeyMatchPattern.EndWith:
                        searchString = "*" + query.MateKey;
                        break;
                    default:
                        searchString = string.Format("*{0}*", query.MateKey);
                        break;
                }
            }
            using (var conn = RedisManager.GetConnection(server, options.EndPoints))
            {
                List<GetKeysResponse> responses = new List<GetKeysResponse>(options.EndPoints.Count);
                foreach (var ep in options.EndPoints)
                {
                    var redisServer = conn.GetServer(string.Format("{0}:{1}", ep.Host, ep.Port));
                    foreach (var db in server.Databases)
                    {
                        if (!int.TryParse(db, out int dbIndex))
                        {
                            continue;
                        }
                        var keys = redisServer.Keys(dbIndex, searchString, query.PageSize, 0, (query.Page - 1) * query.PageSize, CommandFlags.None);
                        List<CacheKey> itemList = keys.Select(c => { CacheKey key = ConstantCacheKey.Create(c); return key; }).ToList();
                        var totalCount = await redisServer.DatabaseSizeAsync(dbIndex).ConfigureAwait(false);
                        var keyItemPaging = new CachePaging<CacheKey>(query.Page, query.PageSize, totalCount, itemList);
                        responses.Add(new GetKeysResponse()
                        {
                            Success = true,
                            Keys = keyItemPaging,
                            CacheServer = server,
                            EndPoint = ep,
                            Database = new RedisDatabase()
                            {
                                Index = dbIndex,
                                Name = dbIndex.ToString()
                            }
                        });
                    }
                }
                return responses;
            }
        }

        #endregion

        #region Clear data

        /// <summary>
        /// clear database data
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>clear data response</returns>
        public async Task<IEnumerable<ClearDataResponse>> ClearDataAsync(CacheServer server, ClearDataOptions options)
        {
            if (options.EndPoints.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(ClearDataOptions)}.{nameof(ClearDataOptions.EndPoints)}");
            }
            if (server.Databases.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(CacheServer)}.{nameof(CacheServer.Databases)}");
            }
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            using (var conn = RedisManager.GetConnection(server, options.EndPoints))
            {
                List<ClearDataResponse> responses = new List<ClearDataResponse>();
                foreach (var ep in options.EndPoints)
                {
                    var redisServer = conn.GetServer(string.Format("{0}:{1}", ep.Host, ep.Port));
                    foreach (var db in server.Databases)
                    {
                        if (!int.TryParse(db, out int dbIndex))
                        {
                            continue;
                        }
                        await redisServer.FlushDatabaseAsync(dbIndex, cmdFlags).ConfigureAwait(false);
                        responses.Add(new ClearDataResponse()
                        {
                            Success = true,
                            CacheServer = server,
                            EndPoint = ep,
                            Database = new RedisDatabase()
                            {
                                Index = dbIndex,
                                Name = dbIndex.ToString()
                            }
                        });
                    }
                }
                return responses;
            }
        }

        #endregion

        #region Get cache item detail

        /// <summary>
        /// get cache item detail
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>get key detail response</returns>
        public async Task<IEnumerable<GetDetailResponse>> GetKeyDetailAsync(CacheServer server, GetDetailOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(GetDetailOptions)}.{nameof(GetDetailOptions.Key)}");
            }
            if (options.EndPoints.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(GetDetailOptions)}.{nameof(GetDetailOptions.EndPoints)}");
            }
            if (server.Databases.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(CacheServer)}.{nameof(CacheServer.Databases)}");
            }
            using (var conn = RedisManager.GetConnection(server, options.EndPoints))
            {
                List<GetDetailResponse> responses = new List<GetDetailResponse>();
                foreach (var db in server.Databases)
                {
                    if (!int.TryParse(db, out var dbIndex))
                    {
                        continue;
                    }
                    var redisDatabase = conn.GetDatabase(dbIndex);
                    var redisKeyType = redisDatabase.KeyTypeAsync(options.Key.GetActualKey()).ConfigureAwait(false);
                    var cacheKeyType = RedisManager.GetCacheKeyType(redisKeyType.ToString());
                    CacheEntry keyItem = new CacheEntry()
                    {
                        Key = options.Key.GetActualKey(),
                        Type = cacheKeyType
                    };
                    switch (cacheKeyType)
                    {
                        case CacheKeyType.String:
                            keyItem.Value = await redisDatabase.StringGetAsync(keyItem.Key.GetActualKey()).ConfigureAwait(false);
                            break;
                        case CacheKeyType.List:
                            List<string> listValues = new List<string>();
                            var listResults = await redisDatabase.ListRangeAsync(keyItem.Key.GetActualKey(), 0, -1, CommandFlags.None).ConfigureAwait(false);
                            listValues.AddRange(listResults.Select(c => (string)c));
                            keyItem.Value = listValues;
                            break;
                        case CacheKeyType.Set:
                            List<string> setValues = new List<string>();
                            var setResults = await redisDatabase.SetMembersAsync(keyItem.Key.GetActualKey(), CommandFlags.None).ConfigureAwait(false);
                            setValues.AddRange(setResults.Select(c => (string)c));
                            keyItem.Value = setValues;
                            break;
                        case CacheKeyType.SortedSet:
                            List<string> sortSetValues = new List<string>();
                            var sortedResults = await redisDatabase.SortedSetRangeByRankAsync(keyItem.Key.GetActualKey()).ConfigureAwait(false);
                            sortSetValues.AddRange(sortedResults.Select(c => (string)c));
                            keyItem.Value = sortSetValues;
                            break;
                        case CacheKeyType.Hash:
                            Dictionary<string, string> hashValues = new Dictionary<string, string>();
                            var objValues = await redisDatabase.HashGetAllAsync(keyItem.Key.GetActualKey()).ConfigureAwait(false);
                            foreach (var obj in objValues)
                            {
                                hashValues.Add(obj.Name, obj.Value);
                            }
                            keyItem.Value = hashValues;
                            break;
                    }
                    responses.Add(new GetDetailResponse()
                    {
                        Success = true,
                        CacheEntry = keyItem,
                        CacheServer = server,
                        Database = new CacheDatabase()
                        {
                            Index = dbIndex,
                            Name = dbIndex.ToString()
                        }
                    });
                }
                return responses;
            }
        }

        #endregion

        #region Get server configuration

        /// <summary>
        /// get server configuration
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>get server config response</returns>
        public async Task<IEnumerable<GetServerConfigurationResponse>> GetServerConfigurationAsync(CacheServer server, GetServerConfigurationOptions options)
        {
            if (options.EndPoints.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(GetServerConfigurationOptions)}.{nameof(GetServerConfigurationOptions.EndPoints)}");
            }
            List<GetServerConfigurationResponse> responses = new List<GetServerConfigurationResponse>();
            using (var conn = RedisManager.GetConnection(server, options.EndPoints))
            {
                foreach (var ep in options.EndPoints)
                {
                    var redisServer = conn.GetServer(string.Format("{0}:{1}", ep.Host, ep.Port));
                    var configs = await redisServer.ConfigGetAsync("*").ConfigureAwait(false);
                    if (configs.IsNullOrEmpty())
                    {
                        continue;
                    }
                    RedisServerConfiguration config = new RedisServerConfiguration();

                    #region Configuration info

                    foreach (var cfg in configs)
                    {
                        string key = cfg.Key.ToLower();
                        switch (key)
                        {
                            case "daemonize":
                                config.Daemonize = cfg.Value.ToLower() == "yes";
                                break;
                            case "pidfile":
                                config.PidFile = cfg.Value;
                                break;
                            case "port":
                                int port = 0;
                                if (!int.TryParse(cfg.Value, out port))
                                {
                                    port = 6379;
                                }
                                config.Port = port;
                                break;
                            case "bind":
                                config.Host = cfg.Value;
                                break;
                            case "timeout":
                                long timeOut = 0;
                                long.TryParse(cfg.Value, out timeOut);
                                config.TimeOut = timeOut;
                                break;
                            case "loglevel":
                                LogLevel logLevel = LogLevel.Verbose;
                                switch (cfg.Value)
                                {
                                    case "debug":
                                        logLevel = LogLevel.Debug;
                                        break;
                                    case "verbose":
                                        logLevel = LogLevel.Verbose;
                                        break;
                                    case "notice":
                                        logLevel = LogLevel.Notice;
                                        break;
                                    case "warning":
                                        logLevel = LogLevel.Warning;
                                        break;
                                }
                                config.LogLevel = logLevel;
                                break;
                            case "logfile":
                                config.LogFile = cfg.Value;
                                break;
                            case "databases":
                                int dataBaseCount = 0;
                                int.TryParse(cfg.Value, out dataBaseCount);
                                config.DatabaseCount = dataBaseCount;
                                break;
                            case "save":
                                if (string.IsNullOrWhiteSpace(cfg.Value))
                                {
                                    continue;
                                }
                                var valueArray = cfg.Value.LSplit(" ");
                                List<DataChangeSaveOptions> saveInfos = new List<DataChangeSaveOptions>();
                                for (var i = 0; i < valueArray.Length; i += 2)
                                {
                                    if (valueArray.Length <= i + 1)
                                    {
                                        break;
                                    }
                                    long seconds = 0;
                                    long.TryParse(valueArray[i], out seconds);
                                    long changes = 0;
                                    long.TryParse(valueArray[i + 1], out changes);
                                    saveInfos.Add(new DataChangeSaveOptions()
                                    {
                                        Seconds = seconds,
                                        Changes = changes
                                    });
                                }
                                config.SaveConfiguration = saveInfos;
                                break;
                            case "rdbcompression":
                                config.RdbCompression = string.IsNullOrWhiteSpace(cfg.Value) ? true : string.Equals(cfg.Value, "yes", StringComparison.OrdinalIgnoreCase);
                                break;
                            case "dbfilename":
                                config.DatabaseFileName = cfg.Value;
                                break;
                            case "dir":
                                config.DatabaseDirectory = cfg.Value;
                                break;
                            case "slaveof":
                                if (string.IsNullOrWhiteSpace(cfg.Value))
                                {
                                    continue;
                                }
                                string[] masterArray = cfg.Value.LSplit(" ");
                                config.MasterHost = masterArray[0];
                                if (masterArray.Length > 1)
                                {
                                    int masterPort = 0;
                                    int.TryParse(masterArray[1], out masterPort);
                                    config.MasterPort = masterPort;
                                }
                                else
                                {
                                    config.MasterPort = 6379;
                                }
                                break;
                            case "masterauth":
                                config.MasterPassword = cfg.Value;
                                break;
                            case "requirepass":
                                config.Password = cfg.Value;
                                break;
                            case "maxclients":
                                int maxClient = 0;
                                int.TryParse(cfg.Value, out maxClient);
                                config.MaxClient = maxClient;
                                break;
                            case "maxmemory":
                                long maxMemory = 0;
                                long.TryParse(cfg.Value, out maxMemory);
                                config.MaxMemory = maxMemory;
                                break;
                            case "appendonly":
                                config.AppendOnly = cfg.Value.ToLower() == "yes";
                                break;
                            case "appendfilename":
                                config.AppendFileName = cfg.Value;
                                break;
                            case "appendfsync":
                                AppendfSync appendSync = AppendfSync.EverySecond;
                                switch (cfg.Value)
                                {
                                    case "no":
                                        appendSync = AppendfSync.No;
                                        break;
                                    case "always":
                                        appendSync = AppendfSync.Always;
                                        break;
                                }
                                config.AppendfSync = appendSync;
                                break;
                            case "vm-enabled":
                                config.EnabledVirtualMemory = cfg.Value.ToLower() == "yes";
                                break;
                            case "vm-swap-file":
                                config.VirtualMemorySwapFile = cfg.Value;
                                break;
                            case "vm-max-memory":
                                long vmMaxMemory = 0;
                                long.TryParse(cfg.Value, out vmMaxMemory);
                                config.MaxVirtualMemory = vmMaxMemory;
                                break;
                            case "vm-page-size":
                                int vmPageSize = 0;
                                int.TryParse(cfg.Value, out vmPageSize);
                                config.VirtualMemoryPageSize = vmPageSize;
                                break;
                            case "vm-pages":
                                long vmPages = 0;
                                long.TryParse(cfg.Value, out vmPages);
                                config.VirtualMemoryPages = vmPages;
                                break;
                            case "vm-max-threads":
                                int vmMaxThreads = 0;
                                int.TryParse(cfg.Value, out vmMaxThreads);
                                config.VirtualMemoryMaxThreads = vmMaxThreads;
                                break;
                            case "glueoutputbuf":
                                config.Glueoutputbuf = cfg.Value.ToLower() == "yes";
                                break;
                            case "activerehashing":
                                config.ActivereHashing = cfg.Value.ToLower() == "yes";
                                break;
                            case "include":
                                config.IncludeConfigurationFile = cfg.Value;
                                break;
                        }
                    }

                    #endregion

                    responses.Add(new GetServerConfigurationResponse()
                    {
                        ServerConfiguration = config,
                        Success = true,
                        CacheServer = server,
                        EndPoint = ep
                    });
                }
                return responses;
            }
        }

        #endregion

        #region Save server configuration

        /// <summary>
        /// save server configuration
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>save server config response</returns>
        public async Task<IEnumerable<SaveServerConfigurationResponse>> SaveServerConfigurationAsync(CacheServer server, SaveServerConfigurationOptions options)
        {
            if (!(options.ServerConfiguration is RedisServerConfiguration config))
            {
                return Array.Empty<SaveServerConfigurationResponse>();
            }
            if (options.EndPoints.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(SaveServerConfigurationOptions)}.{nameof(SaveServerConfigurationOptions.EndPoints)}");
            }
            List<SaveServerConfigurationResponse> responses = new List<SaveServerConfigurationResponse>(options.EndPoints.Count);
            using (var conn = RedisManager.GetConnection(server, options.EndPoints))
            {
                foreach (var ep in options.EndPoints)
                {
                    var redisServer = conn.GetServer(string.Format("{0}:{1}", ep.Host, ep.Port));
                    if (!string.IsNullOrWhiteSpace(config.Host))
                    {
                        redisServer.ConfigSet("bind", config.Host);
                    }
                    if (config.TimeOut >= 0)
                    {
                        redisServer.ConfigSet("timeout", config.TimeOut);
                    }
                    redisServer.ConfigSet("loglevel", config.LogLevel.ToString().ToLower());
                    string saveConfigValue = string.Empty;
                    if (!config.SaveConfiguration.IsNullOrEmpty())
                    {
                        List<string> configList = new List<string>();
                        foreach (var saveCfg in config.SaveConfiguration)
                        {
                            configList.Add(saveCfg.Seconds.ToString());
                            configList.Add(saveCfg.Changes.ToString());
                        }
                        saveConfigValue = string.Join(" ", configList);
                    }
                    redisServer.ConfigSet("save", saveConfigValue);
                    redisServer.ConfigSet("rdbcompression", config.RdbCompression ? "yes" : "no");
                    if (!config.DatabaseFileName.IsNullOrEmpty())
                    {
                        redisServer.ConfigSet("dbfilename", config.DatabaseFileName);
                    }
                    if (!string.IsNullOrWhiteSpace(config.DatabaseDirectory))
                    {
                        redisServer.ConfigSet("dir", config.DatabaseDirectory);
                    }
                    if (!string.IsNullOrWhiteSpace(config.MasterHost))
                    {
                        string masterUrl = string.Format("{0} {1}", config.Host, config.Port > 0 ? config.Port : 6379);
                        redisServer.ConfigSet("slaveof", masterUrl);
                    }
                    if (config.MasterPassword != null)
                    {
                        redisServer.ConfigSet("masterauth", config.MasterPassword);
                    }
                    if (config.Password != null)
                    {
                        redisServer.ConfigSet("requirepass", config.Password);
                    }
                    if (config.MaxClient >= 0)
                    {
                        redisServer.ConfigSet("maxclients", config.MaxClient);
                    }
                    if (config.MaxMemory >= 0)
                    {
                        redisServer.ConfigSet("maxmemory", config.MaxMemory);
                    }
                    redisServer.ConfigSet("appendonly", config.AppendOnly ? "yes" : "no");
                    if (!string.IsNullOrWhiteSpace(config.AppendFileName))
                    {
                        redisServer.ConfigSet("appendfilename", config.AppendFileName);
                    }
                    string appendfSyncVal = "everysec";
                    switch (config.AppendfSync)
                    {
                        case AppendfSync.Always:
                            appendfSyncVal = "always";
                            break;
                        case AppendfSync.EverySecond:
                            appendfSyncVal = "everysec";
                            break;
                        case AppendfSync.No:
                            appendfSyncVal = "no";
                            break;
                    }
                    redisServer.ConfigSet("appendfsync", appendfSyncVal);
                    if (!string.IsNullOrWhiteSpace(config.VirtualMemorySwapFile))
                    {
                        redisServer.ConfigSet("vm-swap-file", config.VirtualMemorySwapFile);
                    }
                    if (config.VirtualMemoryMaxThreads > 0)
                    {
                        redisServer.ConfigSet("vm-max-threads", config.VirtualMemoryMaxThreads);
                    }
                    redisServer.ConfigSet("activerehashing", config.ActivereHashing ? "yes" : "no");
                    if (!string.IsNullOrWhiteSpace(config.IncludeConfigurationFile))
                    {
                        redisServer.ConfigSet("include", config.IncludeConfigurationFile);
                    }
                    await redisServer.ConfigRewriteAsync().ConfigureAwait(false);
                    responses.Add(new SaveServerConfigurationResponse()
                    {
                        Success = true,
                        CacheServer = server,
                        EndPoint = ep
                    });
                }
                return responses;
            }
        }

        #endregion

        #endregion

        #region Util

        /// <summary>
        /// Get key script
        /// </summary>
        /// <param name="keyIndex">Key index</param>
        /// <returns>Return key script</returns>
        static string Keys(int keyIndex)
        {
            return $"KEYS[{keyIndex}]";
        }

        /// <summary>
        /// Get arg index
        /// </summary>
        /// <param name="argIndex">Arg index</param>
        /// <returns>Return arg script</returns>
        static string Arg(int argIndex)
        {
            return $"ARGV[{argIndex}]";
        }

        /// <summary>
        /// Get refresh expiration script
        /// </summary>
        /// <param name="keyCount">Refresh key index</param>
        /// <param name="exKeyIndex">Expire key index</param>
        /// <param name="refreshCurrentTimeArgIndex">Whether refresh current time arg index</param>
        /// <param name="hasNewExArgIndex">Whether has new expiration time arg index</param>
        /// <param name="newTimeArgIndex">New expiration time argindex</param>
        /// <returns></returns>
        static string GetRefreshExpirationScript(int argOffset = 0, int keyOffset = 0, int keyCount = 1, int refreshCurrentTimeArgIndex = 3, int hasNewExArgIndex = 4, int newTimeArgIndex = 5)
        {
            refreshCurrentTimeArgIndex += argOffset;
            hasNewExArgIndex += argOffset;
            newTimeArgIndex += argOffset;
            return $@"local exkey=''
local ckey=''
if {Arg(refreshCurrentTimeArgIndex)}=='1' 
then
    for ki={1 + keyOffset},{keyCount + keyOffset}
    do
        ckey=KEYS[ki]
        exkey=ckey..'{RedisManager.ExpirationKeySuffix}' 
        local ct=redis.call('GET',exkey)
        if ct 
        then
            local rs=redis.call('EXPIRE',ckey,ct)
            if rs 
            then
                redis.call('SET',exkey,ct,'EX',ct)
            end
        end
    end
else
    for ki={1 + keyOffset},{keyCount + keyOffset}
    do
        ckey=KEYS[ki]
        exkey=ckey..'{RedisManager.ExpirationKeySuffix}'
        local nt=tonumber({Arg(newTimeArgIndex)})
        if nt>0
        then
            local rs=redis.call('EXPIRE',ckey,nt)
            if rs and {Arg(hasNewExArgIndex)}=='1'
            then
                redis.call('SET',exkey,nt,'EX',nt)
            end
        elseif nt<0
        then
            redis.call('PERSIST',ckey)
        end
    end
end";
        }

        /// <summary>
        /// Get expiration key
        /// </summary>
        /// <param name="cacheKey">Cache key</param>
        /// <returns></returns>
        static string GetExpirationKey(string cacheKey)
        {
            return $"{cacheKey}{RedisManager.ExpirationKeySuffix}";
        }

        /// <summary>
        /// Format sorted set range boundary
        /// </summary>
        /// <param name="value">Value</param>
        /// <param name="exclude">Exclude type</param>
        /// <returns></returns>
        static string FormatSortedSetRangeBoundary(string value, bool startValue, BoundaryExclude exclude)
        {
            switch (exclude)
            {
                case BoundaryExclude.None:
                default:
                    return $"[{value}";
                case BoundaryExclude.Both:
                    return $"({value}";
                case BoundaryExclude.Start:
                    return startValue ? $"({value}" : $"[{value}";
                case BoundaryExclude.Stop:
                    return startValue ? $"[{value}" : $"({value}";
            }
        }

        /// <summary>
        /// Format sorted set range boundary
        /// </summary>
        /// <param name="score">Score vlaue</param>
        /// <param name="startValue">Whether is start score</param>
        /// <param name="exclude">Exclude options</param>
        /// <returns></returns>
        static string FormatSortedSetScoreRangeBoundary(double score, bool startValue, BoundaryExclude exclude)
        {
            switch (exclude)
            {
                case BoundaryExclude.None:
                default:
                    return score.ToString();
                case BoundaryExclude.Both:
                    return $"({score}";
                case BoundaryExclude.Start:
                    return startValue ? $"({score}" : $"{score}";
                case BoundaryExclude.Stop:
                    return startValue ? $"{score}" : $"({score}";
            }
        }

        static IEnumerable<T> GetNoDatabaseResponse<T>(CacheServer server) where T : CacheResponse, new()
        {
            if (CacheManager.Configuration.ThrowNoDatabaseException)
            {
                throw new EZNEWException("No cache database specified");
            }
            return new T[1] { CacheResponse.NoDatabase<T>(server) };
        }

        static IEnumerable<T> GetNoValueResponse<T>(CacheServer server) where T : CacheResponse, new()
        {
            return new T[1] { CacheResponse.FailResponse<T>("", "No value specified", server) };
        }

        static IEnumerable<T> GetNoKeyResponse<T>(CacheServer server) where T : CacheResponse, new()
        {
            return new T[1] { CacheResponse.FailResponse<T>("", "No key specified", server) };
        }

        #endregion
    }
}
