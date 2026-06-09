using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Sixnet.Cache.Hash.Parameters;
using Sixnet.Cache.Hash.Results;
using Sixnet.Cache.Keys.Parameters;
using Sixnet.Cache.Keys.Results;
using Sixnet.Cache.List.Parameters;
using Sixnet.Cache.List.Results;
using Sixnet.Cache.Server.Parameters;
using Sixnet.Cache.Server.Response;
using Sixnet.Cache.Set.Parameters;
using Sixnet.Cache.Set.Results;
using Sixnet.Cache.SortedSet;
using Sixnet.Cache.SortedSet.Parameters;
using Sixnet.Cache.SortedSet.Results;
using Sixnet.Cache.String.Parameters;
using Sixnet.Cache.String.Results;
using Sixnet.Exceptions;
using StackExchange.Redis;

namespace Sixnet.Cache.Redis
{
    public partial class SixnetRedisProvider : ISixnetCacheProvider
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
        /// <param name="parameter">String set range parameter</param>
        /// <returns>Return string set range result</returns>
        public SixnetStringSetRangeResult StringSetRange(SixnetCacheServer server, SixnetStringSetRangeParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetStringSetRangeParameter)}.{nameof(SixnetStringSetRangeParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetStringSetRangeStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new SixnetStringSetRangeResult()
            {
                Success = true,
                CacheServer = server,
                Database = database,
                NewValueLength = (long)result
            };
        }

        SixnetRedisStatement GetStringSetRangeStatement(SixnetStringSetRangeParameter parameter)
        {
            var script = $@"local len=redis.call('SETRANGE',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return len";
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[] { parameter.Key.GetActualKey() };
            var parameters = new RedisValue[]
            {
                parameter.Offset,
                parameter.Value,
                parameter.Expiration==null,//refresh current time
                expire.Item1 && SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                SixnetRedisManager.GetTotalSeconds(expire.Item2),//expire time seconds
            };
            var commandFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = commandFlags
            };
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
        /// <param name="parameter">String set bit parameter</param>
        /// <returns>Return string set bit result</returns>
        public SixnetStringSetBitResult StringSetBit(SixnetCacheServer server, SixnetStringSetBitParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetStringSetBitParameter)}.{nameof(SixnetStringSetBitParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetStringSetBitStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new SixnetStringSetBitResult()
            {
                Success = true,
                OldBitValue = (bool)result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetStringSetBitStatement(SixnetStringSetBitParameter parameter)
        {
            var script = $@"local obv=redis.call('SETBIT',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return obv";
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Offset,
                parameter.Bit,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                SixnetRedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region StringSet

        /// <summary>
        /// Set key to hold the string value. If key already holds a value, it is overwritten,
        /// regardless of its type.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">String set parameter</param>
        /// <returns>Return string set result</returns>
        public SixnetStringSetResult StringSet(SixnetCacheServer server, SixnetStringSetParameter parameter)
        {
            if (parameter?.Items.IsNullOrEmpty() ?? true)
            {
                return GetNoValueResponse<SixnetStringSetResult>(server);
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetStringSetStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new SixnetStringSetResult()
            {
                CacheServer = server,
                Database = database,
                Success = true,
                Results = ((RedisValue[])result)?.Select(c => new SixnetStringEntrySetResult() { SetSuccess = true, Key = c }).ToList()
            };
        }

        SixnetRedisStatement GetStringSetStatement(SixnetStringSetParameter parameter)
        {
            var itemCount = parameter.Items.Count;
            var valueCount = itemCount * 5;
            var allowSlidingExpire = SixnetRedisManager.AllowSlidingExpiration();
            RedisKey[] setKeys = new RedisKey[itemCount];
            RedisValue[] setValues = new RedisValue[valueCount];
            for (var i = 0; i < itemCount; i++)
            {
                var nowItem = parameter.Items[i];
                var nowExpire = SixnetRedisManager.GetExpiration(nowItem.Expiration);
                setKeys[i] = nowItem.Key.GetActualKey();

                var argIndex = i * 5;
                var allowSliding = nowExpire.Item1 && allowSlidingExpire;
                setValues[argIndex] = nowItem.Value.ToNullableString();
                setValues[argIndex + 1] = nowItem.Expiration == null;
                setValues[argIndex + 2] = allowSliding;
                setValues[argIndex + 3] = nowExpire.Item2.HasValue ? SixnetRedisManager.GetTotalSeconds(nowExpire.Item2) : (allowSliding ? 0 : -1);
                setValues[argIndex + 4] = SixnetRedisManager.GetSetWhenCommand(nowItem.When);
            }
            var script = $@"local skeys={{}}
local ckey=''
local exkey=''
local argBi=1
local sr=true
for ki=1,{itemCount}
do
    argBi=(ki-1)*5+1
    ckey=KEYS[ki]
    exkey=ckey..'{SixnetRedisManager.ExpirationKeySuffix}'
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
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = setKeys,
                Parameters = setValues,
                Flags = cmdFlags
            };
        }

        #endregion

        #region StringLength

        /// <summary>
        /// Returns the length of the string value stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">String length parameter</param>
        /// <returns>Return string length result</returns>
        public SixnetStringLengthResult StringLength(SixnetCacheServer server, SixnetStringLengthParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetStringLengthParameter)}.{nameof(SixnetStringLengthParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetStringLengthStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new SixnetStringLengthResult()
            {
                Success = true,
                Length = (long)result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetStringLengthStatement(SixnetStringLengthParameter parameter)
        {
            var script = $@"local obv=redis.call('STRLEN',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return obv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">String increment parameter</param>
        /// <returns>Return string increment result</returns>
        public SixnetStringIncrementResult StringIncrement(SixnetCacheServer server, SixnetStringIncrementParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetStringIncrementParameter)}.{nameof(SixnetStringIncrementParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetStringIncrementStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new SixnetStringIncrementResult()
            {
                Success = true,
                NewValue = (long)result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetStringIncrementStatement(SixnetStringIncrementParameter parameter)
        {
            var script = $@"local obv=redis.call('INCRBY',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return obv";
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Value,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                SixnetRedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region StringGetWithExpiry

        /// <summary>
        /// Get the value of key. If the key does not exist the special value nil is returned.
        /// An error is returned if the value stored at key is not a string, because GET
        /// only handles string values.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">String get with expiry ption</param>
        /// <returns>Return string get with expiry result</returns>
        public SixnetStringGetWithExpiryResult StringGetWithExpiry(SixnetCacheServer server, SixnetStringGetWithExpiryParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetStringGetWithExpiryParameter)}.{nameof(SixnetStringGetWithExpiryParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetStringGetWithExpiryStatement(parameter);
            var result = (RedisValue[])(ExecuteStatement(server, database, statement));
            return new SixnetStringGetWithExpiryResult()
            {
                Success = true,
                Value = result[0],
                Expiry = TimeSpan.FromSeconds((long)result[1]),
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetStringGetWithExpiryStatement(SixnetStringGetWithExpiryParameter parameter)
        {
            var script = $@"local obv=redis.call('GET',{Keys(1)})
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
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region StringGetSet

        /// <summary>
        /// Atomically sets key to value and returns the old value stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">String get set parameter</param>
        /// <returns>Return string get set result</returns>
        public SixnetStringGetSetResult StringGetSet(SixnetCacheServer server, SixnetStringGetSetParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetStringGetSetParameter)}.{nameof(SixnetStringGetSetParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetStringGetSetStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new SixnetStringGetSetResult()
            {
                Success = true,
                OldValue = (string)result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetStringGetSetStatement(SixnetStringGetSetParameter parameter)
        {
            var script = $@"local ov=redis.call('GETSET',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return ov";
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.NewValue,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                SixnetRedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">String get range parameter</param>
        /// <returns>Return string get range result</returns>
        public SixnetStringGetRangeResult StringGetRange(SixnetCacheServer server, SixnetStringGetRangeParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetStringGetRangeParameter)}.{nameof(SixnetStringGetRangeParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetStringGetRangeStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new SixnetStringGetRangeResult()
            {
                Success = true,
                Value = (string)result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetStringGetRangeStatement(SixnetStringGetRangeParameter parameter)
        {
            var script = $@"local ov=redis.call('GETRANGE',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return ov";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Start,
                parameter.End,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region StringGetBit

        /// <summary>
        /// Returns the bit value at offset in the string value stored at key. When offset
        /// is beyond the string length, the string is assumed to be a contiguous space with
        /// 0 bits
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">String get bit parameter</param>
        /// <returns>Return string get bit result</returns>
        public SixnetStringGetBitResult StringGetBit(SixnetCacheServer server, SixnetStringGetBitParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetStringGetBitParameter)}.{nameof(SixnetStringGetBitParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetStringGetBitStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new SixnetStringGetBitResult()
            {
                Success = true,
                Bit = (bool)result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetStringGetBitStatement(SixnetStringGetBitParameter parameter)
        {
            var script = $@"local ov=redis.call('GETBIT',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return ov";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Offset,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region StringGet

        /// <summary>
        /// Returns the values of all specified keys. For every key that does not hold a
        /// string value or does not exist, the special value nil is returned.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">String get parameter</param>
        /// <returns>Return string get result</returns>
        public SixnetStringGetResult StringGet(SixnetCacheServer server, SixnetStringGetParameter parameter)
        {
            if (parameter?.Keys.IsNullOrEmpty() ?? true)
            {
                return GetNoKeyResponse<SixnetStringGetResult>(server);
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetStringGetStatement(parameter);
            var result = (RedisValue[])(ExecuteStatement(server, database, statement));
            return new SixnetStringGetResult()
            {
                Success = true,
                Values = result.Select(c =>
                {
                    string stringValue = c;
                    var valueArray = stringValue.LSplit("$::$");
                    return new SixnetCacheEntry()
                    {
                        Key = valueArray[0],
                        Value = valueArray.Length > 1 ? valueArray[1] : null
                    };
                }).ToList(),
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetStringGetStatement(SixnetStringGetParameter parameter)
        {
            var keys = parameter.Keys.Select(c => { RedisKey rv = c.GetActualKey(); return rv; }).ToArray();
            var script = $@"local vals={{}}
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
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">String decrement parameter</param>
        /// <returns>Return string decrement result</returns>
        public SixnetStringDecrementResult StringDecrement(SixnetCacheServer server, SixnetStringDecrementParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetStringDecrementParameter)}.{nameof(SixnetStringDecrementParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetStringDecrementStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new SixnetStringDecrementResult()
            {
                Success = true,
                NewValue = (long)result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetStringDecrementStatement(SixnetStringDecrementParameter parameter)
        {
            var script = $@"local obv=redis.call('DECRBY',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return obv";
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Value,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                SixnetRedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">String bit position parameter</param>
        /// <returns>Return string bit position result</returns>
        public SixnetStringBitPositionResult StringBitPosition(SixnetCacheServer server, SixnetStringBitPositionParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetStringBitPositionParameter)}.{nameof(SixnetStringBitPositionParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetStringBitPositionStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetStringBitPositionResult()
            {
                Success = true,
                Position = result,
                HasValue = result >= 0,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetStringBitPositionStatement(SixnetStringBitPositionParameter parameter)
        {
            var script = $@"local obv=redis.call('BITPOS',{Keys(1)},{Arg(1)},{Arg(2)},{Arg(3)})
{GetRefreshExpirationScript(1)}
return obv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Bit,
                parameter.Start,
                parameter.End,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">String bit operation parameter</param>
        /// <returns>Return string bit operation result</returns>
        public SixnetStringBitOperationResult StringBitOperation(SixnetCacheServer server, SixnetStringBitOperationParameter parameter)
        {
            if (parameter?.Keys.IsNullOrEmpty() ?? true)
            {
                return GetNoKeyResponse<SixnetStringBitOperationResult>(server);
            }
            if (string.IsNullOrWhiteSpace(parameter?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SixnetStringBitOperationParameter)}.{nameof(SixnetStringBitOperationParameter.DestinationKey)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetStringBitOperationStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetStringBitOperationResult()
            {
                Success = true,
                DestinationValueLength = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetStringBitOperationStatement(SixnetStringBitOperationParameter parameter)
        {
            var keys = new RedisKey[parameter.Keys.Count + 1];
            var keyParameters = new string[parameter.Keys.Count + 1];
            keys[0] = parameter.DestinationKey.GetActualKey();
            keyParameters[0] = "KEYS[1]";
            for (var i = 0; i < parameter.Keys.Count; i++)
            {
                keys[i + 1] = parameter.Keys[i].GetActualKey();
                keyParameters[i + 1] = $"KEYS[{2 + i}]";
            }
            var script = $@"local obv=redis.call('BITOP',{Arg(1)},{string.Join(",", keyParameters)})
{GetRefreshExpirationScript(-1)}
{GetRefreshExpirationScript(2, 1, parameter.Keys.Count)}
return obv";
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            bool allowSlidingExpiration = SixnetRedisManager.AllowSlidingExpiration();
            var parameters = new RedisValue[]
            {
                SixnetRedisManager.GetBitOperator(parameter.Bitwise),
                parameter.Expiration==null,//refresh current time
                expire.Item1&&allowSlidingExpiration,//whether allow set refresh time
                SixnetRedisManager.GetTotalSeconds(expire.Item2),//expire time seconds,
                true,//refresh current time-source key
                allowSlidingExpiration,//whether allow set refresh time-source key,
                0//expire time seconds-source key
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">String bit count parameter</param>
        /// <returns>Return string bit count result</returns>
        public SixnetStringBitCountResult StringBitCount(SixnetCacheServer server, SixnetStringBitCountParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetStringBitCountParameter)}.{nameof(SixnetStringBitCountParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetStringBitCountStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetStringBitCountResult()
            {
                Success = true,
                BitNum = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetStringBitCountStatement(SixnetStringBitCountParameter parameter)
        {
            var script = $@"local obv=redis.call('BITCOUNT',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return obv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Start,
                parameter.End,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region StringAppend

        /// <summary>
        /// If key already exists and is a string, this options appends the value at the
        /// end of the string. If key does not exist it is created and set as an empty string,
        /// so APPEND will be similar to SET in this special case.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">String append parameter</param>
        /// <returns>Return string append result</returns>
        public SixnetStringAppendResult StringAppend(SixnetCacheServer server, SixnetStringAppendParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetStringAppendParameter)}.{nameof(SixnetStringAppendParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetStringAppendStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetStringAppendResult()
            {
                Success = true,
                NewValueLength = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetStringAppendStatement(SixnetStringAppendParameter parameter)
        {
            var script = $@"local obv=redis.call('APPEND',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return obv";
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Value,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                SixnetRedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">List trim parameter</param>
        /// <returns>Return list trim result</returns>
        public SixnetListTrimResult ListTrim(SixnetCacheServer server, SixnetListTrimParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetListTrimParameter)}.{nameof(SixnetListTrimParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetListTrimStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetListTrimResult()
            {
                Success = true,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetListTrimStatement(SixnetListTrimParameter parameter)
        {
            var script = $@"redis.call('LTRIM',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Start,
                parameter.Stop,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region ListSetByIndex

        /// <summary>
        /// Sets the list element at index to value. For more information on the index argument,
        ///  see ListGetByIndex. An error is returned for out of range indexes.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">List set by index parameter</param>
        /// <returns>Return list set by index result</returns>
        public SixnetListSetByIndexResult ListSetByIndex(SixnetCacheServer server, SixnetListSetByIndexParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetListSetByIndexParameter)}.{nameof(SixnetListSetByIndexParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetListSetByIndexStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SixnetListSetByIndexResult()
            {
                Success = string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase),
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetListSetByIndexStatement(SixnetListSetByIndexParameter parameter)
        {
            var script = $@"local obv=redis.call('LSET',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return obv['ok']";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Index,
                parameter.Value,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">List right push parameter</param>
        /// <returns>Return list right push</returns>
        public SixnetListRightPushResult ListRightPush(SixnetCacheServer server, SixnetListRightPushParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetListRightPushParameter)}.{nameof(SixnetListRightPushParameter.Key)}");
            }
            if (parameter?.Values.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentException($"{nameof(SixnetListRightPushParameter)}.{nameof(SixnetListRightPushParameter.Values)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetListRightPushStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetListRightPushResult()
            {
                Success = true,
                NewListLength = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetListRightPushStatement(SixnetListRightPushParameter parameter)
        {
            var values = new RedisValue[parameter.Values.Count + 3];
            var valueParameters = new string[parameter.Values.Count];
            for (var i = 0; i < parameter.Values.Count; i++)
            {
                values[i] = parameter.Values[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            var script = $@"local obv=redis.call('RPUSH',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(parameter.Values.Count - 2)}
return obv";
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            values[values.Length - 3] = parameter.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && SixnetRedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = SixnetRedisManager.GetTotalSeconds(expire.Item2);//expire time seconds
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = values,
                Flags = cmdFlags
            };
        }

        #endregion

        #region ListRightPopLeftPush

        /// <summary>
        /// Atomically returns and removes the last element (tail) of the list stored at
        /// source, and pushes the element at the first element (head) of the list stored
        /// at destination.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">List right pop left push parameter</param>
        /// <returns>Return list right pop left result</returns>
        public SixnetListRightPopLeftPushResult ListRightPopLeftPush(SixnetCacheServer server, SixnetListRightPopLeftPushParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.SourceKey))
            {
                throw new ArgumentNullException($"{nameof(SixnetListRightPopLeftPushParameter)}.{nameof(SixnetListRightPopLeftPushParameter.SourceKey)}");
            }
            if (string.IsNullOrWhiteSpace(parameter?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SixnetListRightPopLeftPushParameter)}.{nameof(SixnetListRightPopLeftPushParameter.DestinationKey)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetListRightPopLeftPushStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SixnetListRightPopLeftPushResult()
            {
                Success = true,
                PopValue = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetListRightPopLeftPushStatement(SixnetListRightPopLeftPushParameter parameter)
        {
            var script = $@"local pv=redis.call('RPOPLPUSH',{Keys(1)},{Keys(2)})
{GetRefreshExpirationScript(-2)}
{GetRefreshExpirationScript(1, 1)}
return pv";
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            bool allowSlidingExpiration = SixnetRedisManager.AllowSlidingExpiration();
            var keys = new RedisKey[]
            {
                parameter.SourceKey.GetActualKey(),
                parameter.DestinationKey.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time-source key
                allowSlidingExpiration,//whether allow set refresh time-source key
                0,//expire time seconds-source key

                parameter.Expiration==null,//refresh current time
                expire.Item1&&allowSlidingExpiration,//whether allow set refresh time
                SixnetRedisManager.GetTotalSeconds(expire.Item2)//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region ListRightPop

        /// <summary>
        /// Removes and returns the last element of the list stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">List right pop parameter</param>
        /// <returns>Return list right pop result</returns>
        public SixnetListRightPopResult ListRightPop(SixnetCacheServer server, SixnetListRightPopParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetListRightPopParameter)}.{nameof(SixnetListRightPopParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetListRightPopStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SixnetListRightPopResult()
            {
                Success = true,
                PopValue = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetListRightPopStatement(SixnetListRightPopParameter parameter)
        {
            var script = $@"local pv=redis.call('RPOP',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">List remove parameter</param>
        /// <returns>Return list remove result</returns>
        public SixnetListRemoveResult ListRemove(SixnetCacheServer server, SixnetListRemoveParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetListRemoveParameter)}.{nameof(SixnetListRemoveParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetListRemoveStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetListRemoveResult()
            {
                Success = true,
                RemoveCount = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetListRemoveStatement(SixnetListRemoveParameter parameter)
        {
            var script = $@"local rc=redis.call('LREM',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return rc";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Count,
                parameter.Value,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">Options</param>
        /// <returns>list range result</returns>
        public SixnetListRangeResult ListRange(SixnetCacheServer server, SixnetListRangeParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetListRangeParameter)}.{nameof(SixnetListRangeParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetListRangeStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SixnetListRangeResult()
            {
                Success = true,
                Values = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetListRangeStatement(SixnetListRangeParameter parameter)
        {
            var script = $@"local rc=redis.call('LRANGE',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return rc";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Start,
                parameter.Stop,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region ListLength

        /// <summary>
        /// Returns the length of the list stored at key. If key does not exist, it is interpreted
        ///  as an empty list and 0 is returned.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>list length result</returns>
        public SixnetListLengthResult ListLength(SixnetCacheServer server, SixnetListLengthParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetListLengthParameter)}.{nameof(SixnetListLengthParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetListLengthStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetListLengthResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetListLengthStatement(SixnetListLengthParameter parameter)
        {
            var script = $@"local len=redis.call('LLEN',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return len";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region ListLeftPush

        /// <summary>
        /// Insert the specified value at the head of the list stored at key. If key does
        ///  not exist, it is created as empty list before performing the push operations.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">List left push parameter</param>
        /// <returns>Return list left push result</returns>
        public SixnetListLeftPushResult ListLeftPush(SixnetCacheServer server, SixnetListLeftPushParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetListLeftPushParameter)}.{nameof(SixnetListLeftPushParameter.Key)}");
            }
            if (parameter?.Values.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentException($"{nameof(SixnetListRightPushParameter)}.{nameof(SixnetListRightPushParameter.Values)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetListLeftPushStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetListLeftPushResult()
            {
                Success = true,
                NewListLength = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetListLeftPushStatement(SixnetListLeftPushParameter parameter)
        {
            var values = new RedisValue[parameter.Values.Count + 3];
            var valueParameters = new string[parameter.Values.Count];
            for (var i = 0; i < parameter.Values.Count; i++)
            {
                values[i] = parameter.Values[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            var script = $@"local obv=redis.call('LPUSH',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(parameter.Values.Count - 2)}
return obv";
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            values[values.Length - 3] = parameter.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && SixnetRedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = SixnetRedisManager.GetTotalSeconds(expire.Item2);//expire time seconds
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = values,
                Flags = cmdFlags
            };
        }

        #endregion

        #region ListLeftPop

        /// <summary>
        /// Removes and returns the first element of the list stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">List left pop parameter</param>
        /// <returns>list left pop result</returns>
        public SixnetListLeftPopResult ListLeftPop(SixnetCacheServer server, SixnetListLeftPopParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetListLeftPopParameter)}.{nameof(SixnetListLeftPopParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetListLeftPopStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SixnetListLeftPopResult()
            {
                Success = true,
                PopValue = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetListLeftPopStatement(SixnetListLeftPopParameter parameter)
        {
            var script = $@"local pv=redis.call('LPOP',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region ListInsertBefore

        /// <summary>
        /// Inserts value in the list stored at key either before or after the reference
        /// value pivot. When key does not exist, it is considered an empty list and no operation
        /// is performed.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">List insert before parameter</param>
        /// <returns>Return list insert begore result</returns>
        public SixnetListInsertBeforeResult ListInsertBefore(SixnetCacheServer server, SixnetListInsertBeforeParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetListInsertBeforeParameter)}.{nameof(SixnetListInsertBeforeParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetListInsertBeforeStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetListInsertBeforeResult()
            {
                Success = result > 0,
                NewListLength = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetListInsertBeforeStatement(SixnetListInsertBeforeParameter parameter)
        {
            var script = $@"local pv=redis.call('LINSERT',{Keys(1)},'BEFORE',{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.PivotValue,
                parameter.InsertValue,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region ListInsertAfter

        /// <summary>
        /// Inserts value in the list stored at key either before or after the reference
        /// value pivot. When key does not exist, it is considered an empty list and no operation
        /// is performed.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">List insert after parameter</param>
        /// <returns>Return list insert after result</returns>
        public SixnetListInsertAfterResult ListInsertAfter(SixnetCacheServer server, SixnetListInsertAfterParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetListInsertAfterParameter)}.{nameof(SixnetListInsertAfterParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetListInsertAfterStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetListInsertAfterResult()
            {
                Success = result > 0,
                NewListLength = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetListInsertAfterStatement(SixnetListInsertAfterParameter parameter)
        {
            var script = $@"local pv=redis.call('LINSERT',{Keys(1)},'AFTER',{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.PivotValue,
                parameter.InsertValue,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">List get by index parameter</param>
        /// <returns>Return list get by index result</returns>
        public SixnetListGetByIndexResult ListGetByIndex(SixnetCacheServer server, SixnetListGetByIndexParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetListInsertAfterParameter)}.{nameof(SixnetListInsertAfterParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetListGetByIndexStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SixnetListGetByIndexResult()
            {
                Success = true,
                Value = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetListGetByIndexStatement(SixnetListGetByIndexParameter parameter)
        {
            var script = $@"local pv=redis.call('LINDEX',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Index,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #endregion

        #region Hash

        #region HashValues

        /// <summary>
        /// Returns all values in the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Hash values parameter</param>
        /// <returns>Return hash values result</returns>
        public SixnetHashValuesResult HashValues(SixnetCacheServer server, SixnetHashValuesParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetHashValuesParameter)}.{nameof(SixnetHashValuesParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetHashValuesStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SixnetHashValuesResult()
            {
                Success = true,
                Values = result.Select(c => { dynamic value = c; return value; }).ToList(),
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetHashValuesStatement(SixnetHashValuesParameter parameter)
        {
            var script = $@"local pv=redis.call('HVALS',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region HashSet

        /// <summary>
        /// Sets field in the hash stored at key to value. If key does not exist, a new key
        ///  holding a hash is created. If field already exists in the hash, it is overwritten.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Hash set parameter</param>
        /// <returns>Return hash set result</returns>
        public SixnetHashSetResult HashSet(SixnetCacheServer server, SixnetHashSetParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetHashSetParameter)}.{nameof(SixnetHashSetParameter.Key)}");
            }
            if (parameter?.Items.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SixnetHashSetParameter)}.{nameof(SixnetHashSetParameter.Items)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetHashSetStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SixnetHashSetResult()
            {
                Success = string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase),
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetHashSetStatement(SixnetHashSetParameter parameter)
        {
            var valueCount = parameter.Items.Count * 2;
            var values = new RedisValue[valueCount + 3];
            var valueParameters = new string[valueCount];
            int valueIndex = 0;
            foreach (var valueItem in parameter.Items)
            {
                values[valueIndex] = valueItem.Key;
                values[valueIndex + 1] = valueItem.Value;
                valueParameters[valueIndex] = $"{Arg(valueIndex + 1)}";
                valueParameters[valueIndex + 1] = $"{Arg(valueIndex + 2)}";
                valueIndex += 2;
            }
            var script = $@"local obv=redis.call('HMSET',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(valueCount - 2)}
return obv['ok']";
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            values[values.Length - 3] = parameter.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && SixnetRedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = SixnetRedisManager.GetTotalSeconds(expire.Item2);//expire time seconds
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = values,
                Flags = cmdFlags
            };
        }

        #endregion

        #region HashLength

        /// <summary>
        /// Returns the number of fields contained in the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Hash length parameter</param>
        /// <returns>Return hash length result</returns>
        public SixnetHashLengthResult HashLength(SixnetCacheServer server, SixnetHashLengthParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetHashLengthParameter)}.{nameof(SixnetHashLengthParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetHashLengthStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetHashLengthResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetHashLengthStatement(SixnetHashLengthParameter parameter)
        {
            var script = $@"local pv=redis.call('HLEN',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region HashKeys

        /// <summary>
        /// Returns all field names in the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Hash key parameter</param>
        /// <returns>Return hash keys result</returns>
        public SixnetHashKeysResult HashKeys(SixnetCacheServer server, SixnetHashKeysParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetHashKeysParameter)}.{nameof(SixnetHashKeysParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetHashKeysStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SixnetHashKeysResult()
            {
                Success = true,
                HashKeys = result.Select(c => { string key = c; return key; }).ToList(),
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetHashKeysStatement(SixnetHashKeysParameter parameter)
        {
            var script = $@"local pv=redis.call('HKEYS',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">Hash increment parameter</param>
        /// <returns>Return hash increment result</returns>
        public SixnetHashIncrementResult HashIncrement(SixnetCacheServer server, SixnetHashIncrementParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetHashIncrementParameter)}.{nameof(SixnetHashIncrementParameter.Key)}");
            }
            if (parameter?.IncrementValue == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetHashIncrementParameter)}.{nameof(SixnetHashIncrementParameter.IncrementValue)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var cacheKey = parameter.Key.GetActualKey();
            var newValue = parameter.IncrementValue;
            var dataType = parameter.IncrementValue.GetType();
            var integerValue = false;
            var typeCode = Type.GetTypeCode(dataType);
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
            var statement = GetHashIncrementStatement(parameter, integerValue, cacheKey);
            var newCacheValue = ExecuteStatement(server, database, statement);
            if (integerValue)
            {
                newValue = SixnetObjectExtensions.ConvertTo((long)newCacheValue, dataType);
            }
            else
            {
                newValue = SixnetObjectExtensions.ConvertTo((double)newCacheValue, dataType);
            }
            return new SixnetHashIncrementResult()
            {
                Success = true,
                NewValue = newValue,
                Key = cacheKey,
                HashField = parameter.HashField,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetHashIncrementStatement(SixnetHashIncrementParameter parameter, bool integerValue, string cacheKey)
        {
            var keys = new RedisKey[1] { cacheKey };
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            var values = new RedisValue[]
            {
                parameter.HashField,
                parameter.IncrementValue,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                SixnetRedisManager.GetTotalSeconds(expire.Item2),//expire time seconds
            };
            var script = "";

            script = @$"local obv=redis.call('{(integerValue ? "HINCRBY" : "HINCRBYFLOAT")}',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript(-2)}
return obv";
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = values,
                Flags = cmdFlags
            };
        }

        #endregion

        #region HashGet

        /// <summary>
        /// Returns the value associated with field in the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Hash get parameter</param>
        /// <returns>Return hash get result</returns>
        public SixnetHashGetResult HashGet(SixnetCacheServer server, SixnetHashGetParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetHashGetParameter)}.{nameof(SixnetHashGetParameter.Key)}");
            }
            if (string.IsNullOrWhiteSpace(parameter?.HashField))
            {
                throw new ArgumentNullException($"{nameof(SixnetHashGetParameter)}.{nameof(SixnetHashGetParameter.HashField)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetHashGetStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SixnetHashGetResult()
            {
                Success = true,
                Value = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetHashGetStatement(SixnetHashGetParameter parameter)
        {
            var script = $@"local pv=redis.call('HGET',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var keys = new RedisKey[1]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[4]
            {
                parameter.HashField,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region HashGetAll

        /// <summary>
        /// Returns all fields and values of the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Hash get all parameter</param>
        /// <returns>Return hash get all result</returns>
        public SixnetHashGetAllResult HashGetAll(SixnetCacheServer server, SixnetHashGetAllParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetHashGetAllParameter)}.{nameof(SixnetHashGetAllParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetHashGetAllStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            var values = new Dictionary<string, dynamic>(result.Length / 2);
            for (var i = 0; i < result.Length; i += 2)
            {
                values[result[i]] = result[i + 1];
            }
            return new SixnetHashGetAllResult()
            {
                Success = true,
                HashValues = values,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetHashGetAllStatement(SixnetHashGetAllParameter parameter)
        {
            var script = $@"local pv=redis.call('HGETALL',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[1]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[3]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region HashExists

        /// <summary>
        /// Returns if field is an existing field in the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Options</param>
        /// <returns>hash exists result</returns>
        public SixnetHashExistsResult HashExist(SixnetCacheServer server, SixnetHashExistsParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetHashExistsParameter)}.{nameof(SixnetHashExistsParameter.Key)}");
            }
            if (string.IsNullOrWhiteSpace(parameter?.HashField))
            {
                throw new ArgumentNullException($"{nameof(SixnetHashExistsParameter)}.{nameof(SixnetHashExistsParameter.HashField)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetHashExistStatement(parameter);
            var result = (int)ExecuteStatement(server, database, statement);
            return new SixnetHashExistsResult()
            {
                Success = true,
                HasField = result == 1,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetHashExistStatement(SixnetHashExistsParameter parameter)
        {
            var script = $@"local pv=redis.call('HEXISTS',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var keys = new RedisKey[1]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[4]
            {
                parameter.HashField,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region HashDelete

        /// <summary>
        /// Removes the specified fields from the hash stored at key. Non-existing fields
        /// are ignored. Non-existing keys are treated as empty hashes and this options returns 0
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Hash delete parameter</param>
        /// <returns>Return hash delete result</returns>
        public SixnetHashDeleteResult HashDelete(SixnetCacheServer server, SixnetHashDeleteParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetHashDeleteParameter)}.{nameof(SixnetHashDeleteParameter.Key)}");
            }
            if (parameter.HashFields.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(SixnetHashDeleteParameter)}.{nameof(SixnetHashDeleteParameter.HashFields)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetHashDeleteStatement(parameter);
            var result = (int)ExecuteStatement(server, database, statement);
            return new SixnetHashDeleteResult()
            {
                Success = result > 0,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetHashDeleteStatement(SixnetHashDeleteParameter parameter)
        {
            var values = new RedisValue[parameter.HashFields.Count + 3];
            var valueParameters = new string[parameter.HashFields.Count];
            for (var i = 0; i < parameter.HashFields.Count; i++)
            {
                values[i] = parameter.HashFields[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            values[values.Length - 3] = true;//refresh current time
            values[values.Length - 2] = SixnetRedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = 0;//expire time seconds
            var script = $@"local pv=redis.call('HDEL',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(valueParameters.Length - 2)}
return pv";
            var keys = new RedisKey[1]
            {
                parameter.Key.GetActualKey()
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = values,
                Flags = cmdFlags
            };
        }

        #endregion

        #region HashDecrement

        /// <summary>
        /// Decrement the specified field of an hash stored at key, and representing a floating
        ///  point number, by the specified decrement. If the field does not exist, it is
        ///  set to 0 before performing the operation.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Hash decrement parameter</param>
        /// <returns>Return hash decrement result</returns>
        public SixnetHashDecrementResult HashDecrement(SixnetCacheServer server, SixnetHashDecrementParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetHashDecrementParameter)}.{nameof(SixnetHashDecrementParameter.Key)}");
            }
            if (parameter?.DecrementValue == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetHashDecrementParameter)}.{nameof(SixnetHashDecrementParameter.DecrementValue)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var dataType = parameter.DecrementValue.GetType();
            var typeCode = Type.GetTypeCode(dataType);
            dynamic newValue = parameter.DecrementValue;
            var cacheKey = parameter.Key.GetActualKey();
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
            var statement = GetHashDecrementStatement(parameter, integerValue, cacheKey);
            var newCacheValue = ExecuteStatement(server, database, statement);
            if (integerValue)
            {
                newValue = SixnetObjectExtensions.ConvertTo((long)newCacheValue, dataType);
            }
            else
            {
                newValue = SixnetObjectExtensions.ConvertTo((double)newCacheValue, dataType);
            }
            return new SixnetHashDecrementResult()
            {
                Success = true,
                NewValue = newValue,
                Key = cacheKey,
                HashField = parameter.HashField,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetHashDecrementStatement(SixnetHashDecrementParameter parameter, bool integerValue, string cacheKey)
        {
            var keys = new RedisKey[1] { cacheKey };
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            var values = new RedisValue[]
            {
                parameter.HashField,
                -parameter.DecrementValue,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                SixnetRedisManager.GetTotalSeconds(expire.Item2),//expire time seconds
            };
            var script = "";
            script = @$"local obv=redis.call('{(integerValue ? "HINCRBY" : "HINCRBYFLOAT")}',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript(-2)}
return obv";
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = values,
                Flags = cmdFlags
            };
        }

        #endregion

        #region HashScan

        /// <summary>
        /// The HSCAN options is used to incrementally iterate over a hash
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Hash scan parameter</param>
        /// <returns>Return hash scan result</returns>
        public SixnetHashScanResult HashScan(SixnetCacheServer server, SixnetHashScanParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetHashScanParameter)}.{nameof(SixnetHashScanParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetHashScanStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
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
            return new SixnetHashScanResult()
            {
                Success = true,
                Cursor = newCursor,
                HashValues = values ?? new Dictionary<string, dynamic>(0),
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetHashScanStatement(SixnetHashScanParameter parameter)
        {
            var script = $@"{GetRefreshExpirationScript(1)}
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
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[6]
            {
                parameter.Cursor,
                SixnetRedisManager.GetMatchPattern(parameter.Pattern,parameter.PatternType),
                parameter.PageSize,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">Set remove parameter</param>
        /// <returns>Return set remove result</returns>
        public SixnetSetRemoveResult SetRemove(SixnetCacheServer server, SixnetSetRemoveParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSetRemoveParameter)}.{nameof(SixnetSetRemoveParameter.Key)}");
            }
            if (parameter.Members.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SixnetSetRemoveParameter)}.{nameof(SixnetSetRemoveParameter.Members)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSetRemoveStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetSetRemoveResult()
            {
                Success = true,
                RemoveCount = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSetRemoveStatement(SixnetSetRemoveParameter parameter)
        {
            var values = new RedisValue[parameter.Members.Count + 3];
            var valueParameters = new string[parameter.Members.Count];
            for (var i = 0; i < parameter.Members.Count; i++)
            {
                values[i] = parameter.Members[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            values[values.Length - 3] = true;//refresh current time
            values[values.Length - 2] = SixnetRedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = 0;//expire time seconds
            var script = $@"local obv=redis.call('SREM',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(parameter.Members.Count - 2)}
return obv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = values,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">Set random members parameter</param>
        /// <returns>Return set random members result</returns>
        public SixnetSetRandomMembersResult SetRandomMembers(SixnetCacheServer server, SixnetSetRandomMembersParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSetRandomMembersParameter)}.{nameof(SixnetSetRandomMembersParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSetRandomMembersStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SixnetSetRandomMembersResult()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSetRandomMembersStatement(SixnetSetRandomMembersParameter parameter)
        {
            var script = $@"local pv=redis.call('SRANDMEMBER',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var keys = new RedisKey[]
             {
                parameter.Key.GetActualKey()
             };
            var parameters = new RedisValue[]
            {
                parameter.Count,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SetRandomMember

        /// <summary>
        /// Return a random element from the set value stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Set random member parameter</param>
        /// <returns>Return set random member</returns>
        public SixnetSetRandomMemberResult SetRandomMember(SixnetCacheServer server, SixnetSetRandomMemberParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSetRandomMemberParameter)}.{nameof(SixnetSetRandomMemberParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSetRandomMemberStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SixnetSetRandomMemberResult()
            {
                Success = true,
                Member = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSetRandomMemberStatement(SixnetSetRandomMemberParameter parameter)
        {
            var script = $@"local pv=redis.call('SRANDMEMBER',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SetPop

        /// <summary>
        /// Removes and returns a random element from the set value stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Set pop parameter</param>
        /// <returns>Return set pop result</returns>
        public SixnetSetPopResult SetPop(SixnetCacheServer server, SixnetSetPopParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSetPopParameter)}.{nameof(SixnetSetPopParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSetPopStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SixnetSetPopResult()
            {
                Success = true,
                PopValue = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSetPopStatement(SixnetSetPopParameter parameter)
        {
            var script = $@"local pv=redis.call('SPOP',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">Set move parameter</param>
        /// <returns>Return set move result</returns>
        public SixnetSetMoveResult SetMove(SixnetCacheServer server, SixnetSetMoveParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.SourceKey))
            {
                throw new ArgumentNullException($"{nameof(SixnetSetMoveParameter)}.{nameof(SixnetSetMoveParameter.SourceKey)}");
            }
            if (string.IsNullOrWhiteSpace(parameter?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SixnetSetMoveParameter)}.{nameof(SixnetSetMoveParameter.DestinationKey)}");
            }
            if (string.IsNullOrEmpty(parameter?.MoveMember))
            {
                throw new ArgumentNullException($"{nameof(SixnetSetMoveParameter)}.{nameof(SixnetSetMoveParameter.MoveMember)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSetMoveStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SixnetSetMoveResult()
            {
                Success = result == "1",
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSetMoveStatement(SixnetSetMoveParameter parameter)
        {
            var script = $@"local pv=redis.call('SMOVE',{Keys(1)},{Keys(2)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
{GetRefreshExpirationScript(2, 1)}
return pv";
            var allowSliding = SixnetRedisManager.AllowSlidingExpiration();
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[]
            {
                parameter.SourceKey.GetActualKey(),
                parameter.DestinationKey.GetActualKey(),
            };
            var parameters = new RedisValue[]
            {
                parameter.MoveMember,
                true,//refresh current time
                allowSliding,//whether allow set refresh time
                0,//expire time seconds
                parameter.Expiration==null,//refresh current time-destination key
                expire.Item1&&allowSliding,//whether allow set refresh time-destination key
                SixnetRedisManager.GetTotalSeconds(expire.Item2)//expire time seconds-destination key
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SetMembers

        /// <summary>
        /// Returns all the members of the set value stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Set members parameter</param>
        /// <returns>Return set members result</returns>
        public SixnetSetMembersResult SetMembers(SixnetCacheServer server, SixnetSetMembersParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSetMembersParameter)}.{nameof(SixnetSetMembersParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSetMembersStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SixnetSetMembersResult()
            {
                Success = true,
                Members = result?.Select(c => { string member = c; return member; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSetMembersStatement(SixnetSetMembersParameter parameter)
        {
            var script = $@"local pv=redis.call('SMEMBERS',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SetLength

        /// <summary>
        /// Returns the set cardinality (number of elements) of the set stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Set length parameter</param>
        /// <returns>Return set length result</returns>
        public SixnetSetLengthResult SetLength(SixnetCacheServer server, SixnetSetLengthParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSetLengthParameter)}.{nameof(SixnetSetLengthParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSetLengthStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetSetLengthResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSetLengthStatement(SixnetSetLengthParameter parameter)
        {
            var script = $@"local pv=redis.call('SCARD',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SetContains

        /// <summary>
        /// Returns if member is a member of the set stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Set contains parameter</param>
        /// <returns>Return set contains result</returns>
        public SixnetSetContainsResult SetContains(SixnetCacheServer server, SixnetSetContainsParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSetContainsParameter)}.{nameof(SixnetSetContainsParameter.Key)}");
            }
            if (parameter.Member == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetSetContainsParameter)}.{nameof(SixnetSetContainsParameter.Member)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSetContainsStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SixnetSetContainsResult()
            {
                Success = true,
                ContainsValue = result == "1",
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSetContainsStatement(SixnetSetContainsParameter parameter)
        {
            var script = $@"local pv=redis.call('SISMEMBER',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Member,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SetCombine

        /// <summary>
        /// Returns the members of the set resulting from the specified operation against
        /// the given sets.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Set combine parameter</param>
        /// <returns>Return set combine result</returns>
        public SixnetSetCombineResult SetCombine(SixnetCacheServer server, SixnetSetCombineParameter parameter)
        {
            if (parameter?.Keys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SixnetSetCombineParameter)}.{nameof(SixnetSetCombineParameter.Keys)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSetCombineStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SixnetSetCombineResult()
            {
                Success = true,
                CombineValues = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSetCombineStatement(SixnetSetCombineParameter parameter)
        {
            var keys = new RedisKey[parameter.Keys.Count];
            var keyParameters = new List<string>(parameter.Keys.Count);
            for (var i = 0; i < parameter.Keys.Count; i++)
            {
                keys[i] = parameter.Keys[i].GetActualKey();
                keyParameters.Add($"{Keys(i + 1)}");
            }
            var script = $@"local pv=redis.call('{SixnetRedisManager.GetSetCombineCommand(parameter.CombineOperation)}',{string.Join(",", keyParameters)})
{GetRefreshExpirationScript(-2, keyCount: keys.Length)}
return pv";
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SetCombineAndStore

        /// <summary>
        /// This options is equal to SetCombine, but instead of returning the resulting set,
        ///  it is stored in destination. If destination already exists, it is overwritten.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Set combine and store parameter</param>
        /// <returns>Return set combine and store result</returns>
        public SixnetSetCombineAndStoreResult SetCombineAndStore(SixnetCacheServer server, SixnetSetCombineAndStoreParameter parameter)
        {
            if (parameter?.SourceKeys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SixnetSetCombineAndStoreParameter)}.{nameof(SixnetSetCombineAndStoreParameter.SourceKeys)}");
            }
            if (string.IsNullOrWhiteSpace(parameter.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SixnetSetCombineAndStoreParameter)}.{nameof(SixnetSetCombineAndStoreParameter.DestinationKey)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSetCombineAndStoreStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetSetCombineAndStoreResult()
            {
                Success = true,
                Count = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSetCombineAndStoreStatement(SixnetSetCombineAndStoreParameter parameter)
        {
            var keys = new RedisKey[parameter.SourceKeys.Count + 1];
            var keyParameters = new List<string>(parameter.SourceKeys.Count + 1);
            keys[0] = parameter.DestinationKey.GetActualKey();
            keyParameters.Add($"{Keys(1)}");
            for (var i = 0; i < parameter.SourceKeys.Count; i++)
            {
                keys[i + 1] = parameter.SourceKeys[i].GetActualKey();
                keyParameters.Add($"{Keys(i + 2)}");
            }
            var script = $@"local pv=redis.call('{SixnetRedisManager.GetSetCombineStoreCommand(parameter.CombineOperation)}STORE',{string.Join(",", keyParameters)})
{GetRefreshExpirationScript(-2, 1, keyCount: keys.Length - 1)}
{GetRefreshExpirationScript(1)}
return pv";
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            bool allowSliding = SixnetRedisManager.AllowSlidingExpiration();
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                allowSliding,//whether allow set refresh time
                0,//expire time seconds
                parameter.Expiration==null,// des key
                expire.Item1&&allowSliding,//des key
                SixnetRedisManager.GetTotalSeconds(expire.Item2)
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SetAdd

        /// <summary>
        /// Add the specified member to the set stored at key. Specified members that are
        /// already a member of this set are ignored. If key does not exist, a new set is
        /// created before adding the specified members.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Set add parameter</param>
        /// <returns>Return set add result</returns>
        public SixnetSetAddResult SetAdd(SixnetCacheServer server, SixnetSetAddParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSetAddParameter)}.{nameof(SixnetSetAddParameter.Key)}");
            }
            if (parameter.Members.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SixnetSetAddParameter)}.{nameof(SixnetSetAddParameter.Members)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSetAddStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetSetAddResult()
            {
                Success = result > 0,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSetAddStatement(SixnetSetAddParameter parameter)
        {
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            var values = new RedisValue[parameter.Members.Count + 3];
            var valueParameters = new string[parameter.Members.Count];
            for (var i = 0; i < parameter.Members.Count; i++)
            {
                values[i] = parameter.Members[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            values[values.Length - 3] = parameter.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && SixnetRedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = SixnetRedisManager.GetTotalSeconds(expire.Item2);//expire time seconds
            var script = $@"local obv=redis.call('SADD',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(parameter.Members.Count - 2)}
return obv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = values,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">Sorted set score parameter</param>
        /// <returns>Return sorted set score result</returns>
        public SixnetSortedSetScoreResult SortedSetScore(SixnetCacheServer server, SixnetSortedSetScoreParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetScoreParameter)}.{nameof(SixnetSortedSetScoreParameter.Key)}");
            }
            if (parameter.Member == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetScoreParameter)}.{nameof(SixnetSortedSetScoreParameter.Member)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortedSetScoreStatement(parameter);
            var result = (double?)ExecuteStatement(server, database, statement);
            return new SixnetSortedSetScoreResult()
            {
                Success = true,
                Score = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortedSetScoreStatement(SixnetSortedSetScoreParameter parameter)
        {
            var script = $@"local pv=redis.call('ZSCORE',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Member,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SortedSetRemoveRangeByValue

        /// <summary>
        /// When all the elements in a sorted set are inserted with the same score, in order
        /// to force lexicographical ordering, this options removes all elements in the sorted
        /// set stored at key between the lexicographical range specified by min and max.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Sorted set remove range by value parameter</param>
        /// <returns>Return sorted set remove range by value result</returns>
        public SixnetSortedSetRemoveRangeByValueResult SortedSetRemoveRangeByValue(SixnetCacheServer server, SixnetSortedSetRemoveRangeByValueParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetRemoveRangeByValueParameter)}.{nameof(SixnetSortedSetRemoveRangeByValueParameter.Key)}");
            }
            if (parameter.MinValue == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetRemoveRangeByValueParameter)}.{nameof(SixnetSortedSetRemoveRangeByValueParameter.MinValue)}");
            }
            if (parameter.MaxValue == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetRemoveRangeByValueParameter)}.{nameof(SixnetSortedSetRemoveRangeByValueParameter.MaxValue)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortedSetRemoveRangeByValueStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetSortedSetRemoveRangeByValueResult()
            {
                RemoveCount = result,
                Success = true,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortedSetRemoveRangeByValueStatement(SixnetSortedSetRemoveRangeByValueParameter parameter)
        {
            var script = $@"local pv=redis.call('ZREMRANGEBYLEX',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                FormatSortedSetRangeBoundary(parameter.MinValue,true,parameter.Exclude),
                FormatSortedSetRangeBoundary(parameter.MaxValue,false,parameter.Exclude),
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SortedSetRemoveRangeByScore

        /// <summary>
        /// Removes all elements in the sorted set stored at key with a score between min
        ///  and max (inclusive by default).
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Sorted set remove range by score parameter</param>
        /// <returns>Return sorted set remove range by score result</returns>
        public SixnetSortedSetRemoveRangeByScoreResult SortedSetRemoveRangeByScore(SixnetCacheServer server, SixnetSortedSetRemoveRangeByScoreParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetRemoveRangeByScoreParameter)}.{nameof(SixnetSortedSetRemoveRangeByScoreParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortedSetRemoveRangeByScoreStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetSortedSetRemoveRangeByScoreResult()
            {
                RemoveCount = result,
                Success = true,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortedSetRemoveRangeByScoreStatement(SixnetSortedSetRemoveRangeByScoreParameter parameter)
        {
            var script = $@"local pv=redis.call('ZREMRANGEBYSCORE',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                FormatSortedSetScoreRangeBoundary(parameter.Start,true,parameter.Exclude),
                FormatSortedSetScoreRangeBoundary(parameter.Stop,false,parameter.Exclude),
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">Sorted set remove range by rank parameter</param>
        /// <returns>Return sorted set remove range by rank result</returns>
        public SixnetSortedSetRemoveRangeByRankResult SortedSetRemoveRangeByRank(SixnetCacheServer server, SixnetSortedSetRemoveRangeByRankParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetRemoveRangeByRankParameter)}.{nameof(SixnetSortedSetRemoveRangeByRankParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortedSetRemoveRangeByRankStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetSortedSetRemoveRangeByRankResult()
            {
                RemoveCount = result,
                Success = true,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortedSetRemoveRangeByRankStatement(SixnetSortedSetRemoveRangeByRankParameter parameter)
        {
            var script = $@"local pv=redis.call('ZREMRANGEBYRANK',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Start,
                parameter.Stop,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SortedSetRemove

        /// <summary>
        /// Removes the specified members from the sorted set stored at key. Non existing
        /// members are ignored.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Sorted set remove parameter</param>
        /// <returns>sorted set remove result</returns>
        public SixnetSortedSetRemoveResult SortedSetRemove(SixnetCacheServer server, SixnetSortedSetRemoveParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetRemoveParameter)}.{nameof(SixnetSortedSetRemoveParameter.Key)}");
            }
            if (parameter.RemoveMembers.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SixnetSortedSetRemoveParameter)}.{nameof(SixnetSortedSetRemoveParameter.RemoveMembers)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortedSetRemoveStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetSortedSetRemoveResult()
            {
                Success = true,
                RemoveCount = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortedSetRemoveStatement(SixnetSortedSetRemoveParameter parameter)
        {
            var values = new RedisValue[parameter.RemoveMembers.Count + 3];
            var valueParameters = new string[parameter.RemoveMembers.Count];
            for (var i = 0; i < parameter.RemoveMembers.Count; i++)
            {
                values[i] = parameter.RemoveMembers[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            values[values.Length - 3] = true;//refresh current time
            values[values.Length - 2] = SixnetRedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = 0;//expire time seconds
            var script = $@"local obv=redis.call('ZREM',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(parameter.RemoveMembers.Count - 2)}
return obv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = values,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SortedSetRank

        /// <summary>
        /// Returns the rank of member in the sorted set stored at key, by default with the
        /// scores ordered from low to high. The rank (or index) is 0-based, which means
        /// that the member with the lowest score has rank 0.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>sorted set rank result</returns>
        public SixnetSortedSetRankResult SortedSetRank(SixnetCacheServer server, SixnetSortedSetRankParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetRankParameter)}.{nameof(SixnetSortedSetRankParameter.Key)}");
            }
            if (parameter.Member == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetRankParameter)}.{nameof(SixnetSortedSetRankParameter.Member)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortedSetRankStatement(parameter);
            var result = (long?)ExecuteStatement(server, database, statement);
            return new SixnetSortedSetRankResult()
            {
                Success = true,
                Rank = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortedSetRankStatement(SixnetSortedSetRankParameter parameter)
        {
            var script = $@"local pv=redis.call('Z{(parameter.Order == SixnetCacheOrder.Descending ? "REV" : "")}RANK',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Member,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SortedSetRangeByValue

        /// <summary>
        /// When all the elements in a sorted set are inserted with the same score, in order
        /// to force lexicographical ordering, this options returns all the elements in the
        /// sorted set at key with a value between min and max.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Sorted set range by value parameter</param>
        /// <returns>sorted set range by value result</returns>
        public SixnetSortedSetRangeByValueResult SortedSetRangeByValue(SixnetCacheServer server, SixnetSortedSetRangeByValueParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetRemoveRangeByValueParameter)}.{nameof(SixnetSortedSetRemoveRangeByValueParameter.Key)}");
            }
            if (parameter.MinValue == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetRemoveRangeByValueParameter)}.{nameof(SixnetSortedSetRemoveRangeByValueParameter.MinValue)}");
            }
            if (parameter.MaxValue == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetRemoveRangeByValueParameter)}.{nameof(SixnetSortedSetRemoveRangeByValueParameter.MaxValue)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByValueStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SixnetSortedSetRangeByValueResult()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortedSetRangeByValueStatement(SixnetSortedSetRangeByValueParameter parameter)
        {
            var command = "ZRANGEBYLEX";
            string beginValue = string.Empty;
            string endValue = string.Empty;
            if (parameter.Order == SixnetCacheOrder.Descending)
            {
                command = "ZREVRANGEBYLEX";
                beginValue = FormatSortedSetRangeBoundary(parameter.MaxValue, false, parameter.Exclude);
                endValue = FormatSortedSetRangeBoundary(parameter.MinValue, true, parameter.Exclude);
            }
            else
            {
                beginValue = FormatSortedSetRangeBoundary(parameter.MinValue, true, parameter.Exclude);
                endValue = FormatSortedSetRangeBoundary(parameter.MaxValue, false, parameter.Exclude);
            }
            var script = $@"local pv=redis.call('{command}',{Keys(1)},{Arg(1)},{Arg(2)},'LIMIT',{Arg(3)},{Arg(4)})
{GetRefreshExpirationScript(2)}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                beginValue,
                endValue,
                parameter.Offset,
                parameter.Count,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">Option</param>
        /// <returns>Return sorted set range by score with scores result</returns>
        public SixnetSortedSetRangeByScoreWithScoresResult SortedSetRangeByScoreWithScores(SixnetCacheServer server, SixnetSortedSetRangeByScoreWithScoresParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetRangeByScoreWithScoresParameter)}.{nameof(SixnetSortedSetRangeByScoreWithScoresParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByScoreWithScoresStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            List<SixnetSortedSetMember> members = new List<SixnetSortedSetMember>(result?.Length / 2 ?? 0);
            for (var i = 0; i < result.Length; i += 2)
            {
                var value = result[i];
                double.TryParse(result[i + 1], out var score);
                members.Add(new SixnetSortedSetMember
                {
                    Value = value,
                    Score = score
                });
            }
            return new SixnetSortedSetRangeByScoreWithScoresResult()
            {
                Success = true,
                Members = members,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortedSetRangeByScoreWithScoresStatement(SixnetSortedSetRangeByScoreWithScoresParameter parameter)
        {
            var command = "ZRANGEBYSCORE";
            string beginValue = "";
            string endValue = "";
            if (parameter.Order == SixnetCacheOrder.Descending)
            {
                command = "ZREVRANGEBYSCORE";
                beginValue = FormatSortedSetScoreRangeBoundary(parameter.Stop, false, parameter.Exclude);
                endValue = FormatSortedSetScoreRangeBoundary(parameter.Start, true, parameter.Exclude);
            }
            else
            {
                beginValue = FormatSortedSetScoreRangeBoundary(parameter.Start, true, parameter.Exclude);
                endValue = FormatSortedSetScoreRangeBoundary(parameter.Stop, false, parameter.Exclude);
            }
            var script = $@"local pv=redis.call('{command}',{Keys(1)},{Arg(1)},{Arg(2)},'WITHSCORES','LIMIT',{Arg(3)},{Arg(4)})
{GetRefreshExpirationScript(2)}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                beginValue,
                endValue,
                parameter.Offset,
                parameter.Count,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">Options</param>
        /// <returns>sorted set range by score result</returns>
        public SixnetSortedSetRangeByScoreResult SortedSetRangeByScore(SixnetCacheServer server, SixnetSortedSetRangeByScoreParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetRangeByScoreParameter)}.{nameof(SixnetSortedSetRangeByScoreParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByScoreStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SixnetSortedSetRangeByScoreResult()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortedSetRangeByScoreStatement(SixnetSortedSetRangeByScoreParameter parameter)
        {
            var command = "ZRANGEBYSCORE";
            string beginValue = "";
            string endValue = "";
            if (parameter.Order == SixnetCacheOrder.Descending)
            {
                command = "ZREVRANGEBYSCORE";
                beginValue = FormatSortedSetScoreRangeBoundary(parameter.Stop, false, parameter.Exclude);
                endValue = FormatSortedSetScoreRangeBoundary(parameter.Start, true, parameter.Exclude);
            }
            else
            {
                beginValue = FormatSortedSetScoreRangeBoundary(parameter.Start, true, parameter.Exclude);
                endValue = FormatSortedSetScoreRangeBoundary(parameter.Stop, false, parameter.Exclude);
            }
            var script = $@"local pv=redis.call('{command}',{Keys(1)},{Arg(1)},{Arg(2)},'LIMIT',{Arg(3)},{Arg(4)})
{GetRefreshExpirationScript(2)}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                beginValue,
                endValue,
                parameter.Offset,
                parameter.Count,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">Option</param>
        /// <returns>Return sorted set range by rank with scores result</returns>
        public SixnetSortedSetRangeByRankWithScoresResult SortedSetRangeByRankWithScores(SixnetCacheServer server, SixnetSortedSetRangeByRankWithScoresParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetRangeByRankWithScoresParameter)}.{nameof(SixnetSortedSetRangeByRankWithScoresParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByRankWithScoresStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            List<SixnetSortedSetMember> members = new List<SixnetSortedSetMember>(result?.Length / 2 ?? 0);
            for (var i = 0; i < result.Length; i += 2)
            {
                var value = result[i];
                double.TryParse(result[i + 1], out var score);
                members.Add(new SixnetSortedSetMember
                {
                    Value = value,
                    Score = score
                });
            }
            return new SixnetSortedSetRangeByRankWithScoresResult()
            {
                Success = true,
                Members = members,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortedSetRangeByRankWithScoresStatement(SixnetSortedSetRangeByRankWithScoresParameter parameter)
        {
            var script = $@"local pv=redis.call('Z{(parameter.Order == SixnetCacheOrder.Descending ? "REV" : "")}RANGE',{Keys(1)},{Arg(1)},{Arg(2)},'WITHSCORES')
{GetRefreshExpirationScript()}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Start,
                parameter.Stop,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">Options</param>
        /// <returns>sorted set range by rank result</returns>
        public SixnetSortedSetRangeByRankResult SortedSetRangeByRank(SixnetCacheServer server, SixnetSortedSetRangeByRankParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetRangeByRankParameter)}.{nameof(SixnetSortedSetRangeByRankParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByRankStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SixnetSortedSetRangeByRankResult()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortedSetRangeByRankStatement(SixnetSortedSetRangeByRankParameter parameter)
        {
            var script = $@"local pv=redis.call('Z{(parameter.Order == SixnetCacheOrder.Descending ? "REV" : "")}RANGE',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Start,
                parameter.Stop,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SortedSetLengthByValue

        /// <summary>
        /// When all the elements in a sorted set are inserted with the same score, in order
        /// to force lexicographical ordering, this options returns the number of elements
        /// in the sorted set at key with a value between min and max.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Option</param>
        /// <returns>Return sorted set lenght by value result</returns>
        public SixnetSortedSetLengthByValueResult SortedSetLengthByValue(SixnetCacheServer server, SixnetSortedSetLengthByValueParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetLengthByValueParameter)}.{nameof(SixnetSortedSetLengthByValueParameter.Key)}");
            }
            if (parameter.MinValue == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetLengthByValueParameter)}.{nameof(SixnetSortedSetLengthByValueParameter.MinValue)}");
            }
            if (parameter.MaxValue == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetLengthByValueParameter)}.{nameof(SixnetSortedSetLengthByValueParameter.MaxValue)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortedSetLengthByValueStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetSortedSetLengthByValueResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortedSetLengthByValueStatement(SixnetSortedSetLengthByValueParameter parameter)
        {
            var script = $@"local pv=redis.call('ZLEXCOUNT',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                $"[{parameter.MinValue}",
                $"[{parameter.MaxValue}",
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SortedSetLength

        /// <summary>
        /// Returns the sorted set cardinality (number of elements) of the sorted set stored
        /// at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Option</param>
        /// <returns>Return sorted set length result</returns>
        public SixnetSortedSetLengthResult SortedSetLength(SixnetCacheServer server, SixnetSortedSetLengthParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetLengthByValueParameter)}.{nameof(SixnetSortedSetLengthByValueParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortedSetLengthStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetSortedSetLengthResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortedSetLengthStatement(SixnetSortedSetLengthParameter parameter)
        {
            var script = $@"local pv=redis.call('ZCARD',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SortedSetIncrement

        /// <summary>
        /// Increments the score of member in the sorted set stored at key by increment.
        /// If member does not exist in the sorted set, it is added with increment as its
        /// score (as if its previous score was 0.0).
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Option</param>
        /// <returns>Return sorted set increment result</returns>
        public SixnetSortedSetIncrementResult SortedSetIncrement(SixnetCacheServer server, SixnetSortedSetIncrementParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetIncrementParameter)}.{nameof(SixnetSortedSetIncrementParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortedSetIncrementStatement(parameter);
            var result = (double)ExecuteStatement(server, database, statement);
            return new SixnetSortedSetIncrementResult()
            {
                Success = true,
                NewScore = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortedSetIncrementStatement(SixnetSortedSetIncrementParameter parameter)
        {
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
             {
                parameter.IncrementScore,
                parameter.Member,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                SixnetRedisManager.GetTotalSeconds(expire.Item2),//expire time seconds
             };
            var script = $@"local pv=redis.call('ZINCRBY',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SortedSetDecrement

        /// <summary>
        /// Decrements the score of member in the sorted set stored at key by decrement.
        /// If member does not exist in the sorted set, it is added with -decrement as its
        /// score (as if its previous score was 0.0).
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Option</param>
        /// <returns>Return sorted set decrement result</returns>
        public SixnetSortedSetDecrementResult SortedSetDecrement(SixnetCacheServer server, SixnetSortedSetDecrementParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetDecrementParameter)}.{nameof(SixnetSortedSetDecrementParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortedSetDecrementStatement(parameter);
            var result = (double)ExecuteStatement(server, database, statement);
            return new SixnetSortedSetDecrementResult()
            {
                Success = true,
                NewScore = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortedSetDecrementStatement(SixnetSortedSetDecrementParameter parameter)
        {
            var script = $@"local pv=redis.call('ZINCRBY',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                -parameter.DecrementScore,
                parameter.Member,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                SixnetRedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region SortedSetCombineAndStore

        /// <summary>
        /// Computes a set operation over multiple sorted sets (optionally using per-set
        /// weights), and stores the result in destination, optionally performing a specific
        /// aggregation (defaults to sum)
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>sorted set combine and store result</returns>
        public SixnetSortedSetCombineAndStoreResult SortedSetCombineAndStore(SixnetCacheServer server, SixnetSortedSetCombineAndStoreParameter parameter)
        {
            if (parameter?.SourceKeys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetCombineAndStoreParameter)}.{nameof(SixnetSortedSetCombineAndStoreParameter.SourceKeys)}");
            }
            if (string.IsNullOrWhiteSpace(parameter.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetCombineAndStoreParameter)}.{nameof(SixnetSortedSetCombineAndStoreParameter.DestinationKey)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortedSetCombineAndStoreStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetSortedSetCombineAndStoreResult()
            {
                Success = true,
                NewSetLength = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortedSetCombineAndStoreStatement(SixnetSortedSetCombineAndStoreParameter parameter)
        {
            var keys = new RedisKey[parameter.SourceKeys.Count + 1];
            var keyParameters = new List<string>(parameter.SourceKeys.Count);
            var weights = new double[parameter.SourceKeys.Count];
            keys[0] = parameter.DestinationKey.GetActualKey();
            for (var i = 0; i < parameter.SourceKeys.Count; i++)
            {
                keys[i + 1] = parameter.SourceKeys[i].GetActualKey();
                keyParameters.Add($"{Keys(i + 2)}");
                weights[i] = parameter.Weights?.ElementAt(i) ?? 1;
            }
            var optionScript = new StringBuilder();
            var script = $@"local pv=redis.call('{SixnetRedisManager.GetSortedSetCombineCommand(parameter.CombineOperation)}',{Keys(1)},'{keyParameters.Count}',{string.Join(",", keyParameters)},'WEIGHTS',{string.Join(",", weights)},'AGGREGATE','{SixnetRedisManager.GetSortedSetAggregateName(parameter.Aggregate)}')
{GetRefreshExpirationScript(1)}
{GetRefreshExpirationScript(-2, 1, keyCount: keys.Length - 1)}
return pv";
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            var allowSliding = SixnetRedisManager.AllowSlidingExpiration();
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                allowSliding,//whether allow set refresh time
                0,//expire time seconds
                parameter.Expiration==null,// des key
                expire.Item1&&allowSliding,//des key
                SixnetRedisManager.GetTotalSeconds(expire.Item2)
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">Options</param>
        /// <returns>sorted set add result</returns>
        public SixnetSortedSetAddResult SortedSetAdd(SixnetCacheServer server, SixnetSortedSetAddParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortedSetAddParameter)}.{nameof(SixnetSortedSetAddParameter.Key)}");
            }
            if (parameter.Members.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SixnetSortedSetAddParameter)}.{nameof(SixnetSortedSetAddParameter.Members)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortedSetAddStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetSortedSetAddResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortedSetAddStatement(SixnetSortedSetAddParameter parameter)
        {
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            var valueCount = parameter.Members.Count * 2;
            var values = new RedisValue[valueCount + 3];
            var valueParameters = new string[valueCount];
            for (var i = 0; i < parameter.Members.Count; i++)
            {
                var member = parameter.Members[i];
                var argIndex = i * 2;
                values[argIndex] = member?.Score;
                values[argIndex + 1] = member?.Value;
                valueParameters[argIndex] = $"{Arg(argIndex + 1)}";
                valueParameters[argIndex + 1] = $"{Arg(argIndex + 2)}";
            }
            values[values.Length - 3] = parameter.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && SixnetRedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = SixnetRedisManager.GetTotalSeconds(expire.Item2);//expire time seconds
            var script = $@"local obv=redis.call('ZADD',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(valueCount - 2)}
return obv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = values,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">Options</param>
        /// <returns>sort result</returns>
        public SixnetSortResult Sort(SixnetCacheServer server, SixnetSortParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortParameter)}.{nameof(SixnetSortParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SixnetSortResult()
            {
                Success = true,
                Values = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortStatement(SixnetSortParameter parameter)
        {
            var script = $@"local obv=redis.call('SORT',{Keys(1)}{(string.IsNullOrWhiteSpace(parameter.By) ? string.Empty : $",'BY','{parameter.By}'")},'LIMIT',{Arg(1)},{Arg(2)}{(parameter.Gets.IsNullOrEmpty() ? string.Empty : $",{string.Join(",", parameter.Gets.Select(c => $"'GET','{c}'"))}")},{(parameter.Order == SixnetCacheOrder.Descending ? "'DESC'" : "'ASC'")}{(parameter.SortType == SixnetCacheSortType.Alphabetic ? ",'ALPHA'" : string.Empty)})
{GetRefreshExpirationScript()}
return obv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Offset,
                parameter.Count,
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">Options</param>
        /// <returns>sort and store result</returns>
        public SixnetSortAndStoreResult SortAndStore(SixnetCacheServer server, SixnetSortAndStoreParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.SourceKey))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortAndStoreParameter)}.{nameof(SixnetSortAndStoreParameter.SourceKey)}");
            }
            if (string.IsNullOrWhiteSpace(parameter?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SixnetSortAndStoreParameter)}.{nameof(SixnetSortAndStoreParameter.DestinationKey)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetSortAndStoreStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetSortAndStoreResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetSortAndStoreStatement(SixnetSortAndStoreParameter parameter)
        {
            var script = $@"local obv=redis.call('SORT',{Keys(1)}{(string.IsNullOrWhiteSpace(parameter.By) ? string.Empty : $",'BY','{parameter.By}'")},'LIMIT',{Arg(1)},{Arg(2)}{(parameter.Gets.IsNullOrEmpty() ? string.Empty : $",{string.Join(",", parameter.Gets.Select(c => $"'GET','{c}'"))}")},{(parameter.Order == SixnetCacheOrder.Descending ? "'DESC'" : "'ASC'")}{(parameter.SortType == SixnetCacheSortType.Alphabetic ? ",'ALPHA'" : string.Empty)},'STORE',{Keys(2)})
{GetRefreshExpirationScript()}
{GetRefreshExpirationScript(3, 1)}
return obv";
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            var allowSliding = SixnetRedisManager.AllowSlidingExpiration();
            var keys = new RedisKey[]
            {
                parameter.SourceKey.GetActualKey(),
                parameter.DestinationKey.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Offset,
                parameter.Count,
                true,//refresh current time
                allowSliding,//whether allow set refresh time
                0,//expire time seconds
                parameter.Expiration==null,//refresh current time-des key
                expire.Item1&&allowSliding,//allow set refresh time-deskey
                SixnetRedisManager.GetTotalSeconds(expire.Item2)//-deskey
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">Options</param>
        /// <returns>key type result</returns>
        public SixnetTypeResult KeyType(SixnetCacheServer server, SixnetTypeParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetTypeParameter)}.{nameof(SixnetTypeParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetKeyTypeStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SixnetTypeResult()
            {
                Success = true,
                KeyType = SixnetRedisManager.GetCacheKeyType(result),
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetKeyTypeStatement(SixnetTypeParameter parameter)
        {
            var script = $@"local obv=redis.call('TYPE',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return obv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region KeyTimeToLive

        /// <summary>
        /// Returns the remaining time to live of a key that has a timeout. This introspection
        /// capability allows a Redis client to check how many seconds a given key will continue
        /// to be part of the dataset.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>key time to live result</returns>
        public SixnetTimeToLiveResult KeyTimeToLive(SixnetCacheServer server, SixnetTimeToLiveParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetTimeToLiveParameter)}.{nameof(SixnetTimeToLiveParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetKeyTimeToLiveStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetTimeToLiveResult()
            {
                Success = true,
                TimeToLiveSeconds = result,
                KeyExist = result != -2,
                Perpetual = result == -1,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetKeyTimeToLiveStatement(SixnetTimeToLiveParameter parameter)
        {
            var script = $@"local obv=redis.call('TTL',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return obv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region KeyRestore

        /// <summary>
        /// Create a key associated with a value that is obtained by deserializing the provided
        /// serialized value (obtained via DUMP). If ttl is 0 the key is created without
        /// any expire, otherwise the specified expire time(in milliseconds) is set.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>key restore result</returns>
        public SixnetRestoreResult KeyRestore(SixnetCacheServer server, SixnetRestoreParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetRestoreParameter)}.{nameof(SixnetRestoreParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetKeyRestoreStatement(parameter);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new SixnetRestoreResult()
            {
                Success = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetKeyRestoreStatement(SixnetRestoreParameter parameter)
        {
            var script = $@"local obv= string.lower(tostring(redis.call('RESTORE',{Keys(1)},'0',{Arg(1)})))=='ok'
{GetRefreshExpirationScript(-1)}
return obv";
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Value,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                SixnetRedisManager.GetTotalSeconds(expire.Item2),//expire time seconds
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region KeyRename

        /// <summary>
        /// Renames key to newkey. It returns an error when the source and destination names
        /// are the same, or when key does not exist.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>key rename result</returns>
        public SixnetRenameResult KeyRename(SixnetCacheServer server, SixnetRenameParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetRenameParameter)}.{nameof(SixnetRenameParameter.Key)}");
            }
            if (string.IsNullOrWhiteSpace(parameter?.NewKey))
            {
                throw new ArgumentNullException($"{nameof(SixnetRenameParameter)}.{nameof(SixnetRenameParameter.NewKey)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetKeyRenameStatement(parameter);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new SixnetRenameResult()
            {
                Success = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetKeyRenameStatement(SixnetRenameParameter parameter)
        {
            var cacheKey = parameter.Key.GetActualKey();
            var newCacheKey = parameter.NewKey.GetActualKey();
            var script = $@"{GetRefreshExpirationScript(-2)}
local obv=string.lower(tostring(redis.call('{(parameter.WhenNewKeyNotExists ? "RENAMENX" : "RENAME")}',{Keys(1)},{Keys(2)})))
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
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region KeyRandom

        /// <summary>
        /// Return a random key from the currently selected database.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>key random result</returns>
        public SixnetRandomResult KeyRandom(SixnetCacheServer server, SixnetRandomParameter parameter)
        {
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetKeyRandomStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SixnetRandomResult()
            {
                Success = true,
                Key = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetKeyRandomStatement(SixnetRandomParameter parameter)
        {
            var script = $@"local obv=redis.call('RANDOMKEY')
if obv
then
    local exkey=obv..'{SixnetRedisManager.ExpirationKeySuffix}' 
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
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region KeyPersist

        /// <summary>
        /// Remove the existing timeout on key, turning the key from volatile (a key with
        /// an expire set) to persistent (a key that will never expire as no timeout is associated).
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>key persist result</returns>
        public SixnetPersistResult KeyPersist(SixnetCacheServer server, SixnetPersistParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetPersistParameter)}.{nameof(SixnetPersistParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetKeyPersistStatement(parameter);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new SixnetPersistResult()
            {
                Success = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetKeyPersistStatement(SixnetPersistParameter parameter)
        {
            var cacheKey = parameter.Key.GetActualKey();
            var keys = new RedisKey[1] { cacheKey };
            var parameters = new RedisValue[0];
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            var script = $@"local obv=redis.call('PERSIST',{Keys(1)})==1
if obv
then
    redis.call('DEL','{GetExpirationKey(cacheKey)}')
end
return obv";
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
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
        /// <param name="parameter">Options</param>
        /// <returns>key move result</returns>
        public SixnetMoveResult KeyMove(SixnetCacheServer server, SixnetMoveParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetMoveParameter)}.{nameof(SixnetMoveParameter.Key)}");
            }
            if (!int.TryParse(parameter.DatabaseName, out var dbIndex) || dbIndex < 0)
            {
                throw new ArgumentException($"{nameof(SixnetMoveParameter)}.{nameof(SixnetMoveParameter.DatabaseName)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetKeyMoveStatement(parameter);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new SixnetMoveResult()
            {
                Success = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetKeyMoveStatement(SixnetMoveParameter parameter)
        {
            var cacheKey = parameter.Key.GetActualKey();
            var keys = new RedisKey[1] { cacheKey };
            var parameters = new RedisValue[1]
            {
                parameter.DatabaseName
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            var script = $@"local obv=redis.call('MOVE',{Keys(1)},{Arg(1)})==1
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
        redis.call('SELECT','{parameter.DatabaseName}')
        redis.call('EXPIRE','{cacheKey}',ct)
        redis.call('SET',exkey,ct,'EX',ct)
    end
end
return obv";
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region KeyMigrate

        /// <summary>
        /// Atomically transfer a key from a source Redis instance to a destination Redis
        /// instance. On success the key is deleted from the original instance by default,
        /// and is guaranteed to exist in the target instance.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>key migrate result</returns>
        public SixnetMigrateKeyResult KeyMigrate(SixnetCacheServer server, SixnetMigrateKeyParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetMigrateKeyParameter)}.{nameof(SixnetMigrateKeyParameter.Key)}");
            }
            if (parameter.Destination == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetMigrateKeyParameter)}.{nameof(SixnetMigrateKeyParameter.Destination)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetKeyMigrateStatement(parameter);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new SixnetMigrateKeyResult()
            {
                Success = true,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetKeyMigrateStatement(SixnetMigrateKeyParameter parameter)
        {
            var cacheKey = parameter.Key.GetActualKey();
            var keys = new RedisKey[1] { cacheKey };
            var parameters = new RedisValue[1]
            {
                parameter.CopyCurrent
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            var script = $@"local obv=string.lower(tostring(redis.call('MIGRATE','{parameter.Destination.Port}','{parameter.Destination.Host}',{Keys(1)},'{parameter.TimeOutMilliseconds}'{(parameter.CopyCurrent ? ",'COPY'" : string.Empty)}{(parameter.ReplaceDestination ? ",'REPLACE'" : string.Empty)})))
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
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region KeyExpire

        /// <summary>
        /// Set a timeout on key. After the timeout has expired, the key will automatically
        /// be deleted. A key with an associated timeout is said to be volatile in Redis
        /// terminology.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>key expire result</returns>
        public SixnetExpireResult KeyExpire(SixnetCacheServer server, SixnetExpireParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetExpireParameter)}.{nameof(SixnetExpireParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetKeyExpireStatement(parameter);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new SixnetExpireResult()
            {
                Success = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetKeyExpireStatement(SixnetExpireParameter parameter)
        {
            var cacheKey = parameter.Key.GetActualKey();
            var expire = SixnetRedisManager.GetExpiration(parameter.Expiration);
            var seconds = SixnetRedisManager.GetTotalSeconds(expire.Item2);
            var keys = new RedisKey[0];
            var parameters = new RedisValue[0];
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            var script = $@"local rs=redis.call('EXPIRE','{cacheKey}','{seconds}')==1
if rs and '{(expire.Item1 && SixnetRedisManager.AllowSlidingExpiration() ? "1" : "0")}'=='1'
then
    redis.call('SET','{GetExpirationKey(cacheKey)}','{seconds}','EX','{seconds}')
end
return rs";
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion;

        #region KeyDump

        /// <summary>
        /// Serialize the value stored at key in a Redis-specific format and return it to
        /// the user. The returned value can be synthesized back into a Redis key using the
        /// RESTORE parameter.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>key dump result</returns>
        public SixnetDumpResult KeyDump(SixnetCacheServer server, SixnetDumpParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetDumpParameter)}.{nameof(SixnetDumpParameter.Key)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetKeyDumpStatement(parameter);
            var result = (byte[])ExecuteStatement(server, database, statement);
            return new SixnetDumpResult()
            {
                Success = true,
                ByteValues = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetKeyDumpStatement(SixnetDumpParameter parameter)
        {
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            var script = $@"local pv=redis.call('DUMP',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            return new SixnetRedisStatement()
            {
                Script = script,
                Keys = keys,
                Parameters = parameters,
                Flags = cmdFlags
            };
        }

        #endregion

        #region KeyDelete

        /// <summary>
        /// Removes the specified keys. A key is ignored if it does not exist.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>key delete result</returns>
        public SixnetDeleteResult KeyDelete(SixnetCacheServer server, SixnetDeleteParameter parameter)
        {
            if (parameter?.Keys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SixnetDeleteParameter)}.{nameof(SixnetDeleteParameter.Keys)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetKeyDeleteStatement(parameter);
            var count = database.RemoteDatabase.KeyDelete(statement.Keys, statement.Flags);
            return new SixnetDeleteResult()
            {
                Success = true,
                DeleteCount = count,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetKeyDeleteStatement(SixnetDeleteParameter parameter)
        {
            var keys = parameter.Keys.Select(c => { RedisKey key = c.GetActualKey(); return key; }).ToArray();
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            return new SixnetRedisStatement()
            {
                Keys = keys,
                Flags = cmdFlags
            };
        }

        #endregion

        #region KeyExist

        /// <summary>
        /// key exist
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns></returns>
        public SixnetExistResult KeyExist(SixnetCacheServer server, SixnetExistParameter parameter)
        {
            if (parameter.Keys.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(SixnetExistParameter)}.{nameof(SixnetExistParameter.Keys)}");
            }
            var database = SixnetRedisManager.GetDatabase(server);
            var statement = GetKeyExistStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SixnetExistResult()
            {
                Success = true,
                KeyCount = result,
                CacheServer = server,
                Database = database
            };
        }

        SixnetRedisStatement GetKeyExistStatement(SixnetExistParameter parameter)
        {
            var redisKeys = new RedisKey[parameter.Keys.Count];
            var redisKeyParameters = new List<string>(parameter.Keys.Count);
            for (var i = 0; i < parameter.Keys.Count; i++)
            {
                redisKeys[i] = parameter.Keys[i].GetActualKey();
                redisKeyParameters.Add($"{Keys(i + 1)}");
            }
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                SixnetRedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            var script = $@"local pv=redis.call('EXISTS',{string.Join(",", redisKeyParameters)})
{GetRefreshExpirationScript(-2)}
return pv";
            return new SixnetRedisStatement()
            {
                Script = script,
                Parameters = parameters,
                Keys = redisKeys,
                Flags = cmdFlags
            };
        }

        #endregion

        #region KeyScan

        /// <summary>
        /// Key scan
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="parameter">parameter</param>
        /// <returns></returns>
        public SixnetScanResult KeyScan(SixnetCacheServer server, SixnetScanParameter parameter)
        {
            var database = SixnetRedisManager.GetDatabase(server);
            var scanResults = (RedisResult[])database.RemoteDatabase.Execute("SCAN", parameter.Cursor, "MATCH", parameter.Pattern);
            return new SixnetScanResult()
            {
                Cursor = (long)scanResults[0],
                Keys = ((string[])scanResults[1])?.Select(c => { SixnetCacheKey key = ConstantCacheKey.Create(c); return key; })?.ToList() ?? new List<SixnetCacheKey>(0)
            };
        }

        #endregion

        #endregion

        #region Server

        #region Get all data base

        /// <summary>
        /// Get all database
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>Return get all database result</returns>
        public SixnetGetAllDataBaseResult GetAllDataBase(SixnetCacheServer server, SixnetGetAllDataBaseParameter parameter)
        {
            if (server == null)
            {
                throw new ArgumentNullException($"{nameof(server)}");
            }
            if (parameter?.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetGetAllDataBaseParameter)}.{nameof(SixnetGetAllDataBaseParameter.EndPoint)}");
            }
            using (var conn = SixnetRedisManager.GetConnection(server, new SixnetCacheEndPoint[1] { parameter.EndPoint }))
            {
                var response = new SixnetGetAllDataBaseResult()
                {
                    Success = true,
                    CacheServer = server,
                    EndPoint = parameter.EndPoint
                };
                var configs = conn.GetServer(string.Format("{0}:{1}", parameter.EndPoint.Host, parameter.EndPoint.Port)).ConfigGet("databases");
                if (!configs.IsNullOrEmpty())
                {
                    var databaseConfig = configs.FirstOrDefault(c => string.Equals(c.Key, "databases", StringComparison.OrdinalIgnoreCase));
                    var dataBaseSize = databaseConfig.Value.ToInt32();
                    var databaseList = new List<SixnetCacheDatabase>(dataBaseSize);
                    for (var d = 0; d < dataBaseSize; d++)
                    {
                        databaseList.Add(new SixnetCacheDatabase()
                        {
                            Index = d,
                            Name = $"{d}"
                        });
                    };
                }
                return response;
            }
        }

        #endregion

        #region Query keys

        /// <summary>
        /// Query keys
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>Return get keys result</returns>
        public SixnetGetKeysResult GetKeys(SixnetCacheServer server, SixnetGetKeysParameter parameter)
        {
            if (server == null)
            {
                throw new ArgumentNullException($"{nameof(server)}");
            }
            if (parameter?.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetGetAllDataBaseParameter)}.{nameof(SixnetGetAllDataBaseParameter.EndPoint)}");
            }
            if (!int.TryParse(server.Database, out int dbIndex))
            {
                throw new SixnetException($"Redis database {server.Database} is invalid");
            }

            var query = parameter.Query;
            var searchString = "*";
            if (query != null && !string.IsNullOrWhiteSpace(query.MateKey))
            {
                switch (query.Type)
                {
                    case SixnetKeyMatchPattern.StartWith:
                        searchString = query.MateKey + "*";
                        break;
                    case SixnetKeyMatchPattern.EndWith:
                        searchString = "*" + query.MateKey;
                        break;
                    default:
                        searchString = string.Format("*{0}*", query.MateKey);
                        break;
                }
            }
            using (var conn = SixnetRedisManager.GetConnection(server, new SixnetCacheEndPoint[1] { parameter.EndPoint }))
            {
                var redisServer = conn.GetServer(string.Format("{0}:{1}", parameter.EndPoint.Host, parameter.EndPoint.Port));
                var keys = redisServer.Keys(dbIndex, searchString, query.PageSize, 0, (query.Page - 1) * query.PageSize, CommandFlags.None);
                var itemList = keys.Select(c => { SixnetCacheKey key = ConstantCacheKey.Create(c); return key; }).ToList();
                var totalCount = redisServer.DatabaseSize(dbIndex);
                var keyItemPaging = new SixnetCachePaging<SixnetCacheKey>(query.Page, query.PageSize, totalCount, itemList);
                return new SixnetGetKeysResult()
                {
                    Success = true,
                    Keys = keyItemPaging,
                    CacheServer = server,
                    EndPoint = parameter.EndPoint,
                    Database = new SixnetRedisDatabase()
                    {
                        Index = dbIndex,
                        Name = dbIndex.ToString()
                    }
                };
            }
        }

        #endregion

        #region Clear data

        /// <summary>
        /// clear database data
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>clear data result</returns>
        public SixnetClearDataResult ClearData(SixnetCacheServer server, SixnetClearDataParameter parameter)
        {
            if (parameter.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetClearDataParameter)}.{nameof(SixnetClearDataParameter.EndPoint)}");
            }
            if (!int.TryParse(server.Database, out int dbIndex))
            {
                throw new SixnetException($"Redis database {server.Database} is invalid");
            }
            var cmdFlags = SixnetRedisManager.GetCommandFlags(parameter.CommandFlags);
            using (var conn = SixnetRedisManager.GetConnection(server, new SixnetCacheEndPoint[1] { parameter.EndPoint }))
            {
                var redisServer = conn.GetServer(string.Format("{0}:{1}", parameter.EndPoint.Host, parameter.EndPoint.Port));
                redisServer.FlushDatabase(dbIndex, cmdFlags);
                return new SixnetClearDataResult()
                {
                    Success = true,
                    CacheServer = server,
                    EndPoint = parameter.EndPoint,
                    Database = new SixnetRedisDatabase()
                    {
                        Index = dbIndex,
                        Name = dbIndex.ToString()
                    }
                };
            }
        }

        #endregion

        #region Get cache item detail

        /// <summary>
        /// get cache item detail
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>get key detail result</returns>
        public SixnetGetDetailResult GetKeyDetail(SixnetCacheServer server, SixnetGetDetailParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SixnetGetDetailParameter)}.{nameof(SixnetGetDetailParameter.Key)}");
            }
            if (parameter.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetGetDetailParameter)}.{nameof(SixnetGetDetailParameter.EndPoint)}");
            }
            if (!int.TryParse(server.Database, out int dbIndex))
            {
                throw new SixnetException($"Redis database {server.Database} is invalid");
            }
            using (var conn = SixnetRedisManager.GetConnection(server, new SixnetCacheEndPoint[1] { parameter.EndPoint }))
            {
                var redisDatabase = conn.GetDatabase(dbIndex);
                var redisKeyType = redisDatabase.KeyType(parameter.Key.GetActualKey());
                var cacheKeyType = SixnetRedisManager.GetCacheKeyType(redisKeyType.ToString());
                var keyItem = new SixnetCacheEntry()
                {
                    Key = parameter.Key.GetActualKey(),
                    Type = cacheKeyType
                };
                switch (cacheKeyType)
                {
                    case SixnetCacheKeyType.String:
                        keyItem.Value = redisDatabase.StringGetAsync(keyItem.Key.GetActualKey());
                        break;
                    case SixnetCacheKeyType.List:
                        var listValues = new List<string>();
                        var listResults = redisDatabase.ListRange(keyItem.Key.GetActualKey(), 0, -1, CommandFlags.None);
                        listValues.AddRange(listResults.Select(c => (string)c));
                        keyItem.Value = listValues;
                        break;
                    case SixnetCacheKeyType.Set:
                        var setValues = new List<string>();
                        var setResults = redisDatabase.SetMembers(keyItem.Key.GetActualKey(), CommandFlags.None);
                        setValues.AddRange(setResults.Select(c => (string)c));
                        keyItem.Value = setValues;
                        break;
                    case SixnetCacheKeyType.SortedSet:
                        var sortSetValues = new List<string>();
                        var sortedResults = redisDatabase.SortedSetRangeByRank(keyItem.Key.GetActualKey());
                        sortSetValues.AddRange(sortedResults.Select(c => (string)c));
                        keyItem.Value = sortSetValues;
                        break;
                    case SixnetCacheKeyType.Hash:
                        var hashValues = new Dictionary<string, string>();
                        var objValues = redisDatabase.HashGetAll(keyItem.Key.GetActualKey());
                        foreach (var obj in objValues)
                        {
                            hashValues.Add(obj.Name, obj.Value);
                        }
                        keyItem.Value = hashValues;
                        break;
                }
                return new SixnetGetDetailResult()
                {
                    Success = true,
                    CacheEntry = keyItem,
                    CacheServer = server,
                    Database = new SixnetCacheDatabase()
                    {
                        Index = dbIndex,
                        Name = dbIndex.ToString()
                    }
                };
            }
        }

        #endregion

        #region Get server configuration

        /// <summary>
        /// get server configuration
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>get server config result</returns>
        public SixnetGetServerConfigurationResult GetServerConfiguration(SixnetCacheServer server, SixnetGetServerConfigurationParameter parameter)
        {
            if (parameter?.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetGetServerConfigurationParameter)}.{nameof(SixnetGetServerConfigurationParameter.EndPoint)}");
            }
            using (var conn = SixnetRedisManager.GetConnection(server, new SixnetCacheEndPoint[1] { parameter.EndPoint }))
            {
                var config = new SixnetRedisServerConfiguration();
                var redisServer = conn.GetServer(string.Format("{0}:{1}", parameter.EndPoint.Host, parameter.EndPoint.Port));
                var configs = redisServer.ConfigGet("*");
                if (!configs.IsNullOrEmpty())
                {
                    #region Configuration info

                    foreach (var cfg in configs)
                    {
                        var key = cfg.Key.ToLower();
                        switch (key)
                        {
                            case "daemonize":
                                config.Daemonize = cfg.Value.ToLower() == "yes";
                                break;
                            case "pidfile":
                                config.PidFile = cfg.Value;
                                break;
                            case "port":
                                var port = 0;
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
                                var logLevel = SixnetCacheLogLevel.Verbose;
                                switch (cfg.Value)
                                {
                                    case "debug":
                                        logLevel = SixnetCacheLogLevel.Debug;
                                        break;
                                    case "verbose":
                                        logLevel = SixnetCacheLogLevel.Verbose;
                                        break;
                                    case "notice":
                                        logLevel = SixnetCacheLogLevel.Notice;
                                        break;
                                    case "warning":
                                        logLevel = SixnetCacheLogLevel.Warning;
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
                                var saveInfos = new List<SixnetDataChangeSaveParameter>();
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
                                    saveInfos.Add(new SixnetDataChangeSaveParameter()
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
                                var masterArray = cfg.Value.LSplit(" ");
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
                                var appendSync = SixnetAppendfSync.EverySecond;
                                switch (cfg.Value)
                                {
                                    case "no":
                                        appendSync = SixnetAppendfSync.No;
                                        break;
                                    case "always":
                                        appendSync = SixnetAppendfSync.Always;
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
                }
                return new SixnetGetServerConfigurationResult()
                {
                    ServerConfiguration = config,
                    Success = true,
                    CacheServer = server,
                    EndPoint = parameter.EndPoint
                };
            }
        }

        #endregion

        #region Save server configuration

        /// <summary>
        /// save server configuration
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="parameter">Options</param>
        /// <returns>save server config result</returns>
        public SixnetSaveServerConfigurationResult SaveServerConfiguration(SixnetCacheServer server, SixnetSaveServerConfigurationParameter parameter)
        {
            if (!(parameter?.ServerConfiguration is SixnetRedisServerConfiguration config))
            {
                throw new SixnetException($"{nameof(SixnetSaveServerConfigurationParameter.ServerConfiguration)} is not {nameof(SixnetRedisServerConfiguration)}");
            }
            if (parameter?.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(SixnetSaveServerConfigurationParameter)}.{nameof(SixnetSaveServerConfigurationParameter.EndPoint)}");
            }
            using (var conn = SixnetRedisManager.GetConnection(server, new SixnetCacheEndPoint[1] { parameter.EndPoint }))
            {
                var redisServer = conn.GetServer(string.Format("{0}:{1}", parameter.EndPoint.Host, parameter.EndPoint.Port));
                if (!string.IsNullOrWhiteSpace(config.Host))
                {
                    redisServer.ConfigSet("bind", config.Host);
                }
                if (config.TimeOut >= 0)
                {
                    redisServer.ConfigSet("timeout", config.TimeOut);
                }
                redisServer.ConfigSet("loglevel", config.LogLevel.ToString().ToLower());
                var saveConfigValue = string.Empty;
                if (!config.SaveConfiguration.IsNullOrEmpty())
                {
                    var configList = new List<string>();
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
                    var masterUrl = string.Format("{0} {1}", config.Host, config.Port > 0 ? config.Port : 6379);
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
                var appendfSyncVal = "everysec";
                switch (config.AppendfSync)
                {
                    case SixnetAppendfSync.Always:
                        appendfSyncVal = "always";
                        break;
                    case SixnetAppendfSync.EverySecond:
                        appendfSyncVal = "everysec";
                        break;
                    case SixnetAppendfSync.No:
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
                redisServer.ConfigRewrite();
                return new SixnetSaveServerConfigurationResult()
                {
                    Success = true,
                    CacheServer = server,
                    EndPoint = parameter.EndPoint
                };
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
        /// <param name="keyOffset">Expire key index</param>
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
        exkey=ckey..'{SixnetRedisManager.ExpirationKeySuffix}' 
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
        exkey=ckey..'{SixnetRedisManager.ExpirationKeySuffix}'
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
            return $"{cacheKey}{SixnetRedisManager.ExpirationKeySuffix}";
        }

        /// <summary>
        /// Format sorted set range boundary
        /// </summary>
        /// <param name="value">Value</param>
        /// <param name="exclude">Exclude type</param>
        /// <returns></returns>
        static string FormatSortedSetRangeBoundary(string value, bool startValue, SixnetBoundaryExclude exclude)
        {
            switch (exclude)
            {
                case SixnetBoundaryExclude.None:
                default:
                    return $"[{value}";
                case SixnetBoundaryExclude.Both:
                    return $"({value}";
                case SixnetBoundaryExclude.Start:
                    return startValue ? $"({value}" : $"[{value}";
                case SixnetBoundaryExclude.Stop:
                    return startValue ? $"[{value}" : $"({value}";
            }
        }

        /// <summary>
        /// Format sorted set range boundary
        /// </summary>
        /// <param name="score">Score vlaue</param>
        /// <param name="startValue">Whether is start score</param>
        /// <param name="exclude">Exclude parameter</param>
        /// <returns></returns>
        static string FormatSortedSetScoreRangeBoundary(double score, bool startValue, SixnetBoundaryExclude exclude)
        {
            switch (exclude)
            {
                case SixnetBoundaryExclude.None:
                default:
                    return score.ToString();
                case SixnetBoundaryExclude.Both:
                    return $"({score}";
                case SixnetBoundaryExclude.Start:
                    return startValue ? $"({score}" : $"{score}";
                case SixnetBoundaryExclude.Stop:
                    return startValue ? $"{score}" : $"({score}";
            }
        }

        static T GetNoDatabaseResponse<T>(SixnetCacheServer server) where T : SixnetCacheResult, new()
        {
            if (SixnetCacher.ThrowOnMissingDatabase)
            {
                throw new SixnetException("No cache database specified");
            }
            return SixnetCacheResult.NoDatabase<T>(server);
        }

        static T GetNoValueResponse<T>(SixnetCacheServer server) where T : SixnetCacheResult, new()
        {
            return SixnetCacheResult.FailResponse<T>("", "No value specified", server);
        }

        static T GetNoKeyResponse<T>(SixnetCacheServer server) where T : SixnetCacheResult, new()
        {
            return SixnetCacheResult.FailResponse<T>("", "No key specified", server);
        }

        RedisResult ExecuteStatement(SixnetCacheServer server, SixnetRedisDatabase database, SixnetRedisStatement statement)
        {
            return database.RemoteDatabase.ScriptEvaluate(statement.Script, statement.Keys, statement.Parameters, statement.Flags);
        }

        #endregion
    }
}
