using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
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
using Sixnet.Cache.String;
using Sixnet.Cache.String.Parameters;
using Sixnet.Cache.String.Results;
using Sixnet.Exceptions;
using StackExchange.Redis;

namespace Sixnet.Cache.Redis
{
    public partial class RedisProvider : ISixnetCacheProvider
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
        public StringSetRangeResult StringSetRange(CacheServer server, StringSetRangeParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringSetRangeParameter)}.{nameof(StringSetRangeParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringSetRangeStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new StringSetRangeResult()
            {
                Success = true,
                CacheServer = server,
                Database = database,
                NewValueLength = (long)result
            };
        }

        RedisStatement GetStringSetRangeStatement(StringSetRangeParameter parameter)
        {
            var script = $@"local len=redis.call('SETRANGE',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return len";
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[] { parameter.Key.GetActualKey() };
            var parameters = new RedisValue[]
            {
                parameter.Offset,
                parameter.Value,
                parameter.Expiration==null,//refresh current time
                expire.Item1 && RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds
            };
            var commandFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public StringSetBitResult StringSetBit(CacheServer server, StringSetBitParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringSetBitParameter)}.{nameof(StringSetBitParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringSetBitStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new StringSetBitResult()
            {
                Success = true,
                OldBitValue = (bool)result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringSetBitStatement(StringSetBitParameter parameter)
        {
            var script = $@"local obv=redis.call('SETBIT',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return obv";
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Offset,
                parameter.Bit,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public StringSetResult StringSet(CacheServer server, StringSetParameter parameter)
        {
            if (parameter?.Items.IsNullOrEmpty() ?? true)
            {
                return GetNoValueResponse<StringSetResult>(server);
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringSetStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new StringSetResult()
            {
                CacheServer = server,
                Database = database,
                Success = true,
                Results = ((RedisValue[])result)?.Select(c => new StringEntrySetResult() { SetSuccess = true, Key = c }).ToList()
            };
        }

        RedisStatement GetStringSetStatement(StringSetParameter parameter)
        {
            var itemCount = parameter.Items.Count;
            var valueCount = itemCount * 5;
            var allowSlidingExpire = RedisManager.AllowSlidingExpiration();
            RedisKey[] setKeys = new RedisKey[itemCount];
            RedisValue[] setValues = new RedisValue[valueCount];
            for (var i = 0; i < itemCount; i++)
            {
                var nowItem = parameter.Items[i];
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
            var script = $@"local skeys={{}}
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
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public StringLengthResult StringLength(CacheServer server, StringLengthParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringLengthParameter)}.{nameof(StringLengthParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringLengthStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new StringLengthResult()
            {
                Success = true,
                Length = (long)result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringLengthStatement(StringLengthParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public StringIncrementResult StringIncrement(CacheServer server, StringIncrementParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringIncrementParameter)}.{nameof(StringIncrementParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringIncrementStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new StringIncrementResult()
            {
                Success = true,
                NewValue = (long)result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringIncrementStatement(StringIncrementParameter parameter)
        {
            var script = $@"local obv=redis.call('INCRBY',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return obv";
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Value,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public StringGetWithExpiryResult StringGetWithExpiry(CacheServer server, StringGetWithExpiryParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetWithExpiryParameter)}.{nameof(StringGetWithExpiryParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringGetWithExpiryStatement(parameter);
            var result = (RedisValue[])(ExecuteStatement(server, database, statement));
            return new StringGetWithExpiryResult()
            {
                Success = true,
                Value = result[0],
                Expiry = TimeSpan.FromSeconds((long)result[1]),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringGetWithExpiryStatement(StringGetWithExpiryParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public StringGetSetResult StringGetSet(CacheServer server, StringGetSetParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetSetParameter)}.{nameof(StringGetSetParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringGetSetStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new StringGetSetResult()
            {
                Success = true,
                OldValue = (string)result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringGetSetStatement(StringGetSetParameter parameter)
        {
            var script = $@"local ov=redis.call('GETSET',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return ov";
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.NewValue,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public StringGetRangeResult StringGetRange(CacheServer server, StringGetRangeParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetRangeParameter)}.{nameof(StringGetRangeParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringGetRangeStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new StringGetRangeResult()
            {
                Success = true,
                Value = (string)result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringGetRangeStatement(StringGetRangeParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public StringGetBitResult StringGetBit(CacheServer server, StringGetBitParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetBitParameter)}.{nameof(StringGetBitParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringGetBitStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new StringGetBitResult()
            {
                Success = true,
                Bit = (bool)result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringGetBitStatement(StringGetBitParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public StringGetResult StringGet(CacheServer server, StringGetParameter parameter)
        {
            if (parameter?.Keys.IsNullOrEmpty() ?? true)
            {
                return GetNoKeyResponse<StringGetResult>(server);
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringGetStatement(parameter);
            var result = (RedisValue[])(ExecuteStatement(server, database, statement));
            return new StringGetResult()
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
                Database = database
            };
        }

        RedisStatement GetStringGetStatement(StringGetParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public StringDecrementResult StringDecrement(CacheServer server, StringDecrementParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringDecrementParameter)}.{nameof(StringDecrementParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringDecrementStatement(parameter);
            var result = ExecuteStatement(server, database, statement);
            return new StringDecrementResult()
            {
                Success = true,
                NewValue = (long)result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringDecrementStatement(StringDecrementParameter parameter)
        {
            var script = $@"local obv=redis.call('DECRBY',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return obv";
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Value,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public StringBitPositionResult StringBitPosition(CacheServer server, StringBitPositionParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringBitPositionParameter)}.{nameof(StringBitPositionParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringBitPositionStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new StringBitPositionResult()
            {
                Success = true,
                Position = result,
                HasValue = result >= 0,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringBitPositionStatement(StringBitPositionParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public StringBitOperationResult StringBitOperation(CacheServer server, StringBitOperationParameter parameter)
        {
            if (parameter?.Keys.IsNullOrEmpty() ?? true)
            {
                return GetNoKeyResponse<StringBitOperationResult>(server);
            }
            if (string.IsNullOrWhiteSpace(parameter?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(StringBitOperationParameter)}.{nameof(StringBitOperationParameter.DestinationKey)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringBitOperationStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new StringBitOperationResult()
            {
                Success = true,
                DestinationValueLength = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringBitOperationStatement(StringBitOperationParameter parameter)
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
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            bool allowSlidingExpiration = RedisManager.AllowSlidingExpiration();
            var parameters = new RedisValue[]
            {
                RedisManager.GetBitOperator(parameter.Bitwise),
                parameter.Expiration==null,//refresh current time
                expire.Item1&&allowSlidingExpiration,//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds,
                true,//refresh current time-source key
                allowSlidingExpiration,//whether allow set refresh time-source key,
                0//expire time seconds-source key
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public StringBitCountResult StringBitCount(CacheServer server, StringBitCountParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringBitCountParameter)}.{nameof(StringBitCountParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringBitCountStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new StringBitCountResult()
            {
                Success = true,
                BitNum = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringBitCountStatement(StringBitCountParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public StringAppendResult StringAppend(CacheServer server, StringAppendParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringAppendParameter)}.{nameof(StringAppendParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringAppendStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new StringAppendResult()
            {
                Success = true,
                NewValueLength = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringAppendStatement(StringAppendParameter parameter)
        {
            var script = $@"local obv=redis.call('APPEND',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return obv";
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Value,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public ListTrimResult ListTrim(CacheServer server, ListTrimParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListTrimParameter)}.{nameof(ListTrimParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListTrimStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new ListTrimResult()
            {
                Success = true,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListTrimStatement(ListTrimParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public ListSetByIndexResult ListSetByIndex(CacheServer server, ListSetByIndexParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListSetByIndexParameter)}.{nameof(ListSetByIndexParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListSetByIndexStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new ListSetByIndexResult()
            {
                Success = string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListSetByIndexStatement(ListSetByIndexParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public ListRightPushResult ListRightPush(CacheServer server, ListRightPushParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRightPushParameter)}.{nameof(ListRightPushParameter.Key)}");
            }
            if (parameter?.Values.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentException($"{nameof(ListRightPushParameter)}.{nameof(ListRightPushParameter.Values)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListRightPushStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new ListRightPushResult()
            {
                Success = true,
                NewListLength = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListRightPushStatement(ListRightPushParameter parameter)
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
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            values[values.Length - 3] = parameter.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = RedisManager.GetTotalSeconds(expire.Item2);//expire time seconds
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public ListRightPopLeftPushResult ListRightPopLeftPush(CacheServer server, ListRightPopLeftPushParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.SourceKey))
            {
                throw new ArgumentNullException($"{nameof(ListRightPopLeftPushParameter)}.{nameof(ListRightPopLeftPushParameter.SourceKey)}");
            }
            if (string.IsNullOrWhiteSpace(parameter?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(ListRightPopLeftPushParameter)}.{nameof(ListRightPopLeftPushParameter.DestinationKey)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListRightPopLeftPushStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new ListRightPopLeftPushResult()
            {
                Success = true,
                PopValue = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListRightPopLeftPushStatement(ListRightPopLeftPushParameter parameter)
        {
            var script = $@"local pv=redis.call('RPOPLPUSH',{Keys(1)},{Keys(2)})
{GetRefreshExpirationScript(-2)}
{GetRefreshExpirationScript(1, 1)}
return pv";
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            bool allowSlidingExpiration = RedisManager.AllowSlidingExpiration();
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
                RedisManager.GetTotalSeconds(expire.Item2)//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public ListRightPopResult ListRightPop(CacheServer server, ListRightPopParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRightPopParameter)}.{nameof(ListRightPopParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListRightPopStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new ListRightPopResult()
            {
                Success = true,
                PopValue = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListRightPopStatement(ListRightPopParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public ListRemoveResult ListRemove(CacheServer server, ListRemoveParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRemoveParameter)}.{nameof(ListRemoveParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListRemoveStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new ListRemoveResult()
            {
                Success = true,
                RemoveCount = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListRemoveStatement(ListRemoveParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public ListRangeResult ListRange(CacheServer server, ListRangeParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRangeParameter)}.{nameof(ListRangeParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListRangeStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new ListRangeResult()
            {
                Success = true,
                Values = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListRangeStatement(ListRangeParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public ListLengthResult ListLength(CacheServer server, ListLengthParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListLengthParameter)}.{nameof(ListLengthParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListLengthStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new ListLengthResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListLengthStatement(ListLengthParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public ListLeftPushResult ListLeftPush(CacheServer server, ListLeftPushParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListLeftPushParameter)}.{nameof(ListLeftPushParameter.Key)}");
            }
            if (parameter?.Values.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentException($"{nameof(ListRightPushParameter)}.{nameof(ListRightPushParameter.Values)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListLeftPushStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new ListLeftPushResult()
            {
                Success = true,
                NewListLength = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListLeftPushStatement(ListLeftPushParameter parameter)
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
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            values[values.Length - 3] = parameter.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = RedisManager.GetTotalSeconds(expire.Item2);//expire time seconds
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public ListLeftPopResult ListLeftPop(CacheServer server, ListLeftPopParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListLeftPopParameter)}.{nameof(ListLeftPopParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListLeftPopStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new ListLeftPopResult()
            {
                Success = true,
                PopValue = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListLeftPopStatement(ListLeftPopParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public ListInsertBeforeResult ListInsertBefore(CacheServer server, ListInsertBeforeParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListInsertBeforeParameter)}.{nameof(ListInsertBeforeParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListInsertBeforeStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new ListInsertBeforeResult()
            {
                Success = result > 0,
                NewListLength = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListInsertBeforeStatement(ListInsertBeforeParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public ListInsertAfterResult ListInsertAfter(CacheServer server, ListInsertAfterParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListInsertAfterParameter)}.{nameof(ListInsertAfterParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListInsertAfterStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new ListInsertAfterResult()
            {
                Success = result > 0,
                NewListLength = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListInsertAfterStatement(ListInsertAfterParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public ListGetByIndexResult ListGetByIndex(CacheServer server, ListGetByIndexParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListInsertAfterParameter)}.{nameof(ListInsertAfterParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListGetByIndexStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new ListGetByIndexResult()
            {
                Success = true,
                Value = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListGetByIndexStatement(ListGetByIndexParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public HashValuesResult HashValues(CacheServer server, HashValuesParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashValuesParameter)}.{nameof(HashValuesParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashValuesStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new HashValuesResult()
            {
                Success = true,
                Values = result.Select(c => { dynamic value = c; return value; }).ToList(),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashValuesStatement(HashValuesParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public HashSetResult HashSet(CacheServer server, HashSetParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashSetParameter)}.{nameof(HashSetParameter.Key)}");
            }
            if (parameter?.Items.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(HashSetParameter)}.{nameof(HashSetParameter.Items)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashSetStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new HashSetResult()
            {
                Success = string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashSetStatement(HashSetParameter parameter)
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
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            values[values.Length - 3] = parameter.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = RedisManager.GetTotalSeconds(expire.Item2);//expire time seconds
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public HashLengthResult HashLength(CacheServer server, HashLengthParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashLengthParameter)}.{nameof(HashLengthParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashLengthStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new HashLengthResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashLengthStatement(HashLengthParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public HashKeysResult HashKeys(CacheServer server, HashKeysParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashKeysParameter)}.{nameof(HashKeysParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashKeysStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new HashKeysResult()
            {
                Success = true,
                HashKeys = result.Select(c => { string key = c; return key; }).ToList(),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashKeysStatement(HashKeysParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public HashIncrementResult HashIncrement(CacheServer server, HashIncrementParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashIncrementParameter)}.{nameof(HashIncrementParameter.Key)}");
            }
            if (parameter?.IncrementValue == null)
            {
                throw new ArgumentNullException($"{nameof(HashIncrementParameter)}.{nameof(HashIncrementParameter.IncrementValue)}");
            }
            var database = RedisManager.GetDatabase(server);
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
                newValue = ObjectExtensions.ConvertTo((long)newCacheValue, dataType);
            }
            else
            {
                newValue = ObjectExtensions.ConvertTo((double)newCacheValue, dataType);
            }
            return new HashIncrementResult()
            {
                Success = true,
                NewValue = newValue,
                Key = cacheKey,
                HashField = parameter.HashField,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashIncrementStatement(HashIncrementParameter parameter, bool integerValue, string cacheKey)
        {
            var keys = new RedisKey[1] { cacheKey };
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            var values = new RedisValue[]
            {
                parameter.HashField,
                parameter.IncrementValue,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds
            };
            var script = "";

            script = @$"local obv=redis.call('{(integerValue ? "HINCRBY" : "HINCRBYFLOAT")}',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript(-2)}
return obv";
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public HashGetResult HashGet(CacheServer server, HashGetParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashGetParameter)}.{nameof(HashGetParameter.Key)}");
            }
            if (string.IsNullOrWhiteSpace(parameter?.HashField))
            {
                throw new ArgumentNullException($"{nameof(HashGetParameter)}.{nameof(HashGetParameter.HashField)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashGetStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new HashGetResult()
            {
                Success = true,
                Value = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashGetStatement(HashGetParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public HashGetAllResult HashGetAll(CacheServer server, HashGetAllParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashGetAllParameter)}.{nameof(HashGetAllParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashGetAllStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            var values = new Dictionary<string, dynamic>(result.Length / 2);
            for (var i = 0; i < result.Length; i += 2)
            {
                values[result[i]] = result[i + 1];
            }
            return new HashGetAllResult()
            {
                Success = true,
                HashValues = values,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashGetAllStatement(HashGetAllParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public HashExistsResult HashExist(CacheServer server, HashExistsParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashExistsParameter)}.{nameof(HashExistsParameter.Key)}");
            }
            if (string.IsNullOrWhiteSpace(parameter?.HashField))
            {
                throw new ArgumentNullException($"{nameof(HashExistsParameter)}.{nameof(HashExistsParameter.HashField)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashExistStatement(parameter);
            var result = (int)ExecuteStatement(server, database, statement);
            return new HashExistsResult()
            {
                Success = true,
                HasField = result == 1,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashExistStatement(HashExistsParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public HashDeleteResult HashDelete(CacheServer server, HashDeleteParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashDeleteParameter)}.{nameof(HashDeleteParameter.Key)}");
            }
            if (parameter.HashFields.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(HashDeleteParameter)}.{nameof(HashDeleteParameter.HashFields)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashDeleteStatement(parameter);
            var result = (int)ExecuteStatement(server, database, statement);
            return new HashDeleteResult()
            {
                Success = result > 0,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashDeleteStatement(HashDeleteParameter parameter)
        {
            var values = new RedisValue[parameter.HashFields.Count + 3];
            var valueParameters = new string[parameter.HashFields.Count];
            for (var i = 0; i < parameter.HashFields.Count; i++)
            {
                values[i] = parameter.HashFields[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            values[values.Length - 3] = true;//refresh current time
            values[values.Length - 2] = RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = 0;//expire time seconds
            var script = $@"local pv=redis.call('HDEL',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(valueParameters.Length - 2)}
return pv";
            var keys = new RedisKey[1]
            {
                parameter.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public HashDecrementResult HashDecrement(CacheServer server, HashDecrementParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashDecrementParameter)}.{nameof(HashDecrementParameter.Key)}");
            }
            if (parameter?.DecrementValue == null)
            {
                throw new ArgumentNullException($"{nameof(HashDecrementParameter)}.{nameof(HashDecrementParameter.DecrementValue)}");
            }
            var database = RedisManager.GetDatabase(server);
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
                newValue = ObjectExtensions.ConvertTo((long)newCacheValue, dataType);
            }
            else
            {
                newValue = ObjectExtensions.ConvertTo((double)newCacheValue, dataType);
            }
            return new HashDecrementResult()
            {
                Success = true,
                NewValue = newValue,
                Key = cacheKey,
                HashField = parameter.HashField,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashDecrementStatement(HashDecrementParameter parameter, bool integerValue, string cacheKey)
        {
            var keys = new RedisKey[1] { cacheKey };
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            var values = new RedisValue[]
            {
                parameter.HashField,
                -parameter.DecrementValue,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds
            };
            var script = "";
            script = @$"local obv=redis.call('{(integerValue ? "HINCRBY" : "HINCRBYFLOAT")}',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript(-2)}
return obv";
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public HashScanResult HashScan(CacheServer server, HashScanParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashScanParameter)}.{nameof(HashScanParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
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
            return new HashScanResult()
            {
                Success = true,
                Cursor = newCursor,
                HashValues = values ?? new Dictionary<string, dynamic>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashScanStatement(HashScanParameter parameter)
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
                RedisManager.GetMatchPattern(parameter.Pattern,parameter.PatternType),
                parameter.PageSize,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SetRemoveResult SetRemove(CacheServer server, SetRemoveParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetRemoveParameter)}.{nameof(SetRemoveParameter.Key)}");
            }
            if (parameter.RemoveMembers.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SetRemoveParameter)}.{nameof(SetRemoveParameter.RemoveMembers)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetRemoveStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SetRemoveResult()
            {
                Success = true,
                RemoveCount = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetRemoveStatement(SetRemoveParameter parameter)
        {
            var values = new RedisValue[parameter.RemoveMembers.Count + 3];
            var valueParameters = new string[parameter.RemoveMembers.Count];
            for (var i = 0; i < parameter.RemoveMembers.Count; i++)
            {
                values[i] = parameter.RemoveMembers[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            values[values.Length - 3] = true;//refresh current time
            values[values.Length - 2] = RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = 0;//expire time seconds
            var script = $@"local obv=redis.call('SREM',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(parameter.RemoveMembers.Count - 2)}
return obv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SetRandomMembersResult SetRandomMembers(CacheServer server, SetRandomMembersParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetRandomMembersParameter)}.{nameof(SetRandomMembersParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetRandomMembersStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SetRandomMembersResult()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetRandomMembersStatement(SetRandomMembersParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SetRandomMemberResult SetRandomMember(CacheServer server, SetRandomMemberParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetRandomMemberParameter)}.{nameof(SetRandomMemberParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetRandomMemberStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SetRandomMemberResult()
            {
                Success = true,
                Member = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetRandomMemberStatement(SetRandomMemberParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SetPopResult SetPop(CacheServer server, SetPopParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetPopParameter)}.{nameof(SetPopParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetPopStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SetPopResult()
            {
                Success = true,
                PopValue = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetPopStatement(SetPopParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SetMoveResult SetMove(CacheServer server, SetMoveParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.SourceKey))
            {
                throw new ArgumentNullException($"{nameof(SetMoveParameter)}.{nameof(SetMoveParameter.SourceKey)}");
            }
            if (string.IsNullOrWhiteSpace(parameter?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SetMoveParameter)}.{nameof(SetMoveParameter.DestinationKey)}");
            }
            if (string.IsNullOrEmpty(parameter?.MoveMember))
            {
                throw new ArgumentNullException($"{nameof(SetMoveParameter)}.{nameof(SetMoveParameter.MoveMember)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetMoveStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SetMoveResult()
            {
                Success = result == "1",
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetMoveStatement(SetMoveParameter parameter)
        {
            var script = $@"local pv=redis.call('SMOVE',{Keys(1)},{Keys(2)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
{GetRefreshExpirationScript(2, 1)}
return pv";
            var allowSliding = RedisManager.AllowSlidingExpiration();
            var expire = RedisManager.GetExpiration(parameter.Expiration);
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
                RedisManager.GetTotalSeconds(expire.Item2)//expire time seconds-destination key
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SetMembersResult SetMembers(CacheServer server, SetMembersParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetMembersParameter)}.{nameof(SetMembersParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetMembersStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SetMembersResult()
            {
                Success = true,
                Members = result?.Select(c => { string member = c; return member; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetMembersStatement(SetMembersParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SetLengthResult SetLength(CacheServer server, SetLengthParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetLengthParameter)}.{nameof(SetLengthParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetLengthStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SetLengthResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetLengthStatement(SetLengthParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SetContainsResult SetContains(CacheServer server, SetContainsParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetContainsParameter)}.{nameof(SetContainsParameter.Key)}");
            }
            if (parameter.Member == null)
            {
                throw new ArgumentNullException($"{nameof(SetContainsParameter)}.{nameof(SetContainsParameter.Member)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetContainsStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SetContainsResult()
            {
                Success = true,
                ContainsValue = result == "1",
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetContainsStatement(SetContainsParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SetCombineResult SetCombine(CacheServer server, SetCombineParameter parameter)
        {
            if (parameter?.Keys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SetCombineParameter)}.{nameof(SetCombineParameter.Keys)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetCombineStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SetCombineResult()
            {
                Success = true,
                CombineValues = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetCombineStatement(SetCombineParameter parameter)
        {
            var keys = new RedisKey[parameter.Keys.Count];
            var keyParameters = new List<string>(parameter.Keys.Count);
            for (var i = 0; i < parameter.Keys.Count; i++)
            {
                keys[i] = parameter.Keys[i].GetActualKey();
                keyParameters.Add($"{Keys(i + 1)}");
            }
            var script = $@"local pv=redis.call('{RedisManager.GetSetCombineCommand(parameter.CombineOperation)}',{string.Join(",", keyParameters)})
{GetRefreshExpirationScript(-2, keyCount: keys.Length)}
return pv";
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SetCombineAndStoreResult SetCombineAndStore(CacheServer server, SetCombineAndStoreParameter parameter)
        {
            if (parameter?.SourceKeys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SetCombineAndStoreParameter)}.{nameof(SetCombineAndStoreParameter.SourceKeys)}");
            }
            if (string.IsNullOrWhiteSpace(parameter.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SetCombineAndStoreParameter)}.{nameof(SetCombineAndStoreParameter.DestinationKey)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetCombineAndStoreStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SetCombineAndStoreResult()
            {
                Success = true,
                Count = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetCombineAndStoreStatement(SetCombineAndStoreParameter parameter)
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
            var script = $@"local pv=redis.call('{RedisManager.GetSetCombineCommand(parameter.CombineOperation)}STORE',{string.Join(",", keyParameters)})
{GetRefreshExpirationScript(-2, 1, keyCount: keys.Length - 1)}
{GetRefreshExpirationScript(1)}
return pv";
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            bool allowSliding = RedisManager.AllowSlidingExpiration();
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                allowSliding,//whether allow set refresh time
                0,//expire time seconds
                parameter.Expiration==null,// des key
                expire.Item1&&allowSliding,//des key
                RedisManager.GetTotalSeconds(expire.Item2)
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SetAddResult SetAdd(CacheServer server, SetAddParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetAddParameter)}.{nameof(SetAddParameter.Key)}");
            }
            if (parameter.Members.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SetAddParameter)}.{nameof(SetAddParameter.Members)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetAddStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SetAddResult()
            {
                Success = result > 0,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetAddStatement(SetAddParameter parameter)
        {
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            var values = new RedisValue[parameter.Members.Count + 3];
            var valueParameters = new string[parameter.Members.Count];
            for (var i = 0; i < parameter.Members.Count; i++)
            {
                values[i] = parameter.Members[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            values[values.Length - 3] = parameter.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = RedisManager.GetTotalSeconds(expire.Item2);//expire time seconds
            var script = $@"local obv=redis.call('SADD',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(parameter.Members.Count - 2)}
return obv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortedSetScoreResult SortedSetScore(CacheServer server, SortedSetScoreParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetScoreParameter)}.{nameof(SortedSetScoreParameter.Key)}");
            }
            if (parameter.Member == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetScoreParameter)}.{nameof(SortedSetScoreParameter.Member)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetScoreStatement(parameter);
            var result = (double?)ExecuteStatement(server, database, statement);
            return new SortedSetScoreResult()
            {
                Success = true,
                Score = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetScoreStatement(SortedSetScoreParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortedSetRemoveRangeByValueResult SortedSetRemoveRangeByValue(CacheServer server, SortedSetRemoveRangeByValueParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueParameter)}.{nameof(SortedSetRemoveRangeByValueParameter.Key)}");
            }
            if (parameter.MinValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueParameter)}.{nameof(SortedSetRemoveRangeByValueParameter.MinValue)}");
            }
            if (parameter.MaxValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueParameter)}.{nameof(SortedSetRemoveRangeByValueParameter.MaxValue)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRemoveRangeByValueStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortedSetRemoveRangeByValueResult()
            {
                RemoveCount = result,
                Success = true,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRemoveRangeByValueStatement(SortedSetRemoveRangeByValueParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortedSetRemoveRangeByScoreResult SortedSetRemoveRangeByScore(CacheServer server, SortedSetRemoveRangeByScoreParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByScoreParameter)}.{nameof(SortedSetRemoveRangeByScoreParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRemoveRangeByScoreStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortedSetRemoveRangeByScoreResult()
            {
                RemoveCount = result,
                Success = true,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRemoveRangeByScoreStatement(SortedSetRemoveRangeByScoreParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortedSetRemoveRangeByRankResult SortedSetRemoveRangeByRank(CacheServer server, SortedSetRemoveRangeByRankParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByRankParameter)}.{nameof(SortedSetRemoveRangeByRankParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRemoveRangeByRankStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortedSetRemoveRangeByRankResult()
            {
                RemoveCount = result,
                Success = true,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRemoveRangeByRankStatement(SortedSetRemoveRangeByRankParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortedSetRemoveResult SortedSetRemove(CacheServer server, SortedSetRemoveParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveParameter)}.{nameof(SortedSetRemoveParameter.Key)}");
            }
            if (parameter.RemoveMembers.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SortedSetRemoveParameter)}.{nameof(SortedSetRemoveParameter.RemoveMembers)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRemoveStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortedSetRemoveResult()
            {
                Success = true,
                RemoveCount = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRemoveStatement(SortedSetRemoveParameter parameter)
        {
            var values = new RedisValue[parameter.RemoveMembers.Count + 3];
            var valueParameters = new string[parameter.RemoveMembers.Count];
            for (var i = 0; i < parameter.RemoveMembers.Count; i++)
            {
                values[i] = parameter.RemoveMembers[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            values[values.Length - 3] = true;//refresh current time
            values[values.Length - 2] = RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = 0;//expire time seconds
            var script = $@"local obv=redis.call('ZREM',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(parameter.RemoveMembers.Count - 2)}
return obv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortedSetRankResult SortedSetRank(CacheServer server, SortedSetRankParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRankParameter)}.{nameof(SortedSetRankParameter.Key)}");
            }
            if (parameter.Member == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetRankParameter)}.{nameof(SortedSetRankParameter.Member)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRankStatement(parameter);
            var result = (long?)ExecuteStatement(server, database, statement);
            return new SortedSetRankResult()
            {
                Success = true,
                Rank = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRankStatement(SortedSetRankParameter parameter)
        {
            var script = $@"local pv=redis.call('Z{(parameter.Order == CacheOrder.Descending ? "REV" : "")}RANK',{Keys(1)},{Arg(1)})
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortedSetRangeByValueResult SortedSetRangeByValue(CacheServer server, SortedSetRangeByValueParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueParameter)}.{nameof(SortedSetRemoveRangeByValueParameter.Key)}");
            }
            if (parameter.MinValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueParameter)}.{nameof(SortedSetRemoveRangeByValueParameter.MinValue)}");
            }
            if (parameter.MaxValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueParameter)}.{nameof(SortedSetRemoveRangeByValueParameter.MaxValue)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByValueStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SortedSetRangeByValueResult()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRangeByValueStatement(SortedSetRangeByValueParameter parameter)
        {
            var command = "ZRANGEBYLEX";
            string beginValue = string.Empty;
            string endValue = string.Empty;
            if (parameter.Order == CacheOrder.Descending)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortedSetRangeByScoreWithScoresResult SortedSetRangeByScoreWithScores(CacheServer server, SortedSetRangeByScoreWithScoresParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByScoreWithScoresParameter)}.{nameof(SortedSetRangeByScoreWithScoresParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByScoreWithScoresStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
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
            return new SortedSetRangeByScoreWithScoresResult()
            {
                Success = true,
                Members = members,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRangeByScoreWithScoresStatement(SortedSetRangeByScoreWithScoresParameter parameter)
        {
            var command = "ZRANGEBYSCORE";
            string beginValue = "";
            string endValue = "";
            if (parameter.Order == CacheOrder.Descending)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortedSetRangeByScoreResult SortedSetRangeByScore(CacheServer server, SortedSetRangeByScoreParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByScoreParameter)}.{nameof(SortedSetRangeByScoreParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByScoreStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SortedSetRangeByScoreResult()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRangeByScoreStatement(SortedSetRangeByScoreParameter parameter)
        {
            var command = "ZRANGEBYSCORE";
            string beginValue = "";
            string endValue = "";
            if (parameter.Order == CacheOrder.Descending)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortedSetRangeByRankWithScoresResult SortedSetRangeByRankWithScores(CacheServer server, SortedSetRangeByRankWithScoresParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByRankWithScoresParameter)}.{nameof(SortedSetRangeByRankWithScoresParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByRankWithScoresStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
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
            return new SortedSetRangeByRankWithScoresResult()
            {
                Success = true,
                Members = members,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRangeByRankWithScoresStatement(SortedSetRangeByRankWithScoresParameter parameter)
        {
            var script = $@"local pv=redis.call('Z{(parameter.Order == CacheOrder.Descending ? "REV" : "")}RANGE',{Keys(1)},{Arg(1)},{Arg(2)},'WITHSCORES')
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortedSetRangeByRankResult SortedSetRangeByRank(CacheServer server, SortedSetRangeByRankParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByRankParameter)}.{nameof(SortedSetRangeByRankParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByRankStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SortedSetRangeByRankResult()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRangeByRankStatement(SortedSetRangeByRankParameter parameter)
        {
            var script = $@"local pv=redis.call('Z{(parameter.Order == CacheOrder.Descending ? "REV" : "")}RANGE',{Keys(1)},{Arg(1)},{Arg(2)})
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortedSetLengthByValueResult SortedSetLengthByValue(CacheServer server, SortedSetLengthByValueParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetLengthByValueParameter)}.{nameof(SortedSetLengthByValueParameter.Key)}");
            }
            if (parameter.MinValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetLengthByValueParameter)}.{nameof(SortedSetLengthByValueParameter.MinValue)}");
            }
            if (parameter.MaxValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetLengthByValueParameter)}.{nameof(SortedSetLengthByValueParameter.MaxValue)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetLengthByValueStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortedSetLengthByValueResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetLengthByValueStatement(SortedSetLengthByValueParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortedSetLengthResult SortedSetLength(CacheServer server, SortedSetLengthParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetLengthByValueParameter)}.{nameof(SortedSetLengthByValueParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetLengthStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortedSetLengthResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetLengthStatement(SortedSetLengthParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortedSetIncrementResult SortedSetIncrement(CacheServer server, SortedSetIncrementParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetIncrementParameter)}.{nameof(SortedSetIncrementParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetIncrementStatement(parameter);
            var result = (double)ExecuteStatement(server, database, statement);
            return new SortedSetIncrementResult()
            {
                Success = true,
                NewScore = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetIncrementStatement(SortedSetIncrementParameter parameter)
        {
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
             {
                parameter.IncrementScore,
                parameter.Member,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds
             };
            var script = $@"local pv=redis.call('ZINCRBY',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortedSetDecrementResult SortedSetDecrement(CacheServer server, SortedSetDecrementParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetDecrementParameter)}.{nameof(SortedSetDecrementParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetDecrementStatement(parameter);
            var result = (double)ExecuteStatement(server, database, statement);
            return new SortedSetDecrementResult()
            {
                Success = true,
                NewScore = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetDecrementStatement(SortedSetDecrementParameter parameter)
        {
            var script = $@"local pv=redis.call('ZINCRBY',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                -parameter.DecrementScore,
                parameter.Member,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortedSetCombineAndStoreResult SortedSetCombineAndStore(CacheServer server, SortedSetCombineAndStoreParameter parameter)
        {
            if (parameter?.SourceKeys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SortedSetCombineAndStoreParameter)}.{nameof(SortedSetCombineAndStoreParameter.SourceKeys)}");
            }
            if (string.IsNullOrWhiteSpace(parameter.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SortedSetCombineAndStoreParameter)}.{nameof(SortedSetCombineAndStoreParameter.DestinationKey)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetCombineAndStoreStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortedSetCombineAndStoreResult()
            {
                Success = true,
                NewSetLength = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetCombineAndStoreStatement(SortedSetCombineAndStoreParameter parameter)
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
            var script = $@"local pv=redis.call('{RedisManager.GetSortedSetCombineCommand(parameter.CombineOperation)}',{Keys(1)},'{keyParameters.Count}',{string.Join(",", keyParameters)},'WEIGHTS',{string.Join(",", weights)},'AGGREGATE','{RedisManager.GetSortedSetAggregateName(parameter.Aggregate)}')
{GetRefreshExpirationScript(1)}
{GetRefreshExpirationScript(-2, 1, keyCount: keys.Length - 1)}
return pv";
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            var allowSliding = RedisManager.AllowSlidingExpiration();
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                allowSliding,//whether allow set refresh time
                0,//expire time seconds
                parameter.Expiration==null,// des key
                expire.Item1&&allowSliding,//des key
                RedisManager.GetTotalSeconds(expire.Item2)
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortedSetAddResult SortedSetAdd(CacheServer server, SortedSetAddParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetAddParameter)}.{nameof(SortedSetAddParameter.Key)}");
            }
            if (parameter.Members.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SortedSetAddParameter)}.{nameof(SortedSetAddParameter.Members)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetAddStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortedSetAddResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetAddStatement(SortedSetAddParameter parameter)
        {
            var expire = RedisManager.GetExpiration(parameter.Expiration);
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
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = RedisManager.GetTotalSeconds(expire.Item2);//expire time seconds
            var script = $@"local obv=redis.call('ZADD',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(valueCount - 2)}
return obv";
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortResult Sort(CacheServer server, SortParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortParameter)}.{nameof(SortParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortStatement(parameter);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SortResult()
            {
                Success = true,
                Values = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortStatement(SortParameter parameter)
        {
            var script = $@"local obv=redis.call('SORT',{Keys(1)}{(string.IsNullOrWhiteSpace(parameter.By) ? string.Empty : $",'BY','{parameter.By}'")},'LIMIT',{Arg(1)},{Arg(2)}{(parameter.Gets.IsNullOrEmpty() ? string.Empty : $",{string.Join(",", parameter.Gets.Select(c => $"'GET','{c}'"))}")},{(parameter.Order == CacheOrder.Descending ? "'DESC'" : "'ASC'")}{(parameter.SortType == CacheSortType.Alphabetic ? ",'ALPHA'" : string.Empty)})
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public SortAndStoreResult SortAndStore(CacheServer server, SortAndStoreParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.SourceKey))
            {
                throw new ArgumentNullException($"{nameof(SortAndStoreParameter)}.{nameof(SortAndStoreParameter.SourceKey)}");
            }
            if (string.IsNullOrWhiteSpace(parameter?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SortAndStoreParameter)}.{nameof(SortAndStoreParameter.DestinationKey)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortAndStoreStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortAndStoreResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortAndStoreStatement(SortAndStoreParameter parameter)
        {
            var script = $@"local obv=redis.call('SORT',{Keys(1)}{(string.IsNullOrWhiteSpace(parameter.By) ? string.Empty : $",'BY','{parameter.By}'")},'LIMIT',{Arg(1)},{Arg(2)}{(parameter.Gets.IsNullOrEmpty() ? string.Empty : $",{string.Join(",", parameter.Gets.Select(c => $"'GET','{c}'"))}")},{(parameter.Order == CacheOrder.Descending ? "'DESC'" : "'ASC'")}{(parameter.SortType == CacheSortType.Alphabetic ? ",'ALPHA'" : string.Empty)},'STORE',{Keys(2)})
{GetRefreshExpirationScript()}
{GetRefreshExpirationScript(3, 1)}
return obv";
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            var allowSliding = RedisManager.AllowSlidingExpiration();
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
                RedisManager.GetTotalSeconds(expire.Item2)//-deskey
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public TypeResult KeyType(CacheServer server, TypeParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(TypeParameter)}.{nameof(TypeParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyTypeStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new TypeResult()
            {
                Success = true,
                KeyType = RedisManager.GetCacheKeyType(result),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyTypeStatement(TypeParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public TimeToLiveResult KeyTimeToLive(CacheServer server, TimeToLiveParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(TimeToLiveParameter)}.{nameof(TimeToLiveParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyTimeToLiveStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new TimeToLiveResult()
            {
                Success = true,
                TimeToLiveSeconds = result,
                KeyExist = result != -2,
                Perpetual = result == -1,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyTimeToLiveStatement(TimeToLiveParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public RestoreResult KeyRestore(CacheServer server, RestoreParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(RestoreParameter)}.{nameof(RestoreParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyRestoreStatement(parameter);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new RestoreResult()
            {
                Success = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyRestoreStatement(RestoreParameter parameter)
        {
            var script = $@"local obv= string.lower(tostring(redis.call('RESTORE',{Keys(1)},'0',{Arg(1)})))=='ok'
{GetRefreshExpirationScript(-1)}
return obv";
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                parameter.Value,
                parameter.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public RenameResult KeyRename(CacheServer server, RenameParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(RenameParameter)}.{nameof(RenameParameter.Key)}");
            }
            if (string.IsNullOrWhiteSpace(parameter?.NewKey))
            {
                throw new ArgumentNullException($"{nameof(RenameParameter)}.{nameof(RenameParameter.NewKey)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyRenameStatement(parameter);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new RenameResult()
            {
                Success = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyRenameStatement(RenameParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public RandomResult KeyRandom(CacheServer server, RandomParameter parameter)
        {
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyRandomStatement(parameter);
            var result = (string)ExecuteStatement(server, database, statement);
            return new RandomResult()
            {
                Success = true,
                Key = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyRandomStatement(RandomParameter parameter)
        {
            var script = $@"local obv=redis.call('RANDOMKEY')
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
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public PersistResult KeyPersist(CacheServer server, PersistParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(PersistParameter)}.{nameof(PersistParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyPersistStatement(parameter);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new PersistResult()
            {
                Success = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyPersistStatement(PersistParameter parameter)
        {
            var cacheKey = parameter.Key.GetActualKey();
            var keys = new RedisKey[1] { cacheKey };
            var parameters = new RedisValue[0];
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            var script = $@"local obv=redis.call('PERSIST',{Keys(1)})==1
if obv
then
    redis.call('DEL','{GetExpirationKey(cacheKey)}')
end
return obv";
            return new RedisStatement()
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
        public MoveResult KeyMove(CacheServer server, MoveParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(MoveParameter)}.{nameof(MoveParameter.Key)}");
            }
            if (!int.TryParse(parameter.DatabaseName, out var dbIndex) || dbIndex < 0)
            {
                throw new ArgumentException($"{nameof(MoveParameter)}.{nameof(MoveParameter.DatabaseName)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyMoveStatement(parameter);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new MoveResult()
            {
                Success = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyMoveStatement(MoveParameter parameter)
        {
            var cacheKey = parameter.Key.GetActualKey();
            var keys = new RedisKey[1] { cacheKey };
            var parameters = new RedisValue[1]
            {
                parameter.DatabaseName
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
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
            return new RedisStatement()
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
        public MigrateKeyResult KeyMigrate(CacheServer server, MigrateKeyParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(MigrateKeyParameter)}.{nameof(MigrateKeyParameter.Key)}");
            }
            if (parameter.Destination == null)
            {
                throw new ArgumentNullException($"{nameof(MigrateKeyParameter)}.{nameof(MigrateKeyParameter.Destination)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyMigrateStatement(parameter);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new MigrateKeyResult()
            {
                Success = true,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyMigrateStatement(MigrateKeyParameter parameter)
        {
            var cacheKey = parameter.Key.GetActualKey();
            var keys = new RedisKey[1] { cacheKey };
            var parameters = new RedisValue[1]
            {
                parameter.CopyCurrent
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
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
            return new RedisStatement()
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
        public ExpireResult KeyExpire(CacheServer server, ExpireParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ExpireParameter)}.{nameof(ExpireParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyExpireStatement(parameter);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new ExpireResult()
            {
                Success = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyExpireStatement(ExpireParameter parameter)
        {
            var cacheKey = parameter.Key.GetActualKey();
            var expire = RedisManager.GetExpiration(parameter.Expiration);
            var seconds = RedisManager.GetTotalSeconds(expire.Item2);
            var keys = new RedisKey[0];
            var parameters = new RedisValue[0];
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            var script = $@"local rs=redis.call('EXPIRE','{cacheKey}','{seconds}')==1
if rs and '{(expire.Item1 && RedisManager.AllowSlidingExpiration() ? "1" : "0")}'=='1'
then
    redis.call('SET','{GetExpirationKey(cacheKey)}','{seconds}','EX','{seconds}')
end
return rs";
            return new RedisStatement()
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
        public DumpResult KeyDump(CacheServer server, DumpParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(DumpParameter)}.{nameof(DumpParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyDumpStatement(parameter);
            var result = (byte[])ExecuteStatement(server, database, statement);
            return new DumpResult()
            {
                Success = true,
                ByteValues = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyDumpStatement(DumpParameter parameter)
        {
            var keys = new RedisKey[]
            {
                parameter.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            var script = $@"local pv=redis.call('DUMP',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            return new RedisStatement()
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
        public DeleteResult KeyDelete(CacheServer server, DeleteParameter parameter)
        {
            if (parameter?.Keys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(DeleteParameter)}.{nameof(DeleteParameter.Keys)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyDeleteStatement(parameter);
            var count = database.RemoteDatabase.KeyDelete(statement.Keys, statement.Flags);
            return new DeleteResult()
            {
                Success = true,
                DeleteCount = count,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyDeleteStatement(DeleteParameter parameter)
        {
            var keys = parameter.Keys.Select(c => { RedisKey key = c.GetActualKey(); return key; }).ToArray();
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            return new RedisStatement()
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
        public ExistResult KeyExist(CacheServer server, ExistParameter parameter)
        {
            if (parameter.Keys.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(ExistParameter)}.{nameof(ExistParameter.Keys)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyExistStatement(parameter);
            var result = (long)ExecuteStatement(server, database, statement);
            return new ExistResult()
            {
                Success = true,
                KeyCount = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyExistStatement(ExistParameter parameter)
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
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            var script = $@"local pv=redis.call('EXISTS',{string.Join(",", redisKeyParameters)})
{GetRefreshExpirationScript(-2)}
return pv";
            return new RedisStatement()
            {
                Script = script,
                Parameters = parameters,
                Keys = redisKeys,
                Flags = cmdFlags
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
        public GetAllDataBaseResult GetAllDataBase(CacheServer server, GetAllDataBaseParameter parameter)
        {
            if (server == null)
            {
                throw new ArgumentNullException($"{nameof(server)}");
            }
            if (parameter?.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(GetAllDataBaseParameter)}.{nameof(GetAllDataBaseParameter.EndPoint)}");
            }
            using (var conn = RedisManager.GetConnection(server, new CacheEndPoint[1] { parameter.EndPoint }))
            {
                var response = new GetAllDataBaseResult()
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
                    var databaseList = new List<CacheDatabase>(dataBaseSize);
                    for (var d = 0; d < dataBaseSize; d++)
                    {
                        databaseList.Add(new CacheDatabase()
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
        public GetKeysResult GetKeys(CacheServer server, GetKeysParameter parameter)
        {
            if (server == null)
            {
                throw new ArgumentNullException($"{nameof(server)}");
            }
            if (parameter?.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(GetAllDataBaseParameter)}.{nameof(GetAllDataBaseParameter.EndPoint)}");
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
            using (var conn = RedisManager.GetConnection(server, new CacheEndPoint[1] { parameter.EndPoint }))
            {
                var redisServer = conn.GetServer(string.Format("{0}:{1}", parameter.EndPoint.Host, parameter.EndPoint.Port));
                var keys = redisServer.Keys(dbIndex, searchString, query.PageSize, 0, (query.Page - 1) * query.PageSize, CommandFlags.None);
                var itemList = keys.Select(c => { CacheKey key = ConstantCacheKey.Create(c); return key; }).ToList();
                var totalCount = redisServer.DatabaseSize(dbIndex);
                var keyItemPaging = new CachePaging<CacheKey>(query.Page, query.PageSize, totalCount, itemList);
                return new GetKeysResult()
                {
                    Success = true,
                    Keys = keyItemPaging,
                    CacheServer = server,
                    EndPoint = parameter.EndPoint,
                    Database = new RedisDatabase()
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
        public ClearDataResult ClearData(CacheServer server, ClearDataParameter parameter)
        {
            if (parameter.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(ClearDataParameter)}.{nameof(ClearDataParameter.EndPoint)}");
            }
            if (!int.TryParse(server.Database, out int dbIndex))
            {
                throw new SixnetException($"Redis database {server.Database} is invalid");
            }
            var cmdFlags = RedisManager.GetCommandFlags(parameter.CommandFlags);
            using (var conn = RedisManager.GetConnection(server, new CacheEndPoint[1] { parameter.EndPoint }))
            {
                var redisServer = conn.GetServer(string.Format("{0}:{1}", parameter.EndPoint.Host, parameter.EndPoint.Port));
                redisServer.FlushDatabase(dbIndex, cmdFlags);
                return new ClearDataResult()
                {
                    Success = true,
                    CacheServer = server,
                    EndPoint = parameter.EndPoint,
                    Database = new RedisDatabase()
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
        public GetDetailResult GetKeyDetail(CacheServer server, GetDetailParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(GetDetailParameter)}.{nameof(GetDetailParameter.Key)}");
            }
            if (parameter.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(GetDetailParameter)}.{nameof(GetDetailParameter.EndPoint)}");
            }
            if (!int.TryParse(server.Database, out int dbIndex))
            {
                throw new SixnetException($"Redis database {server.Database} is invalid");
            }
            using (var conn = RedisManager.GetConnection(server, new CacheEndPoint[1] { parameter.EndPoint }))
            {
                var redisDatabase = conn.GetDatabase(dbIndex);
                var redisKeyType = redisDatabase.KeyType(parameter.Key.GetActualKey());
                var cacheKeyType = RedisManager.GetCacheKeyType(redisKeyType.ToString());
                var keyItem = new CacheEntry()
                {
                    Key = parameter.Key.GetActualKey(),
                    Type = cacheKeyType
                };
                switch (cacheKeyType)
                {
                    case CacheKeyType.String:
                        keyItem.Value = redisDatabase.StringGetAsync(keyItem.Key.GetActualKey());
                        break;
                    case CacheKeyType.List:
                        var listValues = new List<string>();
                        var listResults = redisDatabase.ListRange(keyItem.Key.GetActualKey(), 0, -1, CommandFlags.None);
                        listValues.AddRange(listResults.Select(c => (string)c));
                        keyItem.Value = listValues;
                        break;
                    case CacheKeyType.Set:
                        var setValues = new List<string>();
                        var setResults = redisDatabase.SetMembers(keyItem.Key.GetActualKey(), CommandFlags.None);
                        setValues.AddRange(setResults.Select(c => (string)c));
                        keyItem.Value = setValues;
                        break;
                    case CacheKeyType.SortedSet:
                        var sortSetValues = new List<string>();
                        var sortedResults = redisDatabase.SortedSetRangeByRank(keyItem.Key.GetActualKey());
                        sortSetValues.AddRange(sortedResults.Select(c => (string)c));
                        keyItem.Value = sortSetValues;
                        break;
                    case CacheKeyType.Hash:
                        var hashValues = new Dictionary<string, string>();
                        var objValues = redisDatabase.HashGetAll(keyItem.Key.GetActualKey());
                        foreach (var obj in objValues)
                        {
                            hashValues.Add(obj.Name, obj.Value);
                        }
                        keyItem.Value = hashValues;
                        break;
                }
                return new GetDetailResult()
                {
                    Success = true,
                    CacheEntry = keyItem,
                    CacheServer = server,
                    Database = new CacheDatabase()
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
        public GetServerConfigurationResult GetServerConfiguration(CacheServer server, GetServerConfigurationParameter parameter)
        {
            if (parameter?.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(GetServerConfigurationParameter)}.{nameof(GetServerConfigurationParameter.EndPoint)}");
            }
            using (var conn = RedisManager.GetConnection(server, new CacheEndPoint[1] { parameter.EndPoint }))
            {
                var config = new RedisServerConfiguration();
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
                                var logLevel = LogLevel.Verbose;
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
                                var saveInfos = new List<DataChangeSaveParameter>();
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
                                    saveInfos.Add(new DataChangeSaveParameter()
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
                                var appendSync = AppendfSync.EverySecond;
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
                }
                return new GetServerConfigurationResult()
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
        public SaveServerConfigurationResult SaveServerConfiguration(CacheServer server, SaveServerConfigurationParameter parameter)
        {
            if (!(parameter?.ServerConfiguration is RedisServerConfiguration config))
            {
                throw new SixnetException($"{nameof(SaveServerConfigurationParameter.ServerConfiguration)} is not {nameof(RedisServerConfiguration)}");
            }
            if (parameter?.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(SaveServerConfigurationParameter)}.{nameof(SaveServerConfigurationParameter.EndPoint)}");
            }
            using (var conn = RedisManager.GetConnection(server, new CacheEndPoint[1] { parameter.EndPoint }))
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
                redisServer.ConfigRewrite();
                return new SaveServerConfigurationResult()
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
        /// <param name="exclude">Exclude parameter</param>
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

        static T GetNoDatabaseResponse<T>(CacheServer server) where T : CacheResult, new()
        {
            if (SixnetCacher.ThrowOnMissingDatabase)
            {
                throw new SixnetException("No cache database specified");
            }
            return CacheResult.NoDatabase<T>(server);
        }

        static T GetNoValueResponse<T>(CacheServer server) where T : CacheResult, new()
        {
            return CacheResult.FailResponse<T>("", "No value specified", server);
        }

        static T GetNoKeyResponse<T>(CacheServer server) where T : CacheResult, new()
        {
            return CacheResult.FailResponse<T>("", "No key specified", server);
        }

        RedisResult ExecuteStatement(CacheServer server, RedisDatabase database, RedisStatement statement)
        {
            return ExecuteStatement(server, database, statement);
        }

        #endregion
    }
}
