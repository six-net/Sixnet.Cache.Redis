using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using Sixnet.Cache.Hash.Options;
using Sixnet.Cache.Hash.Response;
using Sixnet.Cache.Keys.Options;
using Sixnet.Cache.Keys.Response;
using Sixnet.Cache.List.Options;
using Sixnet.Cache.List.Response;
using Sixnet.Cache.Server.Options;
using Sixnet.Cache.Server.Response;
using Sixnet.Cache.Set.Options;
using Sixnet.Cache.Set.Response;
using Sixnet.Cache.SortedSet;
using Sixnet.Cache.SortedSet.Options;
using Sixnet.Cache.SortedSet.Response;
using Sixnet.Cache.String;
using Sixnet.Cache.String.Response;
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
        /// <param name="options">String set range options</param>
        /// <returns>Return string set range response</returns>
        public StringSetRangeResponse StringSetRange(CacheServer server, StringSetRangeOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringSetRangeOptions)}.{nameof(StringSetRangeOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringSetRangeStatement(options);
            var result = ExecuteStatement(server, database, statement);
            return new StringSetRangeResponse()
            {
                Success = true,
                CacheServer = server,
                Database = database,
                NewValueLength = (long)result
            };
        }

        RedisStatement GetStringSetRangeStatement(StringSetRangeOptions options)
        {
            var script = $@"local len=redis.call('SETRANGE',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return len";
            var expire = RedisManager.GetExpiration(options.Expiration);
            var keys = new RedisKey[] { options.Key.GetActualKey() };
            var parameters = new RedisValue[]
            {
                options.Offset,
                options.Value,
                options.Expiration==null,//refresh current time
                expire.Item1 && RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                RedisManager.GetTotalSeconds(expire.Item2),//expire time seconds
            };
            var commandFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">String set bit options</param>
        /// <returns>Return string set bit response</returns>
        public StringSetBitResponse StringSetBit(CacheServer server, StringSetBitOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringSetBitOptions)}.{nameof(StringSetBitOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringSetBitStatement(options);
            var result = ExecuteStatement(server, database, statement);
            return new StringSetBitResponse()
            {
                Success = true,
                OldBitValue = (bool)result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringSetBitStatement(StringSetBitOptions options)
        {
            var script = $@"local obv=redis.call('SETBIT',{Keys(1)},{Arg(1)},{Arg(2)})
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
        /// <param name="options">String set options</param>
        /// <returns>Return string set response</returns>
        public StringSetResponse StringSet(CacheServer server, StringSetOptions options)
        {
            if (options?.Items.IsNullOrEmpty() ?? true)
            {
                return GetNoValueResponse<StringSetResponse>(server);
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringSetStatement(options);
            var result = ExecuteStatement(server, database, statement);
            return new StringSetResponse()
            {
                CacheServer = server,
                Database = database,
                Success = true,
                Results = ((RedisValue[])result)?.Select(c => new StringEntrySetResult() { SetSuccess = true, Key = c }).ToList()
            };
        }

        RedisStatement GetStringSetStatement(StringSetOptions options)
        {
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
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">String length options</param>
        /// <returns>Return string length response</returns>
        public StringLengthResponse StringLength(CacheServer server, StringLengthOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringLengthOptions)}.{nameof(StringLengthOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringLengthStatement(options);
            var result = ExecuteStatement(server, database, statement);
            return new StringLengthResponse()
            {
                Success = true,
                Length = (long)result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringLengthStatement(StringLengthOptions options)
        {
            var script = $@"local obv=redis.call('STRLEN',{Keys(1)})
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
        /// <param name="options">String increment options</param>
        /// <returns>Return string increment response</returns>
        public StringIncrementResponse StringIncrement(CacheServer server, StringIncrementOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringIncrementOptions)}.{nameof(StringIncrementOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringIncrementStatement(options);
            var result = ExecuteStatement(server, database, statement);
            return new StringIncrementResponse()
            {
                Success = true,
                NewValue = (long)result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringIncrementStatement(StringIncrementOptions options)
        {
            var script = $@"local obv=redis.call('INCRBY',{Keys(1)},{Arg(1)})
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
        /// <param name="options">String get with expiry ption</param>
        /// <returns>Return string get with expiry response</returns>
        public StringGetWithExpiryResponse StringGetWithExpiry(CacheServer server, StringGetWithExpiryOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetWithExpiryOptions)}.{nameof(StringGetWithExpiryOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringGetWithExpiryStatement(options);
            var result = (RedisValue[])(ExecuteStatement(server, database, statement));
            return new StringGetWithExpiryResponse()
            {
                Success = true,
                Value = result[0],
                Expiry = TimeSpan.FromSeconds((long)result[1]),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringGetWithExpiryStatement(StringGetWithExpiryOptions options)
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
                options.Key.GetActualKey()
            };
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">String get set options</param>
        /// <returns>Return string get set response</returns>
        public StringGetSetResponse StringGetSet(CacheServer server, StringGetSetOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetSetOptions)}.{nameof(StringGetSetOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringGetSetStatement(options);
            var result = ExecuteStatement(server, database, statement);
            return new StringGetSetResponse()
            {
                Success = true,
                OldValue = (string)result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringGetSetStatement(StringGetSetOptions options)
        {
            var script = $@"local ov=redis.call('GETSET',{Keys(1)},{Arg(1)})
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
        /// <param name="options">String get range options</param>
        /// <returns>Return string get range response</returns>
        public StringGetRangeResponse StringGetRange(CacheServer server, StringGetRangeOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetRangeOptions)}.{nameof(StringGetRangeOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringGetRangeStatement(options);
            var result = ExecuteStatement(server, database, statement);
            return new StringGetRangeResponse()
            {
                Success = true,
                Value = (string)result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringGetRangeStatement(StringGetRangeOptions options)
        {
            var script = $@"local ov=redis.call('GETRANGE',{Keys(1)},{Arg(1)},{Arg(2)})
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
        /// <param name="options">String get bit options</param>
        /// <returns>Return string get bit response</returns>
        public StringGetBitResponse StringGetBit(CacheServer server, StringGetBitOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetBitOptions)}.{nameof(StringGetBitOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringGetBitStatement(options);
            var result = ExecuteStatement(server, database, statement);
            return new StringGetBitResponse()
            {
                Success = true,
                Bit = (bool)result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringGetBitStatement(StringGetBitOptions options)
        {
            var script = $@"local ov=redis.call('GETBIT',{Keys(1)},{Arg(1)})
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
        /// <param name="options">String get options</param>
        /// <returns>Return string get response</returns>
        public StringGetResponse StringGet(CacheServer server, StringGetOptions options)
        {
            if (options?.Keys.IsNullOrEmpty() ?? true)
            {
                return GetNoKeyResponse<StringGetResponse>(server);
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringGetStatement(options);
            var result = (RedisValue[])(ExecuteStatement(server, database, statement));
            return new StringGetResponse()
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

        RedisStatement GetStringGetStatement(StringGetOptions options)
        {
            var keys = options.Keys.Select(c => { RedisKey rv = c.GetActualKey(); return rv; }).ToArray();
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
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">String decrement options</param>
        /// <returns>Return string decrement response</returns>
        public StringDecrementResponse StringDecrement(CacheServer server, StringDecrementOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringDecrementOptions)}.{nameof(StringDecrementOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringDecrementStatement(options);
            var result = ExecuteStatement(server, database, statement);
            return new StringDecrementResponse()
            {
                Success = true,
                NewValue = (long)result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringDecrementStatement(StringDecrementOptions options)
        {
            var script = $@"local obv=redis.call('DECRBY',{Keys(1)},{Arg(1)})
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
        /// <param name="options">String bit position options</param>
        /// <returns>Return string bit position response</returns>
        public StringBitPositionResponse StringBitPosition(CacheServer server, StringBitPositionOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringBitPositionOptions)}.{nameof(StringBitPositionOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringBitPositionStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new StringBitPositionResponse()
            {
                Success = true,
                Position = result,
                HasValue = result >= 0,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringBitPositionStatement(StringBitPositionOptions options)
        {
            var script = $@"local obv=redis.call('BITPOS',{Keys(1)},{Arg(1)},{Arg(2)},{Arg(3)})
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
        /// <param name="options">String bit operation options</param>
        /// <returns>Return string bit operation response</returns>
        public StringBitOperationResponse StringBitOperation(CacheServer server, StringBitOperationOptions options)
        {
            if (options?.Keys.IsNullOrEmpty() ?? true)
            {
                return GetNoKeyResponse<StringBitOperationResponse>(server);
            }
            if (string.IsNullOrWhiteSpace(options?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(StringBitOperationOptions)}.{nameof(StringBitOperationOptions.DestinationKey)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringBitOperationStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new StringBitOperationResponse()
            {
                Success = true,
                DestinationValueLength = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringBitOperationStatement(StringBitOperationOptions options)
        {
            var keys = new RedisKey[options.Keys.Count + 1];
            var keyParameters = new string[options.Keys.Count + 1];
            keys[0] = options.DestinationKey.GetActualKey();
            keyParameters[0] = "KEYS[1]";
            for (var i = 0; i < options.Keys.Count; i++)
            {
                keys[i + 1] = options.Keys[i].GetActualKey();
                keyParameters[i + 1] = $"KEYS[{2 + i}]";
            }
            var script = $@"local obv=redis.call('BITOP',{Arg(1)},{string.Join(",", keyParameters)})
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
        /// <param name="options">String bit count options</param>
        /// <returns>Return string bit count response</returns>
        public StringBitCountResponse StringBitCount(CacheServer server, StringBitCountOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringBitCountOptions)}.{nameof(StringBitCountOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringBitCountStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new StringBitCountResponse()
            {
                Success = true,
                BitNum = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringBitCountStatement(StringBitCountOptions options)
        {
            var script = $@"local obv=redis.call('BITCOUNT',{Keys(1)},{Arg(1)},{Arg(2)})
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
        /// <param name="options">String append options</param>
        /// <returns>Return string append response</returns>
        public StringAppendResponse StringAppend(CacheServer server, StringAppendOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringAppendOptions)}.{nameof(StringAppendOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringAppendStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new StringAppendResponse()
            {
                Success = true,
                NewValueLength = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetStringAppendStatement(StringAppendOptions options)
        {
            var script = $@"local obv=redis.call('APPEND',{Keys(1)},{Arg(1)})
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
        /// <param name="options">List trim options</param>
        /// <returns>Return list trim response</returns>
        public ListTrimResponse ListTrim(CacheServer server, ListTrimOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListTrimOptions)}.{nameof(ListTrimOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListTrimStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new ListTrimResponse()
            {
                Success = true,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListTrimStatement(ListTrimOptions options)
        {
            var script = $@"redis.call('LTRIM',{Keys(1)},{Arg(1)},{Arg(2)})
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
        /// <param name="options">List set by index options</param>
        /// <returns>Return list set by index response</returns>
        public ListSetByIndexResponse ListSetByIndex(CacheServer server, ListSetByIndexOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListSetByIndexOptions)}.{nameof(ListSetByIndexOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListSetByIndexStatement(options);
            var result = (string)ExecuteStatement(server, database, statement);
            return new ListSetByIndexResponse()
            {
                Success = string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListSetByIndexStatement(ListSetByIndexOptions options)
        {
            var script = $@"local obv=redis.call('LSET',{Keys(1)},{Arg(1)},{Arg(2)})
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
        /// <param name="options">List right push options</param>
        /// <returns>Return list right push</returns>
        public ListRightPushResponse ListRightPush(CacheServer server, ListRightPushOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRightPushOptions)}.{nameof(ListRightPushOptions.Key)}");
            }
            if (options?.Values.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentException($"{nameof(ListRightPushOptions)}.{nameof(ListRightPushOptions.Values)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListRightPushStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new ListRightPushResponse()
            {
                Success = true,
                NewListLength = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListRightPushStatement(ListRightPushOptions options)
        {
            var values = new RedisValue[options.Values.Count + 3];
            var valueParameters = new string[options.Values.Count];
            for (var i = 0; i < options.Values.Count; i++)
            {
                values[i] = options.Values[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            var script = $@"local obv=redis.call('RPUSH',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(options.Values.Count - 2)}
return obv";
            var expire = RedisManager.GetExpiration(options.Expiration);
            values[values.Length - 3] = options.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = RedisManager.GetTotalSeconds(expire.Item2);//expire time seconds
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">List right pop left push options</param>
        /// <returns>Return list right pop left response</returns>
        public ListRightPopLeftPushResponse ListRightPopLeftPush(CacheServer server, ListRightPopLeftPushOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.SourceKey))
            {
                throw new ArgumentNullException($"{nameof(ListRightPopLeftPushOptions)}.{nameof(ListRightPopLeftPushOptions.SourceKey)}");
            }
            if (string.IsNullOrWhiteSpace(options?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(ListRightPopLeftPushOptions)}.{nameof(ListRightPopLeftPushOptions.DestinationKey)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListRightPopLeftPushStatement(options);
            var result = (string)ExecuteStatement(server, database, statement);
            return new ListRightPopLeftPushResponse()
            {
                Success = true,
                PopValue = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListRightPopLeftPushStatement(ListRightPopLeftPushOptions options)
        {
            var script = $@"local pv=redis.call('RPOPLPUSH',{Keys(1)},{Keys(2)})
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
        /// <param name="options">List right pop options</param>
        /// <returns>Return list right pop response</returns>
        public ListRightPopResponse ListRightPop(CacheServer server, ListRightPopOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRightPopOptions)}.{nameof(ListRightPopOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListRightPopStatement(options);
            var result = (string)ExecuteStatement(server, database, statement);
            return new ListRightPopResponse()
            {
                Success = true,
                PopValue = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListRightPopStatement(ListRightPopOptions options)
        {
            var script = $@"local pv=redis.call('RPOP',{Keys(1)})
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
        /// <param name="options">List remove options</param>
        /// <returns>Return list remove response</returns>
        public ListRemoveResponse ListRemove(CacheServer server, ListRemoveOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRemoveOptions)}.{nameof(ListRemoveOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListRemoveStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new ListRemoveResponse()
            {
                Success = true,
                RemoveCount = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListRemoveStatement(ListRemoveOptions options)
        {
            var script = $@"local rc=redis.call('LREM',{Keys(1)},{Arg(1)},{Arg(2)})
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
        /// <param name="options">Options</param>
        /// <returns>list range response</returns>
        public ListRangeResponse ListRange(CacheServer server, ListRangeOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRangeOptions)}.{nameof(ListRangeOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListRangeStatement(options);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new ListRangeResponse()
            {
                Success = true,
                Values = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListRangeStatement(ListRangeOptions options)
        {
            var script = $@"local rc=redis.call('LRANGE',{Keys(1)},{Arg(1)},{Arg(2)})
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
        /// <param name="options">Options</param>
        /// <returns>list length response</returns>
        public ListLengthResponse ListLength(CacheServer server, ListLengthOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListLengthOptions)}.{nameof(ListLengthOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListLengthStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new ListLengthResponse()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListLengthStatement(ListLengthOptions options)
        {
            var script = $@"local len=redis.call('LLEN',{Keys(1)})
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
        /// <param name="options">List left push options</param>
        /// <returns>Return list left push response</returns>
        public ListLeftPushResponse ListLeftPush(CacheServer server, ListLeftPushOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListLeftPushOptions)}.{nameof(ListLeftPushOptions.Key)}");
            }
            if (options?.Values.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentException($"{nameof(ListRightPushOptions)}.{nameof(ListRightPushOptions.Values)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListLeftPushStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new ListLeftPushResponse()
            {
                Success = true,
                NewListLength = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListLeftPushStatement(ListLeftPushOptions options)
        {
            var values = new RedisValue[options.Values.Count + 3];
            var valueParameters = new string[options.Values.Count];
            for (var i = 0; i < options.Values.Count; i++)
            {
                values[i] = options.Values[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            var script = $@"local obv=redis.call('LPUSH',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(options.Values.Count - 2)}
return obv";
            var expire = RedisManager.GetExpiration(options.Expiration);
            values[values.Length - 3] = options.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = RedisManager.GetTotalSeconds(expire.Item2);//expire time seconds
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">List left pop options</param>
        /// <returns>list left pop response</returns>
        public ListLeftPopResponse ListLeftPop(CacheServer server, ListLeftPopOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListLeftPopOptions)}.{nameof(ListLeftPopOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListLeftPopStatement(options);
            var result = (string)ExecuteStatement(server, database, statement);
            return new ListLeftPopResponse()
            {
                Success = true,
                PopValue = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListLeftPopStatement(ListLeftPopOptions options)
        {
            var script = $@"local pv=redis.call('LPOP',{Keys(1)})
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
        /// <param name="options">List insert before options</param>
        /// <returns>Return list insert begore response</returns>
        public ListInsertBeforeResponse ListInsertBefore(CacheServer server, ListInsertBeforeOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListInsertBeforeOptions)}.{nameof(ListInsertBeforeOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListInsertBeforeStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new ListInsertBeforeResponse()
            {
                Success = result > 0,
                NewListLength = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListInsertBeforeStatement(ListInsertBeforeOptions options)
        {
            var script = $@"local pv=redis.call('LINSERT',{Keys(1)},'BEFORE',{Arg(1)},{Arg(2)})
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
        /// <param name="options">List insert after options</param>
        /// <returns>Return list insert after response</returns>
        public ListInsertAfterResponse ListInsertAfter(CacheServer server, ListInsertAfterOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListInsertAfterOptions)}.{nameof(ListInsertAfterOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListInsertAfterStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new ListInsertAfterResponse()
            {
                Success = result > 0,
                NewListLength = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListInsertAfterStatement(ListInsertAfterOptions options)
        {
            var script = $@"local pv=redis.call('LINSERT',{Keys(1)},'AFTER',{Arg(1)},{Arg(2)})
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
        /// <param name="options">List get by index options</param>
        /// <returns>Return list get by index response</returns>
        public ListGetByIndexResponse ListGetByIndex(CacheServer server, ListGetByIndexOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListInsertAfterOptions)}.{nameof(ListInsertAfterOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListGetByIndexStatement(options);
            var result = (string)ExecuteStatement(server, database, statement);
            return new ListGetByIndexResponse()
            {
                Success = true,
                Value = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetListGetByIndexStatement(ListGetByIndexOptions options)
        {
            var script = $@"local pv=redis.call('LINDEX',{Keys(1)},{Arg(1)})
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
        /// <param name="options">Hash values options</param>
        /// <returns>Return hash values response</returns>
        public HashValuesResponse HashValues(CacheServer server, HashValuesOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashValuesOptions)}.{nameof(HashValuesOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashValuesStatement(options);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new HashValuesResponse()
            {
                Success = true,
                Values = result.Select(c => { dynamic value = c; return value; }).ToList(),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashValuesStatement(HashValuesOptions options)
        {
            var script = $@"local pv=redis.call('HVALS',{Keys(1)})
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
        /// <param name="options">Hash set options</param>
        /// <returns>Return hash set response</returns>
        public HashSetResponse HashSet(CacheServer server, HashSetOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashSetOptions)}.{nameof(HashSetOptions.Key)}");
            }
            if (options?.Items.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(HashSetOptions)}.{nameof(HashSetOptions.Items)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashSetStatement(options);
            var result = (string)ExecuteStatement(server, database, statement);
            return new HashSetResponse()
            {
                Success = string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashSetStatement(HashSetOptions options)
        {
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
            var script = $@"local obv=redis.call('HMSET',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(valueCount - 2)}
return obv['ok']";
            var expire = RedisManager.GetExpiration(options.Expiration);
            values[values.Length - 3] = options.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = RedisManager.GetTotalSeconds(expire.Item2);//expire time seconds
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">Hash length options</param>
        /// <returns>Return hash length response</returns>
        public HashLengthResponse HashLength(CacheServer server, HashLengthOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashLengthOptions)}.{nameof(HashLengthOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashLengthStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new HashLengthResponse()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashLengthStatement(HashLengthOptions options)
        {
            var script = $@"local pv=redis.call('HLEN',{Keys(1)})
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
        /// <param name="options">Hash key options</param>
        /// <returns>Return hash keys response</returns>
        public HashKeysResponse HashKeys(CacheServer server, HashKeysOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashKeysOptions)}.{nameof(HashKeysOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashKeysStatement(options);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new HashKeysResponse()
            {
                Success = true,
                HashKeys = result.Select(c => { string key = c; return key; }).ToList(),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashKeysStatement(HashKeysOptions options)
        {
            var script = $@"local pv=redis.call('HKEYS',{Keys(1)})
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
        /// <param name="options">Hash increment options</param>
        /// <returns>Return hash increment response</returns>
        public HashIncrementResponse HashIncrement(CacheServer server, HashIncrementOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashIncrementOptions)}.{nameof(HashIncrementOptions.Key)}");
            }
            if (options?.IncrementValue == null)
            {
                throw new ArgumentNullException($"{nameof(HashIncrementOptions)}.{nameof(HashIncrementOptions.IncrementValue)}");
            }
            var database = RedisManager.GetDatabase(server);
            var cacheKey = options.Key.GetActualKey();
            var newValue = options.IncrementValue;
            var dataType = options.IncrementValue.GetType();
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
            var statement = GetHashIncrementStatement(options, integerValue, cacheKey);
            var newCacheValue = ExecuteStatement(server, database, statement);
            if (integerValue)
            {
                newValue = ObjectExtensions.ConvertTo((long)newCacheValue, dataType);
            }
            else
            {
                newValue = ObjectExtensions.ConvertTo((double)newCacheValue, dataType);
            }
            return new HashIncrementResponse()
            {
                Success = true,
                NewValue = newValue,
                Key = cacheKey,
                HashField = options.HashField,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashIncrementStatement(HashIncrementOptions options, bool integerValue, string cacheKey)
        {
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
            var script = "";

            script = @$"local obv=redis.call('{(integerValue ? "HINCRBY" : "HINCRBYFLOAT")}',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript(-2)}
return obv";
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">Hash get options</param>
        /// <returns>Return hash get response</returns>
        public HashGetResponse HashGet(CacheServer server, HashGetOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashGetOptions)}.{nameof(HashGetOptions.Key)}");
            }
            if (string.IsNullOrWhiteSpace(options?.HashField))
            {
                throw new ArgumentNullException($"{nameof(HashGetOptions)}.{nameof(HashGetOptions.HashField)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashGetStatement(options);
            var result = (string)ExecuteStatement(server, database, statement);
            return new HashGetResponse()
            {
                Success = true,
                Value = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashGetStatement(HashGetOptions options)
        {
            var script = $@"local pv=redis.call('HGET',{Keys(1)},{Arg(1)})
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
        /// <param name="options">Hash get all options</param>
        /// <returns>Return hash get all response</returns>
        public HashGetAllResponse HashGetAll(CacheServer server, HashGetAllOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashGetAllOptions)}.{nameof(HashGetAllOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashGetAllStatement(options);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            var values = new Dictionary<string, dynamic>(result.Length / 2);
            for (var i = 0; i < result.Length; i += 2)
            {
                values[result[i]] = result[i + 1];
            }
            return new HashGetAllResponse()
            {
                Success = true,
                HashValues = values,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashGetAllStatement(HashGetAllOptions options)
        {
            var script = $@"local pv=redis.call('HGETALL',{Keys(1)})
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
        /// <param name="options">Options</param>
        /// <returns>hash exists response</returns>
        public HashExistsResponse HashExist(CacheServer server, HashExistsOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashExistsOptions)}.{nameof(HashExistsOptions.Key)}");
            }
            if (string.IsNullOrWhiteSpace(options?.HashField))
            {
                throw new ArgumentNullException($"{nameof(HashExistsOptions)}.{nameof(HashExistsOptions.HashField)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashExistStatement(options);
            var result = (int)ExecuteStatement(server, database, statement);
            return new HashExistsResponse()
            {
                Success = true,
                HasField = result == 1,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashExistStatement(HashExistsOptions options)
        {
            var script = $@"local pv=redis.call('HEXISTS',{Keys(1)},{Arg(1)})
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
        /// <param name="options">Hash delete options</param>
        /// <returns>Return hash delete response</returns>
        public HashDeleteResponse HashDelete(CacheServer server, HashDeleteOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashDeleteOptions)}.{nameof(HashDeleteOptions.Key)}");
            }
            if (options.HashFields.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(HashDeleteOptions)}.{nameof(HashDeleteOptions.HashFields)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashDeleteStatement(options);
            var result = (int)ExecuteStatement(server, database, statement);
            return new HashDeleteResponse()
            {
                Success = result > 0,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashDeleteStatement(HashDeleteOptions options)
        {
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
            var script = $@"local pv=redis.call('HDEL',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(valueParameters.Length - 2)}
return pv";
            var keys = new RedisKey[1]
            {
                options.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">Hash decrement options</param>
        /// <returns>Return hash decrement response</returns>
        public HashDecrementResponse HashDecrement(CacheServer server, HashDecrementOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashDecrementOptions)}.{nameof(HashDecrementOptions.Key)}");
            }
            if (options?.DecrementValue == null)
            {
                throw new ArgumentNullException($"{nameof(HashDecrementOptions)}.{nameof(HashDecrementOptions.DecrementValue)}");
            }
            var database = RedisManager.GetDatabase(server);
            var dataType = options.DecrementValue.GetType();
            var typeCode = Type.GetTypeCode(dataType);
            dynamic newValue = options.DecrementValue;
            var cacheKey = options.Key.GetActualKey();
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
            var statement = GetHashDecrementStatement(options, integerValue, cacheKey);
            var newCacheValue = ExecuteStatement(server, database, statement);
            if (integerValue)
            {
                newValue = ObjectExtensions.ConvertTo((long)newCacheValue, dataType);
            }
            else
            {
                newValue = ObjectExtensions.ConvertTo((double)newCacheValue, dataType);
            }
            return new HashDecrementResponse()
            {
                Success = true,
                NewValue = newValue,
                Key = cacheKey,
                HashField = options.HashField,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashDecrementStatement(HashDecrementOptions options, bool integerValue, string cacheKey)
        {
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
            var script = "";
            script = @$"local obv=redis.call('{(integerValue ? "HINCRBY" : "HINCRBYFLOAT")}',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript(-2)}
return obv";
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">Hash scan options</param>
        /// <returns>Return hash scan response</returns>
        public HashScanResponse HashScan(CacheServer server, HashScanOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashScanOptions)}.{nameof(HashScanOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashScanStatement(options);
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
            return new HashScanResponse()
            {
                Success = true,
                Cursor = newCursor,
                HashValues = values ?? new Dictionary<string, dynamic>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetHashScanStatement(HashScanOptions options)
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
        /// <param name="options">Set remove options</param>
        /// <returns>Return set remove response</returns>
        public SetRemoveResponse SetRemove(CacheServer server, SetRemoveOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetRemoveOptions)}.{nameof(SetRemoveOptions.Key)}");
            }
            if (options.RemoveMembers.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SetRemoveOptions)}.{nameof(SetRemoveOptions.RemoveMembers)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetRemoveStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SetRemoveResponse()
            {
                Success = true,
                RemoveCount = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetRemoveStatement(SetRemoveOptions options)
        {
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
            var script = $@"local obv=redis.call('SREM',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(options.RemoveMembers.Count - 2)}
return obv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">Set random members options</param>
        /// <returns>Return set random members response</returns>
        public SetRandomMembersResponse SetRandomMembers(CacheServer server, SetRandomMembersOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetRandomMembersOptions)}.{nameof(SetRandomMembersOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetRandomMembersStatement(options);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SetRandomMembersResponse()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetRandomMembersStatement(SetRandomMembersOptions options)
        {
            var script = $@"local pv=redis.call('SRANDMEMBER',{Keys(1)},{Arg(1)})
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
        /// <param name="options">Set random member options</param>
        /// <returns>Return set random member</returns>
        public SetRandomMemberResponse SetRandomMember(CacheServer server, SetRandomMemberOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetRandomMemberOptions)}.{nameof(SetRandomMemberOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetRandomMemberStatement(options);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SetRandomMemberResponse()
            {
                Success = true,
                Member = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetRandomMemberStatement(SetRandomMemberOptions options)
        {
            var script = $@"local pv=redis.call('SRANDMEMBER',{Keys(1)})
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
        /// <param name="options">Set pop options</param>
        /// <returns>Return set pop response</returns>
        public SetPopResponse SetPop(CacheServer server, SetPopOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetPopOptions)}.{nameof(SetPopOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetPopStatement(options);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SetPopResponse()
            {
                Success = true,
                PopValue = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetPopStatement(SetPopOptions options)
        {
            var script = $@"local pv=redis.call('SPOP',{Keys(1)})
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
        /// <param name="options">Set move options</param>
        /// <returns>Return set move response</returns>
        public SetMoveResponse SetMove(CacheServer server, SetMoveOptions options)
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
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetMoveStatement(options);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SetMoveResponse()
            {
                Success = result == "1",
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetMoveStatement(SetMoveOptions options)
        {
            var script = $@"local pv=redis.call('SMOVE',{Keys(1)},{Keys(2)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
{GetRefreshExpirationScript(2, 1)}
return pv";
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
        /// <param name="options">Set members options</param>
        /// <returns>Return set members response</returns>
        public SetMembersResponse SetMembers(CacheServer server, SetMembersOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetMembersOptions)}.{nameof(SetMembersOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetMembersStatement(options);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SetMembersResponse()
            {
                Success = true,
                Members = result?.Select(c => { string member = c; return member; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetMembersStatement(SetMembersOptions options)
        {
            var script = $@"local pv=redis.call('SMEMBERS',{Keys(1)})
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
        /// <param name="options">Set length options</param>
        /// <returns>Return set length response</returns>
        public SetLengthResponse SetLength(CacheServer server, SetLengthOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetLengthOptions)}.{nameof(SetLengthOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetLengthStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SetLengthResponse()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetLengthStatement(SetLengthOptions options)
        {
            var script = $@"local pv=redis.call('SCARD',{Keys(1)})
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
        /// <param name="options">Set contains options</param>
        /// <returns>Return set contains response</returns>
        public SetContainsResponse SetContains(CacheServer server, SetContainsOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetContainsOptions)}.{nameof(SetContainsOptions.Key)}");
            }
            if (options.Member == null)
            {
                throw new ArgumentNullException($"{nameof(SetContainsOptions)}.{nameof(SetContainsOptions.Member)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetContainsStatement(options);
            var result = (string)ExecuteStatement(server, database, statement);
            return new SetContainsResponse()
            {
                Success = true,
                ContainsValue = result == "1",
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetContainsStatement(SetContainsOptions options)
        {
            var script = $@"local pv=redis.call('SISMEMBER',{Keys(1)},{Arg(1)})
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
        /// <param name="options">Set combine options</param>
        /// <returns>Return set combine response</returns>
        public SetCombineResponse SetCombine(CacheServer server, SetCombineOptions options)
        {
            if (options?.Keys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SetCombineOptions)}.{nameof(SetCombineOptions.Keys)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetCombineStatement(options);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SetCombineResponse()
            {
                Success = true,
                CombineValues = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetCombineStatement(SetCombineOptions options)
        {
            var keys = new RedisKey[options.Keys.Count];
            var keyParameters = new List<string>(options.Keys.Count);
            for (var i = 0; i < options.Keys.Count; i++)
            {
                keys[i] = options.Keys[i].GetActualKey();
                keyParameters.Add($"{Keys(i + 1)}");
            }
            var script = $@"local pv=redis.call('{RedisManager.GetSetCombineCommand(options.CombineOperation)}',{string.Join(",", keyParameters)})
{GetRefreshExpirationScript(-2, keyCount: keys.Length)}
return pv";
            var parameters = new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">Set combine and store options</param>
        /// <returns>Return set combine and store response</returns>
        public SetCombineAndStoreResponse SetCombineAndStore(CacheServer server, SetCombineAndStoreOptions options)
        {
            if (options?.SourceKeys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SetCombineAndStoreOptions)}.{nameof(SetCombineAndStoreOptions.SourceKeys)}");
            }
            if (string.IsNullOrWhiteSpace(options.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SetCombineAndStoreOptions)}.{nameof(SetCombineAndStoreOptions.DestinationKey)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetCombineAndStoreStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SetCombineAndStoreResponse()
            {
                Success = true,
                Count = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetCombineAndStoreStatement(SetCombineAndStoreOptions options)
        {
            var keys = new RedisKey[options.SourceKeys.Count + 1];
            var keyParameters = new List<string>(options.SourceKeys.Count + 1);
            keys[0] = options.DestinationKey.GetActualKey();
            keyParameters.Add($"{Keys(1)}");
            for (var i = 0; i < options.SourceKeys.Count; i++)
            {
                keys[i + 1] = options.SourceKeys[i].GetActualKey();
                keyParameters.Add($"{Keys(i + 2)}");
            }
            var script = $@"local pv=redis.call('{RedisManager.GetSetCombineCommand(options.CombineOperation)}STORE',{string.Join(",", keyParameters)})
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
        /// <param name="options">Set add options</param>
        /// <returns>Return set add response</returns>
        public SetAddResponse SetAdd(CacheServer server, SetAddOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetAddOptions)}.{nameof(SetAddOptions.Key)}");
            }
            if (options.Members.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SetAddOptions)}.{nameof(SetAddOptions.Members)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetAddStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SetAddResponse()
            {
                Success = result > 0,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSetAddStatement(SetAddOptions options)
        {
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
            values[values.Length - 1] = RedisManager.GetTotalSeconds(expire.Item2);//expire time seconds
            var script = $@"local obv=redis.call('SADD',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(options.Members.Count - 2)}
return obv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">Sorted set score options</param>
        /// <returns>Return sorted set score response</returns>
        public SortedSetScoreResponse SortedSetScore(CacheServer server, SortedSetScoreOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetScoreOptions)}.{nameof(SortedSetScoreOptions.Key)}");
            }
            if (options.Member == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetScoreOptions)}.{nameof(SortedSetScoreOptions.Member)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetScoreStatement(options);
            var result = (double?)ExecuteStatement(server, database, statement);
            return new SortedSetScoreResponse()
            {
                Success = true,
                Score = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetScoreStatement(SortedSetScoreOptions options)
        {
            var script = $@"local pv=redis.call('ZSCORE',{Keys(1)},{Arg(1)})
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
        /// <param name="options">Sorted set remove range by value options</param>
        /// <returns>Return sorted set remove range by value response</returns>
        public SortedSetRemoveRangeByValueResponse SortedSetRemoveRangeByValue(CacheServer server, SortedSetRemoveRangeByValueOptions options)
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
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRemoveRangeByValueStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortedSetRemoveRangeByValueResponse()
            {
                RemoveCount = result,
                Success = true,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRemoveRangeByValueStatement(SortedSetRemoveRangeByValueOptions options)
        {
            var script = $@"local pv=redis.call('ZREMRANGEBYLEX',{Keys(1)},{Arg(1)},{Arg(2)})
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
        /// <param name="options">Sorted set remove range by score options</param>
        /// <returns>Return sorted set remove range by score response</returns>
        public SortedSetRemoveRangeByScoreResponse SortedSetRemoveRangeByScore(CacheServer server, SortedSetRemoveRangeByScoreOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByScoreOptions)}.{nameof(SortedSetRemoveRangeByScoreOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRemoveRangeByScoreStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortedSetRemoveRangeByScoreResponse()
            {
                RemoveCount = result,
                Success = true,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRemoveRangeByScoreStatement(SortedSetRemoveRangeByScoreOptions options)
        {
            var script = $@"local pv=redis.call('ZREMRANGEBYSCORE',{Keys(1)},{Arg(1)},{Arg(2)})
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
        /// <param name="options">Sorted set remove range by rank options</param>
        /// <returns>Return sorted set remove range by rank response</returns>
        public SortedSetRemoveRangeByRankResponse SortedSetRemoveRangeByRank(CacheServer server, SortedSetRemoveRangeByRankOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByRankOptions)}.{nameof(SortedSetRemoveRangeByRankOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRemoveRangeByRankStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortedSetRemoveRangeByRankResponse()
            {
                RemoveCount = result,
                Success = true,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRemoveRangeByRankStatement(SortedSetRemoveRangeByRankOptions options)
        {
            var script = $@"local pv=redis.call('ZREMRANGEBYRANK',{Keys(1)},{Arg(1)},{Arg(2)})
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
        /// <param name="options">Sorted set remove options</param>
        /// <returns>sorted set remove response</returns>
        public SortedSetRemoveResponse SortedSetRemove(CacheServer server, SortedSetRemoveOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveOptions)}.{nameof(SortedSetRemoveOptions.Key)}");
            }
            if (options.RemoveMembers.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SortedSetRemoveOptions)}.{nameof(SortedSetRemoveOptions.RemoveMembers)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRemoveStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortedSetRemoveResponse()
            {
                Success = true,
                RemoveCount = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRemoveStatement(SortedSetRemoveOptions options)
        {
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
            var script = $@"local obv=redis.call('ZREM',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(options.RemoveMembers.Count - 2)}
return obv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">Options</param>
        /// <returns>sorted set rank response</returns>
        public SortedSetRankResponse SortedSetRank(CacheServer server, SortedSetRankOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRankOptions)}.{nameof(SortedSetRankOptions.Key)}");
            }
            if (options.Member == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetRankOptions)}.{nameof(SortedSetRankOptions.Member)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRankStatement(options);
            var result = (long?)ExecuteStatement(server, database, statement);
            return new SortedSetRankResponse()
            {
                Success = true,
                Rank = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRankStatement(SortedSetRankOptions options)
        {
            var script = $@"local pv=redis.call('Z{(options.Order == CacheOrder.Descending ? "REV" : "")}RANK',{Keys(1)},{Arg(1)})
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
        /// <param name="options">Sorted set range by value options</param>
        /// <returns>sorted set range by value response</returns>
        public SortedSetRangeByValueResponse SortedSetRangeByValue(CacheServer server, SortedSetRangeByValueOptions options)
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
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByValueStatement(options);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SortedSetRangeByValueResponse()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRangeByValueStatement(SortedSetRangeByValueOptions options)
        {
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
            var script = $@"local pv=redis.call('{command}',{Keys(1)},{Arg(1)},{Arg(2)},'LIMIT',{Arg(3)},{Arg(4)})
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
        /// <param name="options">Option</param>
        /// <returns>Return sorted set range by score with scores response</returns>
        public SortedSetRangeByScoreWithScoresResponse SortedSetRangeByScoreWithScores(CacheServer server, SortedSetRangeByScoreWithScoresOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByScoreWithScoresOptions)}.{nameof(SortedSetRangeByScoreWithScoresOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByScoreWithScoresStatement(options);
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
            return new SortedSetRangeByScoreWithScoresResponse()
            {
                Success = true,
                Members = members,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRangeByScoreWithScoresStatement(SortedSetRangeByScoreWithScoresOptions options)
        {
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
            var script = $@"local pv=redis.call('{command}',{Keys(1)},{Arg(1)},{Arg(2)},'WITHSCORES','LIMIT',{Arg(3)},{Arg(4)})
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
        /// <param name="options">Options</param>
        /// <returns>sorted set range by score response</returns>
        public SortedSetRangeByScoreResponse SortedSetRangeByScore(CacheServer server, SortedSetRangeByScoreOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByScoreOptions)}.{nameof(SortedSetRangeByScoreOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByScoreStatement(options);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SortedSetRangeByScoreResponse()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRangeByScoreStatement(SortedSetRangeByScoreOptions options)
        {
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
            var script = $@"local pv=redis.call('{command}',{Keys(1)},{Arg(1)},{Arg(2)},'LIMIT',{Arg(3)},{Arg(4)})
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
        /// <param name="options">Option</param>
        /// <returns>Return sorted set range by rank with scores response</returns>
        public SortedSetRangeByRankWithScoresResponse SortedSetRangeByRankWithScores(CacheServer server, SortedSetRangeByRankWithScoresOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByRankWithScoresOptions)}.{nameof(SortedSetRangeByRankWithScoresOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByRankWithScoresStatement(options);
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
            return new SortedSetRangeByRankWithScoresResponse()
            {
                Success = true,
                Members = members,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRangeByRankWithScoresStatement(SortedSetRangeByRankWithScoresOptions options)
        {
            var script = $@"local pv=redis.call('Z{(options.Order == CacheOrder.Descending ? "REV" : "")}RANGE',{Keys(1)},{Arg(1)},{Arg(2)},'WITHSCORES')
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
        /// <param name="options">Options</param>
        /// <returns>sorted set range by rank response</returns>
        public SortedSetRangeByRankResponse SortedSetRangeByRank(CacheServer server, SortedSetRangeByRankOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByRankOptions)}.{nameof(SortedSetRangeByRankOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByRankStatement(options);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SortedSetRangeByRankResponse()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetRangeByRankStatement(SortedSetRangeByRankOptions options)
        {
            var script = $@"local pv=redis.call('Z{(options.Order == CacheOrder.Descending ? "REV" : "")}RANGE',{Keys(1)},{Arg(1)},{Arg(2)})
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
        /// <param name="options">Option</param>
        /// <returns>Return sorted set lenght by value response</returns>
        public SortedSetLengthByValueResponse SortedSetLengthByValue(CacheServer server, SortedSetLengthByValueOptions options)
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
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetLengthByValueStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortedSetLengthByValueResponse()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetLengthByValueStatement(SortedSetLengthByValueOptions options)
        {
            var script = $@"local pv=redis.call('ZLEXCOUNT',{Keys(1)},{Arg(1)},{Arg(2)})
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
        /// <param name="options">Option</param>
        /// <returns>Return sorted set length response</returns>
        public SortedSetLengthResponse SortedSetLength(CacheServer server, SortedSetLengthOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetLengthByValueOptions)}.{nameof(SortedSetLengthByValueOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetLengthStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortedSetLengthResponse()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetLengthStatement(SortedSetLengthOptions options)
        {
            var script = $@"local pv=redis.call('ZCARD',{Keys(1)})
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
        /// <param name="options">Option</param>
        /// <returns>Return sorted set increment response</returns>
        public SortedSetIncrementResponse SortedSetIncrement(CacheServer server, SortedSetIncrementOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetIncrementOptions)}.{nameof(SortedSetIncrementOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetIncrementStatement(options);
            var result = (double)ExecuteStatement(server, database, statement);
            return new SortedSetIncrementResponse()
            {
                Success = true,
                NewScore = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetIncrementStatement(SortedSetIncrementOptions options)
        {
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
            var script = $@"local pv=redis.call('ZINCRBY',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">Option</param>
        /// <returns>Return sorted set decrement response</returns>
        public SortedSetDecrementResponse SortedSetDecrement(CacheServer server, SortedSetDecrementOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetDecrementOptions)}.{nameof(SortedSetDecrementOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetDecrementStatement(options);
            var result = (double)ExecuteStatement(server, database, statement);
            return new SortedSetDecrementResponse()
            {
                Success = true,
                NewScore = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetDecrementStatement(SortedSetDecrementOptions options)
        {
            var script = $@"local pv=redis.call('ZINCRBY',{Keys(1)},{Arg(1)},{Arg(2)})
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
        /// <param name="options">Options</param>
        /// <returns>sorted set combine and store response</returns>
        public SortedSetCombineAndStoreResponse SortedSetCombineAndStore(CacheServer server, SortedSetCombineAndStoreOptions options)
        {
            if (options?.SourceKeys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SortedSetCombineAndStoreOptions)}.{nameof(SortedSetCombineAndStoreOptions.SourceKeys)}");
            }
            if (string.IsNullOrWhiteSpace(options.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SortedSetCombineAndStoreOptions)}.{nameof(SortedSetCombineAndStoreOptions.DestinationKey)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetCombineAndStoreStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortedSetCombineAndStoreResponse()
            {
                Success = true,
                NewSetLength = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetCombineAndStoreStatement(SortedSetCombineAndStoreOptions options)
        {
            var keys = new RedisKey[options.SourceKeys.Count + 1];
            var keyParameters = new List<string>(options.SourceKeys.Count);
            var weights = new double[options.SourceKeys.Count];
            keys[0] = options.DestinationKey.GetActualKey();
            for (var i = 0; i < options.SourceKeys.Count; i++)
            {
                keys[i + 1] = options.SourceKeys[i].GetActualKey();
                keyParameters.Add($"{Keys(i + 2)}");
                weights[i] = options.Weights?.ElementAt(i) ?? 1;
            }
            var optionScript = new StringBuilder();
            var script = $@"local pv=redis.call('{RedisManager.GetSortedSetCombineCommand(options.CombineOperation)}',{Keys(1)},'{keyParameters.Count}',{string.Join(",", keyParameters)},'WEIGHTS',{string.Join(",", weights)},'AGGREGATE','{RedisManager.GetSortedSetAggregateName(options.Aggregate)}')
{GetRefreshExpirationScript(1)}
{GetRefreshExpirationScript(-2, 1, keyCount: keys.Length - 1)}
return pv";
            var expire = RedisManager.GetExpiration(options.Expiration);
            var allowSliding = RedisManager.AllowSlidingExpiration();
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
        /// <param name="options">Options</param>
        /// <returns>sorted set add response</returns>
        public SortedSetAddResponse SortedSetAdd(CacheServer server, SortedSetAddOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetAddOptions)}.{nameof(SortedSetAddOptions.Key)}");
            }
            if (options.Members.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SortedSetAddOptions)}.{nameof(SortedSetAddOptions.Members)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetAddStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortedSetAddResponse()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortedSetAddStatement(SortedSetAddOptions options)
        {
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
            values[values.Length - 1] = RedisManager.GetTotalSeconds(expire.Item2);//expire time seconds
            var script = $@"local obv=redis.call('ZADD',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(valueCount - 2)}
return obv";
            var keys = new RedisKey[]
            {
                options.Key.GetActualKey()
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">Options</param>
        /// <returns>sort response</returns>
        public SortResponse Sort(CacheServer server, SortOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortOptions)}.{nameof(SortOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortStatement(options);
            var result = (RedisValue[])ExecuteStatement(server, database, statement);
            return new SortResponse()
            {
                Success = true,
                Values = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortStatement(SortOptions options)
        {
            var script = $@"local obv=redis.call('SORT',{Keys(1)}{(string.IsNullOrWhiteSpace(options.By) ? string.Empty : $",'BY','{options.By}'")},'LIMIT',{Arg(1)},{Arg(2)}{(options.Gets.IsNullOrEmpty() ? string.Empty : $",{string.Join(",", options.Gets.Select(c => $"'GET','{c}'"))}")},{(options.Order == CacheOrder.Descending ? "'DESC'" : "'ASC'")}{(options.SortType == CacheSortType.Alphabetic ? ",'ALPHA'" : string.Empty)})
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
        /// <param name="options">Options</param>
        /// <returns>sort and store response</returns>
        public SortAndStoreResponse SortAndStore(CacheServer server, SortAndStoreOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.SourceKey))
            {
                throw new ArgumentNullException($"{nameof(SortAndStoreOptions)}.{nameof(SortAndStoreOptions.SourceKey)}");
            }
            if (string.IsNullOrWhiteSpace(options?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SortAndStoreOptions)}.{nameof(SortAndStoreOptions.DestinationKey)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortAndStoreStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new SortAndStoreResponse()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetSortAndStoreStatement(SortAndStoreOptions options)
        {
            var script = $@"local obv=redis.call('SORT',{Keys(1)}{(string.IsNullOrWhiteSpace(options.By) ? string.Empty : $",'BY','{options.By}'")},'LIMIT',{Arg(1)},{Arg(2)}{(options.Gets.IsNullOrEmpty() ? string.Empty : $",{string.Join(",", options.Gets.Select(c => $"'GET','{c}'"))}")},{(options.Order == CacheOrder.Descending ? "'DESC'" : "'ASC'")}{(options.SortType == CacheSortType.Alphabetic ? ",'ALPHA'" : string.Empty)},'STORE',{Keys(2)})
{GetRefreshExpirationScript()}
{GetRefreshExpirationScript(3, 1)}
return obv";
            var expire = RedisManager.GetExpiration(options.Expiration);
            var allowSliding = RedisManager.AllowSlidingExpiration();
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
        /// <param name="options">Options</param>
        /// <returns>key type response</returns>
        public TypeResponse KeyType(CacheServer server, TypeOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(TypeOptions)}.{nameof(TypeOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyTypeStatement(options);
            var result = (string)ExecuteStatement(server, database, statement);
            return new TypeResponse()
            {
                Success = true,
                KeyType = RedisManager.GetCacheKeyType(result),
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyTypeStatement(TypeOptions options)
        {
            var script = $@"local obv=redis.call('TYPE',{Keys(1)})
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
        /// <param name="options">Options</param>
        /// <returns>key time to live response</returns>
        public TimeToLiveResponse KeyTimeToLive(CacheServer server, TimeToLiveOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(TimeToLiveOptions)}.{nameof(TimeToLiveOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyTimeToLiveStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new TimeToLiveResponse()
            {
                Success = true,
                TimeToLiveSeconds = result,
                KeyExist = result != -2,
                Perpetual = result == -1,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyTimeToLiveStatement(TimeToLiveOptions options)
        {
            var script = $@"local obv=redis.call('TTL',{Keys(1)})
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
        /// <param name="options">Options</param>
        /// <returns>key restore response</returns>
        public RestoreResponse KeyRestore(CacheServer server, RestoreOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(RestoreOptions)}.{nameof(RestoreOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyRestoreStatement(options);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new RestoreResponse()
            {
                Success = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyRestoreStatement(RestoreOptions options)
        {
            var script = $@"local obv= string.lower(tostring(redis.call('RESTORE',{Keys(1)},'0',{Arg(1)})))=='ok'
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
        /// <param name="options">Options</param>
        /// <returns>key rename response</returns>
        public RenameResponse KeyRename(CacheServer server, RenameOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(RenameOptions)}.{nameof(RenameOptions.Key)}");
            }
            if (string.IsNullOrWhiteSpace(options?.NewKey))
            {
                throw new ArgumentNullException($"{nameof(RenameOptions)}.{nameof(RenameOptions.NewKey)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyRenameStatement(options);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new RenameResponse()
            {
                Success = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyRenameStatement(RenameOptions options)
        {
            var cacheKey = options.Key.GetActualKey();
            var newCacheKey = options.NewKey.GetActualKey();
            var script = $@"{GetRefreshExpirationScript(-2)}
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
        /// <param name="options">Options</param>
        /// <returns>key random response</returns>
        public RandomResponse KeyRandom(CacheServer server, RandomOptions options)
        {
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyRandomStatement(options);
            var result = (string)ExecuteStatement(server, database, statement);
            return new RandomResponse()
            {
                Success = true,
                Key = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyRandomStatement(RandomOptions options)
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
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">Options</param>
        /// <returns>key persist response</returns>
        public PersistResponse KeyPersist(CacheServer server, PersistOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(PersistOptions)}.{nameof(PersistOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyPersistStatement(options);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new PersistResponse()
            {
                Success = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyPersistStatement(PersistOptions options)
        {
            var cacheKey = options.Key.GetActualKey();
            var keys = new RedisKey[1] { cacheKey };
            var parameters = new RedisValue[0];
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">Options</param>
        /// <returns>key move response</returns>
        public MoveResponse KeyMove(CacheServer server, MoveOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(MoveOptions)}.{nameof(MoveOptions.Key)}");
            }
            if (!int.TryParse(options.DatabaseName, out var dbIndex) || dbIndex < 0)
            {
                throw new ArgumentException($"{nameof(MoveOptions)}.{nameof(MoveOptions.DatabaseName)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyMoveStatement(options);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new MoveResponse()
            {
                Success = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyMoveStatement(MoveOptions options)
        {
            var cacheKey = options.Key.GetActualKey();
            var keys = new RedisKey[1] { cacheKey };
            var parameters = new RedisValue[1]
            {
                options.DatabaseName
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        redis.call('SELECT','{options.DatabaseName}')
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
        /// <param name="options">Options</param>
        /// <returns>key migrate response</returns>
        public MigrateKeyResponse KeyMigrate(CacheServer server, MigrateKeyOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(MigrateKeyOptions)}.{nameof(MigrateKeyOptions.Key)}");
            }
            if (options.Destination == null)
            {
                throw new ArgumentNullException($"{nameof(MigrateKeyOptions)}.{nameof(MigrateKeyOptions.Destination)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyMigrateStatement(options);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new MigrateKeyResponse()
            {
                Success = true,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyMigrateStatement(MigrateKeyOptions options)
        {
            var cacheKey = options.Key.GetActualKey();
            var keys = new RedisKey[1] { cacheKey };
            var parameters = new RedisValue[1]
            {
                options.CopyCurrent
            };
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            var script = $@"local obv=string.lower(tostring(redis.call('MIGRATE','{options.Destination.Port}','{options.Destination.Host}',{Keys(1)},'{options.TimeOutMilliseconds}'{(options.CopyCurrent ? ",'COPY'" : string.Empty)}{(options.ReplaceDestination ? ",'REPLACE'" : string.Empty)})))
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
        /// <param name="options">Options</param>
        /// <returns>key expire response</returns>
        public ExpireResponse KeyExpire(CacheServer server, ExpireOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(ExpireOptions)}.{nameof(ExpireOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyExpireStatement(options);
            var result = (bool)ExecuteStatement(server, database, statement);
            return new ExpireResponse()
            {
                Success = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyExpireStatement(ExpireOptions options)
        {
            var cacheKey = options.Key.GetActualKey();
            var expire = RedisManager.GetExpiration(options.Expiration);
            var seconds = RedisManager.GetTotalSeconds(expire.Item2);
            var keys = new RedisKey[0];
            var parameters = new RedisValue[0];
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// RESTORE options.
        /// </summary>
        /// <param name="server">Server</param>
        /// <param name="options">Options</param>
        /// <returns>key dump response</returns>
        public DumpResponse KeyDump(CacheServer server, DumpOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(DumpOptions)}.{nameof(DumpOptions.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyDumpStatement(options);
            var result = (byte[])ExecuteStatement(server, database, statement);
            return new DumpResponse()
            {
                Success = true,
                ByteValues = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyDumpStatement(DumpOptions options)
        {
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
        /// <param name="options">Options</param>
        /// <returns>key delete response</returns>
        public DeleteResponse KeyDelete(CacheServer server, DeleteOptions options)
        {
            if (options?.Keys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(DeleteOptions)}.{nameof(DeleteOptions.Keys)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyDeleteStatement(options);
            var count = database.RemoteDatabase.KeyDelete(statement.Keys, statement.Flags);
            return new DeleteResponse()
            {
                Success = true,
                DeleteCount = count,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyDeleteStatement(DeleteOptions options)
        {
            var keys = options.Keys.Select(c => { RedisKey key = c.GetActualKey(); return key; }).ToArray();
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
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
        /// <param name="options">Options</param>
        /// <returns></returns>
        public ExistResponse KeyExist(CacheServer server, ExistOptions options)
        {
            if (options.Keys.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(ExistOptions)}.{nameof(ExistOptions.Keys)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyExistStatement(options);
            var result = (long)ExecuteStatement(server, database, statement);
            return new ExistResponse()
            {
                Success = true,
                KeyCount = result,
                CacheServer = server,
                Database = database
            };
        }

        RedisStatement GetKeyExistStatement(ExistOptions options)
        {
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
        /// <param name="options">Options</param>
        /// <returns>Return get all database response</returns>
        public GetAllDataBaseResponse GetAllDataBase(CacheServer server, GetAllDataBaseOptions options)
        {
            if (server == null)
            {
                throw new ArgumentNullException($"{nameof(server)}");
            }
            if (options?.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(GetAllDataBaseOptions)}.{nameof(GetAllDataBaseOptions.EndPoint)}");
            }
            using (var conn = RedisManager.GetConnection(server, new CacheEndPoint[1] { options.EndPoint }))
            {
                var response = new GetAllDataBaseResponse()
                {
                    Success = true,
                    CacheServer = server,
                    EndPoint = options.EndPoint
                };
                var configs = conn.GetServer(string.Format("{0}:{1}", options.EndPoint.Host, options.EndPoint.Port)).ConfigGet("databases");
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
        /// <param name="options">Options</param>
        /// <returns>Return get keys response</returns>
        public GetKeysResponse GetKeys(CacheServer server, GetKeysOptions options)
        {
            if (server == null)
            {
                throw new ArgumentNullException($"{nameof(server)}");
            }
            if (options?.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(GetAllDataBaseOptions)}.{nameof(GetAllDataBaseOptions.EndPoint)}");
            }
            if (!int.TryParse(server.Database, out int dbIndex))
            {
                throw new SixnetException($"Redis database {server.Database} is invalid");
            }

            var query = options.Query;
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
            using (var conn = RedisManager.GetConnection(server, new CacheEndPoint[1] { options.EndPoint }))
            {
                var redisServer = conn.GetServer(string.Format("{0}:{1}", options.EndPoint.Host, options.EndPoint.Port));
                var keys = redisServer.Keys(dbIndex, searchString, query.PageSize, 0, (query.Page - 1) * query.PageSize, CommandFlags.None);
                var itemList = keys.Select(c => { CacheKey key = ConstantCacheKey.Create(c); return key; }).ToList();
                var totalCount = redisServer.DatabaseSize(dbIndex);
                var keyItemPaging = new CachePaging<CacheKey>(query.Page, query.PageSize, totalCount, itemList);
                return new GetKeysResponse()
                {
                    Success = true,
                    Keys = keyItemPaging,
                    CacheServer = server,
                    EndPoint = options.EndPoint,
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
        /// <param name="options">Options</param>
        /// <returns>clear data response</returns>
        public ClearDataResponse ClearData(CacheServer server, ClearDataOptions options)
        {
            if (options.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(ClearDataOptions)}.{nameof(ClearDataOptions.EndPoint)}");
            }
            if (!int.TryParse(server.Database, out int dbIndex))
            {
                throw new SixnetException($"Redis database {server.Database} is invalid");
            }
            var cmdFlags = RedisManager.GetCommandFlags(options.CommandFlags);
            using (var conn = RedisManager.GetConnection(server, new CacheEndPoint[1] { options.EndPoint }))
            {
                var redisServer = conn.GetServer(string.Format("{0}:{1}", options.EndPoint.Host, options.EndPoint.Port));
                redisServer.FlushDatabase(dbIndex, cmdFlags);
                return new ClearDataResponse()
                {
                    Success = true,
                    CacheServer = server,
                    EndPoint = options.EndPoint,
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
        /// <param name="options">Options</param>
        /// <returns>get key detail response</returns>
        public GetDetailResponse GetKeyDetail(CacheServer server, GetDetailOptions options)
        {
            if (string.IsNullOrWhiteSpace(options?.Key))
            {
                throw new ArgumentNullException($"{nameof(GetDetailOptions)}.{nameof(GetDetailOptions.Key)}");
            }
            if (options.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(GetDetailOptions)}.{nameof(GetDetailOptions.EndPoint)}");
            }
            if (!int.TryParse(server.Database, out int dbIndex))
            {
                throw new SixnetException($"Redis database {server.Database} is invalid");
            }
            using (var conn = RedisManager.GetConnection(server, new CacheEndPoint[1] { options.EndPoint }))
            {
                var redisDatabase = conn.GetDatabase(dbIndex);
                var redisKeyType = redisDatabase.KeyType(options.Key.GetActualKey());
                var cacheKeyType = RedisManager.GetCacheKeyType(redisKeyType.ToString());
                var keyItem = new CacheEntry()
                {
                    Key = options.Key.GetActualKey(),
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
                return new GetDetailResponse()
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
        /// <param name="options">Options</param>
        /// <returns>get server config response</returns>
        public GetServerConfigurationResponse GetServerConfiguration(CacheServer server, GetServerConfigurationOptions options)
        {
            if (options?.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(GetServerConfigurationOptions)}.{nameof(GetServerConfigurationOptions.EndPoint)}");
            }
            using (var conn = RedisManager.GetConnection(server, new CacheEndPoint[1] { options.EndPoint }))
            {
                var config = new RedisServerConfiguration();
                var redisServer = conn.GetServer(string.Format("{0}:{1}", options.EndPoint.Host, options.EndPoint.Port));
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
                                var saveInfos = new List<DataChangeSaveOptions>();
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
                return new GetServerConfigurationResponse()
                {
                    ServerConfiguration = config,
                    Success = true,
                    CacheServer = server,
                    EndPoint = options.EndPoint
                };
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
        public SaveServerConfigurationResponse SaveServerConfiguration(CacheServer server, SaveServerConfigurationOptions options)
        {
            if (!(options?.ServerConfiguration is RedisServerConfiguration config))
            {
                throw new SixnetException($"{nameof(SaveServerConfigurationOptions.ServerConfiguration)} is not {nameof(RedisServerConfiguration)}");
            }
            if (options?.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(SaveServerConfigurationOptions)}.{nameof(SaveServerConfigurationOptions.EndPoint)}");
            }
            using (var conn = RedisManager.GetConnection(server, new CacheEndPoint[1] { options.EndPoint }))
            {
                var redisServer = conn.GetServer(string.Format("{0}:{1}", options.EndPoint.Host, options.EndPoint.Port));
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
                return new SaveServerConfigurationResponse()
                {
                    Success = true,
                    CacheServer = server,
                    EndPoint = options.EndPoint
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

        static T GetNoDatabaseResponse<T>(CacheServer server) where T : CacheResponse, new()
        {
            if (SixnetCacher.ThrowOnMissingDatabase)
            {
                throw new SixnetException("No cache database specified");
            }
            return CacheResponse.NoDatabase<T>(server);
        }

        static T GetNoValueResponse<T>(CacheServer server) where T : CacheResponse, new()
        {
            return CacheResponse.FailResponse<T>("", "No value specified", server);
        }

        static T GetNoKeyResponse<T>(CacheServer server) where T : CacheResponse, new()
        {
            return CacheResponse.FailResponse<T>("", "No key specified", server);
        }

        RedisResult ExecuteStatement(CacheServer server, RedisDatabase database, RedisStatement statement)
        {
            return ExecuteStatement(server, database, statement);
        }

        #endregion
    }
}
