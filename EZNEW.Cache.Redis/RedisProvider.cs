using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Threading;
using StackExchange.Redis;
using EZNEW.Cache.Keys.Response;
using EZNEW.Cache.Keys.Request;
using EZNEW.Cache.Hash.Request;
using EZNEW.Cache.Hash.Response;
using EZNEW.Cache.String.Response;
using EZNEW.Cache.String.Request;
using EZNEW.Cache.SortedSet;
using EZNEW.Cache.List.Request;
using EZNEW.Cache.List.Response;
using EZNEW.Cache.SortedSet.Request;
using EZNEW.Cache.SortedSet.Response;
using EZNEW.Cache.Set.Request;
using EZNEW.Cache.Set.Response;
using EZNEW.Cache.Server.Response;
using EZNEW.Cache.Server.Request;
using EZNEW.Cache.Constant;
using EZNEW.Cache.Server;
using EZNEW.ValueType;
using EZNEW.Serialize;
using Microsoft.Extensions.Options;

namespace EZNEW.Cache.Redis
{
    /// <summary>
    /// Implements ICacheProvider by Redis
    /// </summary>
    public class RedisProvider : ICacheProvider
    {
        /// <summary>
        /// Redis connections
        /// </summary>
        static readonly ConcurrentDictionary<string, ConnectionMultiplexer> RedisConnections = new ConcurrentDictionary<string, ConnectionMultiplexer>();

        /// <summary>
        /// Expiration key suffix
        /// </summary>
        const string ExpirationKeySuffix = ":eznewexp";

        #region String

        #region StringSetRange

        /// <summary>
        /// Overwrites part of the string stored at key, starting at the specified offset,
        /// for the entire length of value. If the offset is larger than the current length
        /// of the string at key, the string is padded with zero-bytes to make offset fit.
        /// Non-existing keys are considered as empty strings, so this option will make
        /// sure it holds a string large enough to be able to set value at offset.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">String set range option</param>
        /// <returns>Return string set range response</returns>
        public async Task<StringSetRangeResponse> StringSetRangeAsync(CacheServer server, StringSetRangeOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringSetRangeOption)}.{nameof(StringSetRangeOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local len=redis.call('SETRANGE',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return len";
            var expire = GetExpiration(option.Expiration);
            var result = await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Offset,
                option.Value,
                option.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                GetTotalSeconds(expire.Item2),//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new StringSetRangeResponse()
            {
                Success = true,
                NewValueLength = (long)result
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
        /// <param name="option">String set bit option</param>
        /// <returns>Return string set bit response</returns>
        public async Task<StringSetBitResponse> StringSetBitAsync(CacheServer server, StringSetBitOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringSetBitOption)}.{nameof(StringSetBitOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv=redis.call('SETBIT',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return obv";
            var expire = GetExpiration(option.Expiration);
            var result = await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Offset,
                option.Bit,
                option.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                GetTotalSeconds(expire.Item2),//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new StringSetBitResponse()
            {
                Success = true,
                OldBitValue = (bool)result
            };
        }

        #endregion

        #region StringSet

        /// <summary>
        /// Set key to hold the string value. If key already holds a value, it is overwritten,
        /// regardless of its type.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">String set option</param>
        /// <returns>Return string set response</returns>
        public async Task<StringSetResponse> StringSetAsync(CacheServer server, StringSetOption option)
        {
            if (option?.Items.IsNullOrEmpty() ?? true)
            {
                return new StringSetResponse()
                {
                    Success = false,
                    Message = "No value to operate on is specified"
                };
            }
            var cacheDatabase = GetDatabase(server);
            var itemCount = option.Items.Count;
            var valueCount = itemCount * 5;
            var allowSlidingExpire = RedisManager.AllowSlidingExpiration();
            RedisKey[] setKeys = new RedisKey[itemCount];
            RedisValue[] setValues = new RedisValue[valueCount];
            for (var i = 0; i < itemCount; i++)
            {
                var nowItem = option.Items[i];
                var nowExpire = GetExpiration(nowItem.Expiration);
                setKeys[i] = nowItem.Key.GetActualKey();

                var argIndex = i * 5;
                var allowSliding = nowExpire.Item1 && allowSlidingExpire;
                setValues[argIndex] = nowItem.Value.ToNullableString();
                setValues[argIndex + 1] = nowItem.Expiration == null;
                setValues[argIndex + 2] = allowSliding;
                setValues[argIndex + 3] = nowExpire.Item2.HasValue ? GetTotalSeconds(nowExpire.Item2) : (allowSliding ? 0 : -1);
                setValues[argIndex + 4] = GetSetWhenCommand(nowItem.When);
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
    exkey=ckey..'{ExpirationKeySuffix}'
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
            var result = await cacheDatabase.ScriptEvaluateAsync(script, setKeys, setValues, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);

            return new StringSetResponse()
            {
                Success = true,
                Results = ((RedisValue[])result).Select(c => new StringEntrySetResult() { SetSuccess = true, Key = c }).ToList()
            };
        }

        #endregion

        #region StringLength

        /// <summary>
        /// Returns the length of the string value stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">String length option</param>
        /// <returns>Return string length response</returns>
        public async Task<StringLengthResponse> StringLengthAsync(CacheServer server, StringLengthOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringLengthOption)}.{nameof(StringLengthOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv=redis.call('STRLEN',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return obv";
            var result = await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new StringLengthResponse()
            {
                Success = true,
                Length = (long)result
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
        /// <param name="option">String increment option</param>
        /// <returns>Return string increment response</returns>
        public async Task<StringIncrementResponse> StringIncrementAsync(CacheServer server, StringIncrementOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringIncrementOption)}.{nameof(StringIncrementOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv=redis.call('INCRBY',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return obv";
            var expire = GetExpiration(option.Expiration);
            var result = await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Value,
                option.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                GetTotalSeconds(expire.Item2),//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new StringIncrementResponse()
            {
                Success = true,
                NewValue = (long)result
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
        /// <param name="option">String get with expiry ption</param>
        /// <returns>Return string get with expiry response</returns>
        public async Task<StringGetWithExpiryResponse> StringGetWithExpiryAsync(CacheServer server, StringGetWithExpiryOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetWithExpiryOption)}.{nameof(StringGetWithExpiryOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
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
            var result = (RedisValue[])(await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false));
            return new StringGetWithExpiryResponse()
            {
                Success = true,
                Value = result[0],
                Expiry = TimeSpan.FromSeconds((long)result[1])
            };
        }

        #endregion

        #region StringGetSet

        /// <summary>
        /// Atomically sets key to value and returns the old value stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">String get set option</param>
        /// <returns>Return string get set response</returns>
        public async Task<StringGetSetResponse> StringGetSetAsync(CacheServer server, StringGetSetOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetSetOption)}.{nameof(StringGetSetOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local ov=redis.call('GETSET',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return ov";
            var expire = GetExpiration(option.Expiration);
            var result = await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.NewValue,
                option.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                GetTotalSeconds(expire.Item2),//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new StringGetSetResponse()
            {
                Success = true,
                OldValue = (string)result
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
        /// <param name="option">String get range option</param>
        /// <returns>Return string get range response</returns>
        public async Task<StringGetRangeResponse> StringGetRangeAsync(CacheServer server, StringGetRangeOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetRangeOption)}.{nameof(StringGetRangeOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local ov=redis.call('GETRANGE',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return ov";
            var result = await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Start,
                option.End,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new StringGetRangeResponse()
            {
                Success = true,
                Value = (string)result
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
        /// <param name="option">String get bit option</param>
        /// <returns>Return string get bit response</returns>
        public async Task<StringGetBitResponse> StringGetBitAsync(CacheServer server, StringGetBitOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetBitOption)}.{nameof(StringGetBitOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local ov=redis.call('GETBIT',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return ov";
            var result = await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Offset,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new StringGetBitResponse()
            {
                Success = true,
                Bit = (bool)result
            };
        }

        #endregion

        #region StringGet

        /// <summary>
        /// Returns the values of all specified keys. For every key that does not hold a
        /// string value or does not exist, the special value nil is returned.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">String get option</param>
        /// <returns>Return string get response</returns>
        public async Task<StringGetResponse> StringGetAsync(CacheServer server, StringGetOption option)
        {
            if (option?.Keys.IsNullOrEmpty() ?? true)
            {
                return new StringGetResponse()
                {
                    Success = false,
                    Message = "No key value is specified"
                };
            }
            var cacheDatabase = GetDatabase(server);
            var keys = option.Keys.Select(c => { RedisKey rv = c.GetActualKey(); return rv; }).ToArray();
            string script = $@"local vals={{}}
for ki=1,{keys.Length}
do
    local cv=redis.call('GET',KEYS[ki])
    if cv
    then
        vals[ki]=KEYS[ki]..'$::$'..cv
    end
end
{GetRefreshExpirationScript(-2, keyCount: keys.Length)}
return vals";
            var result = (RedisValue[])(await cacheDatabase.ScriptEvaluateAsync(script, keys,
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false));
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
                        Value = valueArray[1]
                    };
                }).ToList()
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
        /// <param name="option">String decrement option</param>
        /// <returns>Return string decrement response</returns>
        public async Task<StringDecrementResponse> StringDecrementAsync(CacheServer server, StringDecrementOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringDecrementOption)}.{nameof(StringDecrementOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv=redis.call('DECRBY',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return obv";
            var expire = GetExpiration(option.Expiration);
            var result = await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Value,
                option.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                GetTotalSeconds(expire.Item2),//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new StringDecrementResponse()
            {
                Success = true,
                NewValue = (long)result
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
        /// <param name="option">String bit position option</param>
        /// <returns>Return string bit position response</returns>
        public async Task<StringBitPositionResponse> StringBitPositionAsync(CacheServer server, StringBitPositionOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringBitPositionOption)}.{nameof(StringBitPositionOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv=redis.call('BITPOS',{Keys(1)},{Arg(1)},{Arg(2)},{Arg(3)})
{GetRefreshExpirationScript(1)}
return obv";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Bit,
                option.Start,
                option.End,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new StringBitPositionResponse()
            {
                Success = true,
                Position = result,
                HasValue = result >= 0
            };
        }

        #endregion

        #region StringBitOperation

        /// <summary>
        /// Perform a bitwise operation between multiple keys (containing string values)
        ///  and store the result in the destination key. The BITOP option supports four
        ///  bitwise operations; note that NOT is a unary operator: the second key should
        ///  be omitted in this case and only the first key will be considered. The result
        /// of the operation is always stored at destkey.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">String bit operation option</param>
        /// <returns>Return string bit operation response</returns>
        public async Task<StringBitOperationResponse> StringBitOperationAsync(CacheServer server, StringBitOperationOption option)
        {
            if (option?.Keys.IsNullOrEmpty() ?? true)
            {
                return new StringBitOperationResponse()
                {
                    Success = false,
                    DestinationValueLength = 0
                };
            }
            if (string.IsNullOrWhiteSpace(option?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(StringBitOperationOption)}.{nameof(StringBitOperationOption.DestinationKey)}");
            }
            var cacheDatabase = GetDatabase(server);
            var keys = new RedisKey[option.Keys.Count + 1];
            var keyParameters = new string[option.Keys.Count + 1];
            keys[0] = option.DestinationKey.GetActualKey();
            keyParameters[0] = "KEYS[1]";
            for (var i = 0; i < option.Keys.Count; i++)
            {
                keys[i + 1] = option.Keys[i].GetActualKey();
                keyParameters[i + 1] = $"KEYS[{2 + i}]";
            }
            string script = $@"local obv=redis.call('BITOP',{Arg(1)},{string.Join(",", keyParameters)})
{GetRefreshExpirationScript(-1)}
{GetRefreshExpirationScript(2, 1, option.Keys.Count)}
return obv";
            var expire = GetExpiration(option.Expiration);
            bool allowSlidingExpiration = RedisManager.AllowSlidingExpiration();
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, keys,
            new RedisValue[]
            {
                GetBitOperator(option.Bitwise),
                option.Expiration==null,//refresh current time
                expire.Item1&&allowSlidingExpiration,//whether allow set refresh time
                GetTotalSeconds(expire.Item2),//expire time seconds,
                true,//refresh current time-source key
                allowSlidingExpiration,//whether allow set refresh time-source key,
                0//expire time seconds-source key

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new StringBitOperationResponse()
            {
                Success = true,
                DestinationValueLength = result
            };
        }

        #endregion

        #region StringBitCount

        /// <summary>
        /// Count the number of set bits (population counting) in a string. By default all
        /// the bytes contained in the string are examined.It is possible to specify the
        /// counting operation only in an interval passing the additional arguments start
        /// and end. Like for the GETRANGE option start and end can contain negative values
        /// in order to index bytes starting from the end of the string, where -1 is the
        /// last byte, -2 is the penultimate, and so forth.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">String bit count option</param>
        /// <returns>Return string bit count response</returns>
        public async Task<StringBitCountResponse> StringBitCountAsync(CacheServer server, StringBitCountOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringBitCountOption)}.{nameof(StringBitCountOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv=redis.call('BITCOUNT',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return obv";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Start,
                option.End,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new StringBitCountResponse()
            {
                Success = true,
                BitNum = result
            };
        }

        #endregion

        #region StringAppend

        /// <summary>
        /// If key already exists and is a string, this option appends the value at the
        /// end of the string. If key does not exist it is created and set as an empty string,
        /// so APPEND will be similar to SET in this special case.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">String append option</param>
        /// <returns>Return string append response</returns>
        public async Task<StringAppendResponse> StringAppendAsync(CacheServer server, StringAppendOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringAppendOption)}.{nameof(StringAppendOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv=redis.call('APPEND',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return obv";
            var expire = GetExpiration(option.Expiration);
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Value,
                option.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                GetTotalSeconds(expire.Item2),//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new StringAppendResponse()
            {
                Success = true,
                NewValueLength = result
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
        /// <param name="server">server</param>
        /// <param name="option">List trim option</param>
        /// <returns>Return list trim response</returns>
        public async Task<ListTrimResponse> ListTrimAsync(CacheServer server, ListTrimOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListTrimOption)}.{nameof(ListTrimOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"redis.call('LTRIM',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Start,
                option.Stop,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new ListTrimResponse()
            {
                Success = true
            };
        }

        #endregion

        #region ListSetByIndex

        /// <summary>
        /// Sets the list element at index to value. For more information on the index argument,
        ///  see ListGetByIndex. An error is returned for out of range indexes.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">List set by index option</param>
        /// <returns>Return list set by index response</returns>
        public async Task<ListSetByIndexResponse> ListSetByIndexAsync(CacheServer server, ListSetByIndexOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListSetByIndexOption)}.{nameof(ListSetByIndexOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv=redis.call('LSET',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return obv['ok']";
            var result = (string)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Index,
                option.Value,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new ListSetByIndexResponse()
            {
                Success = string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase)
            };
        }

        #endregion

        #region ListRightPush

        /// <summary>
        /// Insert all the specified values at the tail of the list stored at key. If key
        /// does not exist, it is created as empty list before performing the push operation.
        /// Elements are inserted one after the other to the tail of the list, from the leftmost
        /// element to the rightmost element. So for instance the option RPUSH mylist a
        /// b c will result into a list containing a as first element, b as second element
        /// and c as third element.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">List right push option</param>
        /// <returns>Return list right push</returns>
        public async Task<ListRightPushResponse> ListRightPushAsync(CacheServer server, ListRightPushOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRightPushOption)}.{nameof(ListRightPushOption.Key)}");
            }
            if (option?.Values.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentException($"{nameof(ListRightPushOption)}.{nameof(ListRightPushOption.Values)}");
            }
            var cacheDatabase = GetDatabase(server);
            var values = new RedisValue[option.Values.Count + 3];
            var valueParameters = new string[option.Values.Count];
            for (var i = 0; i < option.Values.Count; i++)
            {
                values[i] = option.Values[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            string script = $@"local obv=redis.call('RPUSH',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(option.Values.Count - 2)}
return obv";
            var expire = GetExpiration(option.Expiration);
            values[values.Length - 3] = option.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = expire.Item2?.TotalSeconds ?? 0;//expire time seconds
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            values, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new ListRightPushResponse()
            {
                Success = true,
                NewListLength = result
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
        /// <param name="option">List right pop left push option</param>
        /// <returns>Return list right pop left response</returns>
        public async Task<ListRightPopLeftPushResponse> ListRightPopLeftPushAsync(CacheServer server, ListRightPopLeftPushOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.SourceKey))
            {
                throw new ArgumentNullException($"{nameof(ListRightPopLeftPushOption)}.{nameof(ListRightPopLeftPushOption.SourceKey)}");
            }
            if (string.IsNullOrWhiteSpace(option?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(ListRightPopLeftPushOption)}.{nameof(ListRightPopLeftPushOption.DestinationKey)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('RPOPLPUSH',{Keys(1)},{Keys(2)})
{GetRefreshExpirationScript(-2)}
{GetRefreshExpirationScript(1, 1)}
return pv";
            var expire = GetExpiration(option.Expiration);
            bool allowSlidingExpiration = RedisManager.AllowSlidingExpiration();
            var result = (string)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.SourceKey.GetActualKey(),
                option.DestinationKey.GetActualKey()
            },
            new RedisValue[]
            {
                true,//refresh current time-source key
                allowSlidingExpiration,//whether allow set refresh time-source key
                0,//expire time seconds-source key

                option.Expiration==null,//refresh current time
                expire.Item1&&allowSlidingExpiration,//whether allow set refresh time
                GetTotalSeconds(expire.Item2)//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new ListRightPopLeftPushResponse()
            {
                Success = true,
                PopValue = result
            };
        }

        #endregion

        #region ListRightPop

        /// <summary>
        /// Removes and returns the last element of the list stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">List right pop option</param>
        /// <returns>Return list right pop response</returns>
        public async Task<ListRightPopResponse> ListRightPopAsync(CacheServer server, ListRightPopOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRightPopOption)}.{nameof(ListRightPopOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('RPOP',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var result = (string)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new ListRightPopResponse()
            {
                Success = true,
                PopValue = result
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
        /// <param name="option">List remove option</param>
        /// <returns>Return list remove response</returns>
        public async Task<ListRemoveResponse> ListRemoveAsync(CacheServer server, ListRemoveOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRemoveOption)}.{nameof(ListRemoveOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local rc=redis.call('LREM',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return rc";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Count,
                option.Value,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new ListRemoveResponse()
            {
                Success = true,
                RemoveCount = result
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
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>list range response</returns>
        public async Task<ListRangeResponse> ListRangeAsync(CacheServer server, ListRangeOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRangeOption)}.{nameof(ListRangeOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local rc=redis.call('LRANGE',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return rc";
            var result = (RedisValue[])await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Start,
                option.Stop,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new ListRangeResponse()
            {
                Success = true,
                Values = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0)
            };
        }

        #endregion

        #region ListLength

        /// <summary>
        /// Returns the length of the list stored at key. If key does not exist, it is interpreted
        ///  as an empty list and 0 is returned.
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>list length response</returns>
        public async Task<ListLengthResponse> ListLengthAsync(CacheServer server, ListLengthOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListLengthOption)}.{nameof(ListLengthOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local len=redis.call('LLEN',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return len";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new ListLengthResponse()
            {
                Success = true,
                Length = result
            };
        }

        #endregion

        #region ListLeftPush

        /// <summary>
        /// Insert the specified value at the head of the list stored at key. If key does
        ///  not exist, it is created as empty list before performing the push operations.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">List left push option</param>
        /// <returns>Return list left push response</returns>
        public async Task<ListLeftPushResponse> ListLeftPushAsync(CacheServer server, ListLeftPushOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListLeftPushOption)}.{nameof(ListLeftPushOption.Key)}");
            }
            if (option?.Values.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentException($"{nameof(ListRightPushOption)}.{nameof(ListRightPushOption.Values)}");
            }
            var cacheDatabase = GetDatabase(server);
            var values = new RedisValue[option.Values.Count + 3];
            var valueParameters = new string[option.Values.Count];
            for (var i = 0; i < option.Values.Count; i++)
            {
                values[i] = option.Values[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            string script = $@"local obv=redis.call('LPUSH',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(option.Values.Count - 2)}
return obv";
            var expire = GetExpiration(option.Expiration);
            values[values.Length - 3] = option.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = expire.Item2?.TotalSeconds ?? 0;//expire time seconds
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            values, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new ListLeftPushResponse()
            {
                Success = true,
                NewListLength = result
            };
        }

        #endregion

        #region ListLeftPop

        /// <summary>
        /// Removes and returns the first element of the list stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">List left pop option</param>
        /// <returns>list left pop response</returns>
        public async Task<ListLeftPopResponse> ListLeftPopAsync(CacheServer server, ListLeftPopOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListLeftPopOption)}.{nameof(ListLeftPopOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('LPOP',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var result = (string)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new ListLeftPopResponse()
            {
                Success = true,
                PopValue = result
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
        /// <param name="option">List insert before option</param>
        /// <returns>Return list insert begore response</returns>
        public async Task<ListInsertBeforeResponse> ListInsertBeforeAsync(CacheServer server, ListInsertBeforeOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListInsertBeforeOption)}.{nameof(ListInsertBeforeOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('LINSERT',{Keys(1)},'BEFORE',{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.PivotValue,
                option.InsertValue,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new ListInsertBeforeResponse()
            {
                Success = result > 0,
                NewListLength = result
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
        /// <param name="option">List insert after option</param>
        /// <returns>Return list insert after response</returns>
        public async Task<ListInsertAfterResponse> ListInsertAfterAsync(CacheServer server, ListInsertAfterOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListInsertAfterOption)}.{nameof(ListInsertAfterOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('LINSERT',{Keys(1)},'AFTER',{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.PivotValue,
                option.InsertValue,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new ListInsertAfterResponse()
            {
                Success = result > 0,
                NewListLength = result
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
        /// <param name="option">List get by index option</param>
        /// <returns>Return list get by index response</returns>
        public async Task<ListGetByIndexResponse> ListGetByIndexAsync(CacheServer server, ListGetByIndexOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListInsertAfterOption)}.{nameof(ListInsertAfterOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('LINDEX',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var result = (string)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Index,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new ListGetByIndexResponse()
            {
                Success = true,
                Value = result
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
        /// <param name="option">Hash values option</param>
        /// <returns>Return hash values response</returns>
        public async Task<HashValuesResponse> HashValuesAsync(CacheServer server, HashValuesOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashValuesOption)}.{nameof(HashValuesOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('HVALS',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var result = (RedisValue[])await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new HashValuesResponse()
            {
                Success = true,
                Values = result.Select(c => { dynamic value = c; return value; }).ToList()
            };
        }

        #endregion

        #region HashSet

        /// <summary>
        /// Sets field in the hash stored at key to value. If key does not exist, a new key
        ///  holding a hash is created. If field already exists in the hash, it is overwritten.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Hash set option</param>
        /// <returns>Return hash set response</returns>
        public async Task<HashSetResponse> HashSetAsync(CacheServer server, HashSetOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashSetOption)}.{nameof(HashSetOption.Key)}");
            }
            if (option?.Items.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(HashSetOption)}.{nameof(HashSetOption.Items)}");
            }
            var cacheDatabase = GetDatabase(server);
            var valueCount = option.Items.Count * 2;
            var values = new RedisValue[valueCount + 3];
            var valueParameters = new string[valueCount];
            int valueIndex = 0;
            foreach (var valueItem in option.Items)
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
            var expire = GetExpiration(option.Expiration);
            values[values.Length - 3] = option.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = expire.Item2?.TotalSeconds ?? 0;//expire time seconds
            var result = (string)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            values, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new HashSetResponse()
            {
                Success = string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase)
            };
        }

        #endregion

        #region HashLength

        /// <summary>
        /// Returns the number of fields contained in the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Hash length option</param>
        /// <returns>Return hash length response</returns>
        public async Task<HashLengthResponse> HashLengthAsync(CacheServer server, HashLengthOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashLengthOption)}.{nameof(HashLengthOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('HLEN',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new HashLengthResponse()
            {
                Success = true,
                Length = result
            };
        }

        #endregion

        #region HashKeys

        /// <summary>
        /// Returns all field names in the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Hash key option</param>
        /// <returns>Return hash keys response</returns>
        public async Task<HashKeysResponse> HashKeysAsync(CacheServer server, HashKeysOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashKeysOption)}.{nameof(HashKeysOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('HKEYS',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var result = (RedisValue[])await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new HashKeysResponse()
            {
                Success = true,
                HashKeys = result.Select(c => { string key = c; return key; }).ToList()
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
        /// <param name="option">Hash increment option</param>
        /// <returns>Return hash increment response</returns>
        public async Task<HashIncrementResponse> HashIncrementAsync(CacheServer server, HashIncrementOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashIncrementOption)}.{nameof(HashIncrementOption.Key)}");
            }
            if (option?.IncrementValue == null)
            {
                return CacheResponse.FailResponse<HashIncrementResponse>(CacheCodes.ValuesIsNullOrEmpty);
            }
            var database = GetDatabase(server);
            var dataType = option.IncrementValue.GetType();
            var typeCode = Type.GetTypeCode(dataType);
            dynamic newValue = option.IncrementValue;
            bool operation = false;
            var cacheKey = option.Key.GetActualKey();
            var keys = new RedisKey[1] { cacheKey };
            var expire = GetExpiration(option.Expiration);
            var values = new RedisValue[]
            {
                option.HashField,
                option.IncrementValue,
                option.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                GetTotalSeconds(expire.Item2),//expire time seconds
            };
            string script = "";
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
                    long incrementValue = 0;
                    long.TryParse(option.IncrementValue.ToString(), out incrementValue);
                    script = @$"local obv=redis.call('HINCRBY',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript(-2)}
return obv";
                    var newLongValue = (long)await database.ScriptEvaluateAsync(script, keys, values, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
                    newValue = DataConverter.Convert(newLongValue, dataType);
                    operation = true;
                    break;
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    double doubleIncrementValue = 0;
                    double.TryParse(option.IncrementValue.ToString(), out doubleIncrementValue);
                    script = @$"local obv=redis.call('HINCRBYFLOAT',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript(-2)}
return obv";
                    var newDoubleValue = (double)await database.ScriptEvaluateAsync(script, keys, values, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
                    newValue = DataConverter.Convert(newDoubleValue, dataType);
                    operation = true;
                    break;
            }
            return new HashIncrementResponse()
            {
                Success = operation,
                NewValue = newValue,
                Key = cacheKey,
                HashField = option.HashField
            };
        }

        #endregion

        #region HashGet

        /// <summary>
        /// Returns the value associated with field in the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Hash get option</param>
        /// <returns>Return hash get response</returns>
        public async Task<HashGetResponse> HashGetAsync(CacheServer server, HashGetOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashGetOption)}.{nameof(HashGetOption.Key)}");
            }
            if (string.IsNullOrWhiteSpace(option?.HashField))
            {
                throw new ArgumentNullException($"{nameof(HashGetOption)}.{nameof(HashGetOption.HashField)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('HGET',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var result = (string)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[1]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[4]
            {
                option.HashField,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new HashGetResponse()
            {
                Success = true,
                Value = result
            };
        }

        #endregion

        #region HashGetAll

        /// <summary>
        /// Returns all fields and values of the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Hash get all option</param>
        /// <returns>Return hash get all response</returns>
        public async Task<HashGetAllResponse> HashGetAllAsync(CacheServer server, HashGetAllOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashGetAllOption)}.{nameof(HashGetAllOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('HGETALL',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var result = (RedisValue[])await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[1]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[3]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            Dictionary<string, dynamic> values = new Dictionary<string, dynamic>(result.Length / 2);
            for (var i = 0; i < result.Length; i += 2)
            {
                values[result[i]] = result[i + 1];
            }
            return new HashGetAllResponse()
            {
                Success = true,
                HashValues = values
            };
        }

        #endregion

        #region HashExists

        /// <summary>
        /// Returns if field is an existing field in the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">option</param>
        /// <returns>hash exists response</returns>
        public async Task<HashExistsResponse> HashExistAsync(CacheServer server, HashExistsOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashExistsOption)}.{nameof(HashExistsOption.Key)}");
            }
            if (string.IsNullOrWhiteSpace(option?.HashField))
            {
                throw new ArgumentNullException($"{nameof(HashExistsOption)}.{nameof(HashExistsOption.HashField)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('HEXISTS',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var result = (int)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[1]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[4]
            {
                option.HashField,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new HashExistsResponse()
            {
                Success = true,
                HasField = result == 1
            };
        }

        #endregion

        #region HashDelete

        /// <summary>
        /// Removes the specified fields from the hash stored at key. Non-existing fields
        /// are ignored. Non-existing keys are treated as empty hashes and this option returns 0
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Hash delete option</param>
        /// <returns>Return hash delete response</returns>
        public async Task<HashDeleteResponse> HashDeleteAsync(CacheServer server, HashDeleteOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashDeleteOption)}.{nameof(HashDeleteOption.Key)}");
            }
            if (option.HashFields.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(HashDeleteOption)}.{nameof(HashDeleteOption.HashFields)}");
            }
            var cacheDatabase = GetDatabase(server);
            var values = new RedisValue[option.HashFields.Count + 3];
            var valueParameters = new string[option.HashFields.Count];
            for (var i = 0; i < option.HashFields.Count; i++)
            {
                values[i] = option.HashFields[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            values[values.Length - 3] = true;//refresh current time
            values[values.Length - 2] = RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = 0;//expire time seconds
            string script = $@"local pv=redis.call('HDEL',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(valueParameters.Length - 2)}
return pv";
            var result = (int)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[1]
            {
                option.Key.GetActualKey()
            },
            values, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new HashDeleteResponse()
            {
                Success = result > 0
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
        /// <param name="option">Hash decrement option</param>
        /// <returns>Return hash decrement response</returns>
        public async Task<HashDecrementResponse> HashDecrementAsync(CacheServer server, HashDecrementOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashDecrementOption)}.{nameof(HashDecrementOption.Key)}");
            }
            if (option?.DecrementValue == null)
            {
                return CacheResponse.FailResponse<HashDecrementResponse>(CacheCodes.ValuesIsNullOrEmpty);
            }
            var database = GetDatabase(server);
            var dataType = option.DecrementValue.GetType();
            var typeCode = Type.GetTypeCode(dataType);
            dynamic newValue = option.DecrementValue;
            bool operation = false;
            var cacheKey = option.Key.GetActualKey();
            var keys = new RedisKey[1] { cacheKey };
            var expire = GetExpiration(option.Expiration);
            var values = new RedisValue[]
            {
                option.HashField,
                -option.DecrementValue,
                option.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                GetTotalSeconds(expire.Item2),//expire time seconds
            };
            string script = "";
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
                    long incrementValue = 0;
                    long.TryParse(option.DecrementValue.ToString(), out incrementValue);
                    script = @$"local obv=redis.call('HINCRBY',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript(-2)}
return obv";
                    var newLongValue = (long)await database.ScriptEvaluateAsync(script, keys, values, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
                    newValue = DataConverter.Convert(newLongValue, dataType);
                    operation = true;
                    break;
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    double doubleIncrementValue = 0;
                    double.TryParse(option.DecrementValue.ToString(), out doubleIncrementValue);
                    script = @$"local obv=redis.call('HINCRBYFLOAT',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript(-2)}
return obv";
                    var newDoubleValue = (double)await database.ScriptEvaluateAsync(script, keys, values, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
                    newValue = DataConverter.Convert(newDoubleValue, dataType);
                    operation = true;
                    break;
            }
            return new HashDecrementResponse()
            {
                Success = operation,
                NewValue = newValue,
                Key = cacheKey,
                HashField = option.HashField
            };
        }

        #endregion

        #region HashScan

        /// <summary>
        /// The HSCAN option is used to incrementally iterate over a hash
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Hash scan option</param>
        /// <returns>Return hash scan response</returns>
        public async Task<HashScanResponse> HashScanAsync(CacheServer server, HashScanOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashScanOption)}.{nameof(HashScanOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
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
            var result = (RedisValue[])await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[1]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[6]
            {
                option.Cursor,
                GetMatchPattern(option.Pattern,option.PatternType),
                option.PageSize,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
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
                HashValues = values ?? new Dictionary<string, dynamic>(0)
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
        /// <param name="option">Set remove option</param>
        /// <returns>Return set remove response</returns>
        public async Task<SetRemoveResponse> SetRemoveAsync(CacheServer server, SetRemoveOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetRemoveOption)}.{nameof(SetRemoveOption.Key)}");
            }
            if (option.RemoveMembers.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SetRemoveOption)}.{nameof(SetRemoveOption.RemoveMembers)}");
            }
            var values = new RedisValue[option.RemoveMembers.Count + 3];
            var valueParameters = new string[option.RemoveMembers.Count];
            for (var i = 0; i < option.RemoveMembers.Count; i++)
            {
                values[i] = option.RemoveMembers[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            values[values.Length - 3] = true;//refresh current time
            values[values.Length - 2] = RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = 0;//expire time seconds
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv=redis.call('SREM',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(option.RemoveMembers.Count - 2)}
return obv";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            values, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SetRemoveResponse()
            {
                Success = true,
                RemoveCount = result
            };
        }

        #endregion

        #region SetRandomMembers

        /// <summary>
        /// Return an array of count distinct elements if count is positive. If called with
        /// a negative count the behavior changes and the option is allowed to return the
        /// same element multiple times. In this case the numer of returned elements is the
        /// absolute value of the specified count.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Set random members option</param>
        /// <returns>Return set random members response</returns>
        public async Task<SetRandomMembersResponse> SetRandomMembersAsync(CacheServer server, SetRandomMembersOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetRandomMembersOption)}.{nameof(SetRandomMembersOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('SRANDMEMBER',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var result = (RedisValue[])await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Count,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SetRandomMembersResponse()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0)
            };
        }

        #endregion

        #region SetRandomMember

        /// <summary>
        /// Return a random element from the set value stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Set random member option</param>
        /// <returns>Return set random member</returns>
        public async Task<SetRandomMemberResponse> SetRandomMemberAsync(CacheServer server, SetRandomMemberOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetRandomMemberOption)}.{nameof(SetRandomMemberOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('SRANDMEMBER',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var result = (string)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SetRandomMemberResponse()
            {
                Success = true,
                Member = result
            };
        }

        #endregion

        #region SetPop

        /// <summary>
        /// Removes and returns a random element from the set value stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Set pop option</param>
        /// <returns>Return set pop response</returns>
        public async Task<SetPopResponse> SetPopAsync(CacheServer server, SetPopOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetPopOption)}.{nameof(SetPopOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('SPOP',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var result = (string)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SetPopResponse()
            {
                Success = true,
                PopValue = result
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
        /// <param name="option">Set move option</param>
        /// <returns>Return set move response</returns>
        public async Task<SetMoveResponse> SetMoveAsync(CacheServer server, SetMoveOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.SourceKey))
            {
                throw new ArgumentNullException($"{nameof(SetMoveOption)}.{nameof(SetMoveOption.SourceKey)}");
            }
            if (string.IsNullOrWhiteSpace(option?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SetMoveOption)}.{nameof(SetMoveOption.DestinationKey)}");
            }
            if (string.IsNullOrEmpty(option?.MoveMember))
            {
                throw new ArgumentNullException($"{nameof(SetMoveOption)}.{nameof(SetMoveOption.MoveMember)}");
            }
            string script = $@"local pv=redis.call('SMOVE',{Keys(1)},{Keys(2)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
{GetRefreshExpirationScript(2, 1)}
return pv";
            var cacheDatabase = GetDatabase(server);
            var allowSliding = RedisManager.AllowSlidingExpiration();
            var expire = GetExpiration(option.Expiration);
            var result = (string)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.SourceKey.GetActualKey(),
                option.DestinationKey.GetActualKey(),
            },
            new RedisValue[]
            {
                option.MoveMember,
                true,//refresh current time
                allowSliding,//whether allow set refresh time
                0,//expire time seconds
                option.Expiration==null,//refresh current time-destination key
                expire.Item1&&allowSliding,//whether allow set refresh time-destination key
                GetTotalSeconds(expire.Item2)//expire time seconds-destination key
            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SetMoveResponse()
            {
                Success = result == "1"
            };
        }

        #endregion

        #region SetMembers

        /// <summary>
        /// Returns all the members of the set value stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Set members option</param>
        /// <returns>Return set members response</returns>
        public async Task<SetMembersResponse> SetMembersAsync(CacheServer server, SetMembersOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetMembersOption)}.{nameof(SetMembersOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('SMEMBERS',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var result = (RedisValue[])await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SetMembersResponse()
            {
                Success = true,
                Members = result?.Select(c => { string member = c; return member; }).ToList() ?? new List<string>(0)
            };
        }

        #endregion

        #region SetLength

        /// <summary>
        /// Returns the set cardinality (number of elements) of the set stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Set length option</param>
        /// <returns>Return set length response</returns>
        public async Task<SetLengthResponse> SetLengthAsync(CacheServer server, SetLengthOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetLengthOption)}.{nameof(SetLengthOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('SCARD',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SetLengthResponse()
            {
                Success = true,
                Length = result
            };
        }

        #endregion

        #region SetContains

        /// <summary>
        /// Returns if member is a member of the set stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Set contains option</param>
        /// <returns>Return set contains response</returns>
        public async Task<SetContainsResponse> SetContainsAsync(CacheServer server, SetContainsOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetContainsOption)}.{nameof(SetContainsOption.Key)}");
            }
            if (option.Member == null)
            {
                throw new ArgumentNullException($"{nameof(SetContainsOption)}.{nameof(SetContainsOption.Member)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('SISMEMBER',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var result = (string)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Member,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SetContainsResponse()
            {
                Success = true,
                ContainsValue = result == "1"
            };
        }

        #endregion

        #region SetCombine

        /// <summary>
        /// Returns the members of the set resulting from the specified operation against
        /// the given sets.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Set combine option</param>
        /// <returns>Return set combine response</returns>
        public async Task<SetCombineResponse> SetCombineAsync(CacheServer server, SetCombineOption option)
        {
            if (option?.Keys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SetCombineOption)}.{nameof(SetCombineOption.Keys)}");
            }
            var cacheDatabase = GetDatabase(server);
            var keys = new RedisKey[option.Keys.Count];
            var keyParameters = new List<string>(option.Keys.Count);
            for (var i = 0; i < option.Keys.Count; i++)
            {
                keys[i] = option.Keys[i].GetActualKey();
                keyParameters.Add($"{Keys(i + 1)}");
            }
            string script = $@"local pv=redis.call('{GetSetCombineCommand(option.CombineOperation)}',{string.Join(",", keyParameters)})
{GetRefreshExpirationScript(-2, keyCount: keys.Length)}
return pv";
            var result = (RedisValue[])await cacheDatabase.ScriptEvaluateAsync(script, keys,
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SetCombineResponse()
            {
                Success = true,
                CombineValues = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0)
            };
        }

        #endregion

        #region SetCombineAndStore

        /// <summary>
        /// This option is equal to SetCombine, but instead of returning the resulting set,
        ///  it is stored in destination. If destination already exists, it is overwritten.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Set combine and store option</param>
        /// <returns>Return set combine and store response</returns>
        public async Task<SetCombineAndStoreResponse> SetCombineAndStoreAsync(CacheServer server, SetCombineAndStoreOption option)
        {
            if (option?.SourceKeys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SetCombineAndStoreOption)}.{nameof(SetCombineAndStoreOption.SourceKeys)}");
            }
            if (string.IsNullOrWhiteSpace(option.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SetCombineAndStoreOption)}.{nameof(SetCombineAndStoreOption.DestinationKey)}");
            }
            var cacheDatabase = GetDatabase(server);
            var keys = new RedisKey[option.SourceKeys.Count + 1];
            var keyParameters = new List<string>(option.SourceKeys.Count + 1);
            keys[0] = option.DestinationKey.GetActualKey();
            keyParameters.Add($"{Keys(1)}");
            for (var i = 0; i < option.SourceKeys.Count; i++)
            {
                keys[i + 1] = option.SourceKeys[i].GetActualKey();
                keyParameters.Add($"{Keys(i + 2)}");
            }
            string script = $@"local pv=redis.call('{GetSetCombineCommand(option.CombineOperation)}STORE',{string.Join(",", keyParameters)})
{GetRefreshExpirationScript(-2, 1, keyCount: keys.Length - 1)}
{GetRefreshExpirationScript(1)}
return pv";
            var expire = GetExpiration(option.Expiration);
            bool allowSliding = RedisManager.AllowSlidingExpiration();
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, keys,
            new RedisValue[]
            {
                true,//refresh current time
                allowSliding,//whether allow set refresh time
                0,//expire time seconds
                option.Expiration==null,// des key
                expire.Item1&&allowSliding,//des key
                GetTotalSeconds(expire.Item2)
            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SetCombineAndStoreResponse()
            {
                Success = true,
                Count = result
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
        /// <param name="option">Set add option</param>
        /// <returns>Return set add response</returns>
        public async Task<SetAddResponse> SetAddAsync(CacheServer server, SetAddOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetAddOption)}.{nameof(SetAddOption.Key)}");
            }
            if (option.Members.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SetAddOption)}.{nameof(SetAddOption.Members)}");
            }
            var expire = GetExpiration(option.Expiration);
            var values = new RedisValue[option.Members.Count + 3];
            var valueParameters = new string[option.Members.Count];
            for (var i = 0; i < option.Members.Count; i++)
            {
                values[i] = option.Members[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            values[values.Length - 3] = option.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = expire.Item2?.TotalSeconds ?? 0;//expire time seconds
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv=redis.call('SADD',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(option.Members.Count - 2)}
return obv";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            values, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SetAddResponse()
            {
                Success = result > 0
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
        /// <param name="option">Sorted set score option</param>
        /// <returns>Return sorted set score response</returns>
        public async Task<SortedSetScoreResponse> SortedSetScoreAsync(CacheServer server, SortedSetScoreOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetScoreOption)}.{nameof(SortedSetScoreOption.Key)}");
            }
            if (option.Member == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetScoreOption)}.{nameof(SortedSetScoreOption.Member)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('ZSCORE',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var result = (double?)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Member,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SortedSetScoreResponse()
            {
                Success = true,
                Score = result
            };
        }

        #endregion

        #region SortedSetRemoveRangeByValue

        /// <summary>
        /// When all the elements in a sorted set are inserted with the same score, in order
        /// to force lexicographical ordering, this option removes all elements in the sorted
        /// set stored at key between the lexicographical range specified by min and max.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Sorted set remove range by value option</param>
        /// <returns>Return sorted set remove range by value response</returns>
        public async Task<SortedSetRemoveRangeByValueResponse> SortedSetRemoveRangeByValueAsync(CacheServer server, SortedSetRemoveRangeByValueOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueOption)}.{nameof(SortedSetRemoveRangeByValueOption.Key)}");
            }
            if (option.MinValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueOption)}.{nameof(SortedSetRemoveRangeByValueOption.MinValue)}");
            }
            if (option.MaxValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueOption)}.{nameof(SortedSetRemoveRangeByValueOption.MaxValue)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('ZREMRANGEBYLEX',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                FormatSortedSetRangeBoundary(option.MinValue,true,option.Exclude),
                FormatSortedSetRangeBoundary(option.MaxValue,false,option.Exclude),
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRemoveRangeByValueResponse()
            {
                RemoveCount = result,
                Success = true
            };
        }

        #endregion

        #region SortedSetRemoveRangeByScore

        /// <summary>
        /// Removes all elements in the sorted set stored at key with a score between min
        ///  and max (inclusive by default).
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Sorted set remove range by score option</param>
        /// <returns>Return sorted set remove range by score response</returns>
        public async Task<SortedSetRemoveRangeByScoreResponse> SortedSetRemoveRangeByScoreAsync(CacheServer server, SortedSetRemoveRangeByScoreOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByScoreOption)}.{nameof(SortedSetRemoveRangeByScoreOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('ZREMRANGEBYSCORE',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                FormatSortedSetScoreRangeBoundary(option.Start,true,option.Exclude),
                FormatSortedSetScoreRangeBoundary(option.Stop,false,option.Exclude),
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRemoveRangeByScoreResponse()
            {
                RemoveCount = result,
                Success = true
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
        /// <param name="option">Sorted set remove range by rank option</param>
        /// <returns>Return sorted set remove range by rank response</returns>
        public async Task<SortedSetRemoveRangeByRankResponse> SortedSetRemoveRangeByRankAsync(CacheServer server, SortedSetRemoveRangeByRankOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByRankOption)}.{nameof(SortedSetRemoveRangeByRankOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('ZREMRANGEBYRANK',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Start,
                option.Stop,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRemoveRangeByRankResponse()
            {
                RemoveCount = result,
                Success = true
            };
        }

        #endregion

        #region SortedSetRemove

        /// <summary>
        /// Removes the specified members from the sorted set stored at key. Non existing
        /// members are ignored.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Sorted set remove option</param>
        /// <returns>sorted set remove response</returns>
        public async Task<SortedSetRemoveResponse> SortedSetRemoveAsync(CacheServer server, SortedSetRemoveOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveOption)}.{nameof(SortedSetRemoveOption.Key)}");
            }
            if (option.RemoveMembers.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SortedSetRemoveOption)}.{nameof(SortedSetRemoveOption.RemoveMembers)}");
            }
            var values = new RedisValue[option.RemoveMembers.Count + 3];
            var valueParameters = new string[option.RemoveMembers.Count];
            for (var i = 0; i < option.RemoveMembers.Count; i++)
            {
                values[i] = option.RemoveMembers[i];
                valueParameters[i] = $"{Arg(i + 1)}";
            }
            values[values.Length - 3] = true;//refresh current time
            values[values.Length - 2] = RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = 0;//expire time seconds
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv=redis.call('ZREM',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(option.RemoveMembers.Count - 2)}
return obv";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            values, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRemoveResponse()
            {
                Success = true,
                RemoveCount = result
            };
        }

        #endregion

        #region SortedSetRank

        /// <summary>
        /// Returns the rank of member in the sorted set stored at key, by default with the
        /// scores ordered from low to high. The rank (or index) is 0-based, which means
        /// that the member with the lowest score has rank 0.
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>sorted set rank response</returns>
        public async Task<SortedSetRankResponse> SortedSetRankAsync(CacheServer server, SortedSetRankOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRankOption)}.{nameof(SortedSetRankOption.Key)}");
            }
            if (option.Member == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetRankOption)}.{nameof(SortedSetRankOption.Member)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('Z{(option.Order == CacheOrder.Descending ? "REV" : "")}RANK',{Keys(1)},{Arg(1)})
{GetRefreshExpirationScript(-1)}
return pv";
            var result = (long?)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Member,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRankResponse()
            {
                Success = true,
                Rank = result
            };
        }

        #endregion

        #region SortedSetRangeByValue

        /// <summary>
        /// When all the elements in a sorted set are inserted with the same score, in order
        /// to force lexicographical ordering, this option returns all the elements in the
        /// sorted set at key with a value between min and max.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Sorted set range by value option</param>
        /// <returns>sorted set range by value response</returns>
        public async Task<SortedSetRangeByValueResponse> SortedSetRangeByValueAsync(CacheServer server, SortedSetRangeByValueOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueOption)}.{nameof(SortedSetRemoveRangeByValueOption.Key)}");
            }
            if (option.MinValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueOption)}.{nameof(SortedSetRemoveRangeByValueOption.MinValue)}");
            }
            if (option.MaxValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByValueOption)}.{nameof(SortedSetRemoveRangeByValueOption.MaxValue)}");
            }
            var cacheDatabase = GetDatabase(server);
            var command = "ZRANGEBYLEX";
            string beginValue = string.Empty;
            string endValue = string.Empty;
            if (option.Order == CacheOrder.Descending)
            {
                command = "ZREVRANGEBYLEX";
                beginValue = FormatSortedSetRangeBoundary(option.MaxValue, false, option.Exclude);
                endValue = FormatSortedSetRangeBoundary(option.MinValue, true, option.Exclude);
            }
            else
            {
                beginValue = FormatSortedSetRangeBoundary(option.MinValue, true, option.Exclude);
                endValue = FormatSortedSetRangeBoundary(option.MaxValue, false, option.Exclude);
            }
            string script = $@"local pv=redis.call('{command}',{Keys(1)},{Arg(1)},{Arg(2)},'LIMIT',{Arg(3)},{Arg(4)})
{GetRefreshExpirationScript(2)}
return pv";
            var result = (RedisValue[])await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                beginValue,
                endValue,
                option.Offset,
                option.Count,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRangeByValueResponse()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0)
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
        /// <param name="option">Option</param>
        /// <returns>Return sorted set range by score with scores response</returns>
        public async Task<SortedSetRangeByScoreWithScoresResponse> SortedSetRangeByScoreWithScoresAsync(CacheServer server, SortedSetRangeByScoreWithScoresOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByScoreWithScoresOption)}.{nameof(SortedSetRangeByScoreWithScoresOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            var command = "ZRANGEBYSCORE";
            string beginValue = "";
            string endValue = "";
            if (option.Order == CacheOrder.Descending)
            {
                command = "ZREVRANGEBYSCORE";
                beginValue = FormatSortedSetScoreRangeBoundary(option.Stop, false, option.Exclude);
                endValue = FormatSortedSetScoreRangeBoundary(option.Start, true, option.Exclude);
            }
            else
            {
                beginValue = FormatSortedSetScoreRangeBoundary(option.Start, true, option.Exclude);
                endValue = FormatSortedSetScoreRangeBoundary(option.Stop, false, option.Exclude);
            }
            string script = $@"local pv=redis.call('{command}',{Keys(1)},{Arg(1)},{Arg(2)},'WITHSCORES','LIMIT',{Arg(3)},{Arg(4)})
{GetRefreshExpirationScript(2)}
return pv";
            var result = (RedisValue[])await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                beginValue,
                endValue,
                option.Offset,
                option.Count,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
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
                Members = members
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
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>sorted set range by score response</returns>
        public async Task<SortedSetRangeByScoreResponse> SortedSetRangeByScoreAsync(CacheServer server, SortedSetRangeByScoreOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByScoreOption)}.{nameof(SortedSetRangeByScoreOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            var command = "ZRANGEBYSCORE";
            string beginValue = "";
            string endValue = "";
            if (option.Order == CacheOrder.Descending)
            {
                command = "ZREVRANGEBYSCORE";
                beginValue = FormatSortedSetScoreRangeBoundary(option.Stop, false, option.Exclude);
                endValue = FormatSortedSetScoreRangeBoundary(option.Start, true, option.Exclude);
            }
            else
            {
                beginValue = FormatSortedSetScoreRangeBoundary(option.Start, true, option.Exclude);
                endValue = FormatSortedSetScoreRangeBoundary(option.Stop, false, option.Exclude);
            }
            string script = $@"local pv=redis.call('{command}',{Keys(1)},{Arg(1)},{Arg(2)},'LIMIT',{Arg(3)},{Arg(4)})
{GetRefreshExpirationScript(2)}
return pv";
            var result = (RedisValue[])await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                beginValue,
                endValue,
                option.Offset,
                option.Count,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRangeByScoreResponse()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0)
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
        /// <param name="option">Option</param>
        /// <returns>Return sorted set range by rank with scores response</returns>
        public async Task<SortedSetRangeByRankWithScoresResponse> SortedSetRangeByRankWithScoresAsync(CacheServer server, SortedSetRangeByRankWithScoresOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByRankWithScoresOption)}.{nameof(SortedSetRangeByRankWithScoresOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('Z{(option.Order == CacheOrder.Descending ? "REV" : "")}RANGE',{Keys(1)},{Arg(1)},{Arg(2)},'WITHSCORES')
{GetRefreshExpirationScript()}
return pv";
            var result = (RedisValue[])await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Start,
                option.Stop,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
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
                Members = members
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
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>sorted set range by rank response</returns>
        public async Task<SortedSetRangeByRankResponse> SortedSetRangeByRankAsync(CacheServer server, SortedSetRangeByRankOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByRankOption)}.{nameof(SortedSetRangeByRankOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('Z{(option.Order == CacheOrder.Descending ? "REV" : "")}RANGE',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var result = (RedisValue[])await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Start,
                option.Stop,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SortedSetRangeByRankResponse()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0)
            };
        }

        #endregion

        #region SortedSetLengthByValue

        /// <summary>
        /// When all the elements in a sorted set are inserted with the same score, in order
        /// to force lexicographical ordering, this option returns the number of elements
        /// in the sorted set at key with a value between min and max.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Option</param>
        /// <returns>Return sorted set lenght by value response</returns>
        public async Task<SortedSetLengthByValueResponse> SortedSetLengthByValueAsync(CacheServer server, SortedSetLengthByValueOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetLengthByValueOption)}.{nameof(SortedSetLengthByValueOption.Key)}");
            }
            if (option.MinValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetLengthByValueOption)}.{nameof(SortedSetLengthByValueOption.MinValue)}");
            }
            if (option.MaxValue == null)
            {
                throw new ArgumentNullException($"{nameof(SortedSetLengthByValueOption)}.{nameof(SortedSetLengthByValueOption.MaxValue)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('ZLEXCOUNT',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                $"[{option.MinValue}",
                $"[{option.MaxValue}",
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SortedSetLengthByValueResponse()
            {
                Success = true,
                Length = result
            };
        }

        #endregion

        #region SortedSetLength

        /// <summary>
        /// Returns the sorted set cardinality (number of elements) of the sorted set stored
        /// at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="option">Option</param>
        /// <returns>Return sorted set length response</returns>
        public async Task<SortedSetLengthResponse> SortedSetLengthAsync(CacheServer server, SortedSetLengthOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetLengthByValueOption)}.{nameof(SortedSetLengthByValueOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('ZCARD',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SortedSetLengthResponse()
            {
                Success = true,
                Length = result
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
        /// <param name="option">Option</param>
        /// <returns>Return sorted set increment response</returns>
        public async Task<SortedSetIncrementResponse> SortedSetIncrementAsync(CacheServer server, SortedSetIncrementOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetIncrementOption)}.{nameof(SortedSetIncrementOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('ZINCRBY',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var expire = GetExpiration(option.Expiration);
            var result = (double)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.IncrementScore,
                option.Member,
                option.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                GetTotalSeconds(expire.Item2),//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SortedSetIncrementResponse()
            {
                Success = true,
                NewScore = result
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
        /// <param name="option">Option</param>
        /// <returns>Return sorted set decrement response</returns>
        public async Task<SortedSetDecrementResponse> SortedSetDecrementAsync(CacheServer server, SortedSetDecrementOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetDecrementOption)}.{nameof(SortedSetDecrementOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('ZINCRBY',{Keys(1)},{Arg(1)},{Arg(2)})
{GetRefreshExpirationScript()}
return pv";
            var expire = GetExpiration(option.Expiration);
            var result = (double)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                -option.DecrementScore,
                option.Member,
                option.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                GetTotalSeconds(expire.Item2),//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SortedSetDecrementResponse()
            {
                Success = true,
                NewScore = result
            };
        }

        #endregion

        #region SortedSetCombineAndStore

        /// <summary>
        /// Computes a set operation over multiple sorted sets (optionally using per-set
        /// weights), and stores the result in destination, optionally performing a specific
        /// aggregation (defaults to sum)
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>sorted set combine and store response</returns>
        public async Task<SortedSetCombineAndStoreResponse> SortedSetCombineAndStoreAsync(CacheServer server, SortedSetCombineAndStoreOption option)
        {
            if (option?.SourceKeys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SortedSetCombineAndStoreOption)}.{nameof(SortedSetCombineAndStoreOption.SourceKeys)}");
            }
            if (string.IsNullOrWhiteSpace(option.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SortedSetCombineAndStoreOption)}.{nameof(SortedSetCombineAndStoreOption.DestinationKey)}");
            }
            var cacheDatabase = GetDatabase(server);
            var keys = new RedisKey[option.SourceKeys.Count + 1];
            var keyParameters = new List<string>(option.SourceKeys.Count);
            double[] weights = new double[option.SourceKeys.Count];
            keys[0] = option.DestinationKey.GetActualKey();
            for (var i = 0; i < option.SourceKeys.Count; i++)
            {
                keys[i + 1] = option.SourceKeys[i].GetActualKey();
                keyParameters.Add($"{Keys(i + 2)}");
                weights[i] = option.Weights?.ElementAt(i) ?? 1;
            }
            StringBuilder optionScript = new StringBuilder();
            string script = $@"local pv=redis.call('{GetSortedSetCombineCommand(option.CombineOperation)}',{Keys(1)},'{keyParameters.Count}',{string.Join(",", keyParameters)},'WEIGHTS',{string.Join(",", weights)},'AGGREGATE','{GetSortedSetAggregateName(option.Aggregate)}')
{GetRefreshExpirationScript(1)}
{GetRefreshExpirationScript(-2, 1, keyCount: keys.Length - 1)}
return pv";
            var expire = GetExpiration(option.Expiration);
            bool allowSliding = RedisManager.AllowSlidingExpiration();
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, keys,
            new RedisValue[]
            {
                true,//refresh current time
                allowSliding,//whether allow set refresh time
                0,//expire time seconds
                option.Expiration==null,// des key
                expire.Item1&&allowSliding,//des key
                GetTotalSeconds(expire.Item2)
            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SortedSetCombineAndStoreResponse()
            {
                Success = true,
                NewSetLength = result
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
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>sorted set add response</returns>
        public async Task<SortedSetAddResponse> SortedSetAddAsync(CacheServer server, SortedSetAddOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetAddOption)}.{nameof(SortedSetAddOption.Key)}");
            }
            if (option.Members.IsNullOrEmpty())
            {
                throw new ArgumentException($"{nameof(SortedSetAddOption)}.{nameof(SortedSetAddOption.Members)}");
            }
            var expire = GetExpiration(option.Expiration);
            var valueCount = option.Members.Count * 2;
            var values = new RedisValue[valueCount + 3];
            var valueParameters = new string[valueCount];
            for (var i = 0; i < option.Members.Count; i++)
            {
                var member = option.Members[i];
                var argIndex = i * 2;
                values[argIndex] = member?.Score;
                values[argIndex + 1] = member?.Value;
                valueParameters[argIndex] = $"{Arg(argIndex + 1)}";
                valueParameters[argIndex + 1] = $"{Arg(argIndex + 2)}";
            }
            values[values.Length - 3] = option.Expiration == null;//refresh current time
            values[values.Length - 2] = expire.Item1 && RedisManager.AllowSlidingExpiration();//whether allow set refresh time
            values[values.Length - 1] = expire.Item2?.TotalSeconds ?? 0;//expire time seconds
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv=redis.call('ZADD',{Keys(1)},{string.Join(",", valueParameters)})
{GetRefreshExpirationScript(valueCount - 2)}
return obv";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            values, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SortedSetAddResponse()
            {
                Success = true,
                Length = result
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
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>sort response</returns>
        public async Task<SortResponse> SortAsync(CacheServer server, SortOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortOption)}.{nameof(SortOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv=redis.call('SORT',{Keys(1)}{(string.IsNullOrWhiteSpace(option.By) ? string.Empty : $",'BY','{option.By}'")},'LIMIT',{Arg(1)},{Arg(2)}{(option.Gets.IsNullOrEmpty() ? string.Empty : $",{string.Join(",", option.Gets.Select(c => $"'GET','{c}'"))}")},{(option.Order == CacheOrder.Descending ? "'DESC'" : "'ASC'")}{(option.SortType == CacheSortType.Alphabetic ? ",'ALPHA'" : string.Empty)})
{GetRefreshExpirationScript()}
return obv";
            var result = (RedisValue[])await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Offset,
                option.Count,
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds
            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SortResponse()
            {
                Success = true,
                Values = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0)
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
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>sort and store response</returns>
        public async Task<SortAndStoreResponse> SortAndStoreAsync(CacheServer server, SortAndStoreOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.SourceKey))
            {
                throw new ArgumentNullException($"{nameof(SortAndStoreOption)}.{nameof(SortAndStoreOption.SourceKey)}");
            }
            if (string.IsNullOrWhiteSpace(option?.DestinationKey))
            {
                throw new ArgumentNullException($"{nameof(SortAndStoreOption)}.{nameof(SortAndStoreOption.DestinationKey)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv=redis.call('SORT',{Keys(1)}{(string.IsNullOrWhiteSpace(option.By) ? string.Empty : $",'BY','{option.By}'")},'LIMIT',{Arg(1)},{Arg(2)}{(option.Gets.IsNullOrEmpty() ? string.Empty : $",{string.Join(",", option.Gets.Select(c => $"'GET','{c}'"))}")},{(option.Order == CacheOrder.Descending ? "'DESC'" : "'ASC'")}{(option.SortType == CacheSortType.Alphabetic ? ",'ALPHA'" : string.Empty)},'STORE',{Keys(2)})
{GetRefreshExpirationScript()}
{GetRefreshExpirationScript(3, 1)}
return obv";
            var expire = GetExpiration(option.Expiration);
            bool allowSliding = RedisManager.AllowSlidingExpiration();
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.SourceKey.GetActualKey(),
                option.DestinationKey.GetActualKey()
            },
            new RedisValue[]
            {
                option.Offset,
                option.Count,
                true,//refresh current time
                allowSliding,//whether allow set refresh time
                0,//expire time seconds
                option.Expiration==null,//refresh current time-des key
                expire.Item1&&allowSliding,//allow set refresh time-deskey
                GetTotalSeconds(expire.Item2)//-deskey
            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new SortAndStoreResponse()
            {
                Success = true,
                Length = result
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
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>key type response</returns>
        public async Task<TypeResponse> KeyTypeAsync(CacheServer server, TypeOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(TypeOption)}.{nameof(TypeOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv=redis.call('TYPE',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return obv";
            var result = (string)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new TypeResponse()
            {
                Success = true,
                KeyType = GetCacheKeyType(result)
            };
        }

        #endregion

        #region KeyTimeToLive

        /// <summary>
        /// Returns the remaining time to live of a key that has a timeout. This introspection
        /// capability allows a Redis client to check how many seconds a given key will continue
        /// to be part of the dataset.
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>key time to live response</returns>
        public async Task<TimeToLiveResponse> KeyTimeToLiveAsync(CacheServer server, TimeToLiveOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(TimeToLiveOption)}.{nameof(TimeToLiveOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv=redis.call('TTL',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return obv";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new TimeToLiveResponse()
            {
                Success = true,
                TimeToLiveSeconds = result,
                KeyExist = result != -2,
                Perpetual = result == -1
            };
        }

        #endregion

        #region KeyRestore

        /// <summary>
        /// Create a key associated with a value that is obtained by deserializing the provided
        /// serialized value (obtained via DUMP). If ttl is 0 the key is created without
        /// any expire, otherwise the specified expire time(in milliseconds) is set.
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>key restore response</returns>
        public async Task<RestoreResponse> KeyRestoreAsync(CacheServer server, RestoreOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(RestoreOption)}.{nameof(RestoreOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv= string.lower(tostring(redis.call('RESTORE',{Keys(1)},'0',{Arg(1)})))=='ok'
{GetRefreshExpirationScript(-1)}
return obv";
            var expire = GetExpiration(option.Expiration);
            var result = (bool)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                option.Value,
                option.Expiration==null,//refresh current time
                expire.Item1&&RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                GetTotalSeconds(expire.Item2),//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new RestoreResponse()
            {
                Success = result
            };
        }

        #endregion

        #region KeyRename

        /// <summary>
        /// Renames key to newkey. It returns an error when the source and destination names
        /// are the same, or when key does not exist.
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>key rename response</returns>
        public async Task<RenameResponse> KeyRenameAsync(CacheServer server, RenameOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(RenameOption)}.{nameof(RenameOption.Key)}");
            }
            if (string.IsNullOrWhiteSpace(option?.NewKey))
            {
                throw new ArgumentNullException($"{nameof(RenameOption)}.{nameof(RenameOption.NewKey)}");
            }
            var cacheDatabase = GetDatabase(server);
            string cacheKey = option.Key.GetActualKey();
            string newCacheKey = option.NewKey.GetActualKey();
            string script = $@"{GetRefreshExpirationScript(-2)}
local obv=string.lower(tostring(redis.call('{(option.WhenNewKeyNotExists ? "RENAMENX" : "RENAME")}',{Keys(1)},{Keys(2)})))
if obv=='ok' or obv=='1'
then
    redis.call('RENAME','{GetExpirationKey(cacheKey)}','{GetExpirationKey(newCacheKey)}')
    return true
end
return false";
            var result = (bool)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                cacheKey,
                newCacheKey
            },
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new RenameResponse()
            {
                Success = result
            };
        }

        #endregion

        #region KeyRandom

        /// <summary>
        /// Return a random key from the currently selected database.
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>key random response</returns>
        public async Task<RandomResponse> KeyRandomAsync(CacheServer server, RandomOption option)
        {
            var cacheDatabase = GetDatabase(server);
            string script = $@"local obv=redis.call('RANDOMKEY')
if obv
then
    local exkey=obv..'{ExpirationKeySuffix}' 
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
            var result = (string)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[0], new RedisValue[0], GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new RandomResponse()
            {
                Success = true,
                Key = result
            };
        }

        #endregion

        #region KeyPersist

        /// <summary>
        /// Remove the existing timeout on key, turning the key from volatile (a key with
        /// an expire set) to persistent (a key that will never expire as no timeout is associated).
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>key persist response</returns>
        public async Task<PersistResponse> KeyPersistAsync(CacheServer server, PersistOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(PersistOption)}.{nameof(PersistOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            var cacheKey = option.Key.GetActualKey();
            string script = $@"local obv=redis.call('PERSIST',{Keys(1)})==1
if obv
then
    redis.call('DEL','{GetExpirationKey(cacheKey)}')
end
return obv";
            var result = (bool)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[1] { cacheKey }, new RedisValue[0], GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new PersistResponse()
            {
                Success = result
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
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>key move response</returns>
        public async Task<MoveResponse> KeyMoveAsync(CacheServer server, MoveOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(MoveOption)}.{nameof(MoveOption.Key)}");
            }
            if (option.Database < 0)
            {
                throw new ArgumentException($"{nameof(MoveOption)}.{nameof(MoveOption.Database)}");
            }
            var cacheDatabase = GetDatabase(server);
            var cacheKey = option.Key.GetActualKey();
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
        redis.call('SELECT','{option.Database}')
        redis.call('EXPIRE','{cacheKey}',ct)
        redis.call('SET',exkey,ct,'EX',ct)
    end
end
return obv";
            var result = (bool)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[1] { cacheKey }, new RedisValue[1]
            {
                option.Database
            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new MoveResponse()
            {
                Success = result
            };
        }

        #endregion

        #region KeyMigrate

        /// <summary>
        /// Atomically transfer a key from a source Redis instance to a destination Redis
        /// instance. On success the key is deleted from the original instance by default,
        /// and is guaranteed to exist in the target instance.
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>key migrate response</returns>
        public async Task<MigrateResponse> KeyMigrateAsync(CacheServer server, MigrateOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(MigrateOption)}.{nameof(MigrateOption.Key)}");
            }
            if (option.DestinationServer == null)
            {
                throw new ArgumentNullException($"{nameof(MigrateOption)}.{nameof(MigrateOption.DestinationServer)}");
            }
            var cacheDatabase = GetDatabase(server);
            var cacheKey = option.Key.GetActualKey();
            string script = $@"local obv=string.lower(tostring(redis.call('MIGRATE','{option.DestinationServer.Port}','{option.DestinationServer.Host}',{Keys(1)},'{option.TimeOutMilliseconds}'{(option.CopyCurrent ? ",'COPY'" : string.Empty)}{(option.ReplaceDestination ? ",'REPLACE'" : string.Empty)})))
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
            var result = (bool)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[1] { cacheKey }, new RedisValue[1]
            {
                option.CopyCurrent
            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new MigrateResponse()
            {
                Success = true
            };
        }

        #endregion

        #region KeyExpire

        /// <summary>
        /// Set a timeout on key. After the timeout has expired, the key will automatically
        /// be deleted. A key with an associated timeout is said to be volatile in Redis
        /// terminology.
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>key expire response</returns>
        public async Task<ExpireResponse> KeyExpireAsync(CacheServer server, ExpireOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(ExpireOption)}.{nameof(ExpireOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            var cacheKey = option.Key.GetActualKey();
            var expire = GetExpiration(option.Expiration);
            var seconds = expire.Item2?.TotalSeconds ?? 0;
            string script = $@"local rs=redis.call('EXPIRE','{cacheKey}','{seconds}')==1
if rs and '{(expire.Item1 && RedisManager.AllowSlidingExpiration() ? "1" : "0")}'=='1'
then
    redis.call('SET','{GetExpirationKey(cacheKey)}','{seconds}','EX','{seconds}')
end
return rs";
            var result = (bool)await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[0], new RedisValue[0], GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new ExpireResponse()
            {
                Success = result
            };
        }

        #endregion;

        #region KeyDump

        /// <summary>
        /// Serialize the value stored at key in a Redis-specific format and return it to
        /// the user. The returned value can be synthesized back into a Redis key using the
        /// RESTORE option.
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>key dump response</returns>
        public async Task<DumpResponse> KeyDumpAsync(CacheServer server, DumpOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(DumpOption)}.{nameof(DumpOption.Key)}");
            }
            var cacheDatabase = GetDatabase(server);
            string script = $@"local pv=redis.call('DUMP',{Keys(1)})
{GetRefreshExpirationScript(-2)}
return pv";
            var result = (byte[])await cacheDatabase.ScriptEvaluateAsync(script, new RedisKey[]
            {
                option.Key.GetActualKey()
            },
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new DumpResponse()
            {
                Success = true,
                ByteValues = result
            };
        }

        #endregion

        #region KeyDelete

        /// <summary>
        /// Removes the specified keys. A key is ignored if it does not exist.
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>key delete response</returns>
        public async Task<DeleteResponse> KeyDeleteAsync(CacheServer server, DeleteOption option)
        {
            if (option?.Keys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(DeleteOption)}.{nameof(DeleteOption.Keys)}");
            }
            var cacheDatabase = GetDatabase(server);
            var count = await cacheDatabase.KeyDeleteAsync(option.Keys.Select(c => { RedisKey key = c.GetActualKey(); return key; }).ToArray(), GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new DeleteResponse()
            {
                Success = true,
                DeleteCount = count
            };
        }

        #endregion

        #region KeyExist

        /// <summary>
        /// key exist
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns></returns>
        public async Task<ExistResponse> KeyExistAsync(CacheServer server, ExistOption option)
        {
            if (option.Keys.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(ExistOption)}.{nameof(ExistOption.Keys)}");
            }
            var cacheDatabase = GetDatabase(server);
            var redisKeys = new RedisKey[option.Keys.Count];
            var redisKeyParameters = new List<string>(option.Keys.Count);
            for (var i = 0; i < option.Keys.Count; i++)
            {
                redisKeys[i] = option.Keys[i].GetActualKey();
                redisKeyParameters.Add($"{Keys(i + 1)}");
            }
            string script = $@"local pv=redis.call('EXISTS',{string.Join(",", redisKeyParameters)})
{GetRefreshExpirationScript(-2)}
return pv";
            var result = (long)await cacheDatabase.ScriptEvaluateAsync(script, redisKeys,
            new RedisValue[]
            {
                true,//refresh current time
                RedisManager.AllowSlidingExpiration(),//whether allow set refresh time
                0,//expire time seconds

            }, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            return new ExistResponse()
            {
                Success = true,
                KeyCount = result
            };
        }

        #endregion

        #endregion

        #region Server

        #region Get all data base

        /// <summary>
        /// Get all database
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>Return get all database response</returns>
        public async Task<GetAllDataBaseResponse> GetAllDataBaseAsync(CacheServer server, GetAllDataBaseOption option)
        {
            if (server == null)
            {
                throw new ArgumentNullException($"{nameof(server)}");
            }
            var conn = GetConnection(server);
            var configs = await conn.GetServer(string.Format("{0}:{1}", server.Host, server.Port)).ConfigGetAsync("databases").ConfigureAwait(false);
            if (configs.IsNullOrEmpty())
            {
                return new GetAllDataBaseResponse()
                {
                    Success = true,
                    Databases = new List<CacheDatabase>(0)
                };
            }
            var databaseConfig = configs.FirstOrDefault(c => string.Equals(c.Key, "databases", StringComparison.OrdinalIgnoreCase));
            int dataBaseSize = databaseConfig.Value.ObjToInt32();
            List<CacheDatabase> databaseList = new List<CacheDatabase>(dataBaseSize);
            for (var d = 0; d < dataBaseSize; d++)
            {
                databaseList.Add(new CacheDatabase()
                {
                    Index = d,
                    Name = $"DB{d}"
                });
            };
            return new GetAllDataBaseResponse()
            {
                Success = true,
                Databases = databaseList
            };
        }

        #endregion

        #region Query keys

        /// <summary>
        /// Query keys
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>Return get keys response</returns>
        public async Task<GetKeysResponse> GetKeysAsync(CacheServer server, GetKeysOption option)
        {
            if (server == null)
            {
                throw new ArgumentNullException($"{nameof(server)}");
            }
            var query = option.Query;
            var database = GetDatabase(server);
            string searchString = "*";
            if (query != null && !string.IsNullOrWhiteSpace(query.MateKey))
            {
                switch (query.Type)
                {
                    case PatternType.StartWith:
                        searchString = query.MateKey + "*";
                        break;
                    case PatternType.EndWith:
                        searchString = "*" + query.MateKey;
                        break;
                    default:
                        searchString = string.Format("*{0}*", query.MateKey);
                        break;
                }
            }
            var redisServer = database.Multiplexer.GetServer(string.Format("{0}:{1}", server.Host, server.Port));
            var keys = redisServer.Keys(database.Database, searchString, query.PageSize, 0, (query.Page - 1) * query.PageSize, CommandFlags.None);
            List<CacheKey> itemList = keys.Select(c => { CacheKey key = ConstantCacheKey.Create(c); return key; }).ToList();
            var totalCount = await redisServer.DatabaseSizeAsync(database.Database).ConfigureAwait(false);
            var keyItemPaging = new CachePaging<CacheKey>(query.Page, query.PageSize, totalCount, itemList);
            return new GetKeysResponse()
            {
                Success = true,
                Keys = keyItemPaging
            };
        }

        #endregion

        #region Clear data

        /// <summary>
        /// clear database data
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>clear data response</returns>
        public async Task<ClearDataResponse> ClearDataAsync(CacheServer server, ClearDataOption option)
        {
            if (option.Databases.IsNullOrEmpty())
            {
                return new ClearDataResponse()
                {
                    Success = false,
                    Message = "Databases is null or empty"
                };
            }
            var conn = GetConnection(server);
            var redisServer = conn.GetServer(string.Format("{0}:{1}", server.Host, server.Port));
            foreach (var db in option.Databases)
            {
                await redisServer.FlushDatabaseAsync(db?.Index ?? 0, GetCommandFlags(option.CommandFlags)).ConfigureAwait(false);
            }
            return new ClearDataResponse()
            {
                Success = true
            };
        }

        #endregion

        #region Get cache item detail

        /// <summary>
        /// get cache item detail
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>get key detail response</returns>
        public async Task<GetDetailResponse> GetKeyDetailAsync(CacheServer server, GetDetailOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.Key))
            {
                throw new ArgumentNullException($"{nameof(GetDetailOption)}.{nameof(GetDetailOption.Key)}");
            }
            var keyTypeResponse = await KeyTypeAsync(server, new TypeOption()
            {
                Key = option.Key
            }).ConfigureAwait(false);
            var database = GetDatabase(server);
            CacheEntry keyItem = new CacheEntry()
            {
                Key = option.Key.GetActualKey(),
                Type = keyTypeResponse.KeyType
            };
            switch (keyTypeResponse.KeyType)
            {
                case CacheKeyType.String:
                    keyItem.Value = await database.StringGetAsync(keyItem.Key.GetActualKey()).ConfigureAwait(false);
                    break;
                case CacheKeyType.List:
                    List<string> listValues = new List<string>();
                    var listResults = await database.ListRangeAsync(keyItem.Key.GetActualKey(), 0, -1, CommandFlags.None).ConfigureAwait(false);
                    listValues.AddRange(listResults.Select(c => (string)c));
                    keyItem.Value = listValues;
                    break;
                case CacheKeyType.Set:
                    List<string> setValues = new List<string>();
                    var setResults = await database.SetMembersAsync(keyItem.Key.GetActualKey(), CommandFlags.None).ConfigureAwait(false);
                    setValues.AddRange(setResults.Select(c => (string)c));
                    keyItem.Value = setValues;
                    break;
                case CacheKeyType.SortedSet:
                    List<string> sortSetValues = new List<string>();
                    var sortedResults = await database.SortedSetRangeByRankAsync(keyItem.Key.GetActualKey()).ConfigureAwait(false);
                    sortSetValues.AddRange(sortedResults.Select(c => (string)c));
                    keyItem.Value = sortSetValues;
                    break;
                case CacheKeyType.Hash:
                    Dictionary<string, string> hashValues = new Dictionary<string, string>();
                    var objValues = await database.HashGetAllAsync(keyItem.Key.GetActualKey()).ConfigureAwait(false);
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
                CacheEntry = keyItem
            };
        }

        #endregion

        #region Get server configuration

        /// <summary>
        /// get server configuration
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>get server config response</returns>
        public async Task<GetServerConfigurationResponse> GetServerConfigurationAsync(CacheServer server, GetServerConfigurationOption option)
        {
            var conn = GetConnection(server);
            var redisServer = conn.GetServer(string.Format("{0}:{1}", server.Host, server.Port));
            var configs = await redisServer.ConfigGetAsync("*").ConfigureAwait(false);
            if (configs.IsNullOrEmpty())
            {
                return new GetServerConfigurationResponse()
                {
                    Success = false,
                    ServerConfiguration = null
                };
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
                        List<DataChangeSaveOption> saveInfos = new List<DataChangeSaveOption>();
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
                            saveInfos.Add(new DataChangeSaveOption()
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

            return new GetServerConfigurationResponse()
            {
                ServerConfiguration = config,
                Success = true
            };
        }

        #endregion

        #region Save server configuration

        /// <summary>
        /// save server configuration
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="option">option</param>
        /// <returns>save server config response</returns>
        public async Task<SaveServerConfigurationResponse> SaveServerConfigurationAsync(CacheServer server, SaveServerConfigurationOption option)
        {
            var config = option.ServerConfiguration as RedisServerConfiguration;
            if (config == null)
            {
                return new SaveServerConfigurationResponse()
                {
                    Success = false,
                    Message = "Redis server configuration is null"
                };
            }
            var conn = GetConnection(server);
            var redisServer = conn.GetServer(string.Format("{0}:{1}", server.Host, server.Port));
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
            return new SaveServerConfigurationResponse()
            {
                Success = true
            };
        }

        #endregion

        #endregion

        #region Util

        /// <summary>
        /// Get database index
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <returns></returns>
        static int GetDatabaseIndex(CacheServer server)
        {
            int dbIndex = -1;
            if (server == null)
            {
                return dbIndex;
            }
            if (!int.TryParse(server.Database, out dbIndex))
            {
                dbIndex = -1;
            }
            return dbIndex;
        }

        /// <summary>
        /// Get cache database
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <returns>Return the database</returns>
        static IDatabase GetDatabase(CacheServer server)
        {
            if (server == null)
            {
                throw new ArgumentNullException(nameof(CacheServer));
            }
            var connection = GetConnection(server);
            int dbIndex = GetDatabaseIndex(server);
            IDatabase database = connection.GetDatabase(dbIndex);
            if (database == null)
            {
                throw new Exception("Cache database fetch failed");
            }
            return database;
        }

        /// <summary>
        /// Get connection
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <returns></returns>
        static ConnectionMultiplexer GetConnection(CacheServer server)
        {
            if (server == null)
            {
                throw new ArgumentNullException($"{nameof(CacheServer)}");
            }
            string serverKey = server.IdentityKey;
            if (RedisConnections.TryGetValue(serverKey, out var multiplexer) && multiplexer != null)
            {
                return multiplexer;
            }
            var configOption = new ConfigurationOptions()
            {
                EndPoints =
                {
                    {
                        server.Host,
                        server.Port
                    }
                },
                AllowAdmin = server.AllowAdmin,
                ResolveDns = server.ResolveDns,
                Ssl = server.SSL,
                SyncTimeout = 10000
            };
            if (server.ConnectTimeout > 0)
            {
                configOption.ConnectTimeout = server.ConnectTimeout;
            }
            if (!string.IsNullOrWhiteSpace(server.Password))
            {
                configOption.Password = server.Password;
            }
            if (!string.IsNullOrWhiteSpace(server.ClientName))
            {
                configOption.ClientName = server.ClientName;
            }
            if (!string.IsNullOrWhiteSpace(server.SSLHost))
            {
                configOption.SslHost = server.SSLHost;
            }
            if (server.SyncTimeout > 0)
            {
                configOption.SyncTimeout = server.SyncTimeout;
            }
            if (!string.IsNullOrWhiteSpace(server.TieBreaker))
            {
                configOption.TieBreaker = server.TieBreaker;
            }
            multiplexer = ConnectionMultiplexer.Connect(configOption);
            RedisConnections.TryAdd(serverKey, multiplexer);
            return multiplexer;
        }

        /// <summary>
        /// get option flags
        /// </summary>
        /// <param name="cacheCommandFlags">cache option flags</param>
        /// <returns>option flags</returns>
        CommandFlags GetCommandFlags(CacheCommandFlags cacheCommandFlags)
        {
            CommandFlags cmdFlags = CommandFlags.None;
            switch (cacheCommandFlags)
            {
                case CacheCommandFlags.None:
                default:
                    cmdFlags = CommandFlags.None;
                    break;
                case CacheCommandFlags.DemandMaster:
                    cmdFlags = CommandFlags.DemandMaster;
                    break;
                case CacheCommandFlags.DemandSlave:
                    cmdFlags = CommandFlags.DemandSlave;
                    break;
                case CacheCommandFlags.FireAndForget:
                    cmdFlags = CommandFlags.FireAndForget;
                    break;
                case CacheCommandFlags.HighPriority:
                    cmdFlags = CommandFlags.HighPriority;
                    break;
                case CacheCommandFlags.NoRedirect:
                    cmdFlags = CommandFlags.NoRedirect;
                    break;
                case CacheCommandFlags.NoScriptCache:
                    cmdFlags = CommandFlags.NoScriptCache;
                    break;
                case CacheCommandFlags.PreferSlave:
                    cmdFlags = CommandFlags.PreferSlave;
                    break;
            }
            return cmdFlags;
        }

        /// <summary>
        /// Get key expiration
        /// </summary>
        /// <param name="expiration">Cache expiration</param>
        /// <returns></returns>
        static Tuple<bool, TimeSpan?> GetExpiration(CacheExpiration expiration)
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
        exkey=ckey..'{ExpirationKeySuffix}' 
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
        exkey=ckey..'{ExpirationKeySuffix}'
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
            return $"{cacheKey}{ExpirationKeySuffix}";
        }

        /// <summary>
        /// Get set value command
        /// </summary>
        /// <param name="setWhen"></param>
        /// <returns></returns>
        static string GetSetWhenCommand(CacheSetWhen setWhen)
        {
            switch (setWhen)
            {
                case CacheSetWhen.Always:
                default:
                    return "";
                case CacheSetWhen.Exists:
                    return "XX";
                case CacheSetWhen.NotExists:
                    return "NX";
            }
        }

        /// <summary>
        /// Get the bit operator
        /// </summary>
        /// <param name="bitwise">bitwise</param>
        /// <returns>Return the bit operator</returns>
        static string GetBitOperator(CacheBitwise bitwise)
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
        /// Get match pattern
        /// </summary>
        /// <param name="key">Match key</param>
        /// <param name="patternType">Pattern type</param>
        /// <returns></returns>
        static string GetMatchPattern(string key, PatternType patternType)
        {
            if (string.IsNullOrEmpty(key))
            {
                return "*";
            }
            switch (patternType)
            {
                case PatternType.StartWith:
                    return $"{key}*";
                case PatternType.EndWith:
                    return $"*{key}";
                case PatternType.Include:
                default:
                    return $"*{key}*";
            }
        }

        /// <summary>
        /// Get set combine command
        /// </summary>
        /// <param name="setOperationType"></param>
        /// <returns></returns>
        static string GetSetCombineCommand(CombineOperation operationType)
        {
            switch (operationType)
            {
                case CombineOperation.Union:
                default:
                    return "SUNION";
                case CombineOperation.Difference:
                    return "SDIFF";
                case CombineOperation.Intersect:
                    return "SUNION";
            }
        }

        /// <summary>
        /// Get sorted set combine command
        /// </summary>
        /// <param name="operationType">Set operation type</param>
        /// <returns></returns>
        static string GetSortedSetCombineCommand(CombineOperation operationType)
        {
            switch (operationType)
            {
                case CombineOperation.Union:
                default:
                    return "ZUNIONSTORE";
                case CombineOperation.Difference:
                    throw new InvalidOperationException(nameof(CombineOperation.Difference));
                case CombineOperation.Intersect:
                    return "ZINTERSTORE";
            }
        }

        /// <summary>
        /// Get sorted set aggregate name
        /// </summary>
        /// <param name="aggregate">Aggregate type</param>
        /// <returns></returns>
        static string GetSortedSetAggregateName(SetAggregate aggregate)
        {
            switch (aggregate)
            {
                case SetAggregate.Sum:
                default:
                    return "SUM";
                case SetAggregate.Min:
                    return "MIN";
                case SetAggregate.Max:
                    return "MAX";
            }
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
        /// <param name="exclude">Exclude option</param>
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

        /// <summary>
        /// Get cache key type
        /// </summary>
        /// <param name="typeName">Redis type name</param>
        /// <returns></returns>
        CacheKeyType GetCacheKeyType(string typeName)
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
        long GetTotalSeconds(TimeSpan? timeSpan)
        {
            return (long)(timeSpan?.TotalSeconds ?? 0);
        }

        #endregion
    }
}
