using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
    /// <summary>
    /// Implements ICacheProvider by Redis
    /// </summary>
    public partial class RedisProvider
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
        public async Task<StringSetRangeResult> StringSetRangeAsync(CacheServer server, StringSetRangeParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringSetRangeParameter)}.{nameof(StringSetRangeParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringSetRangeStatement(parameter);
            var result = await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new StringSetRangeResult()
            {
                Success = true,
                CacheServer = server,
                Database = database,
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
        /// <param name="parameter">String set bit parameter</param>
        /// <returns>Return string set bit result</returns>
        public async Task<StringSetBitResult> StringSetBitAsync(CacheServer server, StringSetBitParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringSetBitParameter)}.{nameof(StringSetBitParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringSetBitStatement(parameter);
            var result = await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new StringSetBitResult()
            {
                Success = true,
                OldBitValue = (bool)result,
                CacheServer = server,
                Database = database
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
        public async Task<StringSetResult> StringSetAsync(CacheServer server, StringSetParameter parameter)
        {
            if (parameter?.Items.IsNullOrEmpty() ?? true)
            {
                return GetNoValueResponse<StringSetResult>(server);
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringSetStatement(parameter);
            var result = await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new StringSetResult()
            {
                CacheServer = server,
                Database = database,
                Success = true,
                Results = ((RedisValue[])result)?.Select(c => new StringEntrySetResult() { SetSuccess = true, Key = c }).ToList()
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
        public async Task<StringLengthResult> StringLengthAsync(CacheServer server, StringLengthParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringLengthParameter)}.{nameof(StringLengthParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringLengthStatement(parameter);
            var result = await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new StringLengthResult()
            {
                Success = true,
                Length = (long)result,
                CacheServer = server,
                Database = database
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
        public async Task<StringIncrementResult> StringIncrementAsync(CacheServer server, StringIncrementParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringIncrementParameter)}.{nameof(StringIncrementParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringIncrementStatement(parameter);
            var result = await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new StringIncrementResult()
            {
                Success = true,
                NewValue = (long)result,
                CacheServer = server,
                Database = database
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
        public async Task<StringGetWithExpiryResult> StringGetWithExpiryAsync(CacheServer server, StringGetWithExpiryParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetWithExpiryParameter)}.{nameof(StringGetWithExpiryParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringGetWithExpiryStatement(parameter);
            var result = (RedisValue[])(await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false));
            return new StringGetWithExpiryResult()
            {
                Success = true,
                Value = result[0],
                Expiry = TimeSpan.FromSeconds((long)result[1]),
                CacheServer = server,
                Database = database
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
        public async Task<StringGetSetResult> StringGetSetAsync(CacheServer server, StringGetSetParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetSetParameter)}.{nameof(StringGetSetParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringGetSetStatement(parameter);
            var result = await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new StringGetSetResult()
            {
                Success = true,
                OldValue = (string)result,
                CacheServer = server,
                Database = database
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
        public async Task<StringGetRangeResult> StringGetRangeAsync(CacheServer server, StringGetRangeParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetRangeParameter)}.{nameof(StringGetRangeParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringGetRangeStatement(parameter);
            var result = await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new StringGetRangeResult()
            {
                Success = true,
                Value = (string)result,
                CacheServer = server,
                Database = database
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
        public async Task<StringGetBitResult> StringGetBitAsync(CacheServer server, StringGetBitParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringGetBitParameter)}.{nameof(StringGetBitParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringGetBitStatement(parameter);
            var result = await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new StringGetBitResult()
            {
                Success = true,
                Bit = (bool)result,
                CacheServer = server,
                Database = database
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
        public async Task<StringGetResult> StringGetAsync(CacheServer server, StringGetParameter parameter)
        {
            if (parameter?.Keys.IsNullOrEmpty() ?? true)
            {
                return GetNoKeyResponse<StringGetResult>(server);
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringGetStatement(parameter);
            var result = (RedisValue[])(await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false));
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
        public async Task<StringDecrementResult> StringDecrementAsync(CacheServer server, StringDecrementParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringDecrementParameter)}.{nameof(StringDecrementParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringDecrementStatement(parameter);
            var result = await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new StringDecrementResult()
            {
                Success = true,
                NewValue = (long)result,
                CacheServer = server,
                Database = database
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
        public async Task<StringBitPositionResult> StringBitPositionAsync(CacheServer server, StringBitPositionParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringBitPositionParameter)}.{nameof(StringBitPositionParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringBitPositionStatement(parameter);
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new StringBitPositionResult()
            {
                Success = true,
                Position = result,
                HasValue = result >= 0,
                CacheServer = server,
                Database = database
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
        public async Task<StringBitOperationResult> StringBitOperationAsync(CacheServer server, StringBitOperationParameter parameter)
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
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new StringBitOperationResult()
            {
                Success = true,
                DestinationValueLength = result,
                CacheServer = server,
                Database = database
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
        public async Task<StringBitCountResult> StringBitCountAsync(CacheServer server, StringBitCountParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringBitCountParameter)}.{nameof(StringBitCountParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringBitCountStatement(parameter);
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new StringBitCountResult()
            {
                Success = true,
                BitNum = result,
                CacheServer = server,
                Database = database
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
        public async Task<StringAppendResult> StringAppendAsync(CacheServer server, StringAppendParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(StringAppendParameter)}.{nameof(StringAppendParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetStringAppendStatement(parameter);
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new StringAppendResult()
            {
                Success = true,
                NewValueLength = result,
                CacheServer = server,
                Database = database
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
        public async Task<ListTrimResult> ListTrimAsync(CacheServer server, ListTrimParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListTrimParameter)}.{nameof(ListTrimParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListTrimStatement(parameter);
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new ListTrimResult()
            {
                Success = true,
                CacheServer = server,
                Database = database
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
        public async Task<ListSetByIndexResult> ListSetByIndexAsync(CacheServer server, ListSetByIndexParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListSetByIndexParameter)}.{nameof(ListSetByIndexParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListSetByIndexStatement(parameter);
            var result = (string)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new ListSetByIndexResult()
            {
                Success = string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase),
                CacheServer = server,
                Database = database
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
        public async Task<ListRightPushResult> ListRightPushAsync(CacheServer server, ListRightPushParameter parameter)
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
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new ListRightPushResult()
            {
                Success = true,
                NewListLength = result,
                CacheServer = server,
                Database = database
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
        public async Task<ListRightPopLeftPushResult> ListRightPopLeftPushAsync(CacheServer server, ListRightPopLeftPushParameter parameter)
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
            var result = (string)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new ListRightPopLeftPushResult()
            {
                Success = true,
                PopValue = result,
                CacheServer = server,
                Database = database
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
        public async Task<ListRightPopResult> ListRightPopAsync(CacheServer server, ListRightPopParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRightPopParameter)}.{nameof(ListRightPopParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListRightPopStatement(parameter);
            var result = (string)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new ListRightPopResult()
            {
                Success = true,
                PopValue = result,
                CacheServer = server,
                Database = database
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
        public async Task<ListRemoveResult> ListRemoveAsync(CacheServer server, ListRemoveParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRemoveParameter)}.{nameof(ListRemoveParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListRemoveStatement(parameter);
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new ListRemoveResult()
            {
                Success = true,
                RemoveCount = result,
                CacheServer = server,
                Database = database
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
        public async Task<ListRangeResult> ListRangeAsync(CacheServer server, ListRangeParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListRangeParameter)}.{nameof(ListRangeParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListRangeStatement(parameter);
            var result = (RedisValue[])await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new ListRangeResult()
            {
                Success = true,
                Values = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
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
        public async Task<ListLengthResult> ListLengthAsync(CacheServer server, ListLengthParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListLengthParameter)}.{nameof(ListLengthParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListLengthStatement(parameter);
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new ListLengthResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
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
        public async Task<ListLeftPushResult> ListLeftPushAsync(CacheServer server, ListLeftPushParameter parameter)
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
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new ListLeftPushResult()
            {
                Success = true,
                NewListLength = result,
                CacheServer = server,
                Database = database
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
        public async Task<ListLeftPopResult> ListLeftPopAsync(CacheServer server, ListLeftPopParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListLeftPopParameter)}.{nameof(ListLeftPopParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListLeftPopStatement(parameter);
            var result = (string)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new ListLeftPopResult()
            {
                Success = true,
                PopValue = result,
                CacheServer = server,
                Database = database
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
        public async Task<ListInsertBeforeResult> ListInsertBeforeAsync(CacheServer server, ListInsertBeforeParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListInsertBeforeParameter)}.{nameof(ListInsertBeforeParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListInsertBeforeStatement(parameter);
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new ListInsertBeforeResult()
            {
                Success = result > 0,
                NewListLength = result,
                CacheServer = server,
                Database = database
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
        public async Task<ListInsertAfterResult> ListInsertAfterAsync(CacheServer server, ListInsertAfterParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListInsertAfterParameter)}.{nameof(ListInsertAfterParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListInsertAfterStatement(parameter);
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new ListInsertAfterResult()
            {
                Success = result > 0,
                NewListLength = result,
                CacheServer = server,
                Database = database
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
        public async Task<ListGetByIndexResult> ListGetByIndexAsync(CacheServer server, ListGetByIndexParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ListInsertAfterParameter)}.{nameof(ListInsertAfterParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetListGetByIndexStatement(parameter);
            var result = (string)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new ListGetByIndexResult()
            {
                Success = true,
                Value = result,
                CacheServer = server,
                Database = database
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
        public async Task<HashValuesResult> HashValuesAsync(CacheServer server, HashValuesParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashValuesParameter)}.{nameof(HashValuesParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashValuesStatement(parameter);
            var result = (RedisValue[])await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new HashValuesResult()
            {
                Success = true,
                Values = result.Select(c => { dynamic value = c; return value; }).ToList(),
                CacheServer = server,
                Database = database
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
        public async Task<HashSetResult> HashSetAsync(CacheServer server, HashSetParameter parameter)
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
            var result = (string)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new HashSetResult()
            {
                Success = string.Equals(result, "ok", StringComparison.OrdinalIgnoreCase),
                CacheServer = server,
                Database = database
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
        public async Task<HashLengthResult> HashLengthAsync(CacheServer server, HashLengthParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashLengthParameter)}.{nameof(HashLengthParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashLengthStatement(parameter);
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new HashLengthResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
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
        public async Task<HashKeysResult> HashKeysAsync(CacheServer server, HashKeysParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashKeysParameter)}.{nameof(HashKeysParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashKeysStatement(parameter);
            var result = (RedisValue[])await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new HashKeysResult()
            {
                Success = true,
                HashKeys = result.Select(c => { string key = c; return key; }).ToList(),
                CacheServer = server,
                Database = database
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
        public async Task<HashIncrementResult> HashIncrementAsync(CacheServer server, HashIncrementParameter parameter)
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
            var newCacheValue = await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
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

        #endregion

        #region HashGet

        /// <summary>
        /// Returns the value associated with field in the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Hash get parameter</param>
        /// <returns>Return hash get result</returns>
        public async Task<HashGetResult> HashGetAsync(CacheServer server, HashGetParameter parameter)
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
            var result = (string)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new HashGetResult()
            {
                Success = true,
                Value = result,
                CacheServer = server,
                Database = database
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
        public async Task<HashGetAllResult> HashGetAllAsync(CacheServer server, HashGetAllParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashGetAllParameter)}.{nameof(HashGetAllParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashGetAllStatement(parameter);
            var result = (RedisValue[])await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            Dictionary<string, dynamic> values = new Dictionary<string, dynamic>(result.Length / 2);
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

        #endregion

        #region HashExists

        /// <summary>
        /// Returns if field is an existing field in the hash stored at key.
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Options</param>
        /// <returns>hash exists result</returns>
        public async Task<HashExistsResult> HashExistAsync(CacheServer server, HashExistsParameter parameter)
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
            var result = (int)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new HashExistsResult()
            {
                Success = true,
                HasField = result == 1,
                CacheServer = server,
                Database = database
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
        public async Task<HashDeleteResult> HashDeleteAsync(CacheServer server, HashDeleteParameter parameter)
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
            var result = (int)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new HashDeleteResult()
            {
                Success = result > 0,
                CacheServer = server,
                Database = database
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
        public async Task<HashDecrementResult> HashDecrementAsync(CacheServer server, HashDecrementParameter parameter)
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
            var newCacheValue = await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
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

        #endregion

        #region HashScan

        /// <summary>
        /// The HSCAN options is used to incrementally iterate over a hash
        /// </summary>
        /// <param name="server">Cache server</param>
        /// <param name="parameter">Hash scan parameter</param>
        /// <returns>Return hash scan result</returns>
        public async Task<HashScanResult> HashScanAsync(CacheServer server, HashScanParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(HashScanParameter)}.{nameof(HashScanParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetHashScanStatement(parameter);
            var result = (RedisValue[])await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
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
        public async Task<SetRemoveResult> SetRemoveAsync(CacheServer server, SetRemoveParameter parameter)
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
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SetRemoveResult()
            {
                Success = true,
                RemoveCount = result,
                CacheServer = server,
                Database = database
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
        public async Task<SetRandomMembersResult> SetRandomMembersAsync(CacheServer server, SetRandomMembersParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetRandomMembersParameter)}.{nameof(SetRandomMembersParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetRandomMembersStatement(parameter);
            var result = (RedisValue[])await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SetRandomMembersResult()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
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
        public async Task<SetRandomMemberResult> SetRandomMemberAsync(CacheServer server, SetRandomMemberParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetRandomMemberParameter)}.{nameof(SetRandomMemberParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetRandomMemberStatement(parameter);
            var result = (string)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SetRandomMemberResult()
            {
                Success = true,
                Member = result,
                CacheServer = server,
                Database = database
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
        public async Task<SetPopResult> SetPopAsync(CacheServer server, SetPopParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetPopParameter)}.{nameof(SetPopParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetPopStatement(parameter);
            var result = (string)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SetPopResult()
            {
                Success = true,
                PopValue = result,
                CacheServer = server,
                Database = database
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
        public async Task<SetMoveResult> SetMoveAsync(CacheServer server, SetMoveParameter parameter)
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
            var result = (string)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SetMoveResult()
            {
                Success = result == "1",
                CacheServer = server,
                Database = database
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
        public async Task<SetMembersResult> SetMembersAsync(CacheServer server, SetMembersParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetMembersParameter)}.{nameof(SetMembersParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetMembersStatement(parameter);
            var result = (RedisValue[])await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SetMembersResult()
            {
                Success = true,
                Members = result?.Select(c => { string member = c; return member; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
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
        public async Task<SetLengthResult> SetLengthAsync(CacheServer server, SetLengthParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SetLengthParameter)}.{nameof(SetLengthParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetLengthStatement(parameter);
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SetLengthResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
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
        public async Task<SetContainsResult> SetContainsAsync(CacheServer server, SetContainsParameter parameter)
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
            var result = (string)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SetContainsResult()
            {
                Success = true,
                ContainsValue = result == "1",
                CacheServer = server,
                Database = database
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
        public async Task<SetCombineResult> SetCombineAsync(CacheServer server, SetCombineParameter parameter)
        {
            if (parameter?.Keys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(SetCombineParameter)}.{nameof(SetCombineParameter.Keys)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSetCombineStatement(parameter);
            var result = (RedisValue[])await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SetCombineResult()
            {
                Success = true,
                CombineValues = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
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
        public async Task<SetCombineAndStoreResult> SetCombineAndStoreAsync(CacheServer server, SetCombineAndStoreParameter parameter)
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
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SetCombineAndStoreResult()
            {
                Success = true,
                Count = result,
                CacheServer = server,
                Database = database
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
        public async Task<SetAddResult> SetAddAsync(CacheServer server, SetAddParameter parameter)
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
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SetAddResult()
            {
                Success = result > 0,
                CacheServer = server,
                Database = database
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
        public async Task<SortedSetScoreResult> SortedSetScoreAsync(CacheServer server, SortedSetScoreParameter parameter)
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
            var result = (double?)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SortedSetScoreResult()
            {
                Success = true,
                Score = result,
                CacheServer = server,
                Database = database
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
        public async Task<SortedSetRemoveRangeByValueResult> SortedSetRemoveRangeByValueAsync(CacheServer server, SortedSetRemoveRangeByValueParameter parameter)
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
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SortedSetRemoveRangeByValueResult()
            {
                RemoveCount = result,
                Success = true,
                CacheServer = server,
                Database = database
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
        public async Task<SortedSetRemoveRangeByScoreResult> SortedSetRemoveRangeByScoreAsync(CacheServer server, SortedSetRemoveRangeByScoreParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByScoreParameter)}.{nameof(SortedSetRemoveRangeByScoreParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRemoveRangeByScoreStatement(parameter);
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SortedSetRemoveRangeByScoreResult()
            {
                RemoveCount = result,
                Success = true,
                CacheServer = server,
                Database = database
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
        public async Task<SortedSetRemoveRangeByRankResult> SortedSetRemoveRangeByRankAsync(CacheServer server, SortedSetRemoveRangeByRankParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRemoveRangeByRankParameter)}.{nameof(SortedSetRemoveRangeByRankParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRemoveRangeByRankStatement(parameter);
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SortedSetRemoveRangeByRankResult()
            {
                RemoveCount = result,
                Success = true,
                CacheServer = server,
                Database = database
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
        public async Task<SortedSetRemoveResult> SortedSetRemoveAsync(CacheServer server, SortedSetRemoveParameter parameter)
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
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SortedSetRemoveResult()
            {
                Success = true,
                RemoveCount = result,
                CacheServer = server,
                Database = database
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
        public async Task<SortedSetRankResult> SortedSetRankAsync(CacheServer server, SortedSetRankParameter parameter)
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
            var result = (long?)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SortedSetRankResult()
            {
                Success = true,
                Rank = result,
                CacheServer = server,
                Database = database
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
        public async Task<SortedSetRangeByValueResult> SortedSetRangeByValueAsync(CacheServer server, SortedSetRangeByValueParameter parameter)
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
            var result = (RedisValue[])await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SortedSetRangeByValueResult()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
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
        public async Task<SortedSetRangeByScoreWithScoresResult> SortedSetRangeByScoreWithScoresAsync(CacheServer server, SortedSetRangeByScoreWithScoresParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByScoreWithScoresParameter)}.{nameof(SortedSetRangeByScoreWithScoresParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByScoreWithScoresStatement(parameter);
            var result = (RedisValue[])await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
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
        public async Task<SortedSetRangeByScoreResult> SortedSetRangeByScoreAsync(CacheServer server, SortedSetRangeByScoreParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByScoreParameter)}.{nameof(SortedSetRangeByScoreParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByScoreStatement(parameter);
            var result = (RedisValue[])await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SortedSetRangeByScoreResult()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
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
        public async Task<SortedSetRangeByRankWithScoresResult> SortedSetRangeByRankWithScoresAsync(CacheServer server, SortedSetRangeByRankWithScoresParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByRankWithScoresParameter)}.{nameof(SortedSetRangeByRankWithScoresParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByRankWithScoresStatement(parameter);
            var result = (RedisValue[])await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
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
        public async Task<SortedSetRangeByRankResult> SortedSetRangeByRankAsync(CacheServer server, SortedSetRangeByRankParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetRangeByRankParameter)}.{nameof(SortedSetRangeByRankParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetRangeByRankStatement(parameter);
            var result = (RedisValue[])await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SortedSetRangeByRankResult()
            {
                Success = true,
                Members = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
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
        public async Task<SortedSetLengthByValueResult> SortedSetLengthByValueAsync(CacheServer server, SortedSetLengthByValueParameter parameter)
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
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SortedSetLengthByValueResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
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
        public async Task<SortedSetLengthResult> SortedSetLengthAsync(CacheServer server, SortedSetLengthParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetLengthByValueParameter)}.{nameof(SortedSetLengthByValueParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetLengthStatement(parameter);
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SortedSetLengthResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
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
        public async Task<SortedSetIncrementResult> SortedSetIncrementAsync(CacheServer server, SortedSetIncrementParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetIncrementParameter)}.{nameof(SortedSetIncrementParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetIncrementStatement(parameter);
            var result = (double)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SortedSetIncrementResult()
            {
                Success = true,
                NewScore = result,
                CacheServer = server,
                Database = database
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
        public async Task<SortedSetDecrementResult> SortedSetDecrementAsync(CacheServer server, SortedSetDecrementParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortedSetDecrementParameter)}.{nameof(SortedSetDecrementParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortedSetDecrementStatement(parameter);
            var result = (double)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SortedSetDecrementResult()
            {
                Success = true,
                NewScore = result,
                CacheServer = server,
                Database = database
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
        public async Task<SortedSetCombineAndStoreResult> SortedSetCombineAndStoreAsync(CacheServer server, SortedSetCombineAndStoreParameter parameter)
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
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SortedSetCombineAndStoreResult()
            {
                Success = true,
                NewSetLength = result,
                CacheServer = server,
                Database = database
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
        public async Task<SortedSetAddResult> SortedSetAddAsync(CacheServer server, SortedSetAddParameter parameter)
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
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SortedSetAddResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
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
        public async Task<SortResult> SortAsync(CacheServer server, SortParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(SortParameter)}.{nameof(SortParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetSortStatement(parameter);
            var result = (RedisValue[])await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SortResult()
            {
                Success = true,
                Values = result?.Select(c => { string value = c; return value; }).ToList() ?? new List<string>(0),
                CacheServer = server,
                Database = database
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
        public async Task<SortAndStoreResult> SortAndStoreAsync(CacheServer server, SortAndStoreParameter parameter)
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
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new SortAndStoreResult()
            {
                Success = true,
                Length = result,
                CacheServer = server,
                Database = database
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
        public async Task<TypeResult> KeyTypeAsync(CacheServer server, TypeParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(TypeParameter)}.{nameof(TypeParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyTypeStatement(parameter);
            var result = (string)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new TypeResult()
            {
                Success = true,
                KeyType = RedisManager.GetCacheKeyType(result),
                CacheServer = server,
                Database = database
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
        public async Task<TimeToLiveResult> KeyTimeToLiveAsync(CacheServer server, TimeToLiveParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(TimeToLiveParameter)}.{nameof(TimeToLiveParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyTimeToLiveStatement(parameter);
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
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
        public async Task<RestoreResult> KeyRestoreAsync(CacheServer server, RestoreParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(RestoreParameter)}.{nameof(RestoreParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyRestoreStatement(parameter);
            var result = (bool)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new RestoreResult()
            {
                Success = result,
                CacheServer = server,
                Database = database
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
        public async Task<RenameResult> KeyRenameAsync(CacheServer server, RenameParameter parameter)
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
            var result = (bool)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new RenameResult()
            {
                Success = result,
                CacheServer = server,
                Database = database
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
        public async Task<RandomResult> KeyRandomAsync(CacheServer server, RandomParameter parameter)
        {
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyRandomStatement(parameter);
            var result = (string)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new RandomResult()
            {
                Success = true,
                Key = result,
                CacheServer = server,
                Database = database
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
        public async Task<PersistResult> KeyPersistAsync(CacheServer server, PersistParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(PersistParameter)}.{nameof(PersistParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyPersistStatement(parameter);
            var result = (bool)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new PersistResult()
            {
                Success = result,
                CacheServer = server,
                Database = database
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
        public async Task<MoveResult> KeyMoveAsync(CacheServer server, MoveParameter parameter)
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
            var result = (bool)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new MoveResult()
            {
                Success = result,
                CacheServer = server,
                Database = database
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
        public async Task<MigrateKeyResult> KeyMigrateAsync(CacheServer server, MigrateKeyParameter parameter)
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
            var result = (bool)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new MigrateKeyResult()
            {
                Success = true,
                CacheServer = server,
                Database = database
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
        public async Task<ExpireResult> KeyExpireAsync(CacheServer server, ExpireParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(ExpireParameter)}.{nameof(ExpireParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyExpireStatement(parameter);
            var result = (bool)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new ExpireResult()
            {
                Success = result,
                CacheServer = server,
                Database = database
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
        public async Task<DumpResult> KeyDumpAsync(CacheServer server, DumpParameter parameter)
        {
            if (string.IsNullOrWhiteSpace(parameter?.Key))
            {
                throw new ArgumentNullException($"{nameof(DumpParameter)}.{nameof(DumpParameter.Key)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyDumpStatement(parameter);
            var result = (byte[])await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new DumpResult()
            {
                Success = true,
                ByteValues = result,
                CacheServer = server,
                Database = database
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
        public async Task<DeleteResult> KeyDeleteAsync(CacheServer server, DeleteParameter parameter)
        {
            if (parameter?.Keys.IsNullOrEmpty() ?? true)
            {
                throw new ArgumentNullException($"{nameof(DeleteParameter)}.{nameof(DeleteParameter.Keys)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyDeleteStatement(parameter);
            var count = await database.RemoteDatabase.KeyDeleteAsync(statement.Keys, statement.Flags).ConfigureAwait(false);
            return new DeleteResult()
            {
                Success = true,
                DeleteCount = count,
                CacheServer = server,
                Database = database
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
        public async Task<ExistResult> KeyExistAsync(CacheServer server, ExistParameter parameter)
        {
            if (parameter.Keys.IsNullOrEmpty())
            {
                throw new ArgumentNullException($"{nameof(ExistParameter)}.{nameof(ExistParameter.Keys)}");
            }
            var database = RedisManager.GetDatabase(server);
            var statement = GetKeyExistStatement(parameter);
            var result = (long)await ExecuteStatementAsync(server, database, statement).ConfigureAwait(false);
            return new ExistResult()
            {
                Success = true,
                KeyCount = result,
                CacheServer = server,
                Database = database
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
        public async Task<GetAllDataBaseResult> GetAllDataBaseAsync(CacheServer server, GetAllDataBaseParameter parameter)
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
                var configs = await conn.GetServer(string.Format("{0}:{1}", parameter.EndPoint.Host, parameter.EndPoint.Port)).ConfigGetAsync("databases").ConfigureAwait(false);
                if (!configs.IsNullOrEmpty())
                {
                    var databaseConfig = configs.FirstOrDefault(c => string.Equals(c.Key, "databases", StringComparison.OrdinalIgnoreCase));
                    int dataBaseSize = databaseConfig.Value.ToInt32();
                    List<CacheDatabase> databaseList = new List<CacheDatabase>(dataBaseSize);
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
        public async Task<GetKeysResult> GetKeysAsync(CacheServer server, GetKeysParameter parameter)
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
                var totalCount = await redisServer.DatabaseSizeAsync(dbIndex).ConfigureAwait(false);
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
        public async Task<ClearDataResult> ClearDataAsync(CacheServer server, ClearDataParameter parameter)
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
                await redisServer.FlushDatabaseAsync(dbIndex, cmdFlags).ConfigureAwait(false);
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
        public async Task<GetDetailResult> GetKeyDetailAsync(CacheServer server, GetDetailParameter parameter)
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
                var redisKeyType = redisDatabase.KeyTypeAsync(parameter.Key.GetActualKey()).ConfigureAwait(false);
                var cacheKeyType = RedisManager.GetCacheKeyType(redisKeyType.ToString());
                var keyItem = new CacheEntry()
                {
                    Key = parameter.Key.GetActualKey(),
                    Type = cacheKeyType
                };
                switch (cacheKeyType)
                {
                    case CacheKeyType.String:
                        keyItem.Value = await redisDatabase.StringGetAsync(keyItem.Key.GetActualKey()).ConfigureAwait(false);
                        break;
                    case CacheKeyType.List:
                        var listValues = new List<string>();
                        var listResults = await redisDatabase.ListRangeAsync(keyItem.Key.GetActualKey(), 0, -1, CommandFlags.None).ConfigureAwait(false);
                        listValues.AddRange(listResults.Select(c => (string)c));
                        keyItem.Value = listValues;
                        break;
                    case CacheKeyType.Set:
                        var setValues = new List<string>();
                        var setResults = await redisDatabase.SetMembersAsync(keyItem.Key.GetActualKey(), CommandFlags.None).ConfigureAwait(false);
                        setValues.AddRange(setResults.Select(c => (string)c));
                        keyItem.Value = setValues;
                        break;
                    case CacheKeyType.SortedSet:
                        var sortSetValues = new List<string>();
                        var sortedResults = await redisDatabase.SortedSetRangeByRankAsync(keyItem.Key.GetActualKey()).ConfigureAwait(false);
                        sortSetValues.AddRange(sortedResults.Select(c => (string)c));
                        keyItem.Value = sortSetValues;
                        break;
                    case CacheKeyType.Hash:
                        var hashValues = new Dictionary<string, string>();
                        var objValues = await redisDatabase.HashGetAllAsync(keyItem.Key.GetActualKey()).ConfigureAwait(false);
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
        public async Task<GetServerConfigurationResult> GetServerConfigurationAsync(CacheServer server, GetServerConfigurationParameter parameter)
        {
            if (parameter?.EndPoint == null)
            {
                throw new ArgumentNullException($"{nameof(GetServerConfigurationParameter)}.{nameof(GetServerConfigurationParameter.EndPoint)}");
            }
            using (var conn = RedisManager.GetConnection(server, new CacheEndPoint[1] { parameter.EndPoint }))
            {
                var config = new RedisServerConfiguration();
                var redisServer = conn.GetServer(string.Format("{0}:{1}", parameter.EndPoint.Host, parameter.EndPoint.Port));
                var configs = await redisServer.ConfigGetAsync("*").ConfigureAwait(false);
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
        public async Task<SaveServerConfigurationResult> SaveServerConfigurationAsync(CacheServer server, SaveServerConfigurationParameter parameter)
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
                await redisServer.ConfigRewriteAsync().ConfigureAwait(false);
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

        Task<RedisResult> ExecuteStatementAsync(CacheServer server, RedisDatabase database, RedisStatement statement)
        {
            return database.RemoteDatabase.ScriptEvaluateAsync(statement.Script, statement.Keys, statement.Parameters, statement.Flags);
        }

        #endregion
    }
}
