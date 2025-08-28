# Temporal Query Cache Key Fix

## Critical Bug Found

Temporal queries at different timestamps were sharing the same cache entry, causing queries to return results from the wrong point in time.

## The Problem

The `normalize-temporal-query` function was correctly extracting the base query and timestamp from temporal queries:
```clojure
"SELECT * FROM sales FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01T10:00:00Z'"
=> {:base-query "SELECT * FROM sales"
    :temporal-param "2024-01-01T10:00:00Z"
    :is-temporal true}
```

However, the cache key generation was **intentionally excluding the timestamp**:

```clojure
;; WRONG - stripped timestamp from cache key
client-cache-key (if is-temporal
                   [session-id base-query normalized-params]  ; NO TIMESTAMP!
                   [session-id query normalized-params])
```

This meant:
1. Query at T1 gets executed and cached with key `["session" "SELECT * FROM sales" []]`
2. Query at T2 uses the SAME cache key and returns T1's results (wrong!)
3. Users would see data from the wrong point in time

## Example From Logs

```
[SERVER] INFO  [CACHE-UPDATE] Storing 1 results 
  Type: TEMPORAL 
  Cache key: [default SELECT COUNT(*) as cnt FROM (SELECT * FROM sales where amount < 500) as subq []]
  Timestamp: 2025-08-28T21:43:01.890Z
```

Notice: The cache key has NO timestamp, even though it's a TEMPORAL query!

## The Misunderstanding

The original code had this comment:
```clojure
;; For temporal queries, DON'T include timestamp in cache key - we want to diff across time!
```

This confuses two different concepts:
- **Caching**: Storing results to avoid re-computation (NEEDS unique keys per timestamp)
- **Diffing**: Comparing results between updates (separate concern)

## The Fix

Include the timestamp in cache keys for temporal queries:

```clojure
;; FIXED - includes timestamp for temporal queries
client-cache-key (if is-temporal
                   [session-id base-query normalized-params temporal-param]  ; Includes timestamp!
                   [session-id query normalized-params])
```

Now cache keys look like:
```
[default "SELECT COUNT(*) FROM sales" [] "2025-08-28T21:43:01.890Z"]
```

Each timestamp gets its own cache entry, as it should.

## Impact

### Before Fix
- Temporal queries would return stale/wrong data from different timestamps
- Cache pollution with incorrect results
- Unpredictable query results depending on cache state

### After Fix
- Each temporal query at a specific timestamp has its own cache entry
- Correct results for time-travel queries
- Predictable and consistent behavior

## Testing

Created comprehensive tests verifying:
1. ✅ Different timestamps produce different cache keys
2. ✅ Same timestamp produces same cache key (for cache hits)
3. ✅ Regular (non-temporal) queries unaffected
4. ✅ Cache isolation between different temporal queries

## Related Systems

The fix only affects `client-result-cache` in `kafka_reactive.clj`. Other temporal caching mechanisms were checked and are correct:
- `temporal_cache.clj` - Already uses full SQL (including temporal clause) as key ✅
- `sql_resolver.clj` - Has separate `generate-temporal-cache-key` function ✅

## Backwards Compatibility

No issues - the fix just ensures temporal queries get their own cache entries as they should have from the beginning.