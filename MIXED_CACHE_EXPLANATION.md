# Mixed Temporal/Non-Temporal Cache Explanation

## Why Both Query Types Share the Same Cache

The `client-result-cache` in `kafka_reactive.clj` stores **both** temporal and non-temporal queries. This is **intentional and correct**.

## Purpose of the Cache

The cache is used for:
1. **Diffing** - Computing incremental updates between query executions
2. **Avoiding redundant updates** - Not sending data if nothing changed
3. **Performance** - Reducing data transfer by sending only changes

## Cache Key Structure

### Non-Temporal Queries (3 elements)
```clojure
[session-id query params]
; Example: ["default" "SELECT * FROM sales where amount > 500" []]
```

### Temporal Queries (4 elements - includes timestamp)
```clojure
[session-id base-query params timestamp]
; Example: ["default" "SELECT * FROM sales" [] "2025-08-28T06:31:51.236001Z"]
```

## Why They Share the Cache

1. **Same diffing logic** applies to both query types
2. **Same data structure** for storing results
3. **Same performance benefits** from incremental updates
4. **Simpler code** - one cache, one set of diffing algorithms

## Example Cache Contents

```
client-result-cache = {
  ; Regular query
  ["default" "SELECT * FROM sales where amount > 500" []] 
    -> {:results [...], :timestamp ..., :checksum ...}
  
  ; Temporal query at T1
  ["default" "SELECT * FROM sales" [] "2025-08-28T10:00:00Z"]
    -> {:results [...], :timestamp ..., :checksum ...}
  
  ; Same temporal query at T2 (different cache entry!)
  ["default" "SELECT * FROM sales" [] "2025-08-28T11:00:00Z"]
    -> {:results [...], :timestamp ..., :checksum ...}
}
```

## What Was Fixed

The bug was that temporal queries were using cache keys **without** timestamps:
```clojure
; WRONG - temporal queries at different times shared same cache entry
["default" "SELECT * FROM sales" []]  ; No timestamp!
```

Now fixed to include timestamps:
```clojure
; CORRECT - each temporal query has unique cache key
["default" "SELECT * FROM sales" [] "2025-08-28T10:00:00Z"]
```

## Updated Logging

The logging now clearly distinguishes entry types:
```
[CACHE-UPDATE] Storing 72 results
  Type: TEMPORAL
  Cache key: [default SELECT * FROM sales [] 2025-08-28T10:00:00Z]
  Existing cache entries:
    [TEMPORAL] [default SELECT * FROM sales [] 2025-08-28T10:00:00Z] -> 100 results
    [REGULAR]  [default SELECT * FROM sales where amount > 500 []] -> 14 results
    [TEMPORAL] [default SELECT * FROM sales [] 2025-08-28T11:00:00Z] -> 100 results
```

## Key Takeaways

✅ Both temporal and non-temporal queries belong in the cache
✅ They have different cache key structures (3 vs 4 elements)
✅ Temporal queries include timestamps in their keys
✅ The cache enables efficient incremental updates for both types