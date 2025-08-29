# Templated Temporal Query Fix

## Problem Description

Temporal row count queries with templates were failing because the temporal clause was being placed incorrectly in the SQL query structure.

### Example Scenario

1. **Template Query**: `SELECT COUNT(*) as cnt FROM ({{block-123.sql}}) as subq`
2. **Block SQL**: `SELECT * FROM sales WHERE amount > 100`
3. **Timestamp**: `2024-01-01T00:00:00Z`

### What Was Happening (BROKEN)

```sql
-- Step 1: Add temporal clause to template (WRONG ORDER)
SELECT COUNT(*) as cnt FROM ({{block-123.sql}}) as subq 
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01T00:00:00Z'

-- Step 2: Resolve templates (temporal clause in wrong place!)
SELECT COUNT(*) as cnt FROM ((SELECT * FROM sales WHERE amount > 100)) as subq 
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01T00:00:00Z'
```

The temporal clause ended up OUTSIDE the subquery, not on the actual table access.

### What Should Happen (FIXED)

```sql
-- Step 1: Resolve templates first
SELECT COUNT(*) as cnt FROM (SELECT * FROM sales WHERE amount > 100) as subq

-- Step 2: Add temporal clause to resolved SQL
SELECT COUNT(*) as cnt FROM (SELECT * FROM sales 
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01T00:00:00Z'
WHERE amount > 100) as subq
```

The temporal clause is correctly placed on the actual `FROM sales` table access.

## Root Cause

The issue occurred in multiple places:

1. **reactive_server.clj**: Was adding temporal clause to the original SQL WITH templates
2. **kafka_reactive.clj**: Wasn't handling templated temporal queries specially
3. **Order of operations**: Templates were resolved AFTER temporal clause was added

## The Fix

### 1. Updated `reactive_server.clj`

```clojure
;; OLD: Added temporal to original SQL with templates
(let [sql-to-store (if has-templates?
                     (add-clause-fn original-sql as-of)  ; WRONG!
                     sql-with-temporal)]

;; NEW: Store original WITHOUT temporal for templated queries
(let [sql-to-store (if has-templates?
                     original-sql  ; Temporal added during execution
                     sql-with-temporal)]
```

### 2. Updated `kafka_reactive.clj`

```clojure
;; Special handling for templated temporal queries
resolution-result (if (and has-templates? temporal-timestamp)
                    ;; Resolve templates FIRST
                    (let [clean-query (remove-temporal-clause query)
                          res (resolver/resolve-sql clean-query session-id)
                          ;; THEN add temporal clause
                          resolved-with-temporal (resolver/resolve-with-temporal 
                                                clean-query 
                                                session-id 
                                                temporal-timestamp)]
                      (assoc res :resolved-sql resolved-with-temporal))
                    ;; Normal resolution for other queries
                    (resolver/resolve-sql query session-id))
```

### 3. Enhanced `sql_resolver.clj`

```clojure
(defn resolve-with-temporal
  "Resolve templates FIRST, then add temporal clause"
  [sql session-id as-of]
  (let [{:keys [resolved-sql]} (resolve-sql sql session-id)]
    ;; Add temporal to RESOLVED SQL, not original
    (if as-of
      (add-as-of-clause resolved-sql as-of)
      resolved-sql)))
```

## Impact

### Before Fix
- Templated temporal queries would fail or return wrong results
- Temporal clause was on the outer query wrapper, not the table access
- Row counts for time travel didn't work with templates

### After Fix
- Templates are resolved first, getting the actual SQL
- Temporal clause is added to the resolved SQL
- Temporal clause correctly reaches the innermost table access
- Row counts work correctly for all temporal queries

## Testing

The fix handles multiple complex scenarios:

1. **Simple count queries**: ✅ Works
2. **Templated count queries**: ✅ Fixed
3. **Nested templates**: ✅ Works
4. **Multiple table references**: ✅ Temporal clause on all tables

## Key Insight

The order of operations is critical:
1. **Resolve templates** → Get actual SQL with table references
2. **Add temporal clause** → Clause goes on actual tables
3. **Execute query** → Correct temporal results

This ensures the temporal clause is always placed where the actual data access happens, not on query wrappers or subquery aliases.

## Logging

Added detailed logging for debugging:
- `[TEMPORAL-COUNT-TEMPLATE]` - Tracks templated temporal count queries
- `[SQL-RESOLVER]` - Shows when temporal clause is added after resolution
- Clear indication of template resolution and temporal clause placement

## Performance

No performance impact - actually improves caching because:
- Resolved SQL is used for cache keys (consistent)
- Original SQL stored for re-resolution (flexible)
- Temporal clause placement is now predictable