# Temporal Template Resolution Fix - Inner Query Placement

## Problem

When templated queries were resolved with temporal clauses, the temporal clause was not being added to the resolved template content. This resulted in queries like:

```sql
-- WRONG: No temporal clause on inner query
SELECT product, COUNT(*) AS count 
FROM ((SELECT * FROM sales)) AS source_data  
GROUP BY product ORDER BY count DESC LIMIT 20
```

Instead of:

```sql
-- CORRECT: Temporal clause on inner query
SELECT product, COUNT(*) AS count 
FROM ((SELECT * FROM sales FOR SYSTEM_TIME AS OF TIMESTAMP '2025-08-28T22:43:56.299Z')) AS source_data  
GROUP BY product ORDER BY count DESC LIMIT 20
```

## Root Cause

The template resolution process (`sql-template/resolve-templates`) was:
1. Resolving template references (e.g., `{{block.sql}}` → `SELECT * FROM sales`)
2. Wrapping resolved SQL in parentheses
3. NOT adding temporal clauses to the resolved content

Later attempts to add temporal clauses would fail because the regex-based `add-as-of-clause` function couldn't properly identify where to place the clause in complex nested structures.

## The Solution

Created a new temporal-aware template resolution module (`sql-template-temporal.clj`) that:

1. **Passes temporal context through resolution**: The temporal timestamp is passed through the entire resolution process
2. **Adds temporal to each template piece**: When a template is resolved, the temporal clause is added to that specific piece BEFORE wrapping
3. **Handles nesting correctly**: Nested templates get temporal clauses at the appropriate level

### Key Implementation

```clojure
;; In sql-template-temporal.clj
(defn resolve-templates-with-temporal
  [sql session-state resolved-blocks temporal-timestamp]
  ;; ... resolution logic ...
  (let [;; Resolve the template SQL
        resolved-sql (resolve-templates-with-temporal ...)
        ;; ADD TEMPORAL CLAUSE TO RESOLVED PIECE
        temporal-sql (if temporal-timestamp
                       (add-temporal-to-sql resolved-sql temporal-timestamp)
                       resolved-sql)
        ;; Then wrap as subquery
        wrapped-sql (str "(" temporal-sql ")")]
    ;; Replace template reference with temporal-aware SQL
    (str/replace current-sql template-ref wrapped-sql)))
```

### Updated kafka_reactive.clj

```clojure
;; Special handling for templated temporal queries
(if (and (resolver/has-templates? query) temporal-timestamp)
  ;; Use temporal-aware template resolution
  (template-temporal/resolve-sql-templates-with-deps-and-temporal
    clean-query 
    session-state 
    temporal-timestamp)
  ;; Normal resolution for other queries
  (resolver/resolve-sql query session-id))
```

## Examples

### Before Fix

```sql
-- Template: SELECT COUNT(*) FROM ({{sales-data.sql}}) as subq
-- Resolved: SELECT COUNT(*) FROM ((SELECT * FROM sales)) as subq
-- Missing temporal clause!
```

### After Fix

```sql
-- Template: SELECT COUNT(*) FROM ({{sales-data.sql}}) as subq
-- Resolved: SELECT COUNT(*) FROM ((SELECT * FROM sales FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01T00:00:00Z')) as subq
-- Temporal clause correctly placed!
```

## Impact

✅ **Temporal row counts for templated queries now work**
✅ **Complex nested templates handle temporal correctly**
✅ **Multiple templates in one query each get temporal clauses**
✅ **Proper cache key generation with resolved temporal SQL**

## Testing

Created comprehensive tests showing:
- Simple templates: ✅ Temporal inside parentheses
- Product count query: ✅ Matches expected format
- Multiple templates: ✅ Each gets temporal clause
- Nested templates: ✅ Innermost table gets clause
- Edge cases: ✅ Handled gracefully

## Performance

No performance impact - the resolution happens once per query execution and results are cached properly with the temporal timestamp in the cache key.

## Backwards Compatibility

Fully backwards compatible:
- Non-temporal queries unchanged
- Non-templated queries unchanged  
- Only affects templated queries with temporal clauses