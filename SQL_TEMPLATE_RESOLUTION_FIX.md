# SQL Template Resolution Fix

## Problem Statement
SQL templates (e.g., `{{blockId.sql}}`) were not being consistently resolved across all execution paths, causing:
1. **Failed queries** - Templates reaching the database unresolved
2. **Cache misses** - Cache keys containing template placeholders instead of resolved SQL
3. **Incorrect subscription IDs** - Subscriptions based on unresolved SQL

## Root Causes

### 1. Inconsistent Resolution Points
- **Kafka reactive**: Templates resolved at re-execution time (good)
- **Reactive server**: Templates sometimes stored unresolved
- **Temporal cache**: Using unresolved SQL as cache keys
- **Subscription IDs**: Generated from unresolved SQL

### 2. Cache Key Issues
Template SQL like:
```sql
SELECT COUNT(*) FROM ({{block-123.sql}}) AS subq
```
Was being used directly as cache key instead of resolved SQL:
```sql
SELECT COUNT(*) FROM ((SELECT * FROM sales)) AS subq
```

## Solution Implemented

### 1. Centralized SQL Resolver (`sql_resolver.clj`)
Created a single source of truth for SQL resolution with consistent APIs:

#### Core Functions:
- `resolve-sql` - Main resolution entry point
- `resolve-for-cache-key` - Always returns resolved SQL for caching
- `resolve-for-execution` - Ensures templates resolved before execution
- `generate-subscription-id` - Creates consistent IDs from resolved SQL
- `generate-cache-key` - Normalized cache keys from resolved SQL

#### Key Features:
- **Consistent resolution** - Single code path for all template resolution
- **Dependency tracking** - Returns list of block dependencies
- **Error handling** - Falls back to original SQL on errors
- **Validation** - Can assert no unresolved templates remain

### 2. Updated Kafka Reactive (`kafka_reactive.clj`)
- Now uses `resolver/resolve-sql` for all template resolution
- Simplified complex template resolution logic
- Ensures cache keys use resolved SQL

### 3. Updated Reactive Server (`reactive_server.clj`)
- Uses centralized resolver for template resolution
- Generates subscription IDs from resolved SQL
- Stores original SQL for re-resolution but uses resolved for cache keys

## Critical Design Decision

### Storage vs Cache Keys
- **Store**: Original SQL with templates (for re-resolution with fresh parent SQL)
- **Cache Key**: Always use resolved SQL (for consistent caching)
- **Subscription ID**: Based on resolved SQL (for deduplication)

This ensures:
1. Templates can be re-resolved when parent blocks change
2. Cache keys are consistent across sessions
3. Duplicate subscriptions are properly detected

## Usage Examples

### Before (Inconsistent)
```clojure
;; Different code paths doing resolution differently
(if (re-find #"\{\{[^}]+\.sql\}\}" query)
  (sql-template/resolve-sql-templates query session-state)
  query)

;; Cache key using unresolved SQL
(hash query)  ; Could contain {{blockId.sql}}
```

### After (Consistent)
```clojure
;; All paths use centralized resolver
(let [result (resolver/resolve-sql query session-id)]
  (:resolved-sql result))

;; Cache key always uses resolved SQL
(resolver/generate-cache-key query session-id)
```

## Testing Checklist

### 1. Template Resolution
- [ ] Simple template: `{{block1.sql}}`
- [ ] Nested templates: `{{block1.sql}} containing {{block2.sql}}`
- [ ] Multiple templates: `{{block1.sql}} UNION {{block2.sql}}`
- [ ] Temporal with templates: `{{block1.sql}} FOR SYSTEM_TIME AS OF...`

### 2. Cache Consistency
- [ ] Same resolved SQL gets same cache key
- [ ] Template changes invalidate cache
- [ ] Temporal queries cache correctly

### 3. Subscription Management
- [ ] Duplicate detection works with templates
- [ ] Re-execution resolves fresh parent SQL
- [ ] Dependencies tracked correctly

## Performance Impact
- **Positive**: Better cache hit rates due to consistent keys
- **Positive**: Fewer failed queries from unresolved templates
- **Neutral**: Small overhead from centralized resolution
- **Positive**: Reduced debugging time from consistent behavior

## Migration Notes
No migration needed - changes are backward compatible. Existing cached entries with template placeholders will miss cache but queries will still work.

## Future Improvements

### 1. Compile-Time Validation
Add checks to prevent templates from reaching:
- Database execution
- Cache storage
- Subscription storage

### 2. Template Cycle Detection
Detect and prevent circular template references at resolution time.

### 3. Template Caching
Cache resolved templates with parent block version tracking.

### 4. Performance Monitoring
Track template resolution times and cache hit rates.