# Temporal Query Cache Implementation

## Overview
Implemented browser-side local storage caching for temporal row count queries in the Rabbit Demo app. This reduces API server load by caching immutable temporal query results in the browser.

## Key Components

### 1. `temporal_cache_utils.cljs`
- Manages local storage cache with versioning and TTL
- Cache keys: `reactor_temporal:v1:{hash(sql+timestamp)}`
- Automatic expiration (30 days TTL)
- Size management (max 5000 entries)
- Cache statistics and monitoring

### 2. `row_count_viz.cljs` (Modified)
- Integrated local storage caching into query flow
- Three-tier cache hierarchy:
  1. Browser local storage (persistent)
  2. Memory cache (session-based)
  3. Server cache (already existed)

### 3. `cache_debug_panel.cljs`
- Visual debug panel for monitoring cache performance
- Shows hit rates, cache size, entry counts
- Manual cache clearing capability
- Toggle visibility with button in bottom-right corner

## Query Flow
1. Check local storage cache
2. If miss, check memory cache
3. If miss, execute query against server
4. Cache result in both memory and local storage
5. Server may also cache (existing `temporal_cache.clj`)

## Benefits
- **Reduced Server Load**: Temporal queries are cached permanently in browser
- **Faster UI**: No network round-trip for cached queries
- **Cross-Session Persistence**: Cache survives browser refreshes
- **Automatic Management**: Old entries cleaned up automatically

## Cache Key Design
```clojure
(defn build-cache-key [sql timestamp]
  (let [normalized-sql (-> sql 
                          (str/replace #"\s+" " ")
                          (str/trim)
                          (str/lower-case))
        key-parts [cache-version normalized-sql timestamp]]
    (str cache-prefix (hash key-parts))))
```

## Usage
The caching is automatic and transparent. When the Rabbit Demo app renders temporal row count visualizations:
1. Queries are automatically cached
2. Cache debug panel shows statistics (toggle button in bottom-right)
3. Cache persists across sessions
4. Old entries automatically expire after 30 days

## Testing
1. Start the Rabbit Demo: `shadow-cljs watch rabbit`
2. Run queries with time travel
3. Open cache debug panel (bottom-right button)
4. Refresh browser - cached queries load instantly
5. Check browser DevTools > Application > Local Storage

## Configuration
- `max-cache-entries`: 5000 (configurable in temporal_cache_utils.cljs)
- `cache-ttl-days`: 30 (configurable in temporal_cache_utils.cljs)
- `cache-version`: "v1" (bump to invalidate all caches)