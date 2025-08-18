# XTDB 2.x Migration Analysis for Reactor

## Executive Summary

**Recommendation: WAIT - Complete the sample app with XTDB 1.x first, then migrate**

## Current Status

- **XTDB 2.0.0** was released as GA (Generally Available) on June 12, 2024
- **XTDB 1.24.5** is the latest 1.x version (March 25, 2024)
- 2.x is production-ready but represents a **complete architectural redesign**

## Major Breaking Changes

### 1. Query API Changes
- **1.x**: Custom Clojure API with Datalog queries
- **2.x**: SQL-first with PostgreSQL wire protocol
  - Primary interface is now SQL via JDBC/PostgreSQL
  - XTQL (new query language) replaces Datalog
  - Clojure API now uses JDBC internally

### 2. Storage Architecture
- **1.x**: Every node has full copy of data (RocksDB/LMDB)
- **2.x**: Cloud-native columnar storage on Apache Arrow
  - Nodes only cache frequently accessed data
  - Historical data relegated to object storage
  - No simple file-based persistence option yet

### 3. Node Creation & Management
```clojure
;; XTDB 1.x
(xt/start-node {:xtdb/tx-log {...}
                :xtdb/document-store {...}})

;; XTDB 2.x
;; Runs as a PostgreSQL-compatible server
;; Connect via JDBC:
(require '[next.jdbc :as jdbc])
(def ds (jdbc/get-datasource 
         {:dbtype "postgresql"
          :dbname "xtdb"
          :host "localhost"
          :port 5432}))
```

### 4. Transaction Submission
```clojure
;; XTDB 1.x
(xt/submit-tx node [[::xt/put {:xt/id :todo-1 :text "Learn XTDB"}]])

;; XTDB 2.x - Use SQL
(jdbc/execute! ds ["INSERT INTO todos (id, text) VALUES (?, ?)" 
                   "todo-1" "Learn XTDB"])
```

### 5. Queries
```clojure
;; XTDB 1.x - Datalog
(xt/q (xt/db node)
      '{:find [?e ?text]
        :where [[?e :text ?text]]})

;; XTDB 2.x - SQL
(jdbc/execute! ds ["SELECT * FROM todos"])
```

## Migration Effort Estimate

### High Effort Areas (3-5 days)
1. **Complete rewrite of `xtdb-store.clj`** - Storage abstraction layer
2. **Rewrite `frame_xtdb.clj`** - Re-frame style API
3. **Update all transaction operations** - From custom API to SQL
4. **Query layer rewrite** - Datalog to SQL conversion
5. **Testing infrastructure** - New test fixtures and setup

### Medium Effort Areas (1-2 days)
1. **Dependencies update** - New libraries and versions
2. **Configuration changes** - New node setup patterns
3. **Time-travel implementation** - Different temporal query syntax

### Low Effort Areas (< 1 day)
1. **PostgreSQL connectivity** - Already built-in!
2. **SQL API** - No longer needed, native support

### Total Estimated Effort: 5-8 days

## Benefits of Migrating

### Immediate Benefits
✅ **Native psql support** - Works out of the box
✅ **Better tooling** - Any PostgreSQL tool works
✅ **Simpler deployment** - Standard PostgreSQL operations
✅ **Better performance** - Columnar storage, query optimization

### Drawbacks
❌ **Complete API rewrite** - No backward compatibility
❌ **Loss of Clojure-native feel** - SQL-centric approach
❌ **More complex storage** - Requires object storage for production
❌ **Learning curve** - New concepts and patterns

## Migration Timing Analysis

### Option 1: Migrate NOW (Not Recommended)
**Pros:**
- Get native psql support immediately
- Build on latest technology
- Avoid technical debt

**Cons:**
- 5-8 days of migration work
- Risk of instability (GA but still new)
- Delays sample app development
- Need to relearn APIs

### Option 2: Complete Sample App First, Then Migrate (RECOMMENDED)
**Pros:**
- Faster time to working demo (1-2 days vs 6-10 days)
- Learn current patterns thoroughly
- Have working baseline for comparison
- Can showcase migration as separate effort

**Cons:**
- Temporary technical debt
- psql connectivity remains limited
- Need to migrate eventually

## Recommended Action Plan

### Phase 1: Complete with XTDB 1.x (1-2 days)
1. Finish CLJS client with XTDB backend
2. Complete SSE reactive subscriptions
3. Create full TODO app demo
4. Document current architecture

### Phase 2: Create Migration Branch (5-8 days)
1. Fork project for 2.x migration
2. Update dependencies
3. Rewrite storage layer for SQL
4. Update all queries to SQL
5. Test thoroughly
6. Document migration process

### Phase 3: Compare & Document
1. Run both versions side-by-side
2. Performance comparison
3. Feature parity check
4. Create migration guide

## Code Impact Analysis

### Files Requiring Major Changes
```
src/reactor/xtdb_store.clj          - Complete rewrite
src/reactor/frame_xtdb.clj          - Complete rewrite  
src/reactor/xtdb_query.clj          - Complete rewrite
src/reactor/sql_api.clj             - Can be removed
src/reactor/pgwire.clj              - Can be removed
src/reactor/pg_server.clj           - Can be removed
test/reactor/xtdb_store_test.clj    - Complete rewrite
test/reactor/frame_xtdb_test.clj    - Complete rewrite
```

### Files with Minor Changes
```
project.clj                          - Dependency updates
src/examples/todo_app/server_xtdb.clj - Query updates
```

## Conclusion

While XTDB 2.x offers significant improvements, especially native PostgreSQL support, the migration represents a **complete rewrite** of the storage layer. 

**Recommendation**: Complete the sample app with XTDB 1.x first to:
1. Deliver working functionality quickly
2. Establish a baseline for comparison
3. Learn the domain thoroughly
4. Then tackle migration as a separate, well-understood effort

The HTTP SQL API we built provides adequate SQL access for now, making the immediate migration less critical.