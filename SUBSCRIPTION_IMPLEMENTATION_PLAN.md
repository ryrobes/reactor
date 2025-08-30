# Subscription System Implementation Plan

## Build Order (Bottom-Up Approach)

We'll build from the bottom up, starting with pure functions that can be thoroughly tested, then layering on the coordination logic.

## Phase 1: Pure Function Modules (Week 1)

### 1.1 `reactor.subscriptions.differ` ✅ Testable
**Purpose**: Calculate differences between result sets  
**Dependencies**: None (pure functions)  
**Functions**:
```clojure
(defn calculate-row-diff [old new id-key])
(defn calculate-field-diff [old new id-key])
(defn calculate-structural-diff [old new])
(defn compression-ratio [diff original])
(defn should-send-diff? [diff options])
```
**Tests**: 15+ test cases covering all diff types

### 1.2 `reactor.subscriptions.id-generator` ✅ Testable
**Purpose**: Consistent ID generation (ONE strategy!)  
**Dependencies**: None  
**Functions**:
```clojure
(defn generate-subscription-id [context])
(defn parse-subscription-id [id])
(defn subscription-type [id])
```
**Tests**: 10+ test cases

### 1.3 `reactor.subscriptions.query-analyzer` ✅ Testable
**Purpose**: Extract metadata from SQL  
**Dependencies**: sql-parser  
**Functions**:
```clojure
(defn extract-tables [sql])
(defn extract-parameters [sql])
(defn identify-query-type [sql])
(defn has-templates? [sql])
(defn extract-template-refs [sql])
```
**Tests**: 20+ test cases with various SQL patterns

## Phase 2: State Management (Week 1-2)

### 2.1 `reactor.subscriptions.store`
**Purpose**: Single source of truth for subscriptions  
**Dependencies**: None  
**State**:
```clojure
(defonce store (atom {}))
(defonce indices (atom {}))
```
**Functions**:
```clojure
(defn add! [subscription])
(defn update! [id updates])
(defn delete! [id])
(defn get-subscription [id])
(defn find-by-table [table])
(defn find-by-session [session-id])
(defn rebuild-indices! [])
```
**Tests**: State management tests, concurrent access tests

### 2.2 `reactor.subscriptions.cache`
**Purpose**: Cache query results  
**Dependencies**: None  
**Functions**:
```clojure
(defn cache-result! [subscription-id results])
(defn get-cached [subscription-id])
(defn invalidate! [subscription-id])
(defn cleanup-old! [max-age])
```
**Tests**: Cache expiry, memory management

### 2.3 `reactor.sse.channel-manager`
**Purpose**: Manage SSE channels (separate from subscriptions!)  
**Dependencies**: http-kit  
**Functions**:
```clojure
(defn register-channel! [session-id channel])
(defn unregister-channel! [session-id channel])
(defn get-channels [session-id])
(defn cleanup-dead-channels! [])
(defn broadcast-to-session [session-id message])
```
**Tests**: Channel lifecycle, cleanup

## Phase 3: Business Logic (Week 2)

### 3.1 `reactor.subscriptions.core`
**Purpose**: Subscription lifecycle management  
**Dependencies**: store, id-generator, query-analyzer  
**Functions**:
```clojure
(defn create-subscription [request])
(defn update-subscription [id updates])
(defn delete-subscription [id])
(defn pause-subscription [id])
(defn resume-subscription [id])
```
**Tests**: Full lifecycle tests

### 3.2 `reactor.subscriptions.executor`
**Purpose**: Execute subscription queries  
**Dependencies**: xtdb-store, cache  
**Functions**:
```clojure
(defn execute [subscription])
(defn execute-with-cache [subscription])
(defn execute-batch [subscriptions])
```
**Tests**: Execution with various SQL types

### 3.3 `reactor.reactive.debouncer`
**Purpose**: Debouncing strategies  
**Dependencies**: core.async  
**Functions**:
```clojure
(defn create-debouncer [strategy options])
(defn debounce! [debouncer key fn])
(defn flush! [debouncer])
(defn shutdown! [debouncer])
```
**Strategies**: fixed-delay, sliding-window, adaptive
**Tests**: Timing tests, concurrent requests

## Phase 4: Coordination Layer (Week 2-3)

### 4.1 `reactor.reactive.coordinator`
**Purpose**: Orchestrate reactions  
**Dependencies**: All above modules  
**Functions**:
```clojure
(defn handle-table-change [table])
(defn handle-subscription-change [subscription-id])
(defn execute-and-notify [subscription])
(defn batch-execute-and-notify [subscriptions])
```

### 4.2 `reactor.reactive.kafka-monitor`
**Purpose**: Monitor Kafka for changes (simplified!)  
**Dependencies**: Kafka client, coordinator  
**Functions**:
```clojure
(defn start-monitor! [config])
(defn stop-monitor! [])
(defn process-transaction [tx])
```

## Phase 5: API Layer (Week 3)

### 5.1 `reactor.api.subscription-handler`
**Purpose**: HTTP endpoints for subscriptions  
**Dependencies**: All business logic  
**Endpoints**:
```clojure
(defn handle-create-subscription [req])
(defn handle-delete-subscription [req])
(defn handle-list-subscriptions [req])
(defn handle-sse-connect [req])
```

### 5.2 `reactor.api.sql-handler`
**Purpose**: SQL execution endpoints  
**Dependencies**: pipeline, subscriptions  
**Endpoints**:
```clojure
(defn handle-sql-query [req])
(defn handle-sql-mutation [req])
```

## Migration Strategy

### Step 1: Parallel Running (Week 4)
```clojure
;; In reactive_server.clj
(defn handle-subscription [req]
  (if @use-new-subscription-system?
    (new-handler/handle req)
    (old-handler/handle req)))
```

### Step 2: Gradual Migration
- Start with read-only queries
- Then temporal queries  
- Then mutations
- Finally cascades

### Step 3: Verification
- Compare results between old/new
- Monitor performance
- Check CLJS client compatibility

## Test Coverage Goals

| Module | Target Coverage | Test Types |
|--------|----------------|------------|
| differ | 100% | Unit (pure functions) |
| id-generator | 100% | Unit |
| query-analyzer | 95% | Unit |
| store | 90% | Unit + Concurrent |
| cache | 90% | Unit + Performance |
| executor | 85% | Integration |
| coordinator | 80% | Integration |
| handlers | 75% | Integration |

## Success Metrics

1. **Complexity Reduction**
   - From 1300+ line file to <300 line modules
   - From 11 atoms to 3 atoms
   
2. **Performance**
   - Subscription creation: <10ms
   - Re-execution trigger: <5ms
   - Diff calculation: <20ms for 1000 rows
   
3. **Reliability**
   - No orphaned subscriptions
   - No memory leaks
   - Clean shutdown
   
4. **Maintainability**
   - Each module independently testable
   - Clear interfaces
   - Comprehensive documentation

## First Module to Build

Let's start with **`reactor.subscriptions.differ`** because:
1. It's pure functions (easiest to test)
2. It's currently the most complex embedded logic (300+ lines)
3. It has no dependencies
4. We can verify it works correctly before moving on

Ready to start implementation?