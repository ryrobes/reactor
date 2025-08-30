# Reactor Subscription System - Deep Analysis & Refactoring Plan

## Current Architecture Problems

After deep analysis, the subscription/reactive system is scattered across multiple files with tangled responsibilities:

### 1. **State Management Chaos**
The system uses **11+ global atoms** scattered throughout `kafka_reactive.clj`:
```clojure
active-subscriptions     ; subscription-id -> subscription info
table-to-subs           ; table -> #{subscription-ids}  
session-subscriptions   ; session -> #{keypaths}
client-has-base-data   ; #{[session sub-id client-id]}
sse-channels           ; session -> #{channels}
subscription-dependencies ; parent-block -> #{dependent-subs}
pending-re-executions  ; sub-id -> timestamp
client-result-cache    ; [session sub-id] -> cached results
metrics-cache          ; query cache keys -> metrics
cascade-chain          ; tracking cascades
debounce-executor      ; background thread
```

**Problem**: Each atom is managed independently with no coordination or transactional boundaries.

### 2. **Lifecycle Flow (Current)**

```
CLIENT                                 SERVER
  |                                      |
  ├─[1]─POST /api/subscribe-sql ────────>├─ Parse request
  |     {sql, params}                    ├─ Generate sub-id (3 different strategies!)
  |                                      ├─ Resolve templates (DUPLICATE #1)
  |                                      ├─ Extract tables
  |                                      ├─ Update 6+ atoms independently
  |                                      └─ Return sub-id
  |                                      
  ├─[2]─GET /api/subscribe-sql ─────────>├─ Establish SSE channel
  |     (SSE connection)                 ├─ Clean up old subs (orphan detection)
  |                                      ├─ Register channel in atom
  |                                      └─ Send :connected message
  |
  |                              KAFKA CONSUMER THREAD
  |                                      ├─ Poll Kafka
  |                                      ├─ Parse transaction (regex on raw bytes!)
  |                                      ├─ Find affected tables
  |                                      ├─ Find affected subscriptions
  |                                      └─ Queue for debounced re-execution
  |
  |                              DEBOUNCE EXECUTOR THREAD  
  |                                      ├─ Wait for debounce delay
  |                                      ├─ re-execute-subscription
  |                                      │  ├─ Resolve templates (DUPLICATE #2!)
  |                                      │  ├─ Execute query
  |                                      │  ├─ Calculate diff (300+ lines!)
  |                                      │  └─ Cache results
  |                                      └─ push-to-session
  |                                         └─ Send via SSE
  |
  ├<────── SSE: data ────────────────────┘
  |        {type, subscription-id, diff/results}
```

### 3. **Major Issues Identified**

#### **Duplicate Template Resolution**
- Happens in `/api/sql` handler (reactive_server.clj)
- Again in `re-execute-subscription` (kafka_reactive.clj lines 614-703)
- Different implementations with different bugs!

#### **SSE Management Mixed with Subscriptions**
- SSE channels tracked in subscription module
- Channel lifecycle tangled with subscription lifecycle
- Orphan detection logic scattered

#### **Diff Calculation Embedded**
- 300+ lines of diff logic inside re-execution (lines 313-540)
- Can't be tested independently
- Multiple diff strategies mixed together

#### **Debouncing Hardcoded**
- Background thread management mixed with subscription logic
- Can't swap strategies
- Hard to test

#### **No Clear Boundaries**
- kafka_reactive.clj is 1300+ lines doing EVERYTHING:
  - Subscription management
  - SSE channel management
  - Query execution
  - Template resolution
  - Diff calculation
  - Result caching
  - Debouncing
  - Kafka consumption
  - Metric tracking

## Proposed Clean Architecture

### Core Principles
1. **Single Responsibility** - Each module does ONE thing
2. **Testable** - Pure functions wherever possible
3. **Clear Boundaries** - Well-defined interfaces
4. **State Isolation** - Minimize shared mutable state

### Module Structure

```
reactor.subscriptions/
├── core.clj           - Subscription lifecycle (create, update, delete)
├── store.clj          - Subscription storage (single source of truth)
├── executor.clj       - Query execution (no other concerns)
├── differ.clj         - Diff calculation (pure functions)
├── cache.clj          - Result caching layer
├── session.clj        - Session management

reactor.reactive/
├── coordinator.clj    - Orchestrates reactions to changes
├── debouncer.clj      - Debouncing strategies (pluggable)
├── monitor.clj        - Kafka monitoring (just detection)

reactor.sse/
├── manager.clj        - SSE channel lifecycle
├── broadcaster.clj    - Message broadcasting

reactor.api/
├── sql.clj           - SQL endpoints (thin layer)
├── subscription.clj   - Subscription endpoints (thin layer)
```

### Clean Data Flow

```
CLIENT                          API LAYER              BUSINESS LOGIC
  |                                |                         |
  ├─POST /subscribe───────────────>├─ Parse request          |
  |                                ├─ Validate              |
  |                                └─────────────────────────>├─ subscription/create
  |                                                          ├─ Generate ID (one way!)
  |                                                          ├─ Store in single atom
  |                                                          └─ Return subscription
  |
  ├─GET /subscribe (SSE)──────────>├─ Establish SSE          |
  |                                └─────────────────────────>├─ sse/register-channel
  |                                                          
  |                            KAFKA MONITOR                 REACTIVE COORDINATOR
  |                                ├─ Detect change          |
  |                                └──────────────────────────>├─ Find affected subs
  |                                                            ├─ Apply debounce strategy
  |                                                            └─ Schedule execution
  |                                                          
  |                                                          EXECUTOR
  |                                                            ├─ Get subscription
  |                                                            ├─ Execute query
  |                                                            ├─ Cache results
  |                                                            └─ Return results
  |                                                          
  |                                                          DIFFER
  |                                                            ├─ Compare with cache
  |                                                            ├─ Calculate diff
  |                                                            └─ Return diff/full
  |                                                          
  |                                                          SSE BROADCASTER
  ├<───────SSE: results─────────────────────────────────────────├─ Format message
  |                                                              └─ Send to channels
```

### Subscription State (Single Source of Truth)

```clojure
;; reactor.subscriptions.store
(defonce subscriptions 
  (atom {}))  ; Just ONE atom!

;; Structure:
{subscription-id 
 {:id              subscription-id
  :sql             "SELECT ..."
  :resolved-sql    "SELECT ..." ; Templates already resolved
  :params          [...]
  :tables          ["users" "orders"]
  :session-id      "session-123"
  :client-id       "client-456"
  :created-at      timestamp
  :last-executed   timestamp
  :last-result     {:results [...] :checksum "..."}
  :status          :active/:paused/:error
  :metadata        {...}}}

;; Secondary indices (derived, rebuilt if needed)
(defonce indices
  (atom {:by-table {}      ; table -> #{sub-ids}
         :by-session {}    ; session -> #{sub-ids}
         :by-parent {}}))  ; parent-block -> #{sub-ids}
```

### Key Improvements

#### 1. **Subscription Lifecycle** (reactor.subscriptions.core)
```clojure
(defn create-subscription
  "Create a new subscription - pure function returns subscription map"
  [{:keys [sql params session-id client-id block-id]}]
  {:id (generate-id {:sql sql :session-id session-id :block-id block-id})
   :sql sql
   :resolved-sql (resolve-templates sql session-id) ; Do ONCE
   :params params
   :tables (extract-tables resolved-sql)
   :session-id session-id
   :client-id client-id
   :created-at (System/currentTimeMillis)
   :status :active})

(defn store-subscription!
  "Store subscription and update indices - single transaction"
  [subscription]
  (swap! subscriptions assoc (:id subscription) subscription)
  (update-indices! subscription)
  subscription)
```

#### 2. **Query Execution** (reactor.subscriptions.executor)
```clojure
(defn execute-subscription
  "Execute subscription query - no other concerns!"
  [subscription]
  (let [{:keys [resolved-sql params]} subscription
        result (execute-sql resolved-sql params)]
    {:subscription-id (:id subscription)
     :results (:results result)
     :executed-at (System/currentTimeMillis)}))
```

#### 3. **Diff Calculation** (reactor.subscriptions.differ)
```clojure
(defn calculate-diff
  "Pure function - testable!"
  [old-results new-results options]
  (case (:mode options)
    :none {:type :full :results new-results}
    :row (calculate-row-diff old-results new-results)
    :field (calculate-field-diff old-results new-results)
    :structural (calculate-structural-diff old-results new-results)))
```

#### 4. **SSE Management** (reactor.sse.manager)
```clojure
(defonce channels
  (atom {})) ; session-id -> #{channels}

(defn register-channel!
  "Register SSE channel - separate from subscriptions!"
  [session-id channel]
  (swap! channels update session-id (fnil conj #{}) channel))

(defn broadcast
  "Send message to session - no subscription logic!"
  [session-id message]
  (doseq [channel (get @channels session-id)]
    (send! channel message)))
```

#### 5. **Reactive Coordination** (reactor.reactive.coordinator)
```clojure
(defn handle-table-change
  "Coordinate reaction to table change"
  [table]
  (let [affected-subs (find-affected-subscriptions table)
        grouped (group-by :session-id affected-subs)]
    (doseq [[session-id subs] grouped]
      (debounce session-id 
                #(execute-and-broadcast-all subs)))))
```

## Implementation Plan

### Phase 1: Create New Modules (No Breaking Changes)
1. **Create `reactor.subscriptions.store`** - Unified storage
2. **Create `reactor.subscriptions.differ`** - Extract diff logic
3. **Create `reactor.sse.manager`** - Separate SSE handling
4. **Write comprehensive tests** for each module

### Phase 2: Adapter Layer
1. **Create adapters** that bridge old → new system
2. **Run both in parallel** to verify behavior
3. **Add feature flags** for gradual migration

### Phase 3: Migration
1. **Update endpoints** to use new modules
2. **Migrate subscription by subscription**
3. **Verify CLJS client still works**

### Phase 4: Cleanup
1. **Remove old kafka_reactive.clj code**
2. **Simplify reactive_server.clj**
3. **Archive old implementation**

## Tests to Write

### Unit Tests (Pure Functions)
```clojure
;; test/reactor/subscriptions/differ_test.clj
(deftest test-row-diff
  (testing "Detects added rows"
    (let [old [{:id 1}]
          new [{:id 1} {:id 2}]
          diff (calculate-row-diff old new)]
      (is (= [{:id 2}] (:added diff))))))

(deftest test-field-diff
  (testing "Detects field changes"
    (let [old [{:id 1 :name "Alice" :age 30}]
          new [{:id 1 :name "Alice" :age 31}]
          diff (calculate-field-diff old new)]
      (is (= [{:id 1 :changes {:age 31}}] (:updated diff))))))
```

### Integration Tests
```clojure
;; test/reactor/subscriptions/lifecycle_test.clj
(deftest test-subscription-lifecycle
  (testing "Full subscription lifecycle"
    (let [sub (create-subscription {:sql "SELECT * FROM users"})
          stored (store-subscription! sub)]
      (is (= sub stored))
      (is (get-subscription (:id sub)))
      (delete-subscription! (:id sub))
      (is (nil? (get-subscription (:id sub)))))))
```

## Benefits of This Refactoring

1. **Testability**: Each module can be tested in isolation
2. **Debuggability**: Clear flow, single responsibilities
3. **Maintainability**: 300-line modules vs 1300-line behemoth
4. **Performance**: Can optimize each piece independently
5. **Flexibility**: Swap implementations (e.g., different diff algorithms)
6. **Reliability**: Transactional state updates, no race conditions

## Backwards Compatibility

The CLJS client expects:
- POST `/api/subscribe-sql` to return `{subscription-id}`
- SSE messages in format: `data: {type, subscription-id, results/diff}`
- Same subscription lifecycle

All of this is preserved - we're just reorganizing the internals.

## Next Steps

1. Review this plan and adjust based on your feedback
2. Start with the pure function modules (differ, cache)
3. Write comprehensive tests FIRST
4. Gradually migrate functionality
5. Keep old system running until new one is proven

This refactoring will transform the subscription system from an "ad-hoc logic in squirrel holes" mess into a clean, maintainable architecture.