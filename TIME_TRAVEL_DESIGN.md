# Reactor Time Travel & Undo System Design

## Vision
Built-in time travel that works across client-server boundaries, making Reactor the first reactive state library with **authoritative server-side time travel**.

## Core Concepts

### 1. History Storage Strategy
```clojure
(deftype HistoryAtom [state history future max-history session-histories]
  ;; state: current state
  ;; history: vector of past states with metadata
  ;; future: states that were "undone" (for redo)
  ;; max-history: configurable limit (default 100)
  ;; session-histories: per-user history branches
)
```

### 2. Event Sourcing Hybrid
Instead of storing full snapshots (memory intensive), we could store:
- **Snapshots** every N changes (configurable)
- **Deltas/patches** between snapshots
- **Commands** that led to each change

```clojure
{:timestamp 1234567890
 :event [:add-todo "Buy milk"]
 :patch {:todos {123 {:added true :text "Buy milk"}}}
 :snapshot? false
 :session-id "user-123"
 :checkpoint? false}  ; Named checkpoints for easy navigation
```

### 3. Session-Based Branching
Each user/session gets their own timeline branch:
```
Main Timeline: A -> B -> C -> D (authoritative)
                     \
User 1 Branch:        B' -> C' (speculative)
                         \
User 2 Branch:            B'' -> C'' (speculative)
```

### 4. Scalability Strategies

#### Memory Management
- **Ring buffer** for history (automatic old state pruning)
- **Compression** of similar states (structural sharing)
- **Lazy loading** of history chunks from storage
- **Configurable retention** policies

#### Storage Backends
```clojure
(defprotocol ITimeStore
  (save-state [this state-record])
  (load-range [this from to])
  (prune-before [this timestamp])
  (get-checkpoint [this name]))

;; Implementations:
;; - In-memory (development)
;; - Redis (production, fast)
;; - PostgreSQL (production, durable)
;; - S3/blob storage (long-term archives)
```

### 5. API Design

#### Server-Side
```clojure
;; Create time-travel enabled atom
(def app-state 
  (r/ratom {:todos {}} 
           {:history true
            :max-history 100
            :snapshot-every 10
            :storage (postgres-store conn)}))

;; Time travel operations
(r/undo! app-state)                    ; Step back
(r/redo! app-state)                    ; Step forward
(r/jump-to! app-state timestamp)       ; Jump to specific time
(r/jump-to! app-state :checkpoint-name); Jump to named checkpoint
(r/replay! app-state from to)          ; Replay events in range
(r/branch! app-state session-id)       ; Create session branch
(r/merge-branch! app-state session-id) ; Merge back to main

;; History queries
(r/history app-state)                  ; Get history metadata
(r/history-range app-state from to)    ; Get specific range
(r/checkpoint! app-state "v1.0")       ; Create named checkpoint
```

#### Client-Side (via SSE/HTTP)
```javascript
// SSE receives history updates
eventSource.onmessage = (e) => {
  const msg = JSON.parse(e.data);
  if (msg.type === 'history-update') {
    updateTimeline(msg.history);
  }
};

// HTTP endpoints
POST /api/undo          {session-id: "..."}
POST /api/redo          {session-id: "..."}
POST /api/jump-to       {timestamp: 1234567890}
GET  /api/history       -> [{timestamp, event, ...}, ...]
POST /api/checkpoint    {name: "before-refactor"}
```

### 6. UI Components

#### Time Slider
```
[|----●--------] 12:34:56 "Added todo: Buy milk"
 Past  Now  Future

[Undo] [Redo] [Checkpoint] [Branch]
```

#### History Tree Visualization
```
Main: ●──●──●──●──●
           ╰──●──● (Your branch)
              ╰──● (Alice's branch)
```

### 7. Optimizations

#### Diff Compression
```clojure
;; Instead of storing full states:
{:before {:todos {1 {...} 2 {...} 3 {...}}}
{:after  {:todos {1 {...} 2 {...} 3 {...} 4 {...}}}

;; Store deltas:
{:op :assoc-in
 :path [:todos 4]
 :value {...}}
```

#### Structural Sharing
```clojure
;; Reuse unchanged portions
(def state-1 {:a large-structure :b data})
(def state-2 (assoc state-1 :b new-data))
;; :a is shared between state-1 and state-2
```

#### Smart Snapshots
```clojure
(defn should-snapshot? [history-size last-snapshot-size current-size]
  (or (> history-size 50)                    ; Every 50 changes
      (> current-size (* 2 last-snapshot-size)) ; State doubled
      (expired? last-snapshot 5-minutes)))      ; Time based
```

### 8. Testing Strategy

```clojure
(deftest time-travel-test
  (testing "Basic undo/redo"
    (let [atom (r/ratom {:x 0} {:history true})]
      (swap! atom assoc :x 1)
      (swap! atom assoc :x 2)
      (is (= 2 (:x @atom)))
      (r/undo! atom)
      (is (= 1 (:x @atom)))
      (r/undo! atom)
      (is (= 0 (:x @atom)))
      (r/redo! atom)
      (is (= 1 (:x @atom)))))
  
  (testing "Branch and merge"
    ;; Test session-based branching
    )
  
  (testing "Memory limits"
    ;; Test ring buffer behavior
    )
  
  (testing "Persistence"
    ;; Test storage backends
    ))
```

### 9. Killer Features

#### 1. **Collaborative Undo**
Each user can undo their own actions without affecting others:
```clojure
(r/undo! app-state {:session-id "user-123"})  ; Only undoes user-123's changes
```

#### 2. **Time-Travel Debugging**
```clojure
(r/replay! app-state from to {:speed 0.5})  ; Slow-motion replay
(r/diff app-state t1 t2)                    ; See what changed
```

#### 3. **Checkpointed Deploys**
```clojure
(r/checkpoint! app-state "pre-deploy-2.0")
;; Deploy fails? 
(r/jump-to! app-state :checkpoint "pre-deploy-2.0")
```

#### 4. **Audit Trail**
Built-in compliance and debugging:
```clojure
(r/history app-state {:user "alice"})  ; Everything Alice did
(r/history app-state {:from "2024-01-01"})  ; Time-based audit
```

### 10. Comparison with Re-frame-10x

| Feature | Re-frame-10x | Reactor Time Travel |
|---------|--------------|-------------------|
| Client-side time travel | ✅ | ✅ |
| Server-side time travel | ❌ | ✅ |
| Multi-user branches | ❌ | ✅ |
| Persistent history | ❌ | ✅ |
| Collaborative undo | ❌ | ✅ |
| Production ready | ❌ Debug only | ✅ |
| Memory efficient | ❌ | ✅ (deltas + compression) |
| Named checkpoints | ❌ | ✅ |

## Implementation Plan

### Phase 1: Core History Tracking
- Modify RAtom to track history
- Implement undo/redo operations
- Add ring buffer for memory management

### Phase 2: Optimization
- Delta compression
- Structural sharing
- Smart snapshots

### Phase 3: Persistence
- Storage protocol
- Redis backend
- PostgreSQL backend

### Phase 4: Session Management
- User-specific branches
- Collaborative undo
- Branch merging

### Phase 5: UI & DevTools
- Time slider component
- History tree visualization
- Debugging tools

## Performance Targets
- Undo/redo: < 10ms
- History query: < 50ms for 1000 events
- Memory overhead: < 2x base state size
- Storage: Configurable retention (1 hour to forever)

## This Changes Everything

With built-in time travel, Reactor becomes:
1. **The only state library with server-authoritative time travel**
2. **A complete replacement for event sourcing architectures**
3. **A built-in audit system for compliance**
4. **A debugging powerhouse for production issues**
5. **A collaboration enabler with per-user undo**

This isn't just catching up to Re-frame-10x - it's leaping past it into territory no state management library has explored.

---

*"Time is an illusion. State is not."* - Reactor Philosophy v2