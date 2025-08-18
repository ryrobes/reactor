# Reactor vs Re-frame: A Comparison

## Executive Summary

**Reactor** is a unified reactive state management library that brings Re-frame's elegant event-driven architecture to the server while maintaining compatibility with frontend patterns. Think of it as **"Re-frame for the backend" meets "Firebase without the cloud"** - a hybrid that offers the best of both worlds with dramatically simpler implementation.

## Core Philosophy Comparison

### Re-frame
- **Client-only** reactive state management
- Built on React/Reagent for UI updates
- Complex interceptor chains and effect handlers
- Requires separate backend implementation
- No built-in client-server synchronization

### Reactor
- **Unified** reactive state across frontend AND backend
- Works with any UI library (or no UI at all)
- Simpler reactive atoms with built-in subscriptions
- Server-authoritative state with SSE sync
- **One codebase, two runtimes**

## Architecture Comparison

```
RE-FRAME                          REACTOR
--------                          -------
                                 
[Client Only]                    [Client + Server]
                                 
   Browser                          Browser
      |                                |
   Re-frame                       Reactor Client
      |                           (thin layer)
   App State                           |
      |                               SSE
   Reagent                             |
      |                           Reactor Server
    React                        (authoritative)
                                       |
                                    Database
```

## Feature Comparison

| Feature | Re-frame | Reactor |
|---------|----------|---------|
| **Server-side state** | ❌ Not supported | ✅ First-class citizen |
| **Client-side state** | ✅ Full support | ✅ Full support |
| **Real-time sync** | ❌ DIY WebSockets | ✅ Built-in SSE |
| **Event handling** | ✅ Interceptors + effects | ✅ Simpler event handlers |
| **Subscriptions** | ✅ Derived computations | ✅ Path + derived subs |
| **Time travel** | ✅ Via re-frame-10x | ✅ Built-in time atoms |
| **Rules engine** | ❌ Not built-in | ✅ Cascading rules |
| **Persistence** | ❌ DIY localStorage | ✅ Pluggable backends |
| **Auth/Multi-user** | ❌ Client concern | ✅ Server-managed |

## Code Complexity Comparison

### Re-frame: Setting up a todo app
```clojure
;; 1. Define db schema
(s/def ::todos (s/map-of uuid? ::todo))

;; 2. Register event handlers with interceptors
(rf/reg-event-fx
  :add-todo
  [(rf/inject-cofx :timestamp)]
  (fn [{:keys [db timestamp]} [_ text]]
    {:db (assoc-in db [:todos (random-uuid)] 
                   {:text text :done false :created timestamp})
     :persist-to-local-storage db}))

;; 3. Register subscription
(rf/reg-sub
  :todos
  (fn [db _]
    (:todos db)))

;; 4. Register derived subscription
(rf/reg-sub
  :active-todos
  :<- [:todos]
  (fn [todos _]
    (filter (comp not :done val) todos)))

;; 5. Register effects
(rf/reg-fx
  :persist-to-local-storage
  (fn [db]
    (js/localStorage.setItem "db" (pr-str db))))

;; 6. Setup interceptors
(def standard-interceptors
  [(when ^boolean goog.DEBUG debug)
   (rf/after persist-interceptor)])

;; 7. Initialize app-db
(rf/reg-event-db
  :initialize-db
  (fn [_ _]
    (or (read-local-storage) default-db)))
```

### Reactor: Same functionality
```clojure
;; 1. Create reactive atom with built-in persistence
(def app-state (r/ratom {:todos {}} 
                        {:persist true}))

;; 2. Register event handler (simpler!)
(rf/reg-event-db :add-todo
  (fn [db [text]]
    (assoc-in db [:todos (random-uuid)] 
              {:text text :done false :created (js/Date.)})))

;; 3. Subscription automatically works on paths
(r/subscribe! app-state [:todos] 
  (fn [old new] (println "Todos changed")))

;; 4. Derived subscription with dependency tracking
(r/subscribe! app-state 
  (fn [] (filter (comp not :done val) (:todos @app-state)))
  (fn [old new] (println "Active todos:" new)))

;; That's it! Persistence, subscriptions, and updates all built-in
```

## Server-Side Benefits (Reactor Only)

### 1. Server Authority
```clojure
;; Re-frame: Client manages state (security risk)
(rf/dispatch [:delete-user-data user-id])  ; Anyone can delete anything!

;; Reactor: Server validates everything
(rf/reg-event-db :delete-user-data
  (fn [db [user-id requesting-user]]
    (if (authorized? requesting-user user-id)
      (update db :users dissoc user-id)
      db)))  ; Server enforces business rules
```

### 2. Real-time Sync Without WebSockets
```clojure
;; Re-frame: Build your own sync
;; - Setup WebSocket server
;; - Handle reconnection logic  
;; - Manage connection state
;; - Deal with message ordering
;; - Handle partial updates
;; 😫 Hundreds of lines of code

;; Reactor: Just works
(def app-state (r/ratom {:messages []}))
;; Clients automatically get updates via SSE!
```

### 3. Multi-User State
```clojure
;; Re-frame: DIY everything
;; - User sessions
;; - State isolation
;; - Broadcast logic
;; - Conflict resolution

;; Reactor: Built-in patterns
(r/cursor app-state [:users user-id])  ; User-specific state
(r/subscribe! app-state [:rooms room-id :messages]  ; Room-based state
  (fn [old new] (broadcast-to-room room-id new)))
```

## Why Reactor is "Drastically Simpler"

### 1. **Fewer Concepts**
- **Re-frame**: Interceptors, coeffects, effects, event handlers, subscriptions, subscription signals, app-db
- **Reactor**: Atoms, subscriptions, events. That's it.

### 2. **Less Boilerplate**
- **Re-frame**: Register everything explicitly with careful ordering
- **Reactor**: Reactive by default, register only what needs custom logic

### 3. **Unified Mental Model**
- **Re-frame**: Think differently for client vs server
- **Reactor**: Same patterns everywhere

### 4. **Built-in Solutions**
- **Re-frame**: Integrate 5+ libraries for full-stack
- **Reactor**: Batteries included

## Migration Path

### From Re-frame to Reactor (Client)
```clojure
;; Re-frame way
(rf/reg-event-db :event handler)
(rf/reg-sub :sub handler)
(rf/dispatch [:event data])
@(rf/subscribe [:sub])

;; Reactor way (almost identical!)
(rf/reg-event-db :event handler)  ; Same!
(r/reg-sub :sub handler)          ; Same API!
((:dispatch app) [:event data])   ; Tiny difference
@(r/subscribe app [:sub])          ; Explicit app reference
```

### Adding Server-Side
```clojure
;; Just move your handlers to the server!
(ns my-app.server
  (:require [reactor.frame :as rf]))

;; Your existing Re-frame code works on the server
(def app (rf/create-frame-app initial-state))
(rf/reg-event-db :add-todo ...)  ; Same code!
```

## Performance Comparison

| Metric | Re-frame | Reactor |
|--------|----------|---------|
| **Initial setup** | 200+ LOC | ~50 LOC |
| **Memory overhead** | React + Re-frame | Just atoms |
| **Update latency** | Immediate (client) | +Network (SSE) |
| **Subscription efficiency** | O(n) dependency tracking | O(1) path-based |
| **Server CPU** | N/A | Reactive computation |
| **Network usage** | Manual/REST | Automatic/SSE |

## Use Case Comparison

### When to use Re-frame
- Pure client-side SPAs
- Offline-first applications  
- Complex client-only state
- Existing Re-frame codebases

### When to use Reactor
- Real-time collaborative apps
- Server-authoritative state
- Multi-user applications
- Simpler full-stack apps
- New projects wanting Re-frame patterns
- Games with server-side logic
- Chat/messaging systems
- Live dashboards

## The "Firebase" Comparison

Reactor provides Firebase-like real-time sync but:
- **Self-hosted** (your servers, your data)
- **No vendor lock-in** (it's just Clojure)
- **Simpler pricing** (no pay-per-read)
- **Better performance** (no cloud RTT)
- **Full control** (custom persistence, auth, etc.)

## Summary: Why We Built Reactor

Re-frame is excellent for client-side state, but modern apps need more:

1. **Server authority** for security
2. **Real-time sync** for collaboration  
3. **Simpler architecture** for faster development
4. **Unified patterns** across the stack

Reactor delivers all of this with **less code**, **fewer concepts**, and **better ergonomics** than assembling it yourself with Re-frame + backend + WebSockets + auth + persistence.

**In essence**: Reactor is what you'd build anyway after using Re-frame for a few years and wanting the same elegant patterns on your server. We just did it for you, with a drastically simpler implementation.

---

*"Write less code. Build more features. Ship faster."* - The Reactor Philosophy