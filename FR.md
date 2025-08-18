# Functional Requirements for Reactor: A Clojure Reactive State Management Library

## Overview

Reactor is a Clojure library designed to provide reactive state management with subscriptions, cascading rules, and efficient data handling. It enables server-side re-execution of callbacks on state changes and client-side data pushes via Server-Sent Events (SSE). The system is inspired by frontend libraries like Reagent and Re-frame but adapted for backend Clojure, with a lightweight rule system that avoids the complexity of full rule engines (e.g., no Rete algorithms like in Clara or O'Doyle Rules). Everything is built around Clojure idioms: atoms for state, pure functions for computations, and immutable data structures.

The library abstracts complexities such as deep key efficiency, scaling for large data, persistence, and cross-boundary (server-client) symmetry behind an atom-like interface. Users interact with "ratoms" (reactive atoms) using standard operations like `@ratom`, `swap!`, and `reset!`, without needing to manage cursors, caches, or SSE plumbing directly.

Key principles:
- **Clojure-idiomatic**: Relies on atoms, functions, and macros for declarative sugar. No core.async (too complex); use standard concurrency primitives like threads or agents for any async needs.
- **Reactivity Model**: Changes trigger only relevant subscriptions and rules via dynamic dependency tracking (similar to Reagent reactions).
- **Symmetry**: Server and clients use similar APIs; updates and subscriptions work seamlessly across boundaries.
- **Minimal Dependencies**: Core Clojure; optional http-kit for SSE, core.cache for scaling.
- **Extensibility**: Users can compose with other libs (e.g., Datomic for backing store, Specter for advanced paths).
- **Scope**: Backend-focused, with optional ClojureScript companion for clients. No full Firebase replacement, but enables user-space replication of similar features via raw functions.

## Core Features

### 1. Reactive State Management
- **Central Store**: A ratom (reactive atom) holds the entire application state as an immutable map or similar structure.
  - Creation: `(r/ratom initial-state config-map)`, where `config-map` includes options like `:persist-file`, `:cache-size`, `:offload-dir`, `:sse-path`.
  - Behavior: Behaves like a standard Clojure atom for deref (`@ratom`), `swap!`, and `reset!`, but with built-in reactivity hooks.
  - Nested Structures: Supports deep maps/vectors; updates are thread-safe and synchronous.
  - Heavy Data Handling: Automatically stores metadata (e.g., IDs, hashes, timestamps) in the ratom for large values (e.g., query results); full data lives in a cache or external storage.
- **Updates**: Standard `swap! ratom fn args` or `reset! ratom new-state`.
  - Triggers: Post-update, notify only affected subscriptions and rules.
  - Client Updates: Via POST to `/update` endpoint (middleware provided); e.g., EDN body with `:path`, `:op` (e.g., `:assoc-in`), `:value`.
  - Symmetry: Server swaps push to clients; client POSTs trigger server reactions.

### 2. Subscriptions
- **Purpose**: Allow watching paths or derived values, firing callbacks only on relevant changes.
- **Server-Side Subscriptions**:
  - API: `(r/subscribe! ratom path-or-fn callback opts)`, where:
    - `path-or-fn`: Vector for deep keys (e.g., `[:users :alice :status]`) or a fn for derivations (e.g., `(fn [] (count (get-in @ratom [:users])))`).
    - `callback`: Pure fn taking `old-val` and `new-val`.
    - `opts`: Map with `:key` for unsubscribe, `:once` for single-fire.
  - Behavior: Uses internal "cursors" (lightweight path views) for efficiency; derivations auto-track dependencies via dynamic context.
  - Unsubscribe: `(r/unsubscribe! ratom key)`.
- **Client-Side Subscriptions via SSE**:
  - API: Browser uses `new EventSource("/subscribe?path=users.alice.status&format=edn")`; pushes new-val or diffs on change.
  - Server Handling: Auto-mounted handler at configured path; supports multiple clients per path, auth via tokens.
  - Formats: EDN (default), JSON; optional diffs for bandwidth.
  - Symmetry: Clients receive pushes from server updates; no client-side state ownership.
- **Common Behaviors**:
  - Lazy: Compute/resolve data only on subscription.
  - Queries: Support derived subs for "queries" (e.g., filter active users).
  - Efficiency: No whole-map triggers; use path-trie for pruning notifications.

### 3. Rule System for Cascades
- **Purpose**: Lightweight "when-then" system for reactive side-effects and cascades, as conditional subscriptions.
- **Definition**: `(r/def-rule ratom rule-key path-or-fn cond-fn action-fn)`, where:
  - `rule-key`: Unique ID for enable/disable.
  - `path-or-fn`: Subscription path or derivation (params like `id` can be bound).
  - `cond-fn`: Predicate on new-val (e.g., `(fn [val] (= val :active))`); optional, defaults to always true.
  - `action-fn`: Side-effecting fn (e.g., `swap! ratom ...`, external calls); can trigger cascades.
- **Behavior**: Fires post-subscription trigger if cond holds; idempotent by default.
  - Cascading: Natural via store updates (e.g., rule A updates path X, triggering sub B and rule C).
  - Management: `(r/enable-rule! ratom key)`, `(r/disable-rule! ratom key)`.
  - Grouping: Optional "sessions" for scoped rules.
- **Simplicity**: No full engine; just enhanced watches. Debug logging for firings/traces.
- **Time-Based Rules**: Via subscribable time ratom (see below).

### 4. Time Atom for Cron-Like Scheduling
- **Purpose**: Treat time as reactive data for periodic triggers.
- **Creation**: `(r/time-ratom {:interval :minute})` (options: `:second`, `:minute`, `:hour`; configurable resolutions).
- **Structure**: Ratom like `{:now timestamp :second N :minute M :hour H}`; updates only change relevant keys.
- **Updater**: Background scheduler using `ScheduledExecutorService` or `Timer` (Java interop); e.g., fixed-rate task swaps on interval.
- **Subscriptions/Rules**: Use like any path, e.g., subscribe to `[:minute]` for every-minute callbacks; rules for "cron" jobs (e.g., hourly data refresh).
- **Efficiency**: Check for actual changes before swap; support virtual keys like `:every-5-min`.

## Scaling and Efficiency

### 1. Deep Key Handling
- **Cursors**: Internal surrogate views for paths; e.g., `(r/cursor ratom [:users :active])` derefs to sub-value, watches trigger only on sub-tree changes.
- **Dependency Tracking**: Dynamic vars during reaction eval record derefed paths; minimize recomputes.
- **Avoid Waste**: Cache last values; batch notifications; use hashes/equality checks per path.
- **Scalability**: Path-trie for 1000+ subs; handle large states (1MB+) without perf hits.

### 2. Memory Management for Non-Trivial Data
- **Assumption**: Store fits in memory by default, but supports offloading for large payloads (e.g., SQL results with 1000s of rows).
- **Hybrid Approach**: Root ratom holds metadata (IDs, hashes, timestamps); full data in evictable cache (`core.cache` with LRU/TTL/soft refs).
- **Offloading**: On eviction, serialize to disk/DB (EDN/JSON files or SQLite/Datomic); load on demand during deref/sub.
- **Monitoring**: Background watcher for JVM heap pressure; trigger evictions at thresholds (e.g., 80% usage).
- **Optimizations**: Use transients for building large structures; lazy processing via transducers/eduction; shard ratoms by domain if needed.
- **Trade-offs**: Slight latency on cache misses; metadata changes stay instant.

### 3. Persistence and Rehydration
- **Persist**: `(r/persist! ratom file-path)` serializes `@ratom` to EDN (human-readable, supports all Clojure types); incremental for large data.
- **Rehydrate**: `(r/rehydrate! ratom file-path)` on boot; atomic (temp file then rename).
- **Options**: Configurable; handle non-serializables (exclude or custom tags); compressed (gzip).
- **Alternatives**: Back with Datomic/RocksDB for queryable persistence; load subsets lazily.

## Integration and Extensibility

- **SSE Setup**: Auto-mounts Ring handler for `/subscribe`; uses http-kit for async connections (long-polling fallback).
- **Client Lib**: Optional ClojureScript wrapper for `EventSource` integration, treating remote as local ratom.
- **Security**: Optional auth for subs/updates (e.g., tokens validated in middleware).
- **Error Handling**: Graceful retries, logging; no crashes on bad paths; cycle detection in cascades.
- **Full-Stack**: Server owns state; clients subscribe/update without complexity.
- **Macros**: Sugar like `defsub`, `defrule` for declarations.
- **Firebase-Like Features**: Replicate in user-space:
  - Realtime sync: Via SSE subs.
  - Aggregations: Raw fns in derivations/rules (e.g., transducers over data).
  - Triggers: Rules as flexible Cloud Functions equivalents.
  - Queries: Derived subs with arbitrary logic, avoiding NoSQL quirks.

## Visualization and Debugging

- **Dashboard UI**: Simple web UI (Reagent-based, mounted at `/debug`) showing:
  - Store state (interactive tree view).
  - Active subs (paths, callbacks, last values, trigger counts).
  - Reactions/rules (deps, firings, cascades timeline).
  - Time atom status.
- **Implementation**: Subscribes to meta-ratom logging events; uses Portal for ad-hoc inspectors, Vega-Lite/Hanami for graphs (e.g., dependency trees).
- **Tools Integration**: Compatible with FlowStorm for execution traces, CIDER/Calva for REPL browsing.
- **Mode**: Optional, enabled in dev; no prod overhead.

## Non-Functional Requirements

- **Performance**: Handle 1000+ subs efficiently; low-latency triggers.
- **Testing**: Unit tests for ratom ops, rules; integration for SSE.
- **Documentation**: Examples for setup, subs, rules; comparisons to Re-frame/Firebase.
- **Versioning**: Start minimal (core ratom/subs), iterate on rules/scaling.
