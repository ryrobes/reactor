# Reactor Sample Applications

This directory contains example applications demonstrating the full power of Reactor - a unified reactive state management library for Clojure/ClojureScript.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Browser (ClojureScript)               │
├─────────────────────────────────────────────────────────┤
│  reactor.client                                          │
│  - Remote RAtom (mirrors server state)                   │
│  - SSE abstraction for real-time updates                 │
│  - Re-frame style subscriptions & events                 │
│  - Automatic reconnection & error handling               │
└────────────────┬───────────────────────────────────────┘
                 │ SSE + HTTP
┌────────────────▼───────────────────────────────────────┐
│                    Server (Clojure)                      │
├─────────────────────────────────────────────────────────┤
│  reactor.frame (Server-side Re-frame)                    │
│  - Event handlers with interceptors                      │
│  - Subscription management                               │
│  - Effect handlers                                       │
├─────────────────────────────────────────────────────────┤
│  reactor.core                                            │
│  - Reactive atoms with watches                           │
│  - Cursors for efficient deep access                     │
│  - Rule engine for cascading updates                     │
│  - Time atoms for scheduled events                       │
├─────────────────────────────────────────────────────────┤
│  reactor.sse                                             │
│  - Server-Sent Events for real-time push                 │
│  - Multi-client subscription management                  │
│  - Format negotiation (EDN/JSON)                         │
└─────────────────────────────────────────────────────────┘
```

## Sample Applications

### 1. Todo App
A full-featured todo application with real-time synchronization:
- **Server**: Re-frame style event handling on the server
- **Client**: Reagent UI with server state subscription
- **Features**:
  - Real-time todo updates across all clients
  - Filter views (all/active/completed)
  - Bulk operations
  - Achievement system via rules

### 2. Chat App
A real-time chat application demonstrating:
- Multi-room support
- User presence tracking
- Typing indicators
- Message history
- Auto-moderation rules
- Activity logging

## Quick Start

### Prerequisites
```bash
# Install Node.js dependencies for ClojureScript
npm install

# Install Clojure CLI tools (if not already installed)
# See: https://clojure.org/guides/install_clojure
```

### Running the Todo App

1. **Start the server** (Terminal 1):
```bash
# Using Leiningen
lein run -m examples.todo-app.server 3000

# Or using Clojure CLI
clj -M:todo-server
```

2. **Start the ClojureScript compiler** (Terminal 2):
```bash
npm run dev
# or
npx shadow-cljs watch app
```

3. **Open your browser**:
Navigate to http://localhost:8080

### Running the Chat App

1. **Start the chat server**:
```bash
# Using Leiningen
lein run -m examples.chat-app.server 3001

# Or using Clojure CLI
clj -M:chat-server
```

2. **Use the same ClojureScript build** from the todo app

## Key Concepts

### Server-Side Re-frame (`reactor.frame`)

```clojure
;; Create an app with reactive state
(def app (rf/create-frame-app initial-db))

;; Register subscriptions
(rf/reg-sub :visible-todos
  (fn [db _]
    (filter :visible (:todos db))))

;; Register event handlers
(rf/reg-event-db :add-todo
  (fn [db [text]]
    (update db :todos conj {:text text :done false})))

;; Dispatch events
((:dispatch app) [:add-todo "Learn Reactor"])
```

### Client-Side Integration (`reactor.client`)

```clojure
;; Connect to server
(def app-state (rc/remote-ratom "http://localhost:3000"))

;; Subscribe to server state changes
(rc/subscribe! app-state [:todos]
  (fn [old new]
    (println "Todos updated!")))

;; Send updates to server
(rc/update-server! app-state :add-todo [] "New todo")
```

### Reactive Rules

```clojure
;; Define business logic rules that trigger automatically
(r/def-rule app-db :achievement-unlocked [:completed-count]
  (fn [count] (>= count 10))
  (fn [_ _] (println "Achievement: 10 todos completed!")))
```

## Features Demonstrated

### Reactivity
- **Automatic Updates**: Changes on server instantly reflect on all clients
- **Dependency Tracking**: Subscriptions automatically recompute when dependencies change
- **Efficient Updates**: Only affected subscriptions trigger, not entire state

### Persistence
- **State Snapshots**: Save/restore application state
- **Time Travel**: Replay events to reconstruct state
- **Hot Reload**: Maintain state during development

### Scaling
- **Cursor System**: Efficient access to nested data
- **Path-based Subscriptions**: Subscribe to specific parts of state
- **LRU Caching**: Automatic memory management for large datasets

### Rules Engine
- **Cascading Updates**: Rules trigger other rules automatically
- **Business Logic**: Encode domain rules declaratively
- **Time-based Rules**: Schedule periodic tasks

## Development Workflow

1. **Define your schema** in `shared.cljc`
2. **Create server events** in `server.clj`
3. **Build UI components** in `client.cljs`
4. **Add business rules** for automation
5. **Test with multiple clients** for real-time sync

## Testing

```bash
# Run server tests
lein test

# Run client tests
npm run test

# Run integration tests
lein test :integration
```

## Production Build

```bash
# Build optimized ClojureScript
npm run build

# Create uberjar for server
lein uberjar
```

## Architecture Benefits

- **Unified API**: Same reactive patterns on client and server
- **Real-time by Default**: SSE provides instant updates without polling
- **Server Authority**: Server maintains single source of truth
- **Declarative Logic**: Rules and subscriptions express "what" not "how"
- **Development Speed**: Hot reload + reactive updates = instant feedback

## Next Steps

- Explore the source code in `src/examples/`
- Modify the todo app to add new features
- Build your own application using Reactor
- Check performance with thousands of subscribers
- Implement authentication and authorization