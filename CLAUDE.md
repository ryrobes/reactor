# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Reactor

Reactor is an experimental "re-frame for the full stack" - a unified reactive state management library that extends re-frame's client-side model across client/server boundaries. It provides server-authoritative state with automatic synchronization, built-in time travel, and reactive SQL subscriptions.

## Commands

### Running Examples

```bash
# Magic Counter (simple demo)
lein run -m examples.magic-counter.server/-main
shadow-cljs watch magic

# TODO App
lein run -m examples.todo-app.server-simple  # Basic version
lein run -m examples.todo-app.server         # Full reactive version
shadow-cljs watch todo

# Rabbit Demo (SQL Browser with time travel)
lein run -m examples.rabbit-demo.server/-main
shadow-cljs watch rabbit
```

### Testing & Development

```bash
# Run Clojure tests
lein test

# REPL for server development
lein repl

# ClojureScript REPL
shadow-cljs cljs-repl rabbit  # or 'todo' or 'magic'

# Check for compilation issues
lein check
shadow-cljs compile rabbit

# Clean builds
lein clean
rm -rf .shadow-cljs resources/public/js
```

### Database Setup

Reactor uses XTDB 2.0 which requires a running XTDB server:
```bash
# XTDB should be running on localhost:5432
# Connection: jdbc:xtdb://localhost:5432/xtdb
```

## Architecture

### Core Components

1. **Client (`reactor.core.cljs`)**
   - Provides `subscribe`/`dispatch!` API matching re-frame
   - Manages Server-Sent Events (SSE) connection for real-time updates
   - Handles SQL subscriptions and time travel controls

2. **Server (`reactor.reactive-server.clj`)**
   - Processes events via registered handlers
   - Manages per-session state isolation
   - Integrates with Kafka for reactive SQL subscriptions
   - Provides REST endpoints for SQL queries and mutations

3. **Storage (`reactor.xtdb-store.clj`)**
   - XTDB 2.0 integration via JDBC/PostgreSQL protocol
   - Bitemporal queries for time travel
   - Automatic history tracking

4. **Reactivity (`reactor.kafka-reactive.clj`)**
   - Monitors XTDB transaction log via Kafka
   - Triggers subscription re-execution on data changes
   - Manages SSE channels for pushing updates to clients

5. **Meta-tracking (`reactor.meta-tracking.clj`)**
   - Async tracking of subscriptions, events, reactions, performance
   - Debug tables: `reactor_subscriptions`, `reactor_events`, `reactor_reactions`, `reactor_performance`
   - Uses core.async channels for non-blocking writes

### Key Design Patterns

**Server-Authoritative State**: All state mutations happen server-side through event handlers. Clients subscribe to state changes.

**Reactive SQL Subscriptions**: SQL queries automatically re-execute when underlying tables change, detected via Kafka monitoring of XTDB's transaction log.

**Session Isolation**: Each user session has isolated state stored in XTDB with session-specific prefixes.

**Time Travel**: Built on XTDB's bitemporal capabilities - supports both `SYSTEM_TIME` (when data was written) and `VALID_TIME` (when data was valid).

### Data Flow

1. Client dispatches event → Server handler processes → State updated in XTDB
2. XTDB writes to Kafka log → Kafka consumer detects changes → Finds affected subscriptions
3. Subscriptions re-execute → Results pushed via SSE → Client UI updates

### Important Files

- `src/reactor/kafka_reactive.clj` - Core reactive SQL engine, monitors Kafka for changes
- `src/reactor/reactive_server.clj` - HTTP endpoints for SQL queries and SSE connections
- `src/reactor/meta_tracking.clj` - Async meta-table tracking for debugging
- `src/examples/rabbit_demo/client.cljs` - Complex UI with SQL browser and time travel
- `src/reactor/time_travel_sql.clj` - Time travel SQL query execution

### Common Issues & Solutions

**Compilation errors in client.cljs**: Usually unmatched delimiters. Check bracket/parenthesis balance around toolbar function (lines 1200-1240).

**Meta-tracking tables not populated**: Ensure `track-reaction!` is called in Kafka consumer loop and `track-event!` is called for SQL operations.

**SSE connection issues**: Check CORS headers in server responses. Ensure session-id is consistent between requests.

**XTDB table creation**: Tables are created implicitly on first insert in XTDB 2.0, not with CREATE TABLE.

**Parameter passing to execute-sql**: Use variadic args `(execute-sql node sql param1 param2)` not vectors `[param1 param2]`.

### Debugging

```bash
# Check server logs
tail -f server-rabbit.log

# Monitor Kafka consumer
grep "Tables affected by mutation" server-rabbit.log

# Check meta-tracking tables
psql -U xtdb xtdb -h localhost -c "SELECT * FROM reactor_subscriptions;"

# Debug SSE connections
curl http://localhost:5000/api/subscriptions

# Test SQL execution
curl -X POST http://localhost:5000/api/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM sales LIMIT 5"}'
```