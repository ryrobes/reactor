# SQL Connectivity for XTDB-backed Reactor

## Overview

XTDB 1.x does not natively support PostgreSQL wire protocol, which is required for `psql` and other PostgreSQL clients to connect directly. We've implemented several solutions to provide SQL access to Reactor's XTDB data store.

## Solutions Implemented

### 1. HTTP SQL API (Working)

We've created an HTTP-based SQL API that allows querying XTDB with SQL-like syntax.

**Start the server:**
```bash
lein run -m reactor.sql-api 8080
```

**Query the data:**
```bash
# Get server info
curl http://localhost:8080/info

# List tables
curl http://localhost:8080/tables

# Execute SQL query
curl -X POST http://localhost:8080/sql \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM todos"}'
```

**Files:**
- `/src/reactor/sql_api.clj` - HTTP SQL API implementation

### 2. PostgreSQL Wire Protocol Server (Experimental)

We've implemented a basic PostgreSQL wire protocol server that attempts to emulate PostgreSQL's protocol for XTDB.

**Start the server:**
```bash
lein run -m reactor.pgwire 5433
```

**Connect with psql (experimental):**
```bash
psql -h localhost -p 5433 -U xtdb -d reactor_xtdb
```

**Files:**
- `/src/reactor/pgwire.clj` - PostgreSQL wire protocol implementation

**Status:** The server starts and accepts connections but has issues with the full psql handshake. This is a complex protocol to implement correctly.

### 3. PostgreSQL Foreign Data Wrapper (Proposed)

Use PostgreSQL's Foreign Data Wrapper (FDW) to query XTDB through the HTTP API.

**Files:**
- `/src/reactor/pg_fdw.clj` - FDW setup documentation

## Recommended Approach

For production use with psql connectivity, we recommend:

### Option 1: Upgrade to XTDB 2.x

XTDB 2.x has native PostgreSQL wire protocol support built-in.

**Benefits:**
- Native psql connectivity
- Full SQL support
- No custom protocol implementation needed

**Migration steps:**
1. Update dependencies in `project.clj`:
   ```clojure
   [com.xtdb/xtdb-core "2.0.0-alpha"]
   [com.xtdb/xtdb-pgwire "2.0.0-alpha"]
   ```

2. Configure XTDB node with PostgreSQL wire protocol:
   ```clojure
   (xtdb/start-node
    {:xtdb/pgwire {:port 5432}})
   ```

3. Connect with psql:
   ```bash
   psql -h localhost -p 5432 -U xtdb
   ```

### Option 2: Use Presto/Trino

Deploy Presto or Trino as a SQL query engine that can connect to XTDB and provide PostgreSQL-compatible access.

**Benefits:**
- Production-ready SQL engine
- Supports complex queries
- Can federate multiple data sources

### Option 3: Use the HTTP SQL API

For applications that don't require psql specifically, the HTTP SQL API provides a simple and reliable way to query XTDB with SQL-like syntax.

**Benefits:**
- Simple to implement and maintain
- Works with any HTTP client
- Easy to secure with standard HTTP authentication

## Current Implementation Status

✅ **HTTP SQL API** - Fully functional, ready for use
⚠️ **PostgreSQL Wire Protocol** - Server starts but has protocol compatibility issues
📝 **PostgreSQL FDW** - Documentation provided, requires PostgreSQL setup
🔄 **XTDB 2.x Upgrade** - Recommended for full psql support

## Testing

To test the current HTTP SQL API:

```bash
# Start the server
lein run -m reactor.sql-api 8080

# In another terminal, query the data
curl -X POST http://localhost:8080/sql \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM todos"}' | jq
```

## Next Steps

1. For immediate SQL access: Use the HTTP SQL API
2. For full psql compatibility: Plan migration to XTDB 2.x
3. For complex SQL queries: Consider deploying Presto/Trino