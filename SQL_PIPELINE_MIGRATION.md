# SQL Pipeline Migration Guide

## Overview

The new SQL pipeline (`reactor.sql-pipeline`) provides a centralized, testable, and maintainable approach to SQL execution in Reactor. It replaces the scattered logic currently spread across multiple modules.

## Key Improvements

### Before (Legacy System)
- SQL execution logic scattered across 400+ lines in `/api/sql` handler
- Template resolution happens in 3+ different places
- Cascade logic embedded in HTTP handler
- Session state mutated during request processing
- Difficult to test individual components
- Complex nested conditionals

### After (New Pipeline)
- Single linear pipeline with clear stages
- Each stage has single responsibility
- Pure functions for easy testing
- Immutable context flows through pipeline
- Comprehensive test coverage
- Easy to debug and extend

## Migration Strategy

### Phase 1: Testing (Current)
✅ Pipeline implementation complete
✅ Comprehensive tests written and passing
✅ Integration adapter created
✅ Feature flag added to reactive-server

### Phase 2: Gradual Rollout
1. Enable for specific query types:
```clojure
;; In REPL or startup code
(require '[reactor.reactive-server :as server])

;; Test with low-risk queries first
(server/enable-new-pipeline!)

;; Or use adapter for fine-grained control
(require '[reactor.sql-pipeline-adapter :as adapter])
(adapter/enable-for-query-type! :blocks true)     ; Block queries
(adapter/enable-for-query-type! :temporal true)   ; Temporal queries
(adapter/enable-for-query-type! :mutations false) ; Keep mutations on legacy
(adapter/set-migration-percentage! 10)            ; 10% of traffic
```

2. Monitor for issues:
- Check logs for `[ADAPTER]` and `[PIPELINE]` entries
- Compare results between old and new pipeline
- Watch for performance differences

3. Gradually increase traffic:
```clojure
(adapter/set-migration-percentage! 25)  ; 25% traffic
;; Wait and monitor...
(adapter/set-migration-percentage! 50)  ; 50% traffic
;; Wait and monitor...
(adapter/set-migration-percentage! 100) ; Full traffic
```

### Phase 3: Full Migration
Once confident:
```clojure
;; Enable globally
(server/enable-new-pipeline!)
;; Or
(reset! reactor.reactive-server/use-new-pipeline? true)
```

### Phase 4: Cleanup (Future)
After stable operation:
- Remove legacy SQL handling code from reactive-server.clj
- Clean up duplicate template resolution in kafka-reactive.clj
- Simplify sql-reactive-bridge.clj
- Archive old code for reference

## Testing the Pipeline

### Run Tests
```bash
# Run pipeline tests
lein test reactor.sql-pipeline-test

# Run all tests to ensure no regressions
lein test
```

### Manual Testing
```clojure
;; In REPL
(require '[reactor.sql-pipeline :as pipeline])

;; Test simple query
(pipeline/execute-sql
  {:sql "SELECT * FROM sales LIMIT 5"
   :session-id "test-session"})

;; Test with templates
(pipeline/execute-sql
  {:sql "SELECT * FROM ({{block1.sql}}) WHERE amount > 100"
   :session-id "my-session"
   :block-id "block2"})

;; Test temporal query
(pipeline/execute-sql
  {:sql "SELECT * FROM orders"
   :as-of "2024-01-01T00:00:00Z"
   :session-id "test"})

;; Test mutation
(pipeline/execute-sql
  {:sql "INSERT INTO test (name) VALUES (?)"
   :params ["Test Name"]
   :session-id "test"})
```

## Pipeline Stages

The new pipeline executes these stages in order:

1. **validate-request** - Ensure required fields present
2. **load-session-state** - Load session (read-only)
3. **resolve-templates** - Resolve SQL templates
4. **add-temporal-clause** - Add AS OF clause if needed
5. **extract-metadata** - Extract tables and query type
6. **generate-subscription-id** - Create consistent IDs
7. **register-subscription** - Register with Kafka (queries only)
8. **execute-query** - Run against database
9. **trigger-reactive-updates** - Notify table changes (mutations)
10. **identify-cascade-targets** - Find dependent blocks
11. **trigger-cascades** - Execute cascades async

## Debugging

Enable debug logging:
```clojure
(require '[reactor.sql-pipeline-adapter :as adapter])

;; Debug specific request
(adapter/debug-pipeline-execution
  "SELECT * FROM users"
  nil
  "my-session")
```

Check logs for:
- `[PIPELINE]` - Pipeline execution
- `[ADAPTER]` - Adapter routing decisions
- `[CASCADE]` - Cascade execution

## Rollback

If issues arise:
```clojure
;; Immediate rollback
(server/disable-new-pipeline!)

;; Or disable specific features
(adapter/enable-for-query-type! :blocks false)
(adapter/set-migration-percentage! 0)
```

## Benefits Realized

1. **Testability**: 99 test assertions covering all paths
2. **Maintainability**: Single location for SQL logic
3. **Debuggability**: Linear flow, clear logging
4. **Performance**: Async cascades, debounced reactions
5. **Reliability**: No session state mutations, pure functions
6. **Extensibility**: Easy to add new stages

## Support

Monitor logs and test thoroughly during migration. The system is designed to be gradually adopted with minimal risk.