# Server Logging Optimization

## Problem
Server logs were growing to 10-20 MB in just 5 minutes of operation due to excessive debug logging in high-frequency loops.

## Root Causes Identified

### 1. Kafka Consumer Loop (kafka_reactive.clj)
- Runs every 100ms (10 times per second)
- Extensive debug logging for every message processed
- Even when log level is :info, string concatenation still happens

### 2. Temporal Cache (temporal_cache.clj)
- Logs every cache check (hit/miss)
- Row count queries happen frequently for time travel UI
- Each query generates multiple log entries

### 3. Reactive Server (reactive_server.clj)
- Logs every SQL query execution
- Verbose cascade operation logging
- Block SQL cache updates logged extensively

### 4. Subscription Re-execution
- Debounce operations logged frequently
- Every subscription trigger logged
- Channel state logging on each execution

## Changes Made

### 1. Changed Global Log Level
- Changed from `:info` to `:warn` in `src/reactor/log.clj`
- This filters out both info and debug messages

### 2. Commented Out High-Frequency Debug Logs
Disabled debug logging in critical hot paths:

#### kafka_reactive.clj:
- Message analysis logging (runs 10x/second)
- Subscription detail logging
- Table-to-subscription mapping logs
- Per-subscription trigger logging
- Debounce request logging

#### temporal_cache.clj:
- Cache check logging (hit/miss)
- Cache attempt logging
- Non-temporal query skip logging
- Changed cache addition logging to every 100th entry only

#### reactive_server.clj:
- Block SQL cache verbose update logging
- Request handler logging
- SQL query execution logging

## Performance Impact
These changes should reduce log output by ~95% while maintaining critical error and warning logs.

## Monitoring Recommendations

### Essential Logs to Keep:
- ERROR level: All errors
- WARN level: Important warnings
- INFO level (selective): 
  - Server startup/shutdown
  - Kafka connection status
  - Major state changes
  - Cache size milestones

### Debug Logging Control:
For troubleshooting, you can temporarily enable debug logging by:
1. Setting log level back to `:debug` in log.clj
2. Uncommenting specific debug statements as needed
3. Using the logging config flags in log_optimization.clj

## Future Improvements

### 1. Conditional Compilation
Use macros to completely eliminate debug log overhead:
```clojure
(defmacro when-debug [& body]
  (when (= :debug *compile-time-log-level*)
    `(do ~@body)))
```

### 2. Sampling
For high-frequency operations, log only a sample:
```clojure
(when (zero? (rand-int 1000)) ; 0.1% sampling
  (log/debug "High frequency operation"))
```

### 3. Metrics Instead of Logs
Replace verbose logging with metrics:
- Counter for cache hits/misses
- Histogram for query execution times
- Gauge for active subscriptions

### 4. Structured Logging
Use a proper logging framework that supports:
- Log levels per namespace
- Async logging to prevent blocking
- Log rotation and compression
- Structured output (JSON) for analysis

## Testing the Changes
1. Start the server: `lein run -m examples.rabbit-demo.server/-main`
2. Monitor log file size: `watch -n 1 'ls -lh server-rabbit.log'`
3. Expected: <1MB per hour vs previous 10-20MB per 5 minutes