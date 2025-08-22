# Testing Session Time-Travel

## Setup Complete ✅

Both server implementations now support session time-travel:

1. **`reactor.server`** (used by TODO app) - `/api/session-at` endpoint added
2. **`reactor.reactive-server`** (used by Rabbit demo) - `/api/session-at` endpoint already present

## Testing with TODO App

1. Start the TODO app server:
```bash
lein run -m examples.todo-app.server/-main
```

2. Start the TODO app client:
```bash
shadow-cljs watch todo
```

3. Create some TODOs and make changes to build up session history

4. Note the timestamps as you make changes

5. Test time-travel by loading the app with URL params:
```
http://localhost:8080?session_id=default&at=2025-08-22T20:30:00Z
```
(Replace with your actual timestamp)

## Testing with Rabbit Demo

1. Start the Rabbit demo server:
```bash
lein run -m examples.rabbit-demo.server/-main
```

2. Start the Rabbit demo client:
```bash
shadow-cljs watch rabbit
```

3. Test with URL params:
```
http://localhost:8080?session_id=default&at=2025-08-22T20:30:00Z
```

## Quick Test Script

Run the included test script to create test data:
```bash
lein run -m test-time-travel/test-session-time-travel
```

This will:
- Create a session with multiple states at different timestamps
- Output test URLs you can use
- Show both snapshot and session-at URLs for comparison

## URL Parameter Priority

When multiple parameters are present:
1. `?snapshot=xxx` (highest priority)
2. `?session_id=xxx&at=yyy` 
3. Normal session load (default)

## Troubleshooting

If time-travel doesn't work:
1. Check that timestamps are URL-encoded properly
2. Verify the session exists at that timestamp
3. Check server logs for temporal query execution
4. Ensure XTDB has the historical data (not purged)

## Implementation Details

The implementation:
- Uses XTDB's `AS OF SYSTEM TIME` for temporal queries
- Queries the app's own session table (e.g., `todo_sessions`)
- Returns the same format as snapshots for consistency
- Works transparently with any Reactor app