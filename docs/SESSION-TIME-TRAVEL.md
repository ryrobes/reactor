# Session Time-Travel & Snapshot System

Reactor now supports two methods for loading historical application state:

## 1. Snapshot System (Explicit Checkpoints)
Load a pre-saved snapshot by ID:
```
http://localhost:5000?snapshot=snapshot-123
```

**Characteristics:**
- Requires explicitly saving snapshots with descriptions
- Fast lookup (direct query by snapshot_id)
- Good for important checkpoints, demos, or sharing specific states
- Stored in `reactor_snapshots` table

## 2. Session Time-Travel (Fluid Timeline)
Load any session state at any timestamp:
```
http://localhost:5000?session_id=my-session&at=2025-08-22T20:07:46.351Z
```

**Characteristics:**
- No pre-creation required - works with any valid timestamp
- Uses XTDB's temporal queries (`AS OF SYSTEM TIME`)
- Perfect for replaying sessions or debugging
- Queries the session's own table (e.g., `todo_sessions`)

## Priority Order
When multiple parameters are present:
1. `?snapshot=xxx` (highest priority) - Load a saved snapshot
2. `?session_id=xxx&at=yyy` - Load session at specific timestamp
3. `?session_id=xxx` - Load current state of a specific session
4. Normal session load (default)

## Implementation Details

### Server-side (both `server.clj` and `reactive_server.clj`)
- `/api/snapshot/:id` - Load snapshot by ID
- `/api/session-at/:session_id/:timestamp` - Load session at specific timestamp
- `/api/session-current/:session_id` - Load current state of a session

### Client-side (`core.cljs`)
- `load-snapshot!` - Loads and applies snapshot state
- `load-session-at!` - Loads session state at timestamp
- `load-session-current!` - Loads current state of a specific session
- `init!` - Checks URL params and loads appropriate state

### How It Works
1. Client checks URL parameters on initialization
2. Based on params, determines loading mode (snapshot/session-at/normal)
3. Fetches historical state from server
4. Replaces app-db with loaded state
5. Skips first SSE update to prevent overwriting

## Testing
Run the test script to create sample data:
```bash
lein run -m test-time-travel/test-session-time-travel
```

This will:
- Create a session with multiple state changes
- Generate test URLs with timestamps
- Create a test snapshot for comparison
- Show the priority order in action

## Use Cases

### Snapshot System
- Demo specific features
- Share application states with others
- Create named checkpoints for testing
- Save "golden" states for regression testing

### Session Time-Travel
- Debug issues by replaying exact session states
- Generate replay scripts programmatically
- Analyze how state evolved over time
- Create timeline scrubbers for session playback

### Session Loading (without timestamp)
- Switch between different user sessions
- Load a specific user's current state
- Test multi-user scenarios
- Quick session switching for development

## Notes
- Timestamps must be URL-encoded when used in URLs
- XTDB uses ISO-8601 format with timezone (e.g., `2025-08-22T20:07:46.351Z`)
- Session states don't include query results (`:results` field is stripped)
- Both systems work transparently - apps don't need special handling
