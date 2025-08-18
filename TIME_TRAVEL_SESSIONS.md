# Time Travel: Session vs Global Behavior

## Current Implementation: Global Time Travel

The current time-travel implementation in Reactor operates at the **global database level**. This means:

### What Happens Now
- **Single Timeline**: All users share the same time-travel history
- **Global Undo/Redo**: When any user triggers undo, it affects the entire application state
- **Shared Checkpoints**: Checkpoints are visible and accessible to all users
- **Unified History**: There's one history buffer for the entire application

### Example Scenario
```
User A adds a todo: "Buy milk"
User B adds a todo: "Walk dog"
User A clicks UNDO
→ Result: "Walk dog" is removed (last global action)
```

## Why Global Time Travel?

This design choice was intentional for several reasons:

1. **Server Authority**: The server maintains a single source of truth
2. **Simplicity**: No complex branching or merging of timelines
3. **Consistency**: All users see the same state at all times
4. **Use Cases**: Perfect for:
   - Single-user applications
   - Admin dashboards
   - Development/debugging tools
   - Collaborative editing where everyone agrees on state

## Session-Based Time Travel (Future Enhancement)

While not currently implemented, the architecture supports session-based time travel:

### How It Would Work
```clojure
;; Current: Global time travel
(def app (rf/create-frame-app 
          initial-state 
          {:history true}))

;; Future: Session-based time travel
(def app (rf/create-frame-app 
          initial-state 
          {:history :session  ; Enable per-session history
           :session-store (session-store)}))
```

### Session Architecture
```
┌─────────────────────────────────────┐
│         Global State                 │
│  ┌─────────────────────────────┐    │
│  │    Shared Data (90%)        │    │
│  └─────────────────────────────┘    │
│                                      │
│  ┌──────────┐  ┌──────────┐         │
│  │Session A │  │Session B │  ...    │
│  │ History  │  │ History  │         │
│  └──────────┘  └──────────┘         │
└─────────────────────────────────────┘
```

### Implementation Path

1. **Session Identification**
```clojure
(defn get-session-id [request]
  (or (get-in request [:session :id])
      (get-in request [:headers "x-session-id"])
      (generate-session-id)))
```

2. **Session-Scoped Operations**
```clojure
(r/undo! app-state session-id)  ; Undo for specific session
(r/redo! app-state session-id)  ; Redo for specific session
```

3. **Branching History**
```clojure
;; Each session maintains its own branch
{:global-history [...] 
 :session-branches 
   {"session-123" {:history [...] :position 5}
    "session-456" {:history [...] :position 3}}}
```

## Hybrid Approach (Recommended)

The ideal solution combines both approaches:

### Three-Tier Time Travel
1. **Global History**: System-wide changes (admin actions)
2. **Session History**: User-specific undo/redo
3. **Checkpoint System**: Named save points accessible to all

### Example Configuration
```clojure
(def app (rf/create-frame-app 
  initial-state
  {:history {:global true
             :session true
             :checkpoints :shared
             :max-global-history 1000
             :max-session-history 100}}))
```

### User Experience
- **Personal Undo**: "Undo my last action"
- **Global Revert**: "Restore to yesterday's state" (admin only)
- **Checkpoint Jump**: "Go to 'before-migration' checkpoint"

## Current Demo Apps Behavior

Both the Todo and Chat demo apps currently use **global time travel**:

### Todo App
- All todo operations are globally tracked
- Any user's undo affects all users' todos
- Time scrubber shows global history

### Chat App
- Message history is globally tracked
- Undo removes the last message (from any user)
- Time travel can restore deleted conversations

## How to Test Global Behavior

1. **Open two browser windows** to the same app
2. **Make changes** in both windows
3. **Click undo** in one window
4. **Observe** that the last change (regardless of window) is undone
5. **Both windows** reflect the same state

## Implementation Status

✅ **Implemented**:
- Global time travel with full state snapshots
- Ring buffer for memory management
- Named checkpoints
- Time scrubber UI
- SSE synchronization

🚧 **Ready to Build**:
- Session identification system
- Per-session history buffers
- Session branching and merging
- Conflict resolution strategies

## FAQ

### Q: Why not session-based by default?
**A**: Global time travel is simpler and covers most use cases. Session-based adds complexity that many apps don't need.

### Q: Can I disable time travel for certain operations?
**A**: Yes, you can use regular Clojure atoms for non-tracked state, or implement filters on what gets recorded.

### Q: How does this work with persistence?
**A**: The time-travel system is in-memory by default. For persistence, you'd implement a storage backend that saves snapshots.

### Q: What about performance with many users?
**A**: Global time travel is actually more efficient than session-based, as there's only one history buffer to maintain.

## Future Roadmap

1. **Phase 1** (Current): Global time travel ✅
2. **Phase 2**: Session identification and branching
3. **Phase 3**: Selective time travel (per feature/component)
4. **Phase 4**: Distributed time travel (across servers)
5. **Phase 5**: Time-travel debugging tools and visualizations

## Conclusion

The current global time-travel implementation is:
- **Simple**: One timeline to rule them all
- **Powerful**: Full application state restoration
- **Extensible**: Architecture supports session-based when needed
- **Production-Ready**: Suitable for many real-world applications

For multi-user apps requiring isolated undo/redo, the session-based enhancement can be added without breaking existing functionality. The beauty of Reactor's design is that both models can coexist, giving developers the flexibility to choose the right approach for their needs.