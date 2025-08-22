# Visual Testing Guide

Visual regression testing for Reactor apps using rabbitize and automated screenshot comparison.

## Prerequisites

1. **Install rabbitize** (if not already installed):
   ```bash
   npm install -g rabbitize
   npx playwright install-deps
   ```

2. **Start the app servers** before running tests:

   For rabbit app:
   ```bash
   # Terminal 1: Start server
   lein run -m examples.rabbit-demo.server/-main
   
   # Terminal 2: Start UI
   shadow-cljs watch rabbit
   ```

   For todo app:
   ```bash
   # Terminal 1: Start server
   lein run -m examples.todo-app.server/-main
   
   # Terminal 2: Start UI  
   shadow-cljs watch todo
   ```

## Running Visual Tests

Once servers are running:

```bash
# Run all visual tests
lein test :only reactor.visual-testing-test

# Run specific test
lein test :only reactor.visual-testing-test/test-rabbit-app-specific-snapshot
```

## How It Works

1. **First Run**: Creates a baseline automatically
   - Captures screenshot and DOM structure
   - Stores in database for future comparisons

2. **Subsequent Runs**: Compares against baseline
   - Image similarity check (default threshold: 95%)
   - DOM structure comparison
   - Reports PASS/FAIL with details

3. **Baseline Updates**: When tests pass, baseline is automatically updated
   - Handles intentional UI changes gracefully
   - No manual baseline management needed

## Test Structure

```clojure
(vt/run-visual-test! 
  "app-name"           ; rabbit, todo, or magic
  "test-name"          ; unique test identifier
  "snapshot-id"        ; snapshot to load
  :base-url "http://localhost:8080/rabbit.html"
  :threshold 95.0)     ; % similarity required to pass
```

## Artifacts

Visual test artifacts are stored in:
```
rabbitize-runs/
├── {app-name}/
│   └── {snapshot-id}/
│       └── {timestamp}/
│           ├── screenshots/
│           │   └── 0-post-wait.jpg
│           └── dom_snapshots/
│               └── dom_0.json
```

## Troubleshooting

### "Server not reachable" Error
- Ensure both server and UI are running
- Check the port numbers match (default: 8080)
- Wait for "Compiled successfully" message from shadow-cljs

### Tests Failing Due to Minor Differences
- Adjust threshold parameter (e.g., `:threshold 90.0`)
- Visual differences under threshold will still pass
- Baseline auto-updates on pass for gradual changes

### Port Conflicts
- All Reactor apps use port 8080 by default
- Only run one app at a time for testing
- Or modify shadow-cljs.edn to use different ports

## Database Tables

Visual testing uses two XTDB tables:

- `reactor_visual_baselines`: Stores baseline screenshots and DOM
- `reactor_visual_results`: Stores test results and comparisons

Tables are auto-created on first use.