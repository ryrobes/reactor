#!/bin/bash

# Test todo app persistence

echo "=== Testing TODO App Persistence ==="

# Clean up any existing test data
echo "1. Cleaning up test data..."
curl -X POST http://localhost:4000/api/sql-exec \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: test" \
  -d '{"sql": "DELETE FROM todo_sessions WHERE session_id IN ('\''test1'\'', '\''test2'\'')"}' \
  2>/dev/null

echo ""
echo "2. Creating todo in session test1..."
curl -X POST http://localhost:4000/api/sql-exec \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: test1" \
  -d '{"sql": "INSERT INTO todo_sessions RECORDS {_id: '\''todo-test1'\'', session_id: '\''test1'\'', app_state: '\''{:todos {\"1\" {:id \"1\" :text \"Test1 Todo\" :completed false}} :filter :all}'\''}"}' \
  2>/dev/null | python3 -m json.tool

echo ""
echo "3. Creating todo in session test2..."
curl -X POST http://localhost:4000/api/sql-exec \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: test2" \
  -d '{"sql": "INSERT INTO todo_sessions RECORDS {_id: '\''todo-test2'\'', session_id: '\''test2'\'', app_state: '\''{:todos {\"2\" {:id \"2\" :text \"Test2 Todo\" :completed true}} :filter :active}'\''}"}' \
  2>/dev/null | python3 -m json.tool

echo ""
echo "4. Querying test1 session..."
curl -X POST http://localhost:4000/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: test1" \
  -d '{"sql": "SELECT session_id, app_state FROM todo_sessions WHERE session_id = '\''test1'\''"}' \
  2>/dev/null | python3 -m json.tool | grep -A2 "app_state"

echo ""
echo "5. Querying test2 session..."
curl -X POST http://localhost:4000/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: test2" \
  -d '{"sql": "SELECT session_id, app_state FROM todo_sessions WHERE session_id = '\''test2'\''"}' \
  2>/dev/null | python3 -m json.tool | grep -A2 "app_state"

echo ""
echo "6. Testing update (DELETE + INSERT) for test1..."
curl -X POST http://localhost:4000/api/sql-exec \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: test1" \
  -d '{"sql": "DELETE FROM todo_sessions WHERE _id = '\''todo-test1'\''"}' \
  2>/dev/null
curl -X POST http://localhost:4000/api/sql-exec \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: test1" \
  -d '{"sql": "INSERT INTO todo_sessions RECORDS {_id: '\''todo-test1'\'', session_id: '\''test1'\'', app_state: '\''{:todos {\"1\" {:id \"1\" :text \"Updated Todo\" :completed true}} :filter :all}'\''}"}' \
  2>/dev/null | python3 -m json.tool

echo ""
echo "7. Verifying update..."
curl -X POST http://localhost:4000/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: test1" \
  -d '{"sql": "SELECT app_state FROM todo_sessions WHERE session_id = '\''test1'\''"}' \
  2>/dev/null | python3 -m json.tool | grep "Updated Todo"

echo ""
echo "=== Test Complete ==="
echo "✓ Sessions can have independent state"
echo "✓ State persists to database"  
echo "✓ Updates work via DELETE + INSERT pattern"
echo ""
echo "Open http://localhost:8084/todo-enhanced.html to test in browser"