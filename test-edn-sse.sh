#!/bin/bash

echo "Testing EDN structural diffing with SSE..."

SESSION_ID="edn-sse-test-$(date +%s)"

# Connect SSE to receive updates
echo "Connecting SSE..."
curl -s -N -H "X-Session-ID: $SESSION_ID" \
  http://localhost:5000/api/subscribe-sql 2>/dev/null | while read line; do
  if [[ $line == data:* ]]; then
    # Check if it contains field-diff or structural update
    if echo "$line" | grep -q "field-diff\|structural"; then
      echo "[STRUCTURAL DIFF DETECTED!]"
      echo "$line" | python3 -m json.tool 2>/dev/null | head -50
    else
      echo "[SSE]" $(echo "$line" | cut -c1-150)
    fi
  fi
done &
SSE_PID=$!

sleep 2

# Create initial record
echo "1. Creating todo_sessions with EDN app_state..."
curl -s -X POST http://localhost:5000/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: $SESSION_ID" \
  -d '{
    "sql": "INSERT INTO todo_sessions (_id, app_state) VALUES ('\''sse-test'\'', '\''{:todos [{:id 1 :text \"Buy milk\" :done false}] :counter 0}'\'')"
  }' > /dev/null

sleep 1

# Subscribe to queries
echo "2. Subscribing to todo_sessions query..."
curl -s -X POST http://localhost:5000/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: $SESSION_ID" \
  -d '{
    "sql": "SELECT * FROM todo_sessions WHERE _id = '\''sse-test'\''",
    "subscription-id": "edn-sse-sub"
  }' > /dev/null

sleep 2

# Update the EDN field
echo -e "\n3. Updating EDN (toggling todo done)..."
curl -s -X POST http://localhost:5000/api/sql-exec \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: $SESSION_ID" \
  -d '{
    "sql": "UPDATE todo_sessions SET app_state = '\''{:todos [{:id 1 :text \"Buy milk\" :done true}] :counter 1}'\'' WHERE _id = '\''sse-test'\''"
  }' > /dev/null

sleep 3

# Another update
echo -e "\n4. Adding a new todo..."
curl -s -X POST http://localhost:5000/api/sql-exec \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: $SESSION_ID" \
  -d '{
    "sql": "UPDATE todo_sessions SET app_state = '\''{:todos [{:id 1 :text \"Buy milk\" :done true} {:id 2 :text \"Walk dog\" :done false}] :counter 2}'\'' WHERE _id = '\''sse-test'\''"
  }' > /dev/null

sleep 3

echo -e "\nKilling SSE..."
kill $SSE_PID 2>/dev/null

echo "Test complete!"