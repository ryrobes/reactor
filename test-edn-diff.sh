#!/bin/bash

echo "Testing EDN field structural diffing..."

SESSION_ID="edn-diff-test-$(date +%s)"
SERVER="http://localhost:5000"

# First, create a todo_sessions record with EDN app_state
echo "1. Creating initial todo_sessions record with EDN app_state..."
curl -s -X POST $SERVER/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: $SESSION_ID" \
  -d '{
    "sql": "INSERT INTO todo_sessions (_id, app_state) VALUES ('\''edn-test'\'', '\''{:todos [{:id 1 :text \"Buy milk\" :done false} {:id 2 :text \"Walk dog\" :done false}] :filter :all :counter 0}'\'')"
  }' > /dev/null

sleep 1

# Query the initial state and subscribe
echo "2. Querying initial state with subscription..."
curl -s -X POST $SERVER/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: $SESSION_ID" \
  -d '{
    "sql": "SELECT * FROM todo_sessions WHERE _id = '\''edn-test'\''",
    "subscription-id": "edn-sub"
  }' | python3 -c "import sys, json; d=json.load(sys.stdin); print('Initial app_state:', d['results'][0]['app_state'][:80] + '...' if len(d['results'][0]['app_state']) > 80 else d['results'][0]['app_state'])" 2>/dev/null || echo "Query 1 failed"

sleep 1

# Update just one field in the EDN (toggle first todo's done status)
echo -e "\n3. Updating EDN field (toggling first todo done status)..."
curl -s -X POST $SERVER/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: $SESSION_ID" \
  -d '{
    "sql": "UPDATE todo_sessions SET app_state = '\''{:todos [{:id 1 :text \"Buy milk\" :done true} {:id 2 :text \"Walk dog\" :done false}] :filter :all :counter 1}'\'' WHERE _id = '\''edn-test'\''"
  }' > /dev/null

sleep 2

# Query again - should trigger diff
echo "4. Re-querying (should trigger structural diff)..."
curl -s -X POST $SERVER/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: $SESSION_ID" \
  -d '{
    "sql": "SELECT * FROM todo_sessions WHERE _id = '\''edn-test'\''",
    "subscription-id": "edn-sub"
  }' > /dev/null

sleep 1

# Add a new todo
echo -e "\n5. Adding a new todo to the EDN..."
curl -s -X POST $SERVER/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: $SESSION_ID" \
  -d '{
    "sql": "UPDATE todo_sessions SET app_state = '\''{:todos [{:id 1 :text \"Buy milk\" :done true} {:id 2 :text \"Walk dog\" :done false} {:id 3 :text \"Read book\" :done false}] :filter :all :counter 2}'\'' WHERE _id = '\''edn-test'\''"
  }' > /dev/null

sleep 2

# Query again
echo "6. Re-querying (should show EDN structural diff)..."
curl -s -X POST $SERVER/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: $SESSION_ID" \
  -d '{
    "sql": "SELECT * FROM todo_sessions WHERE _id = '\''edn-test'\''",
    "subscription-id": "edn-sub"
  }' > /dev/null

sleep 1

echo -e "\n7. Checking logs for structural diff markers..."
grep -E "(STRUCT-DIFF|:structural-update|edn-diff|EDN)" rabbit-edn-diff.log | tail -15

echo -e "\nTest complete! Check logs for [STRUCT-DIFF] and structural-update messages."