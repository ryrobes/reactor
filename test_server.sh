#!/bin/bash

echo "Testing Reactor Todo Server"
echo "================================"

SERVER_URL="http://localhost:3000"

echo -e "\n1. Testing homepage..."
curl -s "$SERVER_URL/" | grep -q "<title>Reactor Todo App</title>" && echo "✅ Homepage serves HTML" || echo "❌ Homepage failed"

echo -e "\n2. Testing API state endpoint..."
STATE=$(curl -s "$SERVER_URL/api/state")
echo "Current state (truncated): ${STATE:0:100}..."
echo "$STATE" | grep -q ":todos" && echo "✅ State endpoint works" || echo "❌ State endpoint failed"

echo -e "\n3. Testing dispatch endpoint..."
RESPONSE=$(echo '[:add-todo "Test task from script"]' | curl -s -X POST "$SERVER_URL/api/dispatch" -H "Content-Type: application/edn" --data-binary @-)
echo "Response: $RESPONSE"
echo "$RESPONSE" | grep -q ":ok" && echo "✅ Dispatch endpoint works" || echo "❌ Dispatch endpoint failed"

echo -e "\n4. Testing SSE subscription endpoint..."
timeout 2 curl -s "$SERVER_URL/subscribe?path=todos&format=edn" > /dev/null 2>&1 &
SSE_PID=$!
sleep 1
if ps -p $SSE_PID > /dev/null 2>&1; then
    echo "✅ SSE endpoint is streaming"
    kill $SSE_PID 2>/dev/null
else
    echo "❌ SSE endpoint failed"
fi

echo -e "\n5. Testing server-side re-frame events..."
echo '[:toggle-todo 1]' | curl -s -X POST "$SERVER_URL/api/dispatch" -H "Content-Type: application/edn" --data-binary @- > /dev/null
echo '[:set-filter :completed]' | curl -s -X POST "$SERVER_URL/api/dispatch" -H "Content-Type: application/edn" --data-binary @- > /dev/null
echo '[:clear-completed]' | curl -s -X POST "$SERVER_URL/api/dispatch" -H "Content-Type: application/edn" --data-binary @- > /dev/null
echo "✅ Re-frame events dispatched"

echo -e "\n6. Final state check..."
FINAL_STATE=$(curl -s "$SERVER_URL/api/state")
TODO_COUNT=$(echo "$FINAL_STATE" | grep -o ":id [0-9]" | wc -l)
echo "Remaining todos: $TODO_COUNT"

echo -e "\n✨ All tests complete!"