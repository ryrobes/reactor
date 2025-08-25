#!/bin/bash

echo "Testing temporal query diffing with SSE connection..."

SESSION_ID="test-temporal-sse-$(date +%s)"
NOW=$(date -u +"%Y-%m-%dT%H:%M:%S.000Z")
FIVE_MIN_AGO=$(date -u -d "5 minutes ago" +"%Y-%m-%dT%H:%M:%S.000Z")
TEN_MIN_AGO=$(date -u -d "10 minutes ago" +"%Y-%m-%dT%H:%M:%S.000Z")

echo "Session ID: $SESSION_ID"
echo "Connecting SSE..."

# Connect SSE in background to receive updates
curl -s -N -H "X-Session-ID: $SESSION_ID" \
  http://localhost:5000/api/subscribe-sql 2>/dev/null | while read line; do
  if [[ $line == data:* ]]; then
    echo "[SSE] $line" | head -c 200
    echo
  fi
done &
SSE_PID=$!

sleep 2

echo -e "\n1. First temporal query (10 minutes ago) - expecting FULL update..."
curl -s -X POST http://localhost:5000/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: $SESSION_ID" \
  -d "{
    \"sql\": \"SELECT * FROM sales ORDER BY _id LIMIT 5 FOR SYSTEM_TIME AS OF TIMESTAMP '${TEN_MIN_AGO}'\",
    \"subscription-id\": \"temporal-sse-test\"
  }" > /dev/null

sleep 2

echo -e "\n2. Second temporal query (5 minutes ago) - expecting DIFF update..."
curl -s -X POST http://localhost:5000/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: $SESSION_ID" \
  -d "{
    \"sql\": \"SELECT * FROM sales ORDER BY _id LIMIT 5 FOR SYSTEM_TIME AS OF TIMESTAMP '${FIVE_MIN_AGO}'\",
    \"subscription-id\": \"temporal-sse-test\"
  }" > /dev/null

sleep 2

echo -e "\n3. Third temporal query (now) - expecting DIFF update..."
curl -s -X POST http://localhost:5000/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: $SESSION_ID" \
  -d "{
    \"sql\": \"SELECT * FROM sales ORDER BY _id LIMIT 5 FOR SYSTEM_TIME AS OF TIMESTAMP '${NOW}'\",
    \"subscription-id\": \"temporal-sse-test\"
  }" > /dev/null

sleep 2

echo -e "\nKilling SSE connection..."
kill $SSE_PID 2>/dev/null

echo "Test complete!"