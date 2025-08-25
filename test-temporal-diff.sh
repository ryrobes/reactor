#!/bin/bash

echo "Testing temporal query diffing..."

# Wait for server to be ready
sleep 5

# Get current timestamp and one hour ago
NOW=$(date -u +"%Y-%m-%dT%H:%M:%S.000Z")
HOUR_AGO=$(date -u -d "1 hour ago" +"%Y-%m-%dT%H:%M:%S.000Z")
TWO_HOURS_AGO=$(date -u -d "2 hours ago" +"%Y-%m-%dT%H:%M:%S.000Z")

# Base query that will be used with different timestamps
BASE_SQL="SELECT * FROM sales ORDER BY _id DESC LIMIT 10"

# Create a session
SESSION_ID="test-temporal-$(date +%s)"

echo "Session ID: $SESSION_ID"
echo "Testing with timestamps:"
echo "  - 2 hours ago: $TWO_HOURS_AGO"
echo "  - 1 hour ago: $HOUR_AGO"
echo "  - Now: $NOW"

# First query - 2 hours ago (should send FULL update)
echo -e "\n1. Querying data from 2 hours ago..."
curl -s -X POST http://localhost:5000/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: $SESSION_ID" \
  -d "{
    \"sql\": \"${BASE_SQL} FOR SYSTEM_TIME AS OF TIMESTAMP '${TWO_HOURS_AGO}'\",
    \"subscription-id\": \"temporal-test-sub\"
  }" > /dev/null

sleep 2

# Second query - 1 hour ago (should send DIFF)
echo -e "\n2. Querying data from 1 hour ago (expecting DIFF)..."
curl -s -X POST http://localhost:5000/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: $SESSION_ID" \
  -d "{
    \"sql\": \"${BASE_SQL} FOR SYSTEM_TIME AS OF TIMESTAMP '${HOUR_AGO}'\",
    \"subscription-id\": \"temporal-test-sub\"
  }" > /dev/null

sleep 2

# Third query - Now (should send DIFF)
echo -e "\n3. Querying current data (expecting DIFF)..."
curl -s -X POST http://localhost:5000/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: $SESSION_ID" \
  -d "{
    \"sql\": \"${BASE_SQL} FOR SYSTEM_TIME AS OF TIMESTAMP '${NOW}'\",
    \"subscription-id\": \"temporal-test-sub\"
  }" > /dev/null

echo -e "\n4. Checking server logs for TEMPORAL-DIFF markers..."
tail -50 rabbit-server.log | grep -E "TEMPORAL-(DIFF|SKIP|CACHE)" | tail -10

echo -e "\nTest complete! Check the server logs for [TEMPORAL-DIFF] and [TEMPORAL-SKIP] messages."