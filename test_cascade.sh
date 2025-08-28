#!/bin/bash

# Test cascade query execution

echo "Testing cascade query execution..."
echo ""

# First, create a parent query
echo "1. Creating parent query block..."
curl -X POST http://localhost:5000/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: test-cascade" \
  -d '{
    "sql": "SELECT * FROM sales LIMIT 5",
    "block_id": "parent-block"
  }' | jq .

echo ""
echo "2. Simulating creation of child query with template reference..."
# This would normally be done through the UI by dragging a column
# For now, just log the expected behavior
echo "Child query would contain: SELECT product, COUNT(*) FROM ({{parent-block.sql}}) GROUP BY product"

echo ""
echo "3. When parent-block executes again, it should trigger child blocks..."
echo "(Check server logs for [CASCADE] messages)"

echo ""
echo "Re-executing parent query..."
curl -X POST http://localhost:5000/api/sql \
  -H "Content-Type: application/json" \
  -H "X-Session-ID: test-cascade" \
  -d '{
    "sql": "SELECT * FROM sales LIMIT 10",
    "block_id": "parent-block"
  }' | jq .

echo ""
echo "Check the server logs for cascade execution messages:"
echo "grep CASCADE /path/to/server.log"