#!/bin/bash

echo "Testing session-at and snapshot endpoints..."

# Test session-at endpoint (should return 400 or 404 initially)
echo -e "\n1. Testing /api/session-at endpoint:"
curl -s http://localhost:4000/api/session-at/test-session/2025-01-01T00:00:00Z | jq '.' 2>/dev/null || echo "Server may not be running"

# Test snapshot endpoint
echo -e "\n2. Testing /api/snapshot endpoint:"
curl -s http://localhost:4000/api/snapshot/test-snapshot | jq '.' 2>/dev/null || echo "Server may not be running"

echo -e "\nNote: Run this while your TODO app server is running."
echo "Expected: Both should return 404 (not found) rather than routing errors."
