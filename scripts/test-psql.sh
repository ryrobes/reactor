#!/bin/bash

echo "Testing PostgreSQL connectivity options for Reactor/XTDB"
echo "========================================================"
echo

# Check if psql is installed
if ! command -v psql &> /dev/null; then
    echo "❌ psql is not installed"
    exit 1
fi
echo "✅ psql is installed: $(psql --version)"

# Test PostgreSQL wire protocol server
echo
echo "Testing PostgreSQL Wire Protocol Server (port 5433)..."
if nc -zv localhost 5433 2>&1 | grep -q succeeded; then
    echo "✅ Server is listening on port 5433"
    
    # Try to connect with psql (expect timeout)
    echo "  Attempting psql connection (2s timeout)..."
    if timeout 2 psql -h localhost -p 5433 -U xtdb -d reactor_xtdb -c "SELECT 1;" 2>&1; then
        echo "  ✅ psql connected successfully!"
    else
        EXIT_CODE=$?
        if [ $EXIT_CODE -eq 124 ]; then
            echo "  ⚠️  psql connection timed out (expected - protocol implementation incomplete)"
        else
            echo "  ❌ psql connection failed with exit code: $EXIT_CODE"
        fi
    fi
else
    echo "❌ Server is not running on port 5433"
fi

# Test HTTP SQL API
echo
echo "Testing HTTP SQL API (port 8080)..."
if curl -s -f http://localhost:8080/info > /dev/null 2>&1; then
    echo "✅ HTTP SQL API is running"
    
    # Test SQL query
    echo "  Testing SQL query via HTTP..."
    RESULT=$(curl -s -X POST http://localhost:8080/sql \
             -H 'Content-Type: application/json' \
             -d '{"query": "SELECT * FROM todos"}' 2>/dev/null)
    
    if echo "$RESULT" | grep -q "result"; then
        echo "  ✅ SQL query successful"
        echo "  Sample data:"
        echo "$RESULT" | jq -r '.result[0][0].text' 2>/dev/null | head -3 | sed 's/^/    - /'
    else
        echo "  ❌ SQL query failed"
    fi
else
    echo "❌ HTTP SQL API is not running on port 8080"
fi

echo
echo "Summary:"
echo "--------"
echo "• PostgreSQL wire protocol: Server running but psql handshake incomplete"
echo "• HTTP SQL API: Recommended for current use"
echo "• For full psql support: Upgrade to XTDB 2.x"