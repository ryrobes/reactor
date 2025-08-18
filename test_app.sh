#!/bin/bash

echo "Testing Reactor Todo App..."
echo

echo "1. Current state:"
curl -s http://localhost:3000/api/state | head -1
echo
echo

echo "2. Adding a todo:"
TIMESTAMP=$(date +%s)
echo "[:add-todo \"Test todo $TIMESTAMP\"]" | curl -s -X POST http://localhost:3000/api/dispatch -H "Content-Type: application/edn" --data-binary @-
echo
echo

echo "3. Updated state:"
curl -s http://localhost:3000/api/state | head -1
echo
echo

echo "4. Toggling todo ID 2:"
echo '[:toggle-todo 2]' | curl -s -X POST http://localhost:3000/api/dispatch -H "Content-Type: application/edn" --data-binary @-
echo
echo

echo "5. Final state:"
curl -s http://localhost:3000/api/state | head -1
echo

echo "Test complete!"