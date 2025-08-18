#!/bin/bash

# Reactor TODO App Demo Runner
# Starts both server and client with one command

echo "🚀 Starting Reactor TODO App Demo"
echo "===================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Kill any existing processes on our ports
echo -e "${YELLOW}Cleaning up old processes...${NC}"
lsof -ti:4000 | xargs kill -9 2>/dev/null
lsof -ti:8080 | xargs kill -9 2>/dev/null
lsof -ti:9630 | xargs kill -9 2>/dev/null
sleep 1

# Start the server
echo -e "${GREEN}Starting TODO Server on port 4000...${NC}"
lein run -m examples.todo-app.server 2>&1 | sed "s/^/[SERVER] /" &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Wait for server to start
echo -e "${BLUE}Waiting for server to initialize XTDB...${NC}"

# Poll for server to be ready (up to 30 seconds)
COUNTER=0
while [ $COUNTER -lt 30 ]; do
    if lsof -i:4000 > /dev/null 2>&1; then
        break
    fi
    sleep 1
    echo -n "."
    COUNTER=$((COUNTER + 1))
done

echo ""

# Check if server is running
if ! lsof -i:4000 > /dev/null 2>&1; then
    echo -e "${RED}Error: Server failed to start after 30 seconds!${NC}"
    echo -e "${YELLOW}Check the server output above for errors.${NC}"
    kill $SERVER_PID 2>/dev/null
    exit 1
fi

echo -e "${GREEN}✓ Server is running!${NC}"
echo ""

# Start shadow-cljs
echo -e "${GREEN}Starting ClojureScript client...${NC}"
echo -e "${BLUE}Building and watching with shadow-cljs...${NC}"

# Start shadow-cljs
npx shadow-cljs watch todo &
SHADOW_PID=$!
echo "Shadow-cljs PID: $SHADOW_PID"

echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}✨ TODO App is starting up!${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""
echo -e "${BLUE}Server API:${NC} http://localhost:4000"
echo -e "${BLUE}Client UI:${NC}  http://localhost:8080/todo.html"
echo -e "${BLUE}Post-it UI:${NC} http://localhost:8080/todo-postit.html"
echo ""
echo -e "${YELLOW}Waiting for shadow-cljs to compile...${NC}"
echo -e "${YELLOW}This may take 30-60 seconds on first run.${NC}"
echo ""
echo -e "Press ${RED}Ctrl+C${NC} to stop both server and client"
echo ""

# Function to cleanup on exit
cleanup() {
    echo ""
    echo -e "${YELLOW}Shutting down...${NC}"
    kill $SERVER_PID 2>/dev/null
    kill $SHADOW_PID 2>/dev/null
    lsof -ti:4000 | xargs kill -9 2>/dev/null
    lsof -ti:8080 | xargs kill -9 2>/dev/null
    echo -e "${GREEN}✓ Cleanup complete${NC}"
    exit 0
}

# Set up trap to cleanup on Ctrl+C
trap cleanup INT

# Wait for shadow-cljs to be ready (check for the JS file)
echo -e "${BLUE}Waiting for compilation...${NC}"
while [ ! -f "resources/public/js/todo.js" ]; do
    sleep 2
    echo -n "."
done

echo ""
echo -e "${GREEN}✓ Client compiled!${NC}"
echo ""
echo -e "${GREEN}🎉 Open one of these in your browser:${NC}"
echo -e "${GREEN}   Classic: http://localhost:8080/todo.html${NC}"
echo -e "${GREEN}   Post-it: http://localhost:8080/todo-postit.html${NC}"
echo ""

# Keep script running
wait $SERVER_PID