#!/bin/bash

# Reactor TODO App Demo Runner (Direct ClojureScript compilation)
# Uses cljs.build.api directly without lein-cljsbuild

echo "🚀 Starting Reactor TODO App (Direct ClojureScript Build)"
echo "========================================================="
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
lsof -ti:8083 | xargs kill -9 2>/dev/null
sleep 1

# Clean old builds
echo -e "${YELLOW}Cleaning old builds...${NC}"
rm -rf resources/public/js/todo.js resources/public/js/todo-out/ 2>/dev/null

# Start the server
echo -e "${GREEN}Starting TODO Server on port 4000...${NC}"
lein run -m examples.todo-app.server 2>&1 | sed "s/^/[SERVER] /" &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Wait for server to start
echo -e "${BLUE}Waiting for server to initialize...${NC}"

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

# Build ClojureScript using direct API
echo -e "${GREEN}Building ClojureScript using cljs.build.api...${NC}"
java -cp "$(lein classpath)" clojure.main todo-build.clj 2>&1 | sed "s/^/[BUILD] /"

# Check if build was successful
if [ ! -f "resources/public/js/todo.js" ]; then
    echo -e "${RED}Build failed! Check the output above for errors.${NC}"
    kill $SERVER_PID 2>/dev/null
    exit 1
fi

echo -e "${GREEN}✓ Build complete!${NC}"

# Start simple HTTP server for static files
echo -e "${GREEN}Starting HTTP server on port 8083...${NC}"
cd resources/public && python3 -m http.server 8083 2>&1 | sed "s/^/[HTTP] /" &
HTTP_PID=$!
cd ../..
echo "HTTP server PID: $HTTP_PID"

# Watch for changes and rebuild
echo -e "${GREEN}Starting file watcher...${NC}"
echo -e "${YELLOW}Watching for changes in src/examples/todo_app/ and src/reactor/*.cljs${NC}"

# Simple watch loop
(
while true; do
    # Use find to check for modified files
    CHANGED=$(find src/examples/todo_app -name "*.cljs" -newer resources/public/js/todo.js 2>/dev/null)
    CHANGED2=$(find src/reactor -name "*.cljs" -newer resources/public/js/todo.js 2>/dev/null)
    
    if [ ! -z "$CHANGED" ] || [ ! -z "$CHANGED2" ]; then
        echo -e "${BLUE}Changes detected, rebuilding...${NC}"
        java -cp "$(lein classpath)" clojure.main todo-build.clj 2>&1 | sed "s/^/[BUILD] /"
        echo -e "${GREEN}✓ Rebuild complete at $(date +%H:%M:%S)${NC}"
    fi
    
    sleep 2
done
) &
WATCH_PID=$!
echo "File watcher PID: $WATCH_PID"

echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}✨ TODO App is running!${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""
echo -e "${BLUE}Server API:${NC} http://localhost:4000"
echo -e "${BLUE}Client UI:${NC}  http://localhost:8083/todo.html"
echo -e "${BLUE}Post-it UI:${NC} http://localhost:8083/todo-postit.html"
echo ""
echo -e "${YELLOW}File changes will trigger automatic rebuilds.${NC}"
echo -e "${YELLOW}Refresh your browser to see changes.${NC}"
echo ""
echo -e "Press ${RED}Ctrl+C${NC} to stop all processes"
echo ""

# Function to cleanup on exit
cleanup() {
    echo ""
    echo -e "${YELLOW}Shutting down...${NC}"
    kill $SERVER_PID 2>/dev/null
    kill $HTTP_PID 2>/dev/null
    kill $WATCH_PID 2>/dev/null
    lsof -ti:4000 | xargs kill -9 2>/dev/null
    lsof -ti:8083 | xargs kill -9 2>/dev/null
    echo -e "${GREEN}✓ Cleanup complete${NC}"
    exit 0
}

# Set up trap to cleanup on Ctrl+C
trap cleanup INT

# Keep script running
wait $SERVER_PID