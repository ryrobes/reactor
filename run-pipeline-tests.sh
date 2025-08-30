#!/bin/bash

# Reactor SQL Pipeline Test Runner
# Focused test runner for the new SQL pipeline components

set -e

echo "================================================"
echo "     SQL Pipeline Component Test Runner"
echo "================================================"
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Run unit tests first
echo -e "${YELLOW}Running SQL Pipeline Unit Tests...${NC}"
echo ""

echo "1. Testing SQL Pipeline Core..."
lein test reactor.sql-pipeline-test

echo ""
echo "2. Testing Subscription Differ..."
lein test reactor.subscriptions.differ-test

echo ""
echo "3. Testing Subscription Store..."
lein test reactor.subscriptions.store-test

echo ""
echo "4. Testing SSE Broadcaster..."
lein test reactor.sse.broadcaster-test

echo ""
echo "5. Testing Reactive Coordinator..."
lein test reactor.reactive.coordinator-test

echo ""
echo "6. Testing SQL Pipeline Adapter..."
lein test reactor.sql-pipeline-adapter-test

# Run integration tests
echo ""
echo -e "${YELLOW}Running Integration Tests...${NC}"
echo ""

echo "7. Testing End-to-End Pipeline Flow..."
lein test reactor.sql-pipeline-integration-test

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}    All SQL Pipeline Tests Completed!${NC}"
echo -e "${GREEN}================================================${NC}"