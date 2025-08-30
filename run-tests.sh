#!/bin/bash

# Reactor Test Suite Runner
# Runs all core reactor server tests with detailed output

set -e  # Exit on error

echo "================================================"
echo "         Reactor Test Suite Runner"
echo "================================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Track test results
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0
FAILED_NAMESPACES=()

# Function to run a test namespace
run_test() {
    local namespace=$1
    local description=$2
    
    echo -e "${BLUE}Running: ${description}${NC}"
    echo "Namespace: ${namespace}"
    echo "----------------------------------------"
    
    if lein test ${namespace} 2>&1 | tee test-output.tmp; then
        echo -e "${GREEN}✓ PASSED${NC}"
        ((PASSED_TESTS++))
    else
        echo -e "${RED}✗ FAILED${NC}"
        ((FAILED_TESTS++))
        FAILED_NAMESPACES+=("${namespace}")
    fi
    
    ((TOTAL_TESTS++))
    echo ""
}

# Clean previous test artifacts
echo -e "${YELLOW}Cleaning test artifacts...${NC}"
rm -f test-output.tmp
rm -f test-results.txt
echo ""

# Core Pipeline Tests
echo -e "${YELLOW}=== Core SQL Pipeline Tests ===${NC}"
echo ""

run_test "reactor.sql-pipeline-test" \
         "SQL Pipeline - Core functionality"

run_test "reactor.subscriptions.differ-test" \
         "Subscription Differ - Result set diffing"

run_test "reactor.subscriptions.store-test" \
         "Subscription Store - State management"

run_test "reactor.sse.broadcaster-test" \
         "SSE Broadcaster - Channel management"

run_test "reactor.reactive.coordinator-test" \
         "Reactive Coordinator - Change handling"

# Integration Tests
echo -e "${YELLOW}=== Integration Tests ===${NC}"
echo ""

run_test "reactor.sql-pipeline-integration-test" \
         "SQL Pipeline Integration - End-to-end flows"

run_test "reactor.sql-pipeline-adapter-test" \
         "SQL Pipeline Adapter - Legacy compatibility"

# Legacy/Existing Tests (if they exist)
echo -e "${YELLOW}=== Legacy System Tests ===${NC}"
echo ""

# Check if legacy tests exist before running
if [ -f "test/reactor/core_test.clj" ]; then
    run_test "reactor.core-test" \
             "Core Reactor - Basic functionality"
fi

if [ -f "test/reactor/xtdb_store_test.clj" ]; then
    run_test "reactor.xtdb-store-test" \
             "XTDB Store - Database operations"
fi

if [ -f "test/reactor/kafka_reactive_test.clj" ]; then
    run_test "reactor.kafka-reactive-test" \
             "Kafka Reactive - Event streaming"
fi

# Generate summary report
echo ""
echo "================================================"
echo "              TEST SUMMARY REPORT"
echo "================================================"
echo ""

echo -e "Total Test Namespaces: ${TOTAL_TESTS}"
echo -e "${GREEN}Passed: ${PASSED_TESTS}${NC}"
echo -e "${RED}Failed: ${FAILED_TESTS}${NC}"
echo ""

if [ ${FAILED_TESTS} -gt 0 ]; then
    echo -e "${RED}Failed Namespaces:${NC}"
    for ns in "${FAILED_NAMESPACES[@]}"; do
        echo -e "  - ${ns}"
    done
    echo ""
fi

# Performance check (optional)
echo -e "${YELLOW}Running quick performance check...${NC}"
echo ""

# Create a simple performance test inline
cat > perf-test.clj << 'EOF'
(ns perf-test
  (:require [reactor.sql-pipeline :as pipeline]
            [reactor.subscriptions.store :as store]))

(defn mock-node []
  (reify reactor.xtdb-store/XTDBNode
    (execute [_ _ _] [{:id 1 :data "test"}])))

(defn measure-pipeline-throughput []
  (let [node (mock-node)
        start (System/currentTimeMillis)]
    (dotimes [n 100]
      (pipeline/execute-pipeline
        {:sql "SELECT * FROM test"
         :params []
         :session-id (str "session-" n)
         :node node}))
    (let [elapsed (- (System/currentTimeMillis) start)]
      (println (format "Executed 100 pipeline operations in %dms" elapsed))
      (println (format "Average: %.2fms per operation" (/ elapsed 100.0))))))

(measure-pipeline-throughput)
(store/clear-all!)
(System/exit 0)
EOF

if lein exec perf-test.clj 2>/dev/null; then
    echo -e "${GREEN}Performance check completed${NC}"
else
    echo -e "${YELLOW}Performance check skipped${NC}"
fi

rm -f perf-test.clj
echo ""

# Clean up
rm -f test-output.tmp

# Exit code based on test results
if [ ${FAILED_TESTS} -gt 0 ]; then
    echo -e "${RED}================================================${NC}"
    echo -e "${RED}          TESTS FAILED - FIX REQUIRED${NC}"
    echo -e "${RED}================================================${NC}"
    exit 1
else
    echo -e "${GREEN}================================================${NC}"
    echo -e "${GREEN}         ALL TESTS PASSED SUCCESSFULLY!${NC}"
    echo -e "${GREEN}================================================${NC}"
    exit 0
fi