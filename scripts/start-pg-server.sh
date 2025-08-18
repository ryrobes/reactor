#!/bin/bash

echo "Starting PostgreSQL Wire Protocol Server for XTDB..."
echo "========================================"

# Default port
PORT=${1:-5433}

# Start the server
lein run -m reactor.pgwire $PORT