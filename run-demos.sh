#!/bin/bash

# Reactor Demo Launcher
# Choose which demo to run

echo "🚀 Reactor Demo Launcher"
echo "========================"
echo ""
echo "Choose a demo to run:"
echo "1) Magic Counter - Minimal counter with time travel"
echo "2) TODO App - Full TodoMVC implementation"
echo "3) Exit"
echo ""
read -p "Enter choice [1-3]: " choice

case $choice in
    1)
        echo "Starting Magic Counter..."
        ./run-magic-counter.sh
        ;;
    2)
        echo "Starting TODO App..."
        ./run-todo.sh
        ;;
    3)
        echo "Goodbye!"
        exit 0
        ;;
    *)
        echo "Invalid choice. Please run again and select 1, 2, or 3."
        exit 1
        ;;
esac