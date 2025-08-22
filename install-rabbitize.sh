#!/bin/bash
# Install script for rabbitize and its dependencies

echo "🐰 Installing Rabbitize for Reactor visual testing..."

# Check if npm is installed
if ! command -v npm &> /dev/null; then
    echo "❌ npm is not installed. Please install Node.js and npm first."
    exit 1
fi

# Install rabbitize globally
echo "📦 Installing rabbitize..."
npm install -g rabbitize

if [ $? -ne 0 ]; then
    echo "❌ Failed to install rabbitize"
    exit 1
fi

# Install Playwright dependencies
echo "🎭 Installing Playwright dependencies..."
echo "This may require sudo access for system dependencies..."
sudo npx playwright install-deps

if [ $? -ne 0 ]; then
    echo "⚠️  Warning: Playwright dependencies installation had issues"
    echo "You may need to run: sudo npx playwright install-deps"
else
    echo "✅ Playwright dependencies installed"
fi

# Install Playwright browsers
echo "🌐 Installing Playwright browsers..."
npx playwright install

if [ $? -ne 0 ]; then
    echo "❌ Failed to install Playwright browsers"
    exit 1
fi

echo "✅ Rabbitize installation complete!"
echo ""
echo "You can now use visual testing features in Reactor:"
echo "  - POST /api/rabbitize/capture to capture a snapshot"
echo "  - POST /api/rabbitize/start to start rabbitize manually"
echo "  - POST /api/rabbitize/session to create a browser session"
echo ""
echo "Rabbitize will start automatically when needed."