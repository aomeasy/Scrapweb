#!/usr/bin/env bash
set -o errexit

echo "Starting build process..."

# Upgrade pip
pip install --upgrade pip

# Force reinstall numpy and pandas with no cache
echo "Installing numpy..."
pip install --force-reinstall --no-cache-dir numpy==1.24.3

echo "Installing pandas..."
pip install --force-reinstall --no-cache-dir pandas==2.0.3

# Install other requirements
echo "Installing remaining requirements..."
pip install --no-cache-dir -r requirements.txt

# Install playwright browsers with dependencies
echo "Installing playwright browsers with system dependencies..."
python -m playwright install --with-deps chromium

# Set environment variables for Playwright
export PLAYWRIGHT_BROWSERS_PATH=/opt/render/.cache/ms-playwright

echo "Build completed successfully!"
