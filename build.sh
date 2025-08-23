#!/usr/bin/env bash
set -o errexit

# Upgrade pip
pip install --upgrade pip

# Force reinstall numpy and pandas with no cache
pip install --force-reinstall --no-cache-dir numpy==1.24.3
pip install --force-reinstall --no-cache-dir pandas==2.0.3

# Install other requirements
pip install --no-cache-dir -r requirements.txt

# Install playwright browsers
python -m playwright install chromium
