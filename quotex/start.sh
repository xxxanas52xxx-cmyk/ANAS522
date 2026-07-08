#!/bin/bash
cd "$(dirname "$0")"
echo "=== QUOTEX - Installing dependencies ==="
pip install httpx websockets certifi beautifulsoup4 fake-useragent rich pyfiglet flask flask-cors --quiet --disable-pip-version-check 2>/dev/null
echo "=== Starting server → http://localhost:5000 ==="
python server.py
