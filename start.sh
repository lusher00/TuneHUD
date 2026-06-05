#!/bin/bash
# TuneHUD startup script
# Usage: ./start.sh [config]
# Default config: configs/multi_demo.yaml

CONFIG=${1:-configs/multi_demo.yaml}
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== TuneHUD ==="
echo "Config: $CONFIG"
echo "Stopping any existing processes..."

pkill -f "demo_node.py" 2>/dev/null
pkill -f "main.py" 2>/dev/null
pkill -f "rpicam-vid" 2>/dev/null
sleep 2

cd "$DIR"

echo "Starting demo nodes..."
python demo_node.py &
sleep 1
python demo_node.py --port 8767 &
sleep 1

echo "Starting gateway..."
python main.py --config "$CONFIG"
