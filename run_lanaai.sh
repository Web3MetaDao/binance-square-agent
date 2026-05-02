#!/bin/bash
# Kill any leftover browser/chrome processes first
pkill -9 -f "agent-browser" 2>/dev/null || true
sleep 2
# Run the monitor
cd /root/binance-square-agent
python3 lanaai_cron_monitor.py 2>&1
echo "EXIT_CODE=$?"
