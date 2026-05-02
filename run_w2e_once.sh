#!/bin/bash
set -e
cd /root/binance-square-agent
set -a && source .env && set +a
exec python3 main.py w2e-once
