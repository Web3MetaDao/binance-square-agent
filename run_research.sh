#!/bin/bash
# run_research.sh — cron wrapper for the daily research pipeline.
#
# Usage:
#   ./run_research.sh full            # Run everything (default)
#   ./run_research.sh harvest-only    # Only harvest + parse + store
#   ./run_research.sh backtest-only   # Only backtest + deploy gate
#   ./run_research.sh report-only     # Only dashboard report
#
# Logs are written to logs/research_cron.log (rotated by cron).
# Telegram report is sent on completion (if configured in .env).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Ensure logs directory exists
mkdir -p logs

MODE="${1:-full}"

# Validate mode
case "$MODE" in
    full|harvest-only|backtest-only|report-only)
        ;;
    *)
        echo "Usage: $0 {full|harvest-only|backtest-only|report-only}" >&2
        exit 1
        ;;
esac

TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"
echo "[$TIMESTAMP] Starting research pipeline (mode=$MODE)..."

# Run the pipeline
PYTHONPATH="$SCRIPT_DIR" python3 -m backtest.cron_daily --mode "$MODE" 2>> logs/research_cron.log
EXIT_CODE=$?

TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"
if [ "$EXIT_CODE" -eq 0 ]; then
    echo "[$TIMESTAMP] Research pipeline (mode=$MODE) completed successfully"
else
    echo "[$TIMESTAMP] Research pipeline (mode=$MODE) FAILED with exit code $EXIT_CODE" >&2
fi

exit "$EXIT_CODE"
