#!/bin/bash
# Daily Crypto Data Collector — launchd wrapper
# Runs market data collection + scoring, outputs JSON for Agent consumption.
# Scheduled daily at 08:07 local (00:07 UTC) via launchd.

PROJECT_ROOT="/Users/allen.lee/source/barter-rs"
SCRIPT_DIR="$PROJECT_ROOT/.claude/skills/crypto-data"
DATA_DIR="$PROJECT_ROOT/data/daily"
LOG_DIR="$PROJECT_ROOT/bin/logs"
TODAY=$(date -u +%Y-%m-%d)

mkdir -p "$DATA_DIR/features/$TODAY" "$DATA_DIR/raw/$TODAY" "$LOG_DIR"

LOG_FILE="$LOG_DIR/daily_collector_$TODAY.log"

echo "========================================" | tee -a "$LOG_FILE"
echo "  Crypto Daily Data Collector" | tee -a "$LOG_FILE"
echo "  $(date -u '+%Y-%m-%d %H:%M:%S UTC')" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

# Phase 1: Market Data Collection (full scan)
echo "" | tee -a "$LOG_FILE"
echo "[1/3] Collecting market data..." | tee -a "$LOG_FILE"
if /opt/homebrew/bin/python3 "$SCRIPT_DIR/daily_market_data.py" \
    --no-scan \
    -o "$DATA_DIR/features/$TODAY/market_data.json" \
    2>&1 | tee -a "$LOG_FILE"; then
    # Copy to /tmp for Agent accessibility
    cp "$DATA_DIR/features/$TODAY/market_data.json" /tmp/daily_market_data.json
    echo "  ✓ Market data copied to /tmp/daily_market_data.json" | tee -a "$LOG_FILE"
else
    echo "  ✗ Market data collection FAILED" | tee -a "$LOG_FILE"
fi

# Phase 2: Market Score
echo "" | tee -a "$LOG_FILE"
echo "[2/3] Computing market score..." | tee -a "$LOG_FILE"
if /opt/homebrew/bin/python3 "$SCRIPT_DIR/daily_market_score.py" \
    "$DATA_DIR/features/$TODAY/market_data.json" \
    -o "$DATA_DIR/features/$TODAY/market_score.json" \
    2>&1 | tee -a "$LOG_FILE"; then
    cp "$DATA_DIR/features/$TODAY/market_score.json" /tmp/daily_market_score.json
    echo "  ✓ Market score copied to /tmp/daily_market_score.json" | tee -a "$LOG_FILE"
else
    echo "  ✗ Market score computation FAILED" | tee -a "$LOG_FILE"
fi

# Phase 3: Announcement Tracker (update Event Database)
echo "" | tee -a "$LOG_FILE"
echo "[3/3] Updating announcement event database..." | tee -a "$LOG_FILE"
/opt/homebrew/bin/python3 "$SCRIPT_DIR/announcement_tracker.py" fetch --hours 24 \
    2>&1 | tee -a "$LOG_FILE" || echo "  ✗ Announcement tracker FAILED (non-fatal)" | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "✓ Done. $(date -u '+%H:%M:%S UTC')" | tee -a "$LOG_FILE"
echo "  Market data:  $DATA_DIR/features/$TODAY/market_data.json" | tee -a "$LOG_FILE"
echo "  Market score: $DATA_DIR/features/$TODAY/market_score.json" | tee -a "$LOG_FILE"
echo "  Announcement DB: $DATA_DIR/announcements_db.json" | tee -a "$LOG_FILE"
