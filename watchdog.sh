#\!/bin/bash
# TenderNed Scraper Watchdog (Supabase version)

SCRAPER_DIR="/root/tenderned_scraper"
LOG="$SCRAPER_DIR/watchdog.log"
SCRAPER_LOG="$SCRAPER_DIR/scraper.log"

log() {
    echo "$(date "+%Y-%m-%d %H:%M:%S") $1" >> "$LOG"
}

get_last_id() {
    grep -oP "ID \K[0-9]+" "$SCRAPER_LOG" 2>/dev/null | tail -1
}

is_running() {
    pgrep -f "python.*id_scraper_db.py" > /dev/null
}

start_scraper() {
    cd "$SCRAPER_DIR"
    source venv/bin/activate
    
    LAST_ID=$(get_last_id)
    if [ -n "$LAST_ID" ] && [ "$LAST_ID" -gt 100000 ]; then
        START_ID=$LAST_ID
    else
        START_ID=392000
    fi
    
    log "Starting scraper from ID $START_ID (Supabase mode)"
    nohup python id_scraper_db.py --start $START_ID --end 100000 >> "$SCRAPER_LOG" 2>&1 &
    log "Scraper started with PID $\!"
}

log "Watchdog started (Supabase mode)"

while true; do
    if \! is_running; then
        log "Scraper not running - restarting..."
        start_scraper
        sleep 30
    fi
    sleep 300
done
