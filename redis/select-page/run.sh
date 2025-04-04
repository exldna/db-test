#!/bin/bash
set -eo pipefail

BATCH_SIZE=100

count_total_batches() {
    local total=0
    while read -r key; do
        local elements=$(redis-cli ZCARD "$key" 2>/dev/null || echo 0)
        local batches=$(( (elements + BATCH_SIZE - 1) / BATCH_SIZE ))
        total=$((total + batches))
    done < <(redis-cli --scan | while read -r key; do 
        [ "$(redis-cli TYPE "$key")" = "zset" ] && echo "$key"
    done)
    echo $total
}

show_progress() {
    local current=$1
    local total=$2
    local width=50
    local percent=$((100*current/total))
    local filled=$((width*current/total))
    printf "\r[" >&2
    printf "%${filled}s" | tr ' ' '#' >&2
    printf "%$((width-filled))s" | tr ' ' '-' >&2
    printf "] %3d%% (%d/%d)" $percent $current $total >&2
}

measure() {
    local start=$(date +%s.%N)
    redis-cli ZRANGE "$1" $2 $(( $2 + BATCH_SIZE - 1 )) >/dev/null
    date +%s.%N | awk -v start=$start '{printf "%.3f", ($0-start)*1000}'
}

TOTAL_BATCHES=$(count_total_batches)
CURRENT_BATCH=0

echo "Total batches to process: $TOTAL_BATCHES" >&2

redis-cli --scan | while read -r key; do
    [ "$(redis-cli TYPE "$key")" = "zset" ] || continue
    
    total=$(redis-cli ZCARD "$key")
    for ((offset=0; offset<total; offset+=BATCH_SIZE)); do
        time_ms=$(measure "$key" $offset)
        echo "$key $total $offset $time_ms"

        CURRENT_BATCH=$((CURRENT_BATCH + 1))
        show_progress $CURRENT_BATCH $TOTAL_BATCHES >&2
    done
done

printf "\nDone!\n" >&2
