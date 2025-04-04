#!/bin/bash
set -eo pipefail

BATCH_SIZE=10

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

get_zset_keys() {
    redis-cli --scan | while read -r key; do
        if [ "$(redis-cli TYPE "$key")" = "zset" ]; then
            echo "$key"
        fi
    done
}

get_zset_keys | while read -r key; do
    total=$(redis-cli ZCARD "$key")
    batches=$(( (total + BATCH_SIZE - 1) / BATCH_SIZE ))
    completed=0

    for ((offset=0; offset<total; offset+=BATCH_SIZE)); do
        time_ms=$(measure "$key" $offset)
        echo "$key $total $offset $time_ms"

        completed=$((completed+1))
        show_progress $completed $batches >&2
    done
done

# Clean progress line
printf "\r%$(tput cols)s\r" >&2
