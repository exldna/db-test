#!/bin/bash

TRIES=3
BATCH_SIZE=10

ZSET_KEYS=$(
    redis-cli --raw KEYS "*" |
        while read -r key; do
            if [ "$(redis-cli TYPE "$key")" = "zset" ]; then
                echo "$key"
            fi
        done
)



for i in $(seq 1 $TRIES); do
    # clear system caches
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches

    for ZSET_KEY in $ZSET_KEYS; do
        TOTAL_ELEMENTS=$(redis-cli ZCARD "$ZSET_KEY")
        OFFSET=0
        while [ "$OFFSET" -lt "$TOTAL_ELEMENTS" ]; do
            measure_batch_time "$ZSET_KEY" "$OFFSET" "$TOTAL_ELEMENTS"
            OFFSET=$((OFFSET + BATCH_SIZE))
        done
    done
done
