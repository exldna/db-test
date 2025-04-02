#!/bin/bash

TRIES=3

SYSTEM="$1"
BENCHMARK="$2"
DATA_FILE="$3"

# clear system caches
sync
echo 3 | sudo tee /proc/sys/vm/drop_caches

for i in $(seq 1 $TRIES)
do
    "./${SYSTEM}/${BENCHMARK}/docker.sh ${DATA_FILE}"
done
